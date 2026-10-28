from __future__ import annotations

from pathlib import Path

import pytest
from chia_rs import CoinState, FullBlock, SpendBundle
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia._tests.blockchain.blockchain_test_utils import (
    _validate_and_add_block,
    _validate_and_add_block_multi_result,
)
from chia._tests.blockchain.test_rocks_mempool_flow import _admit, _extend, _reward, _rocks_mempool
from chia._tests.blockchain.test_rocks_reorg import _assert_coin_table_matches_peak
from chia._tests.conftest import ConsensusMode
from chia._tests.util.blockchain import create_blockchain
from chia.consensus.block_body_validation import ForkInfo
from chia.consensus.blockchain import AddBlockResult, Blockchain, ParentHistoryUnavailable
from chia.consensus.generator_tools import get_block_header
from chia.full_node.db.block_filter import filter_matches
from chia.full_node.db.coin_store import RocksCoinStore
from chia.full_node.db.keys import CF_COINS
from chia.full_node.mempool_manager import MempoolManager
from chia.simulator.block_tools import BlockTools
from chia.simulator.wallet_tools import WalletTool
from chia.types.blockchain_format.coin import Coin
from chia.types.condition_opcodes import ConditionOpcode
from chia.types.condition_with_args import ConditionWithArgs
from chia.types.mempool_inclusion_status import MempoolInclusionStatus
from chia.util.casts import int_to_bytes
from chia.util.errors import Err
from chia.util.hash import std_hash


def _store(blockchain: Blockchain) -> RocksCoinStore:
    store = blockchain.coin_store
    assert isinstance(store, RocksCoinStore)
    return store


def _mempool(blockchain: Blockchain) -> MempoolManager:
    return _rocks_mempool(blockchain)


async def _open_mempool(blockchain: Blockchain) -> MempoolManager:
    mempool = _mempool(blockchain)
    peak = blockchain.get_tx_peak()
    assert peak is not None
    await mempool.new_peak(peak, None)
    return mempool


def _farmer_rewards(blocks: list[FullBlock], puzzle: bytes32) -> list[Coin]:
    found: list[Coin] = []
    for block in blocks:
        if not block.is_transaction_block():
            continue
        for coin in block.get_included_reward_coins():
            if coin.puzzle_hash == puzzle:
                found.append(coin)
    return found


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_wallet_reads_recent_block_coins_from_the_block(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A wallet asking for a recent block's coins gets them from the block, not an empty height list."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        receiver = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(6, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward, _timestamp = _reward(blocks, farmer)
        bundle = wallet.generate_signed_transaction(uint64(1_000), receiver, reward)
        output = bundle.additions()[0]
        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        height = int(spent_blocks[-1].height)
        store = _store(blockchain)
        assert await store.undo_floor() is None
        assert await blockchain.coin_list_is_in_the_block(height) is True
        assert await store.get_coins_added_at_height(uint32(height)) == []
        assert await store.get_coins_removed_at_height(uint32(height)) == []
        header_hash = blockchain.height_to_hash(uint32(height))
        assert header_hash is not None
        block = await blockchain.get_full_block(header_hash)
        assert block is not None
        added, removed = await blockchain.coins_in_block(block)
        assert output in added
        assert reward in added or any(coin.puzzle_hash == farmer for coin in added)
        assert removed.get(reward.name()) == reward
        puzzles = {coin.puzzle_hash for coin in added}
        assert receiver in puzzles

        fork = bt.get_consecutive_blocks(
            4,
            blocks,
            seed=b"replace-the-wallet-block",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_at = blocks[-1]
        fork_info = ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash)
        for block in fork[len(blocks) :]:
            await _validate_and_add_block_multi_result(
                blockchain,
                block,
                expected_result=[AddBlockResult.ADDED_AS_ORPHAN, AddBlockResult.NEW_PEAK],
                fork_info=fork_info,
            )
        peak = blockchain.get_peak()
        assert peak is not None and peak.header_hash == fork[-1].header_hash
        replaced_hash = blockchain.height_to_hash(uint32(height))
        assert replaced_hash is not None and replaced_hash != header_hash
        replaced = await blockchain.get_full_block(replaced_hash)
        assert replaced is not None
        new_added, new_removed = await blockchain.coins_in_block(replaced)
        assert output not in new_added
        assert reward.name() not in new_removed
        await _assert_coin_table_matches_peak(blockchain)


def _state_from_block(coin: Coin, created: int, removed: dict[bytes32, Coin], spend_height: int) -> CoinState:
    """The coin state a normal node stores, taken from the block both nodes share."""
    spent = uint32(spend_height) if coin.name() in removed else None
    return CoinState(coin, spent, uint32(created))


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_silo_and_a_normal_node_describe_the_same_coin(
    bt: BlockTools,
    consensus_mode: ConsensusMode,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While the spent note exists, both nodes name the same height. After it is gone, the block still does."""
    del consensus_mode
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 3)
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(5, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward, _timestamp = _reward(blocks, farmer)
        created = await _store(blockchain).creation_height(reward.name())
        assert created is not None
        bundle = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), reward)
        child = bundle.additions()[0]
        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        spend_block = spent_blocks[-1]
        added, removed = await blockchain.coins_in_block(spend_block)
        from_block = _state_from_block(reward, created, removed, int(spend_block.height))
        record = await _store(blockchain).get_coin_record(reward.name())
        assert record is not None
        assert record.coin_state == from_block
        children = await blockchain.child_records(reward.name(), include_spent_coins=True)
        assert child in [item.coin for item in children]
        assert child in added

        chain = spent_blocks
        floor = await _store(blockchain).undo_floor()
        while floor is None or floor < int(spend_block.height):
            chain = bt.get_consecutive_blocks(
                1, chain, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True
            )
            await _validate_and_add_block(blockchain, chain[-1])
            floor = await _store(blockchain).undo_floor()
            assert int(chain[-1].height) < 40
        assert await _store(blockchain).get_coin_record(reward.name()) is None
        with pytest.raises(ParentHistoryUnavailable, match="no children"):
            await blockchain.child_records(reward.name(), include_spent_coins=True)
        still_added, still_removed = await blockchain.coins_in_block(spend_block)
        assert _state_from_block(reward, created, still_removed, int(spend_block.height)) == from_block
        assert child in still_added


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_header_filter_includes_the_spend(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """The header a wallet validates includes the spend. An empty coin-table list does not."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(5, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward, _timestamp = _reward(blocks, farmer)
        bundle = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), reward)
        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        block = spent_blocks[-1]
        foliage = block.foliage_transaction_block
        assert foliage is not None
        assert await blockchain.coin_list_is_in_the_block(int(block.height)) is True
        tx_additions, removal_names = await blockchain.tx_additions_and_removals(block)
        assert bundle.additions()[0] in tx_additions
        assert reward.name() in removal_names
        header = get_block_header(block, (removal_names, tx_additions))
        assert foliage.filter_hash == std_hash(header.transactions_filter)
        stored = await _store(blockchain).get_block_filter(int(block.height))
        assert stored is not None
        assert std_hash(stored) == foliage.filter_hash
        empty = get_block_header(block, ([], []))
        assert std_hash(empty.transactions_filter) != foliage.filter_hash

        fork = bt.get_consecutive_blocks(
            3,
            blocks,
            seed=b"header-follows-winner",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_at = blocks[-1]
        fork_info = ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash)
        for candidate in fork[len(blocks) :]:
            await _validate_and_add_block_multi_result(
                blockchain,
                candidate,
                expected_result=[AddBlockResult.ADDED_AS_ORPHAN, AddBlockResult.NEW_PEAK],
                fork_info=fork_info,
            )
        winner_hash = blockchain.height_to_hash(block.height)
        assert winner_hash is not None and winner_hash != block.header_hash
        winner = await blockchain.get_full_block(winner_hash)
        assert winner is not None and winner.foliage_transaction_block is not None
        winner_additions, winner_removals = await blockchain.tx_additions_and_removals(winner)
        assert reward.name() not in winner_removals
        winner_header = get_block_header(winner, (winner_removals, winner_additions))
        assert winner.foliage_transaction_block.filter_hash == std_hash(winner_header.transactions_filter)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_same_block_create_and_spend_then_fork(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A coin created and spent in one block is not left behind when that block loses a fork."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(6, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        mempool = await _open_mempool(blockchain)
        reward, _born = _reward(blocks, farmer)
        first = wallet.generate_signed_transaction(uint64(reward.amount), wallet.get_new_puzzlehash(), reward)
        middle = first.additions()[0]
        second = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), middle)
        output = second.additions()[0]
        bundle = SpendBundle.aggregate([first, second])
        status, error = await _admit(mempool, bundle)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)

        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _extend(blockchain, mempool, spent_blocks[len(blocks) :])
        store = _store(blockchain)
        middle_record = await store.get_coin_record(middle.name())
        output_record = await store.get_coin_record(output.name())
        assert middle_record is not None and middle_record.spent
        assert middle_record.confirmed_block_index == middle_record.spent_block_index
        assert output_record is not None and not output_record.spent
        children = await blockchain.child_records(reward.name(), include_spent_coins=True)
        assert [record.coin for record in children] == [middle]
        hidden = await blockchain.child_records(reward.name(), include_spent_coins=False)
        assert hidden == []
        outputs = await blockchain.child_records(middle.name(), include_spent_coins=False)
        assert output in [record.coin for record in outputs]
        await _assert_coin_table_matches_peak(blockchain)
        assert mempool.get_spendbundle(bundle.name()) is None

        fork = bt.get_consecutive_blocks(
            4,
            blocks,
            seed=b"drop-ephemeral",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_at = blocks[-1]
        await _extend(
            blockchain,
            mempool,
            fork[len(blocks) :],
            fork_info=ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash),
        )
        assert await store.get_coin_record(middle.name()) is None
        assert await store.get_coin_record(output.name()) is None
        assert await store.spent_mark(middle.name()) is None
        restored = await store.get_coin_record(reward.name())
        assert restored is not None and not restored.spent
        assert await blockchain.child_records(reward.name(), include_spent_coins=True) == []
        await _assert_coin_table_matches_peak(blockchain)
        status, error = await _admit(mempool, bundle)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_bundle_drops_when_one_of_its_coins_is_spent(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A bundle spending two coins leaves the mempool when a block spends only one of them."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(8, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        rewards = _farmer_rewards(blocks, farmer)
        assert len(rewards) >= 2
        first, second = rewards[0], rewards[1]
        store = _store(blockchain)
        reward_children = await blockchain.child_records(first.parent_coin_info, include_spent_coins=True)
        assert first in [record.coin for record in reward_children]
        mempool = await _open_mempool(blockchain)
        receiver = wallet.get_new_puzzlehash()
        bundle = SpendBundle.aggregate(
            [
                wallet.generate_signed_transaction(uint64(1_000), receiver, first),
                wallet.generate_signed_transaction(uint64(1_000), receiver, second),
            ]
        )
        status, error = await _admit(mempool, bundle)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)

        only_first = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), first)
        taken = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=only_first,
            guarantee_transaction_block=True,
        )
        await _extend(blockchain, mempool, taken[len(blocks) :])
        assert mempool.get_spendbundle(bundle.name()) is None
        spent_first = await store.get_coin_record(first.name())
        assert spent_first is not None and spent_first.spent
        still = await store.get_coin_record(second.name())
        assert still is not None and not still.spent
        alone = wallet.generate_signed_transaction(uint64(1_000), receiver, second)
        status, error = await _admit(mempool, alone)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)
        status, error = await _admit(mempool, bundle)
        assert status == MempoolInclusionStatus.FAILED
        assert error == Err.DOUBLE_SPEND
        await _assert_coin_table_matches_peak(blockchain)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_reward_lookup_stops_at_the_peak_and_refuses_a_missing_record(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """The reward scan does not read past the tip, and a missing block or row is not an empty result."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(4, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward = _farmer_rewards(blocks, farmer)[0]
        parent_id = reward.parent_coin_info
        peak = blockchain.get_peak_height()
        assert peak is not None
        asked: list[int] = []
        original_height = blockchain.height_to_hash

        def record_height(height: uint32) -> bytes32 | None:
            asked.append(int(height))
            return original_height(height)

        blockchain.height_to_hash = record_height  # type: ignore[method-assign]
        found = await blockchain.child_records(parent_id, include_spent_coins=True)
        assert reward in [record.coin for record in found]
        assert asked and max(asked) <= int(peak)

        store = _store(blockchain)
        async with store.db.writer_maybe_transaction() as session:
            session.delete(CF_COINS, bytes(reward.name()))
        with pytest.raises(ParentHistoryUnavailable):
            await blockchain.child_records(parent_id, include_spent_coins=True)

        async def missing_block(header_hash: bytes32) -> None:
            del header_hash
            return None

        blockchain.block_store.get_full_block = missing_block  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="missing block"):
            await blockchain.child_records(parent_id, include_spent_coins=True)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_height_lock_uses_the_creation_height(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A relative height lock is measured from the coin's real creation height."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(
            6,
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
            genesis_timestamp=uint64(20_000),
            time_per_block=10,
        )
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        mempool = await _open_mempool(blockchain)
        coin, born_at = _reward(blocks, farmer)
        store = _store(blockchain)
        record = await store.get_coin_record(coin.name())
        peak = blockchain.get_tx_peak()
        assert record is not None and peak is not None and peak.timestamp is not None
        assert int(record.timestamp) == born_at
        relative = int(peak.height) - int(record.confirmed_block_index) + 1
        locked = wallet.generate_signed_transaction(
            uint64(1_000),
            wallet.get_new_puzzlehash(),
            coin,
            condition_dic={
                ConditionOpcode.ASSERT_HEIGHT_RELATIVE: [
                    ConditionWithArgs(ConditionOpcode.ASSERT_HEIGHT_RELATIVE, [int_to_bytes(relative)])
                ]
            },
        )
        status, error = await _admit(mempool, locked)
        assert status == MempoolInclusionStatus.PENDING
        assert error == Err.ASSERT_HEIGHT_RELATIVE_FAILED
        assert mempool.get_spendbundle(locked.name()) is None
        waiting = mempool.get_mempool_item(locked.name(), include_pending=True)
        assert waiting is not None

        ready = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), coin)
        conds = await mempool.pre_validate_spendbundle(ready)
        compared = await store.records_for_spends(list(conds.spends), blockchain.constants.GENESIS_CHALLENGE)
        assert len(compared) == 1
        assert compared[0].coin == record.coin
        assert compared[0].confirmed_block_index == record.confirmed_block_index
        assert compared[0].spent_block_index == 0
        assert compared[0].timestamp == record.timestamp
        status, error = await _admit(mempool, ready)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_a_waiting_spend_is_admitted_on_the_next_peak_without_the_birth_block(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A spend that is too early is retried from the short row when the chain grows."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(
            6,
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
            genesis_timestamp=uint64(20_000),
            time_per_block=10,
        )
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        mempool = await _open_mempool(blockchain)
        coin, _born_at = _reward(blocks, farmer)
        record = await _store(blockchain).get_coin_record(coin.name())
        peak = blockchain.get_tx_peak()
        assert record is not None and peak is not None
        relative = int(peak.height) - int(record.confirmed_block_index) + 1
        locked = wallet.generate_signed_transaction(
            uint64(1_000),
            wallet.get_new_puzzlehash(),
            coin,
            condition_dic={
                ConditionOpcode.ASSERT_HEIGHT_RELATIVE: [
                    ConditionWithArgs(ConditionOpcode.ASSERT_HEIGHT_RELATIVE, [int_to_bytes(relative)])
                ]
            },
        )
        status, error = await _admit(mempool, locked)
        assert (status, error) == (MempoolInclusionStatus.PENDING, Err.ASSERT_HEIGHT_RELATIVE_FAILED)

        more = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
            genesis_timestamp=uint64(20_000),
            time_per_block=10,
        )
        await _extend(blockchain, mempool, more[len(blocks) :])
        assert mempool.get_spendbundle(locked.name()) is not None


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_two_singletons_do_not_collapse_to_one(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """Two unspent singletons with one puzzle are not reported as a single current coin."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(8, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        rewards = _farmer_rewards(blocks, farmer)
        assert len(rewards) >= 2
        puzzle = wallet.get_new_puzzlehash()
        holders: list[Coin] = []
        built = blocks
        for reward in rewards[:2]:
            payment = wallet.generate_signed_transaction(uint64(reward.amount), puzzle, reward)
            holders.append(payment.additions()[0])
            built = bt.get_consecutive_blocks(
                1,
                built,
                farmer_reward_puzzle_hash=farmer,
                transaction_data=payment,
                guarantee_transaction_block=True,
            )
            await _validate_and_add_block(blockchain, built[-1])
        children: list[Coin] = []
        for holder in holders:
            forward = wallet.generate_signed_transaction(uint64(holder.amount), puzzle, holder)
            child = forward.additions()[0]
            assert child.amount == holder.amount and child.puzzle_hash == puzzle
            children.append(child)
            built = bt.get_consecutive_blocks(
                1,
                built,
                farmer_reward_puzzle_hash=farmer,
                transaction_data=forward,
                guarantee_transaction_block=True,
            )
            await _validate_and_add_block(blockchain, built[-1])
        store = _store(blockchain)
        assert await store.get_unspent_lineage_info_for_puzzle_hash(puzzle) is None

        mempool = await _open_mempool(blockchain)
        spend_one = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), children[0])
        status, error = await _admit(mempool, spend_one)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)
        built = bt.get_consecutive_blocks(
            1,
            built,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=spend_one,
            guarantee_transaction_block=True,
        )
        await _extend(blockchain, mempool, built[-1:])
        lineage = await store.get_unspent_lineage_info_for_puzzle_hash(puzzle)
        assert lineage is not None
        assert lineage.coin_id == children[1].name()
        assert lineage.parent_id == holders[1].name()
        other = await store.get_coin_record(children[1].name())
        assert other is not None and not other.spent
        await _assert_coin_table_matches_peak(blockchain)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_fork_past_the_undo_window_rebuilds_from_blocks(
    bt: BlockTools,
    consensus_mode: ConsensusMode,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fork older than the kept spent marks replays the stored blocks and the mempool follows that."""
    del consensus_mode
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 3)
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(8, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        store = _store(blockchain)
        rewards = _farmer_rewards(blocks, farmer)
        assert len(rewards) >= 2
        kept, abandoned_parent = rewards[0], rewards[1]
        kept_height = await store.creation_height(kept.name())
        abandoned_height = await store.creation_height(abandoned_parent.name())
        assert kept_height is not None and abandoned_height is not None
        fork_height = max(kept_height, abandoned_height)
        born_block = blocks[kept_height].foliage_transaction_block
        assert born_block is not None
        born_at = int(born_block.timestamp)
        memo = bytes32(b"\xab" * 32)
        abandoned = wallet.generate_signed_transaction(
            uint64(1_000), wallet.get_new_puzzlehash(), abandoned_parent, memo=memo
        )
        abandoned_child = abandoned.additions()[0]
        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=abandoned,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        hint_height = int(spent_blocks[-1].height)
        assert hint_height > fork_height
        assert filter_matches(await store.get_hint_filter(hint_height) or b"", bytes(memo))
        chain = spent_blocks
        floor = await store.undo_floor()
        while floor is None or floor < hint_height:
            chain = bt.get_consecutive_blocks(
                1,
                chain,
                farmer_reward_puzzle_hash=farmer,
                guarantee_transaction_block=True,
            )
            await _validate_and_add_block(blockchain, chain[-1])
            floor = await store.undo_floor()
            assert int(chain[-1].height) < 40
        assert await store.spent_mark(abandoned_parent.name()) is None

        mempool = await _open_mempool(blockchain)
        waiting = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), kept)
        status, error = await _admit(mempool, waiting)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)

        fork_at = blocks[fork_height]
        assert int(fork_at.height) == fork_height
        assert int(fork_at.height) < int(floor)
        replacement = bt.get_consecutive_blocks(
            12,
            blocks[: fork_height + 1],
            seed=b"rebuild-from-blocks",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_info = ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash)
        await _extend(blockchain, mempool, replacement[fork_height + 1 :], fork_info=fork_info)
        peak = blockchain.get_peak()
        assert peak is not None and peak.header_hash == replacement[-1].header_hash
        # The spent row was already deleted. Replaying the blocks is what brings this coin back.
        brought_back = await store.get_coin_record(abandoned_parent.name())
        assert brought_back is not None and not brought_back.spent and brought_back.coin == abandoned_parent
        assert int(brought_back.confirmed_block_index) == abandoned_height
        assert await store.get_coin_record(abandoned_child.name()) is None
        replacement_hints = await store.get_hint_filter(hint_height)
        assert replacement_hints is None or not filter_matches(replacement_hints, bytes(memo))
        restored = await store.get_coin_record(kept.name())
        assert restored is not None and not restored.spent and restored.coin == kept
        assert int(restored.timestamp) == born_at
        assert mempool.get_spendbundle(waiting.name()) == waiting
        await _assert_coin_table_matches_peak(blockchain)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_hint_filter_follows_the_winning_chain(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A hint stays on the block that created it and is removed with the block that lost the fork."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(5, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward, _born = _reward(blocks, farmer)
        memo = bytes32(b"\xcd" * 32)
        bundle = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), reward, memo=memo)
        hinted = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, hinted[-1])
        store = _store(blockchain)
        height = int(hinted[-1].height)
        assert filter_matches(await store.get_hint_filter(height) or b"", bytes(memo))
        block_filter = await store.get_block_filter(height)
        assert block_filter is not None
        assert filter_matches(block_filter, bytes(bundle.additions()[0].puzzle_hash))
        fork = bt.get_consecutive_blocks(
            4,
            blocks,
            seed=b"drop-hint",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_at = blocks[-1]
        fork_info = ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash)
        for block in fork[len(blocks) :]:
            await _validate_and_add_block_multi_result(
                blockchain,
                block,
                expected_result=[AddBlockResult.ADDED_AS_ORPHAN, AddBlockResult.NEW_PEAK],
                fork_info=fork_info,
            )
        peak = blockchain.get_peak()
        assert peak is not None and peak.header_hash == fork[-1].header_hash
        # The winning block occupies this height, so its filters replace the loser's.
        current_hints = await store.get_hint_filter(height)
        assert current_hints is not None
        assert not filter_matches(current_hints, bytes(memo))
        current_block_filter = await store.get_block_filter(height)
        assert current_block_filter is not None
        assert not filter_matches(current_block_filter, bytes(bundle.additions()[0].puzzle_hash))
        await _assert_coin_table_matches_peak(blockchain)


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_rollback_reads_created_coins_from_the_abandoned_blocks(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A reorg deletes coins the abandoned blocks created, including ones that are not fast-forward coins."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(
            4, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True
        )
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        fork = int(blocks[1].height)
        store = _store(blockchain)
        later_coins = [
            coin
            for block in blocks
            if block.is_transaction_block() and int(block.height) > fork
            for coin in block.get_included_reward_coins()
        ]
        assert later_coins
        for coin in later_coins:
            assert await store.creation_height(coin.name()) == next(
                int(block.height)
                for block in blocks
                if block.is_transaction_block() and coin in block.get_included_reward_coins()
            )
        await blockchain._rollback_coins(fork)
        for coin in later_coins:
            assert await store.creation_height(coin.name()) is None
        kept = [
            coin
            for block in blocks
            if block.is_transaction_block() and int(block.height) <= fork
            for coin in block.get_included_reward_coins()
        ]
        assert kept
        for coin in kept:
            assert await store.creation_height(coin.name()) is not None
