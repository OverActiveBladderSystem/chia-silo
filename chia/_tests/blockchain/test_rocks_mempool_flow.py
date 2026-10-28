from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

import pytest
from chia_rs import CoinRecord, FullBlock, SpendBundle, SpendConditions
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia._tests.blockchain.blockchain_test_utils import (
    _validate_and_add_block,
    _validate_and_add_block_multi_result,
)
from chia._tests.conftest import ConsensusMode
from chia._tests.util.blockchain import create_blockchain
from chia.consensus.block_body_validation import ForkInfo
from chia.consensus.blockchain import AddBlockResult, Blockchain
from chia.full_node.db.coin_store import RocksCoinStore
from chia.full_node.mempool_manager import MempoolManager
from chia.simulator.block_tools import BlockTools
from chia.simulator.wallet_tools import WalletTool
from chia.types.blockchain_format.coin import Coin
from chia.types.condition_opcodes import ConditionOpcode
from chia.types.condition_with_args import ConditionWithArgs
from chia.types.mempool_inclusion_status import MempoolInclusionStatus
from chia.util.casts import int_to_bytes
from chia.util.errors import Err
from chia.util.inline_executor import InlineExecutor
from chia.wallet.util.wallet_sync_utils import sort_coin_states


async def _extend(
    blockchain: Blockchain,
    mempool: MempoolManager,
    blocks: list[FullBlock],
    *,
    fork_info: ForkInfo | None = None,
) -> None:
    """Add blocks the way the node does, and tell the mempool only when the peak moves."""
    for block in blocks:
        before = blockchain.get_peak()
        if fork_info is None:
            await _validate_and_add_block(blockchain, block)
        else:
            await _validate_and_add_block_multi_result(
                blockchain,
                block,
                expected_result=[AddBlockResult.ADDED_AS_ORPHAN, AddBlockResult.NEW_PEAK],
                fork_info=fork_info,
            )
        after = blockchain.get_peak()
        assert after is not None
        if before is not None and after.header_hash == before.header_hash:
            continue
        tx_peak = blockchain.get_tx_peak()
        assert tx_peak is not None
        spent_ids: list[bytes32] = []
        if block.is_transaction_block():
            _added, removed = await blockchain.coins_in_block(block)
            spent_ids = list(removed)
        await mempool.new_peak(tx_peak, spent_ids)


async def _admit(mempool: MempoolManager, bundle: SpendBundle) -> tuple[MempoolInclusionStatus, Err | None]:
    conds = await mempool.pre_validate_spendbundle(bundle)
    info = await mempool.add_spend_bundle(bundle, conds, bundle.name(), uint32(1))
    return info.status, info.error


async def _birth_block_is_not_the_mempool_lookup(names: object) -> list[CoinRecord]:
    """The mempool on Rocks must not open the block that created the coin."""
    raise AssertionError(f"mempool opened the birth block for {names!r}")


def _rocks_mempool(blockchain: Blockchain) -> MempoolManager:
    """The same two lookups the full node gives a Rocks mempool."""
    store = blockchain.coin_store
    assert isinstance(store, RocksCoinStore)
    genesis = blockchain.constants.GENESIS_CHALLENGE

    async def spend_records(spends: Collection[SpendConditions]) -> list[CoinRecord]:
        return await store.records_for_spends(spends, genesis)

    return MempoolManager(
        _birth_block_is_not_the_mempool_lookup,
        store.get_unspent_lineage_info_for_puzzle_hash,
        blockchain.constants,
        InlineExecutor(),
        validation_timeout=30,
        get_spend_records=spend_records,
    )


def _reward(blocks: list[FullBlock], puzzle: bytes32) -> tuple[Coin, int]:
    for block in blocks:
        if not block.is_transaction_block():
            continue
        for coin in block.get_included_reward_coins():
            if coin.puzzle_hash == puzzle:
                assert block.foliage_transaction_block is not None
                return coin, int(block.foliage_transaction_block.timestamp)
    raise AssertionError("reward coin was not created")


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_mempool_follows_a_coin_through_a_fork(
    bt: BlockTools, consensus_mode: ConsensusMode, tmp_path: Path
) -> None:
    """A spend in the mempool tracks the coin: included, undone by a fork, then spent by the other fork."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        store = blockchain.coin_store
        assert isinstance(store, RocksCoinStore)
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(
            6,
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
            genesis_timestamp=uint64(10_000),
            time_per_block=10,
        )
        mempool = _rocks_mempool(blockchain)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        tx_peak = blockchain.get_tx_peak()
        assert tx_peak is not None
        await mempool.new_peak(tx_peak, None)

        spend_coin, born_at = _reward(blocks, farmer)
        record = await store.get_coin_record(spend_coin.name())
        assert record is not None and not record.spent and record.coin == spend_coin
        assert int(record.timestamp) == born_at
        assert int(record.timestamp) > 0
        peak_time = int(tx_peak.timestamp or 0)
        too_soon = peak_time - int(record.timestamp) + 50
        early = wallet.generate_signed_transaction(
            uint64(1_000),
            wallet.get_new_puzzlehash(),
            spend_coin,
            condition_dic={
                ConditionOpcode.ASSERT_SECONDS_RELATIVE: [
                    ConditionWithArgs(ConditionOpcode.ASSERT_SECONDS_RELATIVE, [int_to_bytes(too_soon)])
                ]
            },
        )
        status, error = await _admit(mempool, early)
        assert status == MempoolInclusionStatus.FAILED
        assert error == Err.ASSERT_SECONDS_RELATIVE_FAILED
        assert mempool.get_spendbundle(early.name()) is None

        bundle = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), spend_coin)
        child = bundle.additions()[0]
        status, error = await _admit(mempool, bundle)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)
        assert mempool.get_spendbundle(bundle.name()) == bundle

        spent_chain = bt.get_consecutive_blocks(
            2,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
            time_per_block=10,
        )
        await _extend(blockchain, mempool, spent_chain[len(blocks) :])
        assert mempool.get_spendbundle(bundle.name()) is None
        spent = await store.get_coin_record(spend_coin.name())
        child_record = await store.get_coin_record(child.name())
        assert spent is not None and spent.spent and spent.coin == spend_coin
        assert child_record is not None and not child_record.spent and child_record.coin == child
        status, error = await _admit(mempool, bundle)
        assert status == MempoolInclusionStatus.FAILED
        assert error == Err.DOUBLE_SPEND

        fork = bt.get_consecutive_blocks(
            6,
            blocks,
            seed=b"undo-the-spend",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
            time_per_block=10,
        )
        fork_block = blocks[-1]
        fork_info = ForkInfo(fork_block.height, fork_block.height, fork_block.header_hash)
        await _extend(blockchain, mempool, fork[len(blocks) :], fork_info=fork_info)
        peak = blockchain.get_peak()
        assert peak is not None and peak.header_hash == fork[-1].header_hash
        assert await store.get_coin_record(child.name()) is None
        restored = await store.get_coin_record(spend_coin.name())
        assert restored is not None and not restored.spent and restored.coin == spend_coin
        assert int(restored.timestamp) == born_at
        removed = CoinRecord(child, uint32(0), uint32(0), False, uint64(0))
        assert removed.coin_state.created_height is None
        status, error = await _admit(mempool, bundle)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)

        other = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), spend_coin)
        base = fork[:-1]
        alternate = bt.get_consecutive_blocks(
            4,
            base,
            seed=b"other-spend",
            farmer_reward_puzzle_hash=farmer,
            transaction_data=other,
            guarantee_transaction_block=True,
            time_per_block=10,
        )
        base_block = base[-1]
        await _extend(
            blockchain,
            mempool,
            alternate[len(base) :],
            fork_info=ForkInfo(base_block.height, base_block.height, base_block.header_hash),
        )
        assert mempool.get_spendbundle(bundle.name()) is None
        taken = await store.get_coin_record(spend_coin.name())
        assert taken is not None and taken.spent
        rolled_back = CoinRecord(
            taken.coin,
            taken.confirmed_block_index,
            uint32(0),
            taken.coinbase,
            taken.timestamp,
        )
        ordered = sort_coin_states({rolled_back.coin_state, taken.coin_state})
        assert ordered[-1].spent_height == taken.spent_block_index
        status, error = await _admit(mempool, bundle)
        assert status == MempoolInclusionStatus.FAILED
        assert error == Err.DOUBLE_SPEND


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_singleton_lineage_stays_on_the_current_coin(
    bt: BlockTools,
    consensus_mode: ConsensusMode,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mempool's singleton lookup follows the unspent coin, including after its parent mark is gone."""
    del consensus_mode
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        store = blockchain.coin_store
        assert isinstance(store, RocksCoinStore)
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(
            6,
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        reward, _born = _reward(blocks, farmer)
        unique = wallet.get_new_puzzlehash()
        first = wallet.generate_signed_transaction(uint64(reward.amount), unique, reward)
        holder = first.additions()[0]
        first_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=first,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, first_blocks[-1])
        assert await store.get_unspent_lineage_info_for_puzzle_hash(unique) is None

        second = wallet.generate_signed_transaction(uint64(holder.amount), unique, holder)
        child = second.additions()[0]
        assert child.puzzle_hash == holder.puzzle_hash and child.amount == holder.amount
        second_blocks = bt.get_consecutive_blocks(
            1,
            first_blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=second,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, second_blocks[-1])
        lineage = await store.get_unspent_lineage_info_for_puzzle_hash(unique)
        assert lineage is not None
        assert lineage.coin_id == child.name()
        assert lineage.parent_id == holder.name()
        assert lineage.parent_parent_id == reward.name()

        mempool = _rocks_mempool(blockchain)
        tx_peak = blockchain.get_tx_peak()
        assert tx_peak is not None
        await mempool.new_peak(tx_peak, None)
        spend_child = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), child)
        status, error = await _admit(mempool, spend_child)
        assert (status, error) == (MempoolInclusionStatus.SUCCESS, None)

        spent_blocks = bt.get_consecutive_blocks(
            1,
            second_blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=spend_child,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        assert await store.get_unspent_lineage_info_for_puzzle_hash(unique) is None
        assert mempool.get_spendbundle(spend_child.name()) == spend_child
        tx_peak = blockchain.get_tx_peak()
        assert tx_peak is not None
        _added, removed = await blockchain.coins_in_block(spent_blocks[-1])
        await mempool.new_peak(tx_peak, list(removed))
        assert mempool.get_spendbundle(spend_child.name()) is None

        undo = bt.get_consecutive_blocks(
            3,
            second_blocks,
            seed=b"restore-singleton",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        fork_at = second_blocks[-1]
        fork_info = ForkInfo(fork_at.height, fork_at.height, fork_at.header_hash)
        for block in undo[len(second_blocks) :]:
            await _validate_and_add_block_multi_result(
                blockchain,
                block,
                expected_result=[AddBlockResult.ADDED_AS_ORPHAN, AddBlockResult.NEW_PEAK],
                fork_info=fork_info,
            )
        peak = blockchain.get_peak()
        assert peak is not None and peak.header_hash == undo[-1].header_hash
        restored = await store.get_unspent_lineage_info_for_puzzle_hash(unique)
        assert restored is not None and restored.coin_id == child.name()

        monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 4)
        aged = bt.get_consecutive_blocks(
            5,
            undo,
            seed=b"age-the-parent-mark",
            farmer_reward_puzzle_hash=farmer,
            guarantee_transaction_block=True,
        )
        for block in aged[len(undo) :]:
            await _validate_and_add_block(blockchain, block)
        assert await store.spent_mark(holder.name()) is None
        assert await store.creation_height(child.name()) is not None
        tx_peak = blockchain.get_tx_peak()
        assert tx_peak is not None
        await mempool.new_peak(tx_peak, None)
        # The parent's spent mark is gone, so it must not look spendable.
        again = wallet.generate_signed_transaction(uint64(holder.amount), unique, holder)
        status, error = await _admit(mempool, again)
        assert status == MempoolInclusionStatus.FAILED
        assert error == Err.UNKNOWN_UNSPENT
        still = await store.get_unspent_lineage_info_for_puzzle_hash(unique)
        assert still is None
        live = await store.get_coin_record(child.name())
        assert live is not None and not live.spent and live.coin == child
