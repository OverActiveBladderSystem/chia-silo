from __future__ import annotations

import pytest
from chia_rs import CoinRecord, SpendConditions
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.consensus.blockchain import coin_spent_at_height
from chia.consensus.coinbase import create_farmer_coin
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.full_node.db.block_filter import committed_filter, filter_matches, hint_filter
from chia.full_node.db.chain_db import ChainDB
from chia.full_node.db.coin_store import RocksCoinStore, UndoWindowExceeded
from chia.full_node.db.keys import CF_HEIGHT_TIME, REORG_WINDOW, encode_block_time, height_time_key
from chia.full_node.db.memory import MemoryBackend
from chia.types.blockchain_format.coin import Coin

PARENT = bytes32(b"\x11" * 32)
PUZZLE = bytes32(b"\x22" * 32)


def _coin(amount: int) -> Coin:
    return Coin(PARENT, PUZZLE, uint64(amount))


def test_a_deleted_spent_row_still_counts_when_that_block_removed_the_coin() -> None:
    coin = _coin(5)
    record = CoinRecord(coin, uint32(3), uint32(8), False, uint64(1))
    assert coin_spent_at_height(record, None, uint32(8)) == coin
    assert coin_spent_at_height(record, coin, uint32(9)) is None
    assert coin_spent_at_height(None, coin, uint32(8)) == coin
    assert coin_spent_at_height(None, None, uint32(8)) is None


def _spend(coin: Coin) -> SpendConditions:
    return SpendConditions(
        coin_id=coin.name(),
        parent_id=coin.parent_coin_info,
        puzzle_hash=coin.puzzle_hash,
        coin_amount=int(coin.amount),
        height_relative=None,
        seconds_relative=None,
        before_height_relative=None,
        before_seconds_relative=None,
        birth_height=None,
        birth_seconds=None,
        create_coin=[],
        agg_sig_me=[],
        agg_sig_parent=[],
        agg_sig_puzzle=[],
        agg_sig_amount=[],
        agg_sig_puzzle_amount=[],
        agg_sig_parent_amount=[],
        agg_sig_parent_puzzle=[],
        flags=0,
        execution_cost=0,
        condition_cost=0,
        atom_count=0,
        pair_count=0,
        fingerprint=b"",
    )


@pytest.mark.anyio
async def test_a_later_spend_is_visible_on_the_coin_that_was_added() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = Coin(PARENT, PUZZLE, uint64(10))
    child = Coin(parent.name(), PUZZLE, uint64(10))
    await store.new_block(uint32(1), uint64(1_000), [], [(parent.name(), parent, False)], [])
    await store.new_block(uint32(2), uint64(2_000), [], [(child.name(), child, False)], [parent.name()])
    await store.new_block(uint32(3), uint64(3_000), [], [], [child.name()])

    # Children are not copied into the coin table. The spend heights are.
    assert await store.get_coins_added_at_height(uint32(2)) == []
    assert await store.spent_mark(child.name()) == (2, 3)
    assert await store.parent_spend_height(parent.name()) == 2
    assert await store.get_coin_records_by_parent_ids(True, [parent.name()]) == []


@pytest.mark.anyio
async def test_one_day_of_undo_is_kept_and_older_spends_are_dropped() -> None:
    # 4608 blocks is one day. 18.75 seconds times 4608 is 86400 seconds.
    assert REORG_WINDOW == 4_608
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    kept = _coin(1)
    spent = _coin(2)
    await store.new_block(
        uint32(1),
        uint64(1_000),
        [],
        [(kept.name(), kept, False), (spent.name(), spent, False)],
        [],
    )
    await store.new_block(uint32(2), uint64(2_000), [], [], [spent.name()], hints=[b"\xee" * 32])
    # The block before the window closes still has the 8-byte spent mark.
    for height in range(3, 2 + REORG_WINDOW):
        await store.new_block(uint32(height), uint64(height), [], [], [])
    # Birth height 1 has fallen out of the delete cursor. The spend at height 2 is still inside the window.
    assert await store.spent_mark(spent.name()) == (1, 2)
    assert await store.get_coin_record(spent.name()) is None
    assert await store.undo_floor() == 1
    assert await store.num_unspent() == 1

    await store.new_block(uint32(2 + REORG_WINDOW), uint64(9_000), [], [], [])

    assert await store.undo_floor() == 2
    assert await store.get_coin_record(spent.name()) is None
    assert await store.spent_mark(spent.name()) is None
    assert await store.creation_height(spent.name()) is None
    assert await store.get_coin_record(kept.name()) is None
    assert await store.creation_height(kept.name()) == 1
    assert await store.num_unspent() == 1
    assert await store.get_block_filter(1) == committed_filter([kept.puzzle_hash, spent.puzzle_hash], [])
    assert await store.get_hint_filter(2) == hint_filter([b"\xee" * 32])
    assert filter_matches(await store.get_hint_filter(2), b"\xee" * 32)
    async with store.db.reader_no_transaction() as view:
        assert await view.get(CF_HEIGHT_TIME, height_time_key(1)) == encode_block_time(1_000)
    await store.index_pending_all()
    assert await store.get_coin_records_by_puzzle_hash(False, PUZZLE) == []
    assert await store.get_coin_records_by_puzzle_hash(True, bytes32(b"\xee" * 32)) == []

    # Rolling back onto the floor does not bring the deleted spent coin back.
    await store.rollback_to_block(2)
    assert await store.spent_mark(spent.name()) is None
    assert await store.creation_height(spent.name()) is None
    assert await store.creation_height(kept.name()) == 1
    assert await store.num_unspent() == 1
    with pytest.raises(UndoWindowExceeded):
        await store.rollback_to_block(1)

    genesis = bytes32(b"\xab" * 32)
    live = await store.records_for_spends([_spend(kept)], genesis)
    assert len(live) == 1
    assert live[0].coin == kept
    assert live[0].confirmed_block_index == 1
    assert live[0].spent_block_index == 0
    assert live[0].timestamp == 1_000
    assert live[0].coinbase is False
    assert await store.records_for_spends([_spend(spent)], genesis) == []
    wrong_amount = Coin(kept.parent_coin_info, kept.puzzle_hash, uint64(int(kept.amount) + 1))
    assert await store.records_for_spends([_spend(wrong_amount)], genesis) == []

    before = await store.num_unspent()
    failed_height = 3 + REORG_WINDOW
    with pytest.raises(ValueError, match="Invalid operation to set spent"):
        await store.new_block(uint32(failed_height), uint64(1), [], [], [spent.name()])
    assert await store.spent_mark(spent.name()) is None
    assert await store.num_unspent() == before
    assert await store.get_block_filter(failed_height) is None

    spend_height = 4 + REORG_WINDOW
    await store.new_block(uint32(spend_height), uint64(4_000), [], [], [kept.name()], removed_coins=[kept])
    assert await store.num_unspent() == 0
    assert await store.spent_mark(kept.name()) == (1, spend_height)
    assert await store.get_coin_record(kept.name()) is None
    spent_kept = await store.records_for_spends([_spend(kept)], genesis)
    assert len(spent_kept) == 1
    assert spent_kept[0].confirmed_block_index == 1
    assert spent_kept[0].spent_block_index == spend_height
    assert spent_kept[0].timestamp == 1_000


@pytest.mark.anyio
async def test_a_forgotten_spend_keeps_its_height_and_unspent_child() -> None:
    """The parent's spent mark is gone after the window. The unspent child keeps its creation height.

    The child row is only that height, so a lookup that needs the coin body finds nothing
    until a block reader is attached. The fast-forward row expires with the same window.
    """
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = Coin(PARENT, PUZZLE, uint64(10))
    child = Coin(parent.name(), PUZZLE, uint64(10))
    await store.new_block(uint32(1), uint64(1_000), [], [(parent.name(), parent, False)], [])
    await store.new_block(
        uint32(2), uint64(2_000), [], [(child.name(), child, True)], [parent.name()], removed_coins=[parent]
    )
    fresh = await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE)
    assert fresh is not None
    assert fresh.coin_id == child.name()
    assert fresh.parent_parent_id == PARENT
    for height in range(3, 2 + REORG_WINDOW + 1):
        await store.new_block(uint32(height), uint64(height), [], [], [])

    assert await store.spent_mark(parent.name()) is None
    assert await store.parent_spend_height(parent.name()) is None
    assert await store.get_coin_records_by_parent_ids(False, [parent.name()]) == []
    assert await store.creation_height(child.name()) == 2
    assert await store.get_coin_record(child.name()) is None
    # The fast-forward row lasts one day from the child's creation, then it is deleted.
    assert await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE) is None


@pytest.mark.anyio
async def test_a_spent_singleton_is_not_followed_after_its_row_is_deleted() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = Coin(PARENT, PUZZLE, uint64(10))
    child = Coin(parent.name(), PUZZLE, uint64(10))
    await store.new_block(uint32(1), uint64(1_000), [], [(parent.name(), parent, False)], [])
    await store.new_block(uint32(2), uint64(2_000), [], [(child.name(), child, True)], [parent.name()])
    await store.index_pending_all()
    await store.new_block(uint32(3), uint64(3_000), [], [], [child.name()], removed_coins=[child])
    for height in range(4, 3 + REORG_WINDOW + 1):
        await store.new_block(uint32(height), uint64(height), [], [], [])

    async def child_from_block(coin_name: bytes32, height: int) -> Coin | None:
        assert coin_name == child.name()
        return child

    store.bind_coin_lookup(child_from_block)
    assert await store.creation_height(child.name()) is None
    assert await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE) is None


@pytest.mark.anyio
async def test_a_reward_added_in_a_later_block_stays_a_reward_after_the_birth_list_is_gone() -> None:
    genesis = DEFAULT_CONSTANTS.GENESIS_CHALLENGE
    store = RocksCoinStore(ChainDB(MemoryBackend()), (bytes(genesis[:16]), bytes(genesis[16:])))
    reward = create_farmer_coin(uint32(5), PUZZLE, uint64(100), genesis)
    await store.new_block(uint32(8), uint64(80), [reward], [], [])
    for height in range(9, 8 + REORG_WINDOW + 1):
        await store.new_block(uint32(height), uint64(height), [], [], [])

    assert await store.creation_height(reward.name()) == 8
    assert await store.get_coin_record(reward.name()) is None
    records = await store.records_for_spends([_spend(reward)], genesis)
    assert len(records) == 1
    assert records[0].coin == reward
    assert records[0].coinbase is True
    assert records[0].confirmed_block_index == 8
    assert records[0].timestamp == 80


@pytest.mark.anyio
async def test_a_coin_created_and_spent_in_one_block_keeps_its_child() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = Coin(PARENT, PUZZLE, uint64(10))
    child = Coin(parent.name(), bytes32(b"\x44" * 32), uint64(10))
    await store.new_block(
        uint32(1),
        uint64(1_000),
        [],
        [(parent.name(), parent, False), (child.name(), child, False)],
        [parent.name()],
    )
    assert await store.get_coins_added_at_height(uint32(1)) == []
    assert await store.spent_mark(parent.name()) == (1, 1)
    assert await store.creation_height(child.name()) == 1
    assert await store.num_unspent() == 1
    assert await store.get_coins_removed_at_height(uint32(1)) == []

    missing = _coin(99)
    with pytest.raises(ValueError, match="Invalid operation to set spent"):
        await store.new_block(uint32(2), uint64(2_000), [], [], [missing.name()])
    assert await store.get_coin_record(missing.name()) is None
    assert await store.num_unspent() == 1
    assert await store.get_block_filter(2) is None

    for height in range(2, 1 + REORG_WINDOW + 1):
        await store.new_block(uint32(height), uint64(height), [], [], [])
    assert await store.spent_mark(parent.name()) is None
    assert await store.creation_height(parent.name()) is None
    assert await store.creation_height(child.name()) == 1
    assert await store.get_coin_records_by_parent_ids(False, [parent.name()]) == []
    live = await store.records_for_spends([_spend(child)], bytes32(b"\xab" * 32))
    assert len(live) == 1
    assert live[0].coin == child
    assert live[0].confirmed_block_index == 1
    assert live[0].spent_block_index == 0


@pytest.mark.anyio
async def test_a_hint_is_not_stored_as_an_address() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    coin = _coin(5)
    hint = bytes32(b"\xee" * 32)
    short = b"\x01" * 31
    await store.new_block(uint32(1), uint64(1_000), [], [(coin.name(), coin, False)], [], hints=[hint, short])
    assert await store.get_coin_records_by_puzzle_hash(True, coin.puzzle_hash) == []
    assert await store.get_coin_records_by_puzzle_hash(True, hint) == []
    assert await store.get_block_filter(1) == committed_filter([coin.puzzle_hash], [])
    assert await store.get_hint_filter(1) == hint_filter([hint, short])
    assert filter_matches(await store.get_hint_filter(1), hint)
    assert filter_matches(await store.get_hint_filter(1), short)
    assert await store.creation_height(coin.name()) == 1
    assert await store.get_coin_record(coin.name()) is None
    assert await store.block_timestamp(1) == 1_000
