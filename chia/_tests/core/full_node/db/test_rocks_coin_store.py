from __future__ import annotations

from pathlib import Path

import pytest
from chia_rs import CoinRecord
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.consensus.coinbase import create_farmer_coin, farmer_parent_id
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.full_node.db.chain_db import ChainDB
from chia.full_node.db.coin_codec import (
    HEIGHT_LEN,
    StoredCoin,
    decode_spent_ids,
    encode_spent_mark,
    ff_born_key,
    singleton_puzzle_key,
)
from chia.full_node.full_node_rpc_api import coin_record_for_listed_coin
from chia.full_node.db.coin_store import COIN_INDEX_CHUNK, RocksCoinStore
from chia.full_node.db.keys import (
    CF_BLOCK_FILTER,
    CF_COIN_DELTA,
    CF_COINS,
    CF_HINT_FILTER,
    CF_COINS_BY_PUZZLE_CONFIRMED,
    CF_COINS_BY_PUZZLE_SPENT,
    CF_FF_UNSPENT,
    CF_HEIGHT_TIME,
    CF_META,
    COLUMN_FAMILIES,
    META_COIN_INDEXED,
    REORG_WINDOW,
    encode_block_time,
    height_time_key,
    u32_be,
)
from chia.full_node.db.memory import MemoryBackend
from chia.full_node.db.ops import Op
from chia.full_node.db.rocks import RocksBackend
from chia.types.blockchain_format.coin import Coin

PARENT = bytes32(b"\x11" * 32)
PUZZLE = bytes32(b"\x22" * 32)
OTHER = bytes32(b"\x33" * 32)


def _coin(parent: bytes32, puzzle: bytes32, amount: int) -> Coin:
    return Coin(parent, puzzle, uint64(amount))


def test_a_listed_coin_is_not_given_a_guessed_height() -> None:
    """A missing row is not reported as unspent, and a removal is not reported as born here."""
    coin = _coin(PARENT, PUZZLE, 5)
    kwargs = {
        "coin": coin,
        "block_height": uint32(4),
        "block_timestamp": uint64(40),
        "stored": None,
        "closed": None,
        "birth_timestamp": None,
        "genesis_challenge": bytes32(b"\x00" * 32),
    }
    with pytest.raises(ValueError, match="no stored heights"):
        coin_record_for_listed_coin(**kwargs, added_here=True, removed_here=False)
    with pytest.raises(ValueError, match="no stored heights"):
        coin_record_for_listed_coin(**kwargs, added_here=False, removed_here=True)
    ephemeral = coin_record_for_listed_coin(**kwargs, added_here=True, removed_here=True)
    assert ephemeral.confirmed_block_index == 4
    assert ephemeral.spent_block_index == 4
    stored = CoinRecord(coin, uint32(2), uint32(4), False, uint64(20))
    listed = coin_record_for_listed_coin(
        **{**kwargs, "stored": stored},
        added_here=False,
        removed_here=True,
    )
    assert listed.confirmed_block_index == 2
    assert listed.spent_block_index == 4


@pytest.mark.anyio
async def test_unspent_coin_is_stored_as_its_creation_height() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    created = _coin(PARENT, PUZZLE, 100)
    await store.new_block(uint32(9), uint64(1_700), [], [(created.name(), created, False)], [])
    async with store.db.reader_no_transaction() as view:
        raw = await view.get(CF_COINS, bytes(created.name()))
        stored_time = await view.get(CF_HEIGHT_TIME, height_time_key(9))
    assert raw == (9).to_bytes(HEIGHT_LEN, "little")
    assert stored_time == encode_block_time(1_700)
    # The coin body is in the block, not in this row.
    assert await store.get_coin_record(created.name()) is None
    assert await store.creation_height(created.name()) == 9
    assert await store.block_timestamp(9) == 1_700

    await store.new_block(uint32(10), uint64(1_800), [], [], [created.name()])
    async with store.db.reader_no_transaction() as view:
        spent_raw = await view.get(CF_COINS, bytes(created.name()))
    assert spent_raw == encode_spent_mark(9, 10)
    assert await store.spent_mark(created.name()) == (9, 10)
    assert await store.get_coin_record(created.name()) is None


@pytest.mark.anyio
async def test_empty_check_reads_one_coin_row() -> None:
    """Startup asks this only when no peak was found. The answer is one key, not the coin table."""
    backend = MemoryBackend()
    limits: list[int | None] = []
    original = backend.scan

    async def recording(
        cf: str, start: bytes, end: bytes | None, *, limit: int | None = None
    ) -> list[tuple[bytes, bytes]]:
        if cf == CF_COINS:
            limits.append(limit)
        return await original(cf, start, end, limit=limit)

    backend.scan = recording  # type: ignore[method-assign]
    store = RocksCoinStore(ChainDB(backend))
    assert await store.is_empty() is True
    created = _coin(PARENT, PUZZLE, 100)
    await store.new_block(uint32(1), uint64(1), [], [(created.name(), created, False)], [])
    assert await store.is_empty() is False
    assert limits == [1, 1]


@pytest.mark.anyio
async def test_height_to_time_is_one_exact_time_and_a_reorg_drops_abandoned_heights() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    later = _coin(bytes32(b"\x44" * 32), PUZZLE, 3)
    await store.new_block(
        uint32(1),
        uint64(1_000),
        [],
        [(first.name(), first, False), (second.name(), second, False)],
        [],
    )
    await store.new_block(uint32(3), uint64(3_000), [], [(later.name(), later, False)], [])

    async with store.db.reader_no_transaction() as view:
        rows = await view.scan(CF_HEIGHT_TIME, b"", None)
    assert rows == [
        (height_time_key(1), encode_block_time(1_000)),
        (height_time_key(3), encode_block_time(3_000)),
    ]
    assert await store.block_timestamp(1) == 1_000
    assert await store.block_timestamp(3) == 3_000
    assert await store.creation_height(first.name()) == 1
    assert await store.creation_height(later.name()) == 3

    class _Spend:
        def __init__(self) -> None:
            self.coin_id = first.name()
            self.parent_id = first.parent_coin_info
            self.puzzle_hash = first.puzzle_hash
            self.coin_amount = int(first.amount)

    checked = await store.records_for_spends([_Spend()], bytes32(b"\x00" * 32))  # type: ignore[list-item]
    assert [record.timestamp for record in checked] == [1_000]

    # One second off is a different time. A spend reads the list, not a coin-row copy.
    async with store.db.writer_maybe_transaction() as session:
        session.put(CF_HEIGHT_TIME, height_time_key(1), encode_block_time(1_001))
    shifted = await store.records_for_spends([_Spend()], bytes32(b"\x00" * 32))  # type: ignore[list-item]
    assert [record.timestamp for record in shifted] == [1_001]

    await store.rollback_to_block(1, created_coins=[later])
    async with store.db.reader_no_transaction() as view:
        rows = await view.scan(CF_HEIGHT_TIME, b"", None)
    assert rows == [(height_time_key(1), encode_block_time(1_001))]
    assert await store.creation_height(later.name()) is None
    assert await store.creation_height(first.name()) == 1


@pytest.mark.anyio
async def test_new_block_spend_and_unspent_count() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    created = _coin(PARENT, PUZZLE, 100)
    reward = _coin(PARENT, OTHER, 1_750_000_000_000)
    await store.new_block(uint32(1), uint64(10), [reward], [(created.name(), created, False)], [])
    assert await store.num_unspent() == 2
    assert await store.creation_height(created.name()) == 1
    assert await store.spent_mark(created.name()) is None

    await store.new_block(uint32(2), uint64(20), [], [], [created.name()])
    assert await store.spent_mark(created.name()) == (1, 2)
    assert await store.get_coin_record(created.name()) is None
    assert await store.num_unspent() == 1


@pytest.mark.anyio
async def test_set_spent_mismatch_rolls_back_the_insert() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    created = _coin(PARENT, PUZZLE, 50)
    missing = bytes32(b"\x44" * 32)
    with pytest.raises(ValueError, match="Invalid operation to set spent"):
        await store.new_block(uint32(1), uint64(1), [], [(created.name(), created, True)], [missing])
    assert await store.get_coin_record(created.name()) is None
    assert await store.num_unspent() == 0


@pytest.mark.anyio
async def test_rollback_restores_fast_forward_sentinel() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 100)
    child = _coin(parent.name(), PUZZLE, 100)
    extra = _coin(bytes32(b"\x02" * 32), OTHER, 5)
    await store.new_block(uint32(1), uint64(1), [], [(parent.name(), parent, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [], [parent.name()])
    await store.new_block(uint32(3), uint64(3), [], [(child.name(), child, True), (extra.name(), extra, False)], [])

    changes = await store.rollback_to_block(2, created_coins=[child, extra])
    assert extra.name() in changes
    assert await store.creation_height(extra.name()) is None
    assert await store.creation_height(child.name()) is None

    # child was created at height 3, so it is gone. Parent spend at height 2 remains.
    assert await store.spent_mark(parent.name()) == (1, 2)
    assert await store.get_coin_record(parent.name()) is None

    # The parent coin is in the creating block, so the fast-forward value is the two parent ids.
    kept = _coin(parent.name(), PUZZLE, 100)
    await store.new_block(
        uint32(3),
        uint64(3),
        [],
        [(kept.name(), kept, True)],
        [],
        removed_coins=[parent],
    )
    await store.new_block(uint32(4), uint64(4), [], [], [kept.name()], removed_coins=[kept])
    reported = await store.rollback_to_block(3, removed_coins=[kept])
    assert reported[kept.name()].spent_block_index == 0
    assert await store.creation_height(kept.name()) == 3

    async with store.db.reader_no_transaction() as view:
        fast_forward = await view.scan(CF_FF_UNSPENT, bytes(PUZZLE), bytes(PUZZLE) + b"\xff")
    values = [value for key, value in fast_forward if key.endswith(bytes(kept.name()))]
    assert values == [bytes(parent.name()) + bytes(parent.parent_coin_info)]
    lineage = await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE)
    assert lineage is not None
    assert lineage.coin_id == kept.name()
    assert lineage.parent_id == parent.name()
    assert lineage.parent_parent_id == parent.parent_coin_info


@pytest.mark.anyio
async def test_reorg_drops_fast_forward_coins_named_by_the_birth_list() -> None:
    """A fast-forward coin created above the fork is deleted even when the caller omits it.

    An ordinary coin from that same block is deleted when the caller supplies it,
    which is what the full node does by reading the block.
    """
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 100)
    child = _coin(parent.name(), PUZZLE, 100)
    ordinary = _coin(parent.name(), OTHER, 7)
    await store.new_block(uint32(1), uint64(1), [], [(parent.name(), parent, False)], [])
    await store.new_block(
        uint32(2),
        uint64(2),
        [],
        [(child.name(), child, True), (ordinary.name(), ordinary, False)],
        [parent.name()],
        removed_coins=[parent],
    )
    assert await store.num_unspent() == 2

    await store.rollback_to_block(1)
    assert await store.creation_height(child.name()) is None
    assert await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE) is None
    assert await store.creation_height(ordinary.name()) == 2
    assert await store.creation_height(parent.name()) == 1
    assert await store.spent_mark(parent.name()) is None
    assert await store.num_unspent() == 2

    await store.rollback_to_block(1, created_coins=[ordinary])
    assert await store.creation_height(ordinary.name()) is None
    assert await store.num_unspent() == 1

    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []


@pytest.mark.anyio
async def test_a_same_block_spend_keeps_the_lineage_written_in_that_block() -> None:
    """The spend list can see a fast-forward row written earlier in the same block."""
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 100)
    child = _coin(parent.name(), PUZZLE, 100)
    await store.new_block(uint32(1), uint64(1), [], [(parent.name(), parent, False)], [])
    await store.new_block(
        uint32(2),
        uint64(2),
        [],
        [(child.name(), child, True)],
        [parent.name(), child.name()],
        removed_coins=[parent],
    )
    async with store.db.reader_no_transaction() as view:
        raw = await view.get(CF_COIN_DELTA, u32_be(2))
    assert raw is not None
    by_name = {name: lineage for name, _puzzle, lineage in decode_spent_ids(raw)}
    assert by_name[child.name()] == bytes(parent.name()) + bytes(parent.parent_coin_info)


@pytest.mark.anyio
async def test_fast_forward_lineage_for_one_block_is_one_read() -> None:
    backend = _SaveReadCounter()
    store = RocksCoinStore(ChainDB(backend))
    first_parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 10)
    second_parent = _coin(bytes32(b"\x02" * 32), OTHER, 20)
    first_child = _coin(first_parent.name(), PUZZLE, 10)
    second_child = _coin(second_parent.name(), OTHER, 20)
    await store.new_block(
        uint32(1),
        uint64(1),
        [],
        [(first_parent.name(), first_parent, False), (second_parent.name(), second_parent, False)],
        [],
    )
    await store.new_block(
        uint32(2),
        uint64(2),
        [],
        [(first_child.name(), first_child, True), (second_child.name(), second_child, True)],
        [first_parent.name(), second_parent.name()],
        removed_coins=[first_parent, second_parent],
    )
    backend.clear()
    await store.new_block(
        uint32(3),
        uint64(3),
        [],
        [],
        [first_child.name(), second_child.name()],
        removed_coins=[first_child, second_child],
    )
    assert backend.ff_gets == 0
    assert [set(batch) for batch in backend.ff_batches] == [
        {
            bytes(PUZZLE) + bytes(first_child.name()),
            bytes(OTHER) + bytes(second_child.name()),
        }
    ]


@pytest.mark.anyio
async def test_a_page_of_filters_is_two_reads() -> None:
    """Wallet sync asks for a run of heights. That is two grouped reads, not one per block."""
    backend = _SaveReadCounter()
    store = RocksCoinStore(ChainDB(backend))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    await store.new_block(uint32(1), uint64(1), [], [(first.name(), first, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [(second.name(), second, False)], [])
    backend.clear()
    page = await store.get_filters_for_heights([1, 2])
    assert backend.filter_gets == 0
    assert len(backend.filter_batches) == 2
    assert all(len(batch) == 2 for batch in backend.filter_batches)
    assert page[1][0] == await store.get_block_filter(1)
    assert page[2][1] == await store.get_hint_filter(2)


@pytest.mark.anyio
async def test_import_does_not_create_a_fast_forward_key() -> None:
    """A loaded row has no parent's parent, so it must not leave a key that never expires."""
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    singleton = StoredCoin(bytes32(b"\x51" * 32), 1, -1, False, PUZZLE, PARENT, uint64(100), 0)
    spent = StoredCoin(bytes32(b"\x52" * 32), 1, 4, False, OTHER, PARENT, uint64(5), 0)
    await store.import_coins([singleton, spent])
    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []
        assert await view.get(CF_COIN_DELTA, singleton_puzzle_key(singleton.coin_name)) is None
    assert await store.creation_height(singleton.coin_name) == 1
    assert await store.spent_mark(spent.coin_name) == (1, 4)

    await store.rollback_to_block(3)
    assert await store.spent_mark(spent.coin_name) is None
    assert await store.creation_height(spent.coin_name) == 1
    assert await store.get_unspent_lineage_info_for_puzzle_hash(PUZZLE) is None
    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []


@pytest.mark.anyio
async def test_puzzle_hash_query_filters_spent_coins() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, PUZZLE, 2)
    await store.new_block(
        uint32(5),
        uint64(5),
        [],
        [(first.name(), first, False), (second.name(), second, False)],
        [],
    )
    await store.new_block(uint32(6), uint64(6), [], [], [first.name()])
    # The coin table has no puzzle-hash list. The block is what names these coins.
    assert await store.get_coin_records_by_puzzle_hash(False, PUZZLE) == []
    assert await store.get_coins_added_at_height(uint32(5)) == []
    assert await store.get_coins_removed_at_height(uint32(6)) == []
    assert await store.creation_height(second.name()) == 5
    assert await store.spent_mark(first.name()) == (5, 6)


@pytest.mark.anyio
async def test_one_index_batch_drops_a_fast_forward_key_spent_later_in_the_batch() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 100)
    child = _coin(parent.name(), PUZZLE, 100)
    await store.new_block(uint32(1), uint64(1), [], [(parent.name(), parent, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [], [parent.name()])
    await store.new_block(uint32(3), uint64(3), [], [(child.name(), child, True)], [])
    await store.new_block(uint32(4), uint64(4), [], [], [child.name()])

    await store.index_pending_all()

    async with store.db.reader_no_transaction() as view:
        fast_forward = await view.scan(CF_FF_UNSPENT, b"", None)
    assert fast_forward == []
    assert await store.get_coin_records_by_puzzle_hash(True, PUZZLE) == []


@pytest.mark.anyio
async def test_block_save_defers_lookup_keys_until_index_pending() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    created = _coin(PARENT, PUZZLE, 100)
    reward = _coin(PARENT, OTHER, 1_750_000_000_000)
    await store.new_block(uint32(1), uint64(10), [reward], [(created.name(), created, False)], [])

    assert await store.get_coin_records_by_puzzle_hash(True, PUZZLE) == []
    assert await store.get_coin_records_by_parent_ids(True, [PARENT]) == []
    assert await store.index_pending() is False
    assert await store.creation_height(created.name()) == 1

    await store.new_block(uint32(2), uint64(20), [], [], [created.name()])
    assert await store.get_coin_records_by_puzzle_hash(False, PUZZLE) == []
    assert await store.spent_mark(created.name()) == (1, 2)


class _ApplyCounter(MemoryBackend):
    def __init__(self) -> None:
        super().__init__()
        self.applies = 0

    async def apply(self, ops: list[Op]) -> None:
        self.applies += 1
        await super().apply(ops)


class _CountingBackend(MemoryBackend):
    def __init__(self) -> None:
        super().__init__()
        self.coin_lookups: list[list[bytes]] = []
        self.scans: list[tuple[str, int | None]] = []

    async def get_many(self, cf: str, keys: list[bytes]) -> dict[bytes, bytes]:
        if cf == CF_COINS:
            self.coin_lookups.append(list(keys))
        return await super().get_many(cf, keys)

    async def scan(
        self, cf: str, start: bytes, end: bytes | None, *, limit: int | None = None
    ) -> list[tuple[bytes, bytes]]:
        self.scans.append((cf, limit))
        return await super().scan(cf, start, end, limit=limit)


@pytest.mark.anyio
async def test_block_save_does_not_probe_new_coin_ids() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    created = _coin(PARENT, PUZZLE, 100)
    await store.new_block(
        uint32(1),
        uint64(10),
        [],
        [(created.name(), created, False)],
        [],
        assume_additions_are_new=True,
    )
    assert backend.coin_lookups == []
    assert await store.creation_height(created.name()) == 1
    assert await store.get_coin_record(created.name()) is None
    backend.coin_lookups.clear()

    await store.new_block(uint32(2), uint64(20), [], [], [created.name()], assume_additions_are_new=True)
    assert backend.coin_lookups == [[bytes(created.name())]]
    assert await store.spent_mark(created.name()) == (1, 2)
    assert await store.get_coin_record(created.name()) is None

    repeated = _coin(PARENT, OTHER, 5)
    with pytest.raises(ValueError, match="already exists"):
        await store.new_block(
            uint32(3),
            uint64(30),
            [],
            [(repeated.name(), repeated, False), (repeated.name(), repeated, False)],
            [],
            assume_additions_are_new=True,
        )


@pytest.mark.anyio
async def test_children_come_from_the_block_that_spent_the_parent() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(PARENT, PUZZLE, 50)
    other = _coin(PARENT, OTHER, 7)
    await store.new_block(uint32(1), uint64(10), [], [(parent.name(), parent, False), (other.name(), other, False)], [])
    assert await store.get_coin_records_by_parent_ids(True, [parent.name()]) == []

    first = _coin(parent.name(), PUZZLE, 20)
    second = _coin(parent.name(), OTHER, 30)
    unrelated = _coin(other.name(), PUZZLE, 1)
    await store.new_block(
        uint32(2),
        uint64(20),
        [],
        [(first.name(), first, False), (second.name(), second, False), (unrelated.name(), unrelated, False)],
        [parent.name(), other.name()],
    )
    # The coin table does not list children. It records that the parents were spent here.
    assert await store.parent_spend_height(parent.name()) == 2
    assert await store.parent_spend_height(other.name()) == 2
    assert await store.creation_height(first.name()) == 2
    assert await store.creation_height(unrelated.name()) == 2
    assert await store.get_coin_records_by_parent_ids(True, [parent.name(), other.name()]) == []
    assert await store.get_coin_records_by_parent_ids(True, [bytes32([9] * 32)]) == []


@pytest.mark.anyio
async def test_new_block_still_rejects_a_coin_already_in_the_store() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    created = _coin(PARENT, PUZZLE, 100)
    await store.new_block(uint32(1), uint64(10), [], [(created.name(), created, False)], [])
    with pytest.raises(ValueError, match="already exists"):
        await store.new_block(uint32(2), uint64(20), [], [(created.name(), created, False)], [])


async def _indexed_height(store: RocksCoinStore) -> int:
    async with store.db.reader_no_transaction() as view:
        raw = await view.get(CF_META, META_COIN_INDEXED)
    assert raw is not None
    return int.from_bytes(raw, "big", signed=True)


async def _puzzle_key_count(store: RocksCoinStore) -> int:
    del store
    assert CF_COINS_BY_PUZZLE_CONFIRMED not in COLUMN_FAMILIES
    return 0


@pytest.mark.anyio
async def test_lookup_index_catches_up_in_chunks_and_releases_the_writer() -> None:
    backend = _ApplyCounter()
    store = RocksCoinStore(ChainDB(backend))
    total = COIN_INDEX_CHUNK * 2 + 1
    for height in range(1, total + 1):
        coin = _coin(PARENT, PUZZLE, height)
        await store.new_block(uint32(height), uint64(height), [], [(coin.name(), coin, False)], [])
    assert await _puzzle_key_count(store) == 0

    # One chunk commits and returns the writer. The rest of the backlog stays unindexed.
    saved_applies = backend.applies
    assert await store.index_pending() is False
    assert backend.applies == saved_applies
    async with store.db.writer() as session:
        raw = await session.get(CF_META, META_COIN_INDEXED)
    assert raw is None

    follower = _coin(PARENT, OTHER, 1)
    await store.new_block(uint32(total + 1), uint64(total + 1), [], [(follower.name(), follower, False)], [])
    assert await store.creation_height(follower.name()) == total + 1
    assert await store.get_coin_records_by_puzzle_hash(True, OTHER) == []
    before_rest = backend.applies
    await store.index_pending_all()
    assert backend.applies == before_rest
    assert await store.creation_height(follower.name()) == total + 1


@pytest.mark.anyio
async def test_index_pending_follows_the_next_journal_across_a_height_gap() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    early = _coin(PARENT, PUZZLE, 1)
    late = _coin(PARENT, PUZZLE, 2)
    await store.new_block(uint32(1), uint64(1), [], [(early.name(), early, False)], [])
    await store.new_block(uint32(50), uint64(50), [], [(late.name(), late, False)], [])

    assert await store.index_pending(limit=1) is False
    assert await store.creation_height(early.name()) == 1
    assert await store.creation_height(late.name()) == 50
    assert await store.get_coin_records_by_puzzle_hash(True, PUZZLE) == []


@pytest.mark.anyio
async def test_get_coin_records_keeps_request_order_and_skips_missing() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, PUZZLE, 2)
    await store.new_block(
        uint32(1),
        uint64(1),
        [],
        [(first.name(), first, False), (second.name(), second, False)],
        [],
    )
    backend.coin_lookups.clear()
    missing = bytes32(b"\x55" * 32)
    assert await store.get_coin_records([]) == []
    assert backend.coin_lookups == []

    records = await store.get_coin_records([second.name(), missing, first.name(), second.name()])
    # No block lookup is bound, so a height row cannot be rebuilt. The names were still read once.
    assert records == []
    assert await store.creation_height(first.name()) == 1
    assert await store.creation_height(second.name()) == 1
    # The names are read, then read again so a fork cannot change the row mid-lookup.
    assert len(backend.coin_lookups) == 2
    assert set(backend.coin_lookups[0]) == {bytes(second.name()), bytes(missing), bytes(first.name())}
    assert set(backend.coin_lookups[1]) == {bytes(second.name()), bytes(missing), bytes(first.name())}


@pytest.mark.anyio
async def test_get_coin_records_reads_each_birth_block_once() -> None:
    """The mempool and a wallet peak update ask by name. The block supplies the coin."""
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, PUZZLE, 2)
    await store.new_block(
        uint32(1),
        uint64(1_000),
        [],
        [(first.name(), first, False), (second.name(), second, False)],
        [],
    )
    await store.new_block(uint32(2), uint64(2_000), [], [], [second.name()])
    reads: list[int] = []

    async def coins_at(height: int) -> dict[bytes32, Coin]:
        reads.append(height)
        if height == 1:
            return {first.name(): first, second.name(): second}
        return {}

    store.bind_coins_at_height(coins_at)
    missing = bytes32(b"\x55" * 32)
    records = await store.get_coin_records([second.name(), missing, first.name(), second.name()])
    assert reads == [1]
    assert [record.coin for record in records] == [second, first, second]
    assert records[0].confirmed_block_index == 1
    assert records[0].spent_block_index == 2
    assert records[0].timestamp == 1_000
    assert records[0].spent
    assert records[1].confirmed_block_index == 1
    assert records[1].spent_block_index == 0
    assert not records[1].spent
    assert records[1].timestamp == 1_000


@pytest.mark.anyio
async def test_puzzle_state_cap_does_not_load_the_rest_of_the_history() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    coins = [_coin(PARENT, PUZZLE, amount) for amount in range(1, 31)]
    await store.new_block(uint32(1), uint64(1), [], [(coin.name(), coin, False) for coin in coins], [])
    assert await store.index_pending() is False
    backend.coin_lookups.clear()

    capped = await store.get_coin_states_by_puzzle_hashes(True, {PUZZLE}, uint32(0), max_items=5)
    assert not capped
    assert await store.creation_height(coins[0].name()) == 1


@pytest.mark.anyio
async def test_batch_page_stops_before_later_heights() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    for height in range(1, 21):
        coin = _coin(PARENT, PUZZLE, height)
        await store.new_block(uint32(height), uint64(height), [], [(coin.name(), coin, False)], [])
    await store.index_pending_all()
    backend.coin_lookups.clear()
    backend.scans.clear()

    states, next_height = await store.batch_coin_states_by_puzzle_hashes([PUZZLE], max_items=5, include_hinted=False)
    assert states == []
    assert next_height is None
    assert await store.creation_height(_coin(PARENT, PUZZLE, 1).name()) == 1


@pytest.mark.anyio
async def test_puzzle_state_cap_does_not_scan_the_next_chunk() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    count = 3
    puzzles = [bytes32(i.to_bytes(32, "big")) for i in range(1, count + 1)]
    coins = [_coin(PARENT, puzzle, 1) for puzzle in puzzles]
    await store.new_block(uint32(1), uint64(1), [], [(coin.name(), coin, False) for coin in coins], [])
    assert await store.index_pending() is False
    backend.coin_lookups.clear()
    backend.scans.clear()

    capped = await store.get_coin_states_by_puzzle_hashes(True, set(puzzles), uint32(0), max_items=1)
    assert not capped
    assert await store.creation_height(coins[0].name()) == 1


@pytest.mark.anyio
async def test_index_pending_reads_every_spend_once_and_does_not_write_a_puzzle_list() -> None:
    backend = _CountingBackend()
    store = RocksCoinStore(ChainDB(backend))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    third = _coin(bytes32(b"\x44" * 32), PUZZLE, 3)
    await store.new_block(
        uint32(1),
        uint64(1),
        [],
        [
            (first.name(), first, False),
            (second.name(), second, False),
            (third.name(), third, False),
        ],
        [],
    )
    await store.new_block(uint32(2), uint64(2), [], [], [second.name(), first.name()])
    await store.new_block(uint32(3), uint64(3), [], [], [third.name()])
    backend.coin_lookups.clear()

    assert await store.index_pending() is False
    assert backend.coin_lookups == []
    assert await store.spent_mark(first.name()) == (1, 2)
    assert await store.spent_mark(third.name()) == (1, 3)
    assert CF_COINS_BY_PUZZLE_SPENT not in COLUMN_FAMILIES


class _SaveReadCounter(MemoryBackend):
    """Counts point reads separately from grouped reads during one block save."""

    def __init__(self) -> None:
        super().__init__()
        self.coin_gets = 0
        self.coin_batches: list[list[bytes]] = []
        self.journal_gets = 0
        self.journal_batches: list[list[bytes]] = []
        self.time_gets = 0
        self.time_batches: list[list[bytes]] = []
        self.ff_gets = 0
        self.ff_batches: list[list[bytes]] = []
        self.filter_gets = 0
        self.filter_batches: list[list[bytes]] = []

    def clear(self) -> None:
        self.coin_gets = 0
        self.coin_batches.clear()
        self.journal_gets = 0
        self.journal_batches.clear()
        self.time_gets = 0
        self.time_batches.clear()
        self.ff_gets = 0
        self.ff_batches.clear()
        self.filter_gets = 0
        self.filter_batches.clear()

    async def get(self, cf: str, key: bytes) -> bytes | None:
        if cf == CF_COINS:
            self.coin_gets += 1
        elif cf == CF_COIN_DELTA:
            self.journal_gets += 1
        elif cf == CF_HEIGHT_TIME:
            self.time_gets += 1
        elif cf == CF_FF_UNSPENT:
            self.ff_gets += 1
        elif cf in (CF_BLOCK_FILTER, CF_HINT_FILTER):
            self.filter_gets += 1
        return await super().get(cf, key)

    async def get_many(self, cf: str, keys: list[bytes]) -> dict[bytes, bytes]:
        if cf == CF_COINS:
            self.coin_batches.append(list(keys))
        elif cf == CF_COIN_DELTA:
            self.journal_batches.append(list(keys))
        elif cf == CF_HEIGHT_TIME:
            self.time_batches.append(list(keys))
        elif cf == CF_FF_UNSPENT:
            self.ff_batches.append(list(keys))
        elif cf in (CF_BLOCK_FILTER, CF_HINT_FILTER):
            self.filter_batches.append(list(keys))
        return await super().get_many(cf, keys)


@pytest.mark.anyio
async def test_a_block_save_reads_birth_lists_and_old_spends_in_one_batch() -> None:
    backend = _SaveReadCounter()
    store = RocksCoinStore(ChainDB(backend))
    early = [_coin(PARENT, PUZZLE, amount) for amount in (1, 2, 3)]
    later = [_coin(PARENT, OTHER, amount) for amount in (4, 5)]
    await store.new_block(
        uint32(1),
        uint64(1_000),
        [],
        [(coin.name(), coin, False) for coin in early],
        [],
    )
    await store.new_block(
        uint32(2),
        uint64(2_000),
        [],
        [(coin.name(), coin, False) for coin in later],
        [],
    )
    backend.clear()
    spent = [*early, *later]
    await store.new_block(
        uint32(3),
        uint64(3_000),
        [],
        [],
        [coin.name() for coin in spent],
        removed_coins=spent,
    )
    # The five coin rows are one read. Singleton side keys are one read. No birth list is loaded.
    assert backend.coin_gets == 0
    assert [set(batch) for batch in backend.coin_batches] == [{bytes(coin.name()) for coin in spent}]
    assert backend.time_gets == 0
    assert backend.time_batches == []
    assert [set(batch) for batch in backend.journal_batches] == [
        {singleton_puzzle_key(coin.name()) for coin in spent}
    ]
    for coin in early:
        assert await store.spent_mark(coin.name()) == (1, 3)
    for coin in later:
        assert await store.spent_mark(coin.name()) == (2, 3)
    assert await store.get_coin_record(spent[0].name()) is None

    # Those spends belong to height 3. Saving the block 4608 later drops that journal and those rows.
    forget_height = 3 + REORG_WINDOW
    for height in range(4, forget_height):
        await store.new_block(uint32(height), uint64(height), [], [], [])
    backend.clear()
    await store.new_block(uint32(forget_height), uint64(forget_height), [], [], [])
    assert backend.coin_gets == 0
    assert backend.journal_gets == 0
    assert [set(batch) for batch in backend.journal_batches] == [{u32_be(3), ff_born_key(3)}]
    assert [set(batch) for batch in backend.coin_batches] == [{bytes(coin.name()) for coin in spent}]
    for coin in spent:
        assert await store.get_coin_record(coin.name()) is None

    # Two later coins stay unspent until their birth list has been deleted, then are spent together.
    birth = forget_height + 1
    kept_a = _coin(bytes32(b"\x44" * 32), PUZZLE, 8)
    kept_b = _coin(bytes32(b"\x55" * 32), OTHER, 9)
    await store.new_block(
        uint32(birth),
        uint64(birth),
        [],
        [(kept_a.name(), kept_a, False), (kept_b.name(), kept_b, False)],
        [],
    )
    for height in range(birth + 1, birth + REORG_WINDOW + 1):
        await store.new_block(uint32(height), uint64(height), [], [], [])
    assert await store.creation_height(kept_a.name()) == birth
    assert await store.creation_height(kept_b.name()) == birth
    assert await store.get_coin_record(kept_a.name()) is None
    backend.clear()
    spend_at = birth + REORG_WINDOW + 1
    await store.new_block(
        uint32(spend_at),
        uint64(spend_at),
        [],
        [],
        [kept_a.name(), kept_b.name()],
        removed_coins=[kept_a, kept_b],
    )
    assert backend.coin_gets == 0
    assert backend.time_gets == 0
    assert [set(batch) for batch in backend.coin_batches] == [{bytes(kept_a.name()), bytes(kept_b.name())}]

    class _Spend:
        def __init__(self, coin: Coin) -> None:
            self.coin_id = coin.name()
            self.parent_id = coin.parent_coin_info
            self.puzzle_hash = coin.puzzle_hash
            self.coin_amount = int(coin.amount)

    for coin in (kept_a, kept_b):
        assert await store.spent_mark(coin.name()) == (birth, spend_at)
        records = await store.records_for_spends([_Spend(coin)], bytes32(b"\x00" * 32))  # type: ignore[list-item]
        assert len(records) == 1
        assert records[0].confirmed_block_index == birth
        assert records[0].spent_block_index == spend_at
        assert records[0].timestamp == birth


class _TornCoinRead(MemoryBackend):
    """The first coin load at height 1 swaps that journal for the next block's journal."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.torn = False

    async def get_many(self, cf: str, keys: list[bytes]) -> dict[bytes, bytes]:
        if cf == CF_COINS and self.armed and not self.torn:
            self.torn = True
            journal = self.data[CF_COIN_DELTA].get(u32_be(2))
            assert journal is not None
            self.data[CF_COIN_DELTA][u32_be(1)] = journal
            for key in keys:
                self.data[CF_COINS].pop(key, None)
            return {}
        return await super().get_many(cf, keys)


class _UnstableJournal(MemoryBackend):
    """Every read of the height-1 journal swaps it with the height-2 journal."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.original: bytes | None = None

    async def get(self, cf: str, key: bytes) -> bytes | None:
        raw = await super().get(cf, key)
        if self.armed and cf == CF_COIN_DELTA and key == u32_be(1) and raw is not None and self.original is not None:
            other = self.data[CF_COIN_DELTA].get(u32_be(2))
            assert other is not None
            self.data[CF_COIN_DELTA][key] = other if raw == self.original else self.original
        return raw


@pytest.mark.anyio
async def test_a_height_read_retries_when_a_reorg_lands_during_the_load() -> None:
    backend = _TornCoinRead()
    store = RocksCoinStore(ChainDB(backend))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    await store.new_block(uint32(1), uint64(1), [], [(first.name(), first, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [(second.name(), second, False)], [])
    added, removed = await store.get_additions_and_removals_at_height(uint32(1))
    assert added == []
    assert removed == []
    assert await store.creation_height(first.name()) == 1
    assert await store.creation_height(second.name()) == 2


@pytest.mark.anyio
async def test_a_height_read_stops_when_the_journal_keeps_changing() -> None:
    backend = _UnstableJournal()
    store = RocksCoinStore(ChainDB(backend))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    await store.new_block(uint32(1), uint64(1), [], [(first.name(), first, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [(second.name(), second, False)], [])
    assert await store.get_coins_added_at_height(uint32(1)) == []
    assert await store.creation_height(first.name()) == 1
    assert await store.creation_height(second.name()) == 2


@pytest.mark.anyio
async def test_a_height_has_no_copied_coins_to_race_with_a_commit(tmp_path: Path) -> None:
    chain = ChainDB(RocksBackend(tmp_path / "height.rocksdb", sync="OFF", reader_count=2))
    store = RocksCoinStore(chain)
    try:
        first = _coin(PARENT, PUZZLE, 1)
        second = _coin(PARENT, OTHER, 2)
        await store.new_block(uint32(1), uint64(1), [], [(first.name(), first, False)], [])
        await store.new_block(uint32(2), uint64(2), [], [(second.name(), second, False)], [])
        assert await store.get_coins_added_at_height(uint32(1)) == []
        assert await store.creation_height(first.name()) == 1
        assert await store.creation_height(second.name()) == 2
    finally:
        await chain.close()


@pytest.mark.anyio
async def test_reward_children_are_found_from_the_farmer_parent() -> None:
    genesis = DEFAULT_CONSTANTS.GENESIS_CHALLENGE
    prefixes = (bytes(genesis[:16]), bytes(genesis[16:]))
    store = RocksCoinStore(ChainDB(MemoryBackend()), prefixes)
    reward = create_farmer_coin(uint32(5), PUZZLE, uint64(100), genesis)
    parent_id = farmer_parent_id(uint32(5), genesis)
    await store.new_block(uint32(5), uint64(50), [reward], [], [])

    # The reward coin's body is in the block. This table keeps the creation height only.
    assert await store.creation_height(reward.name()) == 5
    assert await store.get_coin_records_by_parent_ids(True, [parent_id]) == []
    assert await store.get_coin_records_by_parent_ids(True, [bytes32(b"\x09" * 32)]) == []
    assert await store.get_coin_records_by_parent_ids(True, [farmer_parent_id(uint32(9), genesis)]) == []

    plain = RocksCoinStore(ChainDB(MemoryBackend()))
    await plain.new_block(uint32(5), uint64(50), [reward], [], [])
    assert await plain.creation_height(reward.name()) == 5
    assert await plain.get_coin_records_by_parent_ids(True, [parent_id]) == []


@pytest.mark.anyio
async def test_reward_child_is_found_in_the_later_transaction_block() -> None:
    genesis = DEFAULT_CONSTANTS.GENESIS_CHALLENGE
    prefixes = (bytes(genesis[:16]), bytes(genesis[16:]))
    store = RocksCoinStore(ChainDB(MemoryBackend()), prefixes)
    reward = create_farmer_coin(uint32(5), PUZZLE, uint64(100), genesis)
    parent_id = farmer_parent_id(uint32(5), genesis)
    await store.new_block(uint32(8), uint64(80), [reward], [], [])

    assert await store.creation_height(reward.name()) == 8
    assert await store.block_timestamp(8) == 80
    assert await store.get_coin_records_by_parent_ids(True, [parent_id]) == []


@pytest.mark.anyio
async def test_replacing_a_short_unspent_row_does_not_count_it_twice() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    coin = _coin(PARENT, PUZZLE, 5)
    await store.new_block(uint32(1), uint64(1), [], [(coin.name(), coin, False)], [])
    assert await store.num_unspent() == 1
    again = StoredCoin(coin.name(), 1, 0, False, coin.puzzle_hash, coin.parent_coin_info, coin.amount, 1)
    await store.import_coins([again])
    assert await store.num_unspent() == 1
    spent = StoredCoin(coin.name(), 1, 4, False, coin.puzzle_hash, coin.parent_coin_info, coin.amount, 1)
    await store.import_coins([spent])
    assert await store.num_unspent() == 0
    assert await store.spent_mark(coin.name()) == (1, 4)


@pytest.mark.anyio
async def test_a_puzzle_only_spent_list_restores_a_normal_coin_and_then_ages_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An older spent list may carry a puzzle hash and no parent ids.

    A reorg puts the coin back unspent and does not write a fast-forward row.
    When that height falls out of the window, the spent mark and the list are both deleted.
    """
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 4)
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    coin = _coin(PARENT, PUZZLE, 5)
    await store.new_block(uint32(1), uint64(1), [], [(coin.name(), coin, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [], [coin.name()])
    legacy = b"CS1" + (1).to_bytes(4, "big") + bytes(coin.name()) + b"\x01" + bytes(PUZZLE)
    async with store.db.writer_maybe_transaction() as session:
        session.put(CF_COIN_DELTA, u32_be(2), legacy)

    await store.rollback_to_block(1, removed_coins=[coin])
    assert await store.creation_height(coin.name()) == 1
    assert await store.spent_mark(coin.name()) is None
    assert await store.num_unspent() == 1
    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []

    await store.new_block(uint32(2), uint64(2), [], [], [coin.name()])
    async with store.db.writer_maybe_transaction() as session:
        session.put(CF_COIN_DELTA, u32_be(2), legacy)
    await store.new_block(uint32(6), uint64(6), [], [], [])
    assert await store.spent_mark(coin.name()) is None
    assert await store.creation_height(coin.name()) is None
    assert await store.num_unspent() == 0
    async with store.db.reader_no_transaction() as view:
        assert await view.get(CF_COIN_DELTA, u32_be(2)) is None
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []


@pytest.mark.anyio
async def test_a_reorg_after_the_birth_list_expires_does_not_restore_fast_forward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A spend in a later block outlives the birth list. Undoing that spend must not put the fast-forward row back."""
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 4)
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(PARENT, PUZZLE, 10)
    child = _coin(parent.name(), PUZZLE, 10)
    await store.new_block(uint32(1), uint64(1), [], [(parent.name(), parent, False)], [])
    await store.new_block(
        uint32(2),
        uint64(2),
        [],
        [(child.name(), child, True)],
        [parent.name()],
        removed_coins=[parent],
    )
    await store.new_block(uint32(3), uint64(3), [], [], [child.name()], removed_coins=[child])
    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []
    await store.new_block(uint32(6), uint64(6), [], [], [])
    assert await store.undo_floor() == 2
    assert await store.spent_mark(child.name()) == (2, 3)
    async with store.db.reader_no_transaction() as view:
        assert await view.get(CF_COIN_DELTA, ff_born_key(2)) is None

    await store.rollback_to_block(2, removed_coins=[child])
    assert await store.creation_height(child.name()) == 2
    assert await store.spent_mark(child.name()) is None
    assert await store.num_unspent() == 1
    async with store.db.reader_no_transaction() as view:
        assert await view.scan(CF_FF_UNSPENT, b"", None) == []
        assert await view.get(CF_COIN_DELTA, singleton_puzzle_key(child.name())) is None


@pytest.mark.anyio
async def test_a_gap_between_saves_still_forgets_the_heights_in_between(monkeypatch: pytest.MonkeyPatch) -> None:
    """A later block must delete every spend the window has passed, not only its own height."""
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 4)
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    first = _coin(PARENT, PUZZLE, 1)
    second = _coin(PARENT, OTHER, 2)
    await store.new_block(uint32(1), uint64(1), [], [(first.name(), first, False), (second.name(), second, False)], [])
    await store.new_block(uint32(2), uint64(2), [], [], [first.name()])
    await store.new_block(uint32(3), uint64(3), [], [], [second.name()])
    await store.new_block(uint32(6), uint64(6), [], [], [])
    assert await store.spent_mark(first.name()) is None
    assert await store.spent_mark(second.name()) == (1, 3)
    await store.new_block(uint32(9), uint64(9), [], [], [])
    assert await store.spent_mark(second.name()) is None
    assert await store.undo_floor() == 5
