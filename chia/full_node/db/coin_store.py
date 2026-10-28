from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import replace

from chia_rs import CoinRecord, CoinState, SpendConditions
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.consensus.coinbase import is_reward_parent
from chia.full_node.db.block_filter import committed_filter, hint_filter
from chia.full_node.db.chain_db import ChainDB, WriteSession, _View
from chia.full_node.db.coin_codec import (
    HEIGHT_LEN,
    SPENT_MARK_LEN,
    StoredCoin,
    FF_LINEAGE_LEN,
    decode_coin,
    decode_ff_born,
    decode_ff_lineage,
    decode_height,
    decode_spent_ids,
    decode_spent_mark,
    encode_ff_born,
    encode_ff_lineage,
    encode_height,
    encode_row,
    encode_spent_ids,
    encode_spent_mark,
    spent_lineage_entry,
    ff_born_height,
    ff_born_key,
    index_entries,
    is_ff_born_key,
    singleton_puzzle_key,
    spent_id_scan_end,
)
from chia.full_node.db.keys import (
    CF_BLOCK_FILTER,
    CF_CLOSED_SPEND,
    CF_COIN_DELTA,
    CF_COINS,
    CF_FF_UNSPENT,
    CF_HEIGHT_TIME,
    CF_HINT_FILTER,
    CF_META,
    META_COIN_INDEXED,
    META_UNDO_FLOOR,
    META_UNSPENT,
    REORG_WINDOW,
    decode_block_time,
    encode_block_time,
    height_time_key,
    prefix_end,
    u32_be,
)
from chia.types.blockchain_format.coin import Coin
from chia.types.mempool_item import UnspentLineageInfo

log = logging.getLogger(__name__)

# One long-sync batch is at most 32 blocks, and each block journals at most one row.
# Indexing that many releases the chain writer before the next batch needs it.
COIN_INDEX_CHUNK = 32
# A coin row is read, then its block is read. A fork can commit between the two.
# Read the row again and start over while they disagree.
_CONSISTENT_READS = 5


# A reorg can commit between two reads. Retry until the journal and the coin rows agree.


def _row_counts_as_unspent(raw: bytes | None, previous: StoredCoin | None) -> bool:
    """A 4-byte row is unspent even though it does not decode to a stored coin."""
    if previous is not None:
        return previous.spent_index <= 0
    return raw is not None and len(raw) == HEIGHT_LEN


class UndoWindowExceeded(Exception):
    """The fork is older than the kept undo data. The coin tables have to be rebuilt from the blocks."""

    def __init__(self, fork_height: int, undo_floor: int) -> None:
        super().__init__(f"reorg to height {fork_height} is older than undo data kept through height {undo_floor}")
        self.fork_height = fork_height
        self.undo_floor = undo_floor


class RocksCoinStore:
    """Current-peak UTXO set stored in the chain RocksDB."""

    def __init__(self, db: ChainDB, reward_parent_prefixes: tuple[bytes, bytes] | None = None) -> None:
        self.db = db
        # Farmer and pool reward parents are not coins. Each prefix is 16 bytes of
        # the genesis challenge, and the other 16 bytes are the block height.
        self._reward_parent_prefixes = reward_parent_prefixes
        self._parent_at_height: Callable[[bytes32, int], Awaitable[Coin | None]] | None = None
        self._coin_at_height: Callable[[bytes32, int], Awaitable[Coin | None]] | None = None
        self._coins_at_height: Callable[[int], Awaitable[Mapping[bytes32, Coin]]] | None = None

    @classmethod
    async def create(cls, db: ChainDB, reward_parent_prefixes: tuple[bytes, bytes] | None = None) -> RocksCoinStore:
        return cls(db, reward_parent_prefixes)

    def _reward_parent_height(self, parent_id: bytes32) -> int | None:
        prefixes = self._reward_parent_prefixes
        if prefixes is None:
            return None
        raw = bytes(parent_id)
        if raw[:16] not in prefixes:
            return None
        height = int.from_bytes(raw[16:], "big")
        if height > 0xFFFFFFFF:
            return None
        return height

    async def import_coins(self, records: list[StoredCoin], *, assume_new: bool = False) -> None:
        if len(records) == 0:
            return
        async with self.db.writer_maybe_transaction() as session:
            if assume_new:
                existing: dict[bytes, bytes] = {}
            else:
                existing = await session.get_many(CF_COINS, [bytes(record.coin_name) for record in records])
            staged: dict[bytes, StoredCoin] = {}
            remembered: list[StoredCoin] = []
            unspent_delta = 0
            side_keys = [singleton_puzzle_key(record.coin_name) for record in records]
            side_rows = (
                {}
                if assume_new or len(side_keys) == 0
                else await session.get_many(CF_COIN_DELTA, side_keys)
            )
            for record in records:
                key = bytes(record.coin_name)
                raw_prev = None if key in staged or key not in existing else existing[key]
                if key in staged:
                    previous: StoredCoin | None = staged[key]
                elif raw_prev is not None:
                    previous = await self._stored_from_row(session, record.coin_name, raw_prev)
                else:
                    previous = None
                if _row_counts_as_unspent(raw_prev, previous):
                    unspent_delta -= 1
                if previous is not None:
                    self._drop_indexes_now(session, previous)
                if key in staged or key in existing:
                    puzzle = side_rows.get(singleton_puzzle_key(record.coin_name))
                    if puzzle is not None:
                        session.delete(CF_FF_UNSPENT, puzzle + key)
                        session.delete(CF_COIN_DELTA, singleton_puzzle_key(record.coin_name))
                session.put(CF_COINS, key, encode_row(record))
                # No fast-forward key. This record has no parent's parent, and an
                # empty key would stay forever. The block writer stores lineage.
                if record.spent_index <= 0:
                    unspent_delta += 1
                    remembered.append(record)
                else:
                    await self._merge_spent_ids(
                        session,
                        record.spent_index,
                        [spent_lineage_entry(record.coin_name, None, None)],
                    )
                staged[key] = record
            await self._remember_added(session, remembered)
            if unspent_delta != 0:
                await self._add_unspent(session, unspent_delta)

    async def drop_above(self, height: int) -> None:
        """Delete timestamps and filters above `height`. Those heights belong to the abandoned chain."""
        start = b"\x00\x00\x00\x00" if height < 0 else u32_be(height + 1)
        abandoned = b"\xff\xff\xff\xff\x00"
        async with self.db.writer_maybe_transaction() as session:
            session.delete_range(CF_HEIGHT_TIME, start, abandoned)
            session.delete_range(CF_BLOCK_FILTER, start, abandoned)
            session.delete_range(CF_HINT_FILTER, start, abandoned)

    async def clear_coin_tables(self) -> None:
        """Drop coin rows, closed heights, and undo data. Block bodies, filters, and the height-to-time list stay."""
        async with self.db.writer_maybe_transaction() as session:
            for family in (
                CF_COINS,
                CF_COIN_DELTA,
                CF_FF_UNSPENT,
                CF_CLOSED_SPEND,
            ):
                session.delete_range(family, b"", b"\xff" * 80)
            session.put(CF_META, META_UNSPENT, (0).to_bytes(8, "little", signed=False))
            session.delete(CF_META, META_COIN_INDEXED)
            session.delete(CF_META, META_UNDO_FLOOR)

    async def clear_all(self) -> None:
        async with self.db.writer_maybe_transaction() as session:
            for family in (
                CF_COINS,
                CF_COIN_DELTA,
                CF_FF_UNSPENT,
                CF_CLOSED_SPEND,
                CF_HEIGHT_TIME,
                CF_BLOCK_FILTER,
                CF_HINT_FILTER,
            ):
                session.delete_range(family, b"", b"\xff" * 80)
            session.put(CF_META, META_UNSPENT, (0).to_bytes(8, "little", signed=False))
            session.delete(CF_META, META_COIN_INDEXED)

    async def num_unspent(self) -> int:
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_META, META_UNSPENT)
        if raw is None:
            return 0
        return int.from_bytes(raw, "little", signed=False)

    async def new_block(
        self,
        height: uint32,
        timestamp: uint64,
        included_reward_coins: Collection[Coin],
        tx_additions: Collection[tuple[bytes32, Coin, bool]],
        tx_removals: list[bytes32],
        *,
        assume_additions_are_new: bool = False,
        hints: Collection[bytes] = (),
        removed_coins: Collection[Coin] = (),
    ) -> None:
        started = time.monotonic()
        additions = [
            StoredCoin(
                coin_id,
                int(height),
                -1 if same_as_parent else 0,
                False,
                coin.puzzle_hash,
                coin.parent_coin_info,
                coin.amount,
                int(timestamp),
            )
            for coin_id, coin, same_as_parent in tx_additions
        ]
        additions.extend(
            StoredCoin(
                coin.name(),
                int(height),
                0,
                True,
                coin.puzzle_hash,
                coin.parent_coin_info,
                coin.amount,
                int(timestamp),
            )
            for coin in included_reward_coins
        )
        async with self.db.writer_maybe_transaction() as session:
            # Block acceptance has already rejected a repeated output and a repeated
            # spend. Probing the historical coin table for every new id is a miss
            # per coin and dominates db-write. This block's spends are one read.
            # A singleton's puzzle hash is one read of the side keys written when
            # those coins were created. The coin body is not copied into this table.
            seen_additions: set[bytes] = set()
            for record in additions:
                name = bytes(record.coin_name)
                if name in seen_additions:
                    raise ValueError(f"Coin {record.coin_name.hex()} already exists")
                seen_additions.add(name)
            lookup_keys = [bytes(name) for name in tx_removals]
            if not assume_additions_are_new:
                lookup_keys.extend(seen_additions)
            existing = await session.get_many(CF_COINS, lookup_keys) if lookup_keys else {}
            counter_raw = await session.get(CF_META, META_UNSPENT)
            counter = 0 if counter_raw is None else int.from_bytes(counter_raw, "little", signed=False)
            created_by_name = {record.coin_name: record for record in additions}
            bodies = {coin.name(): coin for coin in removed_coins}
            for coin_id, coin, _same in tx_additions:
                bodies[coin_id] = coin
            born_names: list[bytes32] = []
            singleton_keys = [singleton_puzzle_key(name) for name in tx_removals if name not in created_by_name]
            singleton_puzzles = (
                await session.get_many(CF_COIN_DELTA, singleton_keys) if len(singleton_keys) != 0 else {}
            )
            spent_ids: list[tuple[bytes32, bytes32 | None, bytes | None]] = []
            for record in additions:
                name = bytes(record.coin_name)
                if not assume_additions_are_new and existing.get(name) is not None:
                    raise ValueError(f"Coin {record.coin_name.hex()} already exists")
                encoded = encode_height(record.confirmed_index)
                session.put(CF_COINS, name, encoded)
                counter += 1
                existing[name] = encoded
                if record.spent_index == -1:
                    parent_coin = bodies.get(record.parent)
                    if parent_coin is not None:
                        self._put_fast_forward(session, record, parent_coin.parent_coin_info)
                        born_names.append(record.coin_name)
            if tx_removals:
                if height <= 0:
                    raise ValueError(f"spent height must be positive, got {int(height)}")
                prepared: list[tuple[bytes32, bytes | None, bytes | None]] = []
                seen_removals: set[bytes32] = set()
                for name in tx_removals:
                    if name in seen_removals:
                        raise ValueError(f"Coin {name.hex()} spent twice in block {int(height)}")
                    seen_removals.add(name)
                    previous_raw = existing.get(bytes(name))
                    if previous_raw is None or len(previous_raw) != HEIGHT_LEN:
                        prepared.append((name, None, None))
                        continue
                    same_block = created_by_name.get(name)
                    puzzle = None if same_block is None else (
                        bytes(same_block.puzzle_hash) if same_block.spent_index == -1 else None
                    )
                    if puzzle is None:
                        puzzle = singleton_puzzles.get(singleton_puzzle_key(name))
                    prepared.append((name, puzzle, previous_raw))
                lineage_keys = [puzzle + bytes(name) for name, puzzle, _raw in prepared if puzzle is not None]
                lineage_values = (
                    await session.get_many(CF_FF_UNSPENT, lineage_keys) if len(lineage_keys) != 0 else {}
                )
                changed = 0
                for name, puzzle, previous_raw in prepared:
                    if previous_raw is None or len(previous_raw) != HEIGHT_LEN:
                        continue
                    lineage: bytes | None = None
                    if puzzle is not None:
                        stored_lineage = lineage_values.get(puzzle + bytes(name))
                        if stored_lineage is not None and len(stored_lineage) == FF_LINEAGE_LEN:
                            lineage = stored_lineage
                        session.delete(CF_FF_UNSPENT, puzzle + bytes(name))
                        session.delete(CF_COIN_DELTA, singleton_puzzle_key(name))
                    session.put(CF_COINS, bytes(name), encode_spent_mark(decode_height(previous_raw), int(height)))
                    counter -= 1
                    spent_ids.append(spent_lineage_entry(name, puzzle, lineage))
                    changed += 1
                if changed != len(tx_removals):
                    raise ValueError(
                        f"Invalid operation to set spent, total updates {changed} expected {len(tx_removals)}"
                    )
            if counter < 0:
                raise ValueError(f"unspent counter went negative ({counter})")
            session.put(CF_META, META_UNSPENT, counter.to_bytes(8, "little", signed=False))
            # One exact timestamp for this transaction block. Time locks read this list.
            session.put(CF_HEIGHT_TIME, height_time_key(int(height)), encode_block_time(int(timestamp)))
            session.put(
                CF_BLOCK_FILTER,
                height_time_key(int(height)),
                committed_filter([record.puzzle_hash for record in additions], tx_removals),
            )
            session.put(CF_HINT_FILTER, height_time_key(int(height)), hint_filter(hints))
            # Fast-forward keys for new singletons were written above. Address lookup keys are not.
            # The spent-id list is coin ids, so the 8-byte marks can be deleted a day later.
            if spent_ids:
                await self._merge_spent_ids(session, int(height), spent_ids)
            if born_names:
                await self._merge_ff_born(session, int(height), born_names)
            drop_at = int(height) - REORG_WINDOW
            if drop_at >= 0:
                # Transaction blocks are not every height. Forgetting only this
                # exact height would skip the spends in between and then move
                # the floor past them, so those marks would stay forever.
                floor_raw = await session.get(CF_META, META_UNDO_FLOOR)
                previous = -1 if floor_raw is None else int.from_bytes(floor_raw, "big", signed=False)
                for forgotten in range(previous + 1, drop_at + 1):
                    await self._forget_undo_at(session, forgotten)
        elapsed = time.monotonic() - started
        message = (
            f"Height {height}: It took {elapsed:0.2f}s to apply {len(tx_additions)} additions and "
            f"{len(tx_removals)} removals to the coin store."
        )
        log.log(logging.WARNING if elapsed > 10 else logging.DEBUG, message)

    def _drop_indexes_now(self, session: WriteSession, record: StoredCoin) -> None:
        for family, key in index_entries(record):
            if family == CF_COINS:
                continue
            session.delete(family, key)

    async def _stored_from_row(self, view: _View, coin_name: bytes32, raw: bytes) -> StoredCoin | None:
        del view
        if len(raw) == HEIGHT_LEN or len(raw) == SPENT_MARK_LEN:
            # The body is in the block. A 4-byte row is unspent. An 8-byte mark is spent.
            return None
        return decode_coin(coin_name, raw)

    async def _remember_added(self, session: WriteSession, records: list[StoredCoin]) -> None:
        """Record the block time. The coin body is not copied into a birth list."""
        times: dict[int, int] = {}
        for record in records:
            if record.spent_index > 0:
                continue
            times[record.confirmed_index] = record.timestamp
        for height, when in times.items():
            session.put(CF_HEIGHT_TIME, height_time_key(height), encode_block_time(int(when)))

    async def _load_many(self, view: _View, names: list[bytes32]) -> dict[bytes32, StoredCoin]:
        if len(names) == 0:
            return {}
        found = await view.get_many(CF_COINS, [bytes(name) for name in names])
        loaded: dict[bytes32, StoredCoin] = {}
        for name in names:
            raw = found.get(bytes(name))
            if raw is None:
                continue
            if len(raw) == HEIGHT_LEN or len(raw) == SPENT_MARK_LEN:
                continue
            loaded[name] = decode_coin(name, raw)
        await self._stamp_list_times(view, loaded)
        return loaded

    async def _timestamp_at(self, view: _View, height: int) -> int | None:
        raw = await view.get(CF_HEIGHT_TIME, height_time_key(height))
        if raw is None:
            return None
        return decode_block_time(raw)

    async def _stamp_list_times(self, view: _View, loaded: dict[bytes32, StoredCoin]) -> None:
        """The height-to-time list is the creation time a later spend checks."""
        if len(loaded) == 0:
            return
        heights = {record.confirmed_index for record in loaded.values()}
        raws = await view.get_many(CF_HEIGHT_TIME, [height_time_key(height) for height in heights])
        found: dict[int, int] = {}
        for height in heights:
            raw = raws.get(height_time_key(height))
            if raw is not None:
                found[height] = decode_block_time(raw)
        for name, record in list(loaded.items()):
            when = found.get(record.confirmed_index)
            if when is not None and when != record.timestamp:
                loaded[name] = replace(record, timestamp=when)

    async def _add_unspent(self, session: WriteSession, delta: int) -> None:
        raw = await session.get(CF_META, META_UNSPENT)
        current = 0 if raw is None else int.from_bytes(raw, "little", signed=False)
        updated = current + delta
        if updated < 0:
            raise ValueError(f"unspent counter went negative ({updated})")
        session.put(CF_META, META_UNSPENT, updated.to_bytes(8, "little", signed=False))

    async def _merge_spent_ids(
        self,
        session: WriteSession,
        height: int,
        spent: list[tuple[bytes32, bytes32 | None, bytes | None]],
    ) -> None:
        """Remember which coin ids this height spent. The list holds no coin body."""
        key = u32_be(height)
        existing = await session.get(CF_COIN_DELTA, key)
        if existing is None:
            session.put(CF_COIN_DELTA, key, encode_spent_ids(spent))
            return
        session.put(CF_COIN_DELTA, key, encode_spent_ids([*decode_spent_ids(existing), *spent]))

    async def _merge_ff_born(self, session: WriteSession, height: int, names: list[bytes32]) -> None:
        """Coin ids whose fast-forward row was written at this height. Deleted with the one-day window."""
        key = ff_born_key(height)
        existing = await session.get(CF_COIN_DELTA, key)
        prior = [] if existing is None else decode_ff_born(existing)
        session.put(CF_COIN_DELTA, key, encode_ff_born([*prior, *names]))

    def _put_fast_forward(self, session: WriteSession, record: StoredCoin, parent_parent_id: bytes32) -> None:
        """One row for a same-puzzle child, holding the parent ids the mempool rewrites with."""
        session.put(
            CF_FF_UNSPENT,
            bytes(record.puzzle_hash) + bytes(record.coin_name),
            encode_ff_lineage(record.parent, parent_parent_id),
        )
        session.put(CF_COIN_DELTA, singleton_puzzle_key(record.coin_name), bytes(record.puzzle_hash))

    async def index_pending(self, *, limit: int | None = None) -> bool:
        """Nothing is deferred.

        Fast-forward rows and their one-day birth list are written in the block
        that creates the coin. Address keys are not stored.
        """
        del limit
        return False

    async def index_pending_all(self) -> None:
        """Nothing is deferred. Startup does not walk old blocks to fill fast-forward rows."""
        return None

    async def _set_spent(self, coin_names: list[bytes32], index: int) -> None:
        """Mark coins spent at `index`. Opens its own write, matching the SQLite coin store."""
        if len(coin_names) == 0:
            return
        if index <= 0:
            raise ValueError(f"spent height must be positive, got {index}")
        async with self.db.writer_maybe_transaction() as session:
            await self._set_spent_in_session(session, coin_names, index)

    async def _set_spent_in_session(self, session: WriteSession, coin_names: list[bytes32], index: int) -> None:
        raws = await session.get_many(CF_COINS, [bytes(name) for name in coin_names])
        puzzles = await session.get_many(CF_COIN_DELTA, [singleton_puzzle_key(name) for name in coin_names])
        lineage_keys = [
            puzzles[singleton_puzzle_key(name)] + bytes(name)
            for name in coin_names
            if singleton_puzzle_key(name) in puzzles and raws.get(bytes(name)) is not None
        ]
        lineage_values = await session.get_many(CF_FF_UNSPENT, lineage_keys) if len(lineage_keys) != 0 else {}
        updated = 0
        spent_ids: list[tuple[bytes32, bytes32 | None, bytes | None]] = []
        seen: set[bytes32] = set()
        for name in coin_names:
            if name in seen:
                raise ValueError(f"Coin {name.hex()} spent twice at height {index}")
            seen.add(name)
            raw = raws.get(bytes(name))
            if raw is None or len(raw) != HEIGHT_LEN:
                continue
            puzzle = puzzles.get(singleton_puzzle_key(name))
            lineage: bytes | None = None
            if puzzle is not None:
                stored_lineage = lineage_values.get(puzzle + bytes(name))
                if stored_lineage is not None and len(stored_lineage) == FF_LINEAGE_LEN:
                    lineage = stored_lineage
                session.delete(CF_FF_UNSPENT, puzzle + bytes(name))
                session.delete(CF_COIN_DELTA, singleton_puzzle_key(name))
            session.put(CF_COINS, bytes(name), encode_spent_mark(decode_height(raw), index))
            spent_ids.append(spent_lineage_entry(name, puzzle, lineage))
            updated += 1
        if updated != len(coin_names):
            raise ValueError(f"Invalid operation to set spent, total updates {updated} expected {len(coin_names)}")
        await self._add_unspent(session, -updated)
        if spent_ids:
            await self._merge_spent_ids(session, index, spent_ids)

    async def records_for_spends(
        self, spends: Collection[SpendConditions], genesis_challenge: bytes32
    ) -> list[CoinRecord]:
        """Build removal records from the spend plus the short row.

        The unspent row is the creation height. Parent, puzzle, and amount come from
        the spend, and the coin name is the hash of those three. An 8-byte mark means
        the coin is already spent.
        """
        ordered = list(spends)
        if len(ordered) == 0:
            return []
        names = [bytes32(spend.coin_id) for spend in ordered]
        async with self.db.reader_no_transaction() as view:
            raws = await view.get_many(CF_COINS, [bytes(name) for name in names])
            height_of: dict[bytes32, int] = {}
            already_spent: dict[bytes32, int] = {}
            for name in names:
                raw = raws.get(bytes(name))
                if raw is None:
                    continue
                if len(raw) == HEIGHT_LEN:
                    height_of[name] = decode_height(raw)
                elif len(raw) == SPENT_MARK_LEN:
                    created, spent_at = decode_spent_mark(raw)
                    height_of[name] = created
                    already_spent[name] = spent_at
            time_keys = [height_time_key(height) for height in set(height_of.values())]
            time_raw = await view.get_many(CF_HEIGHT_TIME, time_keys) if len(time_keys) != 0 else {}
        records: list[CoinRecord] = []
        for spend, name in zip(ordered, names, strict=True):
            if name not in height_of:
                continue
            height = height_of[name]
            raw_time = time_raw.get(height_time_key(height))
            listed = None if raw_time is None else decode_block_time(raw_time)
            coin = Coin(bytes32(spend.parent_id), bytes32(spend.puzzle_hash), uint64(spend.coin_amount))
            if coin.name() != name:
                continue
            spent_at = already_spent.get(name)
            if spent_at is not None and spent_at > 0:
                records.append(
                    CoinRecord(
                        coin,
                        uint32(height),
                        uint32(spent_at),
                        is_reward_parent(coin.parent_coin_info, genesis_challenge),
                        uint64(0 if listed is None else listed),
                    )
                )
                continue
            timestamp = 0 if listed is None else listed
            created_at = uint32(height)
            records.append(
                CoinRecord(
                    coin,
                    created_at,
                    uint32(0),
                    is_reward_parent(coin.parent_coin_info, genesis_challenge),
                    uint64(timestamp),
                )
            )
        return records

    def has_address_index(self) -> bool:
        return False

    async def get_block_filter(self, height: int) -> bytes | None:
        async with self.db.reader_no_transaction() as view:
            return await view.get(CF_BLOCK_FILTER, height_time_key(height))

    async def get_hint_filter(self, height: int) -> bytes | None:
        async with self.db.reader_no_transaction() as view:
            return await view.get(CF_HINT_FILTER, height_time_key(height))

    async def get_filters_for_heights(self, heights: list[int]) -> dict[int, tuple[bytes | None, bytes | None]]:
        """Committed filter and hint filter for each height. Two reads for the whole page.

        A wallet asks for a short run of heights. One read per height turns that
        into a walk of the chain.
        """
        if len(heights) == 0:
            return {}
        keys = [height_time_key(height) for height in heights]
        async with self.db.reader_no_transaction() as view:
            blocks = await view.get_many(CF_BLOCK_FILTER, keys)
            hints = await view.get_many(CF_HINT_FILTER, keys)
        return {
            height: (blocks.get(key), hints.get(key))
            for height, key in zip(heights, keys, strict=True)
        }

    async def get_coin_record(self, coin_name: bytes32) -> CoinRecord | None:
        found = await self.get_coin_records([coin_name])
        if len(found) == 0:
            return None
        return found[0]

    async def get_coin_records_by_names(
        self,
        include_spent_coins: bool,
        names: list[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2**32) - 1),
    ) -> list[CoinRecord]:
        if len(names) == 0:
            return []
        found: list[CoinRecord] = []
        for record in await self.get_coin_records(names):
            if record.confirmed_block_index < int(start_height) or record.confirmed_block_index >= int(end_height):
                continue
            if not include_spent_coins and record.spent:
                continue
            found.append(record)
        return found

    def set_reward_prefixes(self, prefixes: tuple[bytes, bytes]) -> None:
        """Farmer and pool reward parents are the two halves of the genesis challenge."""
        self._reward_parent_prefixes = prefixes

    def bind_parent_lookup(self, finder: Callable[[bytes32, int], Awaitable[Coin | None]]) -> None:
        """`finder(parent_id, spend_height)` reads the block that spent the parent."""
        self._parent_at_height = finder

    def bind_coin_lookup(self, finder: Callable[[bytes32, int], Awaitable[Coin | None]]) -> None:
        """`finder(coin_id, creation_height)` reads the block that created the coin."""
        self._coin_at_height = finder

    def bind_coins_at_height(self, finder: Callable[[int], Awaitable[Mapping[bytes32, Coin]]]) -> None:
        """One call reads every coin added at that height. Many names from one block share it."""
        self._coins_at_height = finder

    async def _parent_coin(self, parent_id: bytes32, spend_height: int) -> Coin | None:
        finder = self._parent_at_height
        if finder is None or spend_height <= 0:
            return None
        return await finder(parent_id, spend_height)

    async def block_timestamp(self, height: int) -> int | None:
        async with self.db.reader_no_transaction() as view:
            return await self._timestamp_at(view, height)

    async def creation_height(self, coin_name: bytes32) -> int | None:
        """Creation height from the 4-byte row. None when the coin is spent or the row is gone."""
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_COINS, bytes(coin_name))
        if raw is None or len(raw) != HEIGHT_LEN:
            return None
        return decode_height(raw)

    async def spent_mark(self, coin_name: bytes32) -> tuple[int, int] | None:
        """Creation and spend height while the one-day mark is still stored. None after it is deleted."""
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_COINS, bytes(coin_name))
        if raw is None or len(raw) != SPENT_MARK_LEN:
            return None
        return decode_spent_mark(raw)

    async def drop_closed_spends_and_fast_forward(self) -> None:
        """Delete closed-spend rows, fast-forward rows, birth lists, and side keys.

        This does not delete 4-byte spent-id lists or coin rows. New blocks write
        fast-forward rows again, and those rows leave after one day.
        """
        async with self.db.writer_maybe_transaction() as session:
            session.delete_range(CF_CLOSED_SPEND, b"", b"\xff" * 80)
            session.delete_range(CF_FF_UNSPENT, b"", b"\xff" * 80)
            session.delete_range(CF_COIN_DELTA, b"\xfe", b"\xff" * 40)

    async def closed_spend(self, coin_name: bytes32) -> tuple[int, int] | None:
        """Birth and spend height from a closed-spend row, if an older database still has one.

        New blocks do not write this. After the one-day mark is deleted, a question about
        an old block no longer gets these heights from the coin store.
        """
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_CLOSED_SPEND, bytes(coin_name))
        if raw is None or len(raw) != SPENT_MARK_LEN:
            return None
        return decode_spent_mark(raw)

    async def parent_spend_height(self, parent_id: bytes32) -> int | None:
        """Height that spent this parent. None when the parent is unspent or unknown."""
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_COINS, bytes(parent_id))
        if raw is None or len(raw) == HEIGHT_LEN:
            return None
        if len(raw) == SPENT_MARK_LEN:
            _created, spent = decode_spent_mark(raw)
            return spent
        stored = decode_coin(parent_id, raw)
        if stored.spent_index <= 0:
            return None
        return stored.spent_index

    async def _coin_from_block(self, name: bytes32, height: int) -> Coin | None:
        """The coin added at `height`, read after the row lookup has been released."""
        finder = self._coins_at_height
        if finder is not None:
            coin = (await finder(height)).get(name)
            if coin is not None:
                return coin if coin.name() == name else None
        one = self._coin_at_height
        if one is None:
            return None
        coin = await one(name, height)
        if coin is None or coin.name() != name:
            return None
        return coin

    async def _observed_rows_held(self, observed: dict[tuple[str, bytes], bytes | None]) -> bool:
        """True when every row we just based an answer on is still the same bytes."""
        if len(observed) == 0:
            return True
        by_family: dict[str, list[bytes]] = {}
        for (family, key), _row in observed.items():
            by_family.setdefault(family, []).append(key)
        async with self.db.reader_no_transaction() as view:
            for family, keys in by_family.items():
                found = await view.get_many(family, keys)
                for key in keys:
                    if found.get(key) != observed[family, key]:
                        return False
        return True

    async def _assemble_coin_records(
        self, ordered: list[bytes32]
    ) -> tuple[list[CoinRecord], dict[tuple[str, bytes], bytes | None]]:
        short: list[tuple[bytes32, int, int]] = []
        observed: dict[tuple[str, bytes], bytes | None] = {}
        async with self.db.reader_no_transaction() as view:
            raws = await view.get_many(CF_COINS, [bytes(name) for name in ordered])
            loaded: dict[bytes32, StoredCoin] = {}
            heights: set[int] = set()
            for name in ordered:
                raw = raws.get(bytes(name))
                observed[CF_COINS, bytes(name)] = raw
                if raw is None:
                    continue
                if len(raw) == HEIGHT_LEN:
                    created = decode_height(raw)
                    short.append((name, created, 0))
                    heights.add(created)
                elif len(raw) == SPENT_MARK_LEN:
                    created, spent = decode_spent_mark(raw)
                    short.append((name, created, spent))
                    heights.add(created)
                elif name not in loaded:
                    loaded[name] = decode_coin(name, raw)
            await self._stamp_list_times(view, loaded)
            times: dict[int, int] = {}
            if len(heights) != 0:
                time_keys = [height_time_key(height) for height in heights]
                time_raw = await view.get_many(CF_HEIGHT_TIME, time_keys)
                for height in heights:
                    key = height_time_key(height)
                    raw_time = time_raw.get(key)
                    observed[CF_HEIGHT_TIME, key] = raw_time
                    if raw_time is not None:
                        times[height] = decode_block_time(raw_time)
        # The block read must not run while this reader is open. A lookup uses the same database.
        by_height: dict[int, Mapping[bytes32, Coin]] = {}
        if self._coins_at_height is not None:
            for _name, created, _spent in short:
                if created not in by_height:
                    by_height[created] = await self._coins_at_height(created)
        built: dict[bytes32, CoinRecord] = {name: _to_record(stored) for name, stored in loaded.items()}
        seen_short: set[bytes32] = set()
        for name, created, spent in short:
            if name in seen_short:
                continue
            seen_short.add(name)
            coin = by_height.get(created, {}).get(name)
            if coin is None or coin.name() != name:
                coin = await self._coin_from_block(name, created)
            if coin is None:
                continue
            built[name] = CoinRecord(
                coin,
                uint32(created),
                uint32(spent),
                self._reward_parent_height(coin.parent_coin_info) is not None,
                uint64(times.get(created, 0)),
            )
        return [built[name] for name in ordered if name in built], observed

    async def get_coin_records(self, names: Collection[bytes32]) -> list[CoinRecord]:
        """Records for these names, in the order asked. A missing name is left out.

        A 4-byte row and an 8-byte mark do not store the coin body. When a block
        lookup is bound, the body is read from the block that created the coin.
        One read covers every name born at that height. A repeated name is repeated
        in the result. Without a lookup, a short row cannot become a record.

        The row is read again after the block. A fork that changed the row between
        those reads starts the lookup over, so a spent coin is not reported as unspent.
        """
        ordered = list(names)
        if len(ordered) == 0:
            return []
        for _attempt in range(_CONSISTENT_READS):
            records, observed = await self._assemble_coin_records(ordered)
            if await self._observed_rows_held(observed):
                return records
        return []

    async def get_coins_added_at_height(self, height: uint32) -> list[CoinRecord]:
        added, _removed = await self._stable_height_coins(int(height))
        return [_to_record(record) for record in added]

    async def get_coins_removed_at_height(self, height: uint32) -> list[CoinRecord]:
        if height == 0:
            return []
        _added, removed = await self._stable_height_coins(int(height))
        return [_to_record(record) for record in removed]

    async def get_additions_and_removals_at_height(self, height: uint32) -> tuple[list[CoinRecord], list[CoinRecord]]:
        added, removed = await self._stable_height_coins(int(height))
        if height == 0:
            removed = []
        return [_to_record(record) for record in added], [_to_record(record) for record in removed]

    async def _stable_height_coins(self, height: int) -> tuple[list[StoredCoin], list[StoredCoin]]:
        """Additions and removals are not copied into the coin table. The block has them."""
        del height
        return [], []

    async def undo_floor(self) -> int | None:
        """Highest height whose spent marks have been deleted. None until the window fills."""
        async with self.db.reader_no_transaction() as view:
            raw = await view.get(CF_META, META_UNDO_FLOOR)
        if raw is None:
            return None
        return int.from_bytes(raw, "big", signed=False)

    async def _forget_undo_at(self, session: WriteSession, height: int) -> None:
        """Delete the 8-byte marks for coins spent at this height, and fast-forward rows born here.

        An unspent coin keeps its 4-byte creation height. A spent coin is removed.
        A second spend finds no row. A fast-forward row is kept only for this same window.
        """
        spent_key = u32_be(height)
        born_key = ff_born_key(height)
        found = await session.get_many(CF_COIN_DELTA, [spent_key, born_key])
        raw = found.get(spent_key)
        if raw is not None:
            spent = decode_spent_ids(raw)
            names = [bytes(name) for name, _puzzle, _lineage in spent]
            rows = await session.get_many(CF_COINS, names) if len(names) != 0 else {}
            for name, _puzzle, _lineage in spent:
                row = rows.get(bytes(name))
                if row is None or len(row) != SPENT_MARK_LEN:
                    continue
                _created, spent_at = decode_spent_mark(row)
                if spent_at != height:
                    continue
                session.delete(CF_COINS, bytes(name))
            session.delete(CF_COIN_DELTA, spent_key)
        await self._expire_fast_forward_born_at(session, height, found.get(born_key))
        session.put(CF_META, META_UNDO_FLOOR, int(height).to_bytes(4, "big", signed=False))

    async def _expire_fast_forward_born_at(self, session: WriteSession, height: int, raw: bytes | None) -> None:
        """Drop fast-forward rows created at `height`. The unspent coin row stays.

        `raw` is the birth list already read with the spent-id list. A missing list
        is None and is not read again.
        """
        key = ff_born_key(height)
        if raw is None:
            return
        names = decode_ff_born(raw)
        side_keys = [singleton_puzzle_key(name) for name in names]
        puzzles = await session.get_many(CF_COIN_DELTA, side_keys) if len(side_keys) != 0 else {}
        for name in names:
            puzzle = puzzles.get(singleton_puzzle_key(name))
            if puzzle is None:
                continue
            session.delete(CF_FF_UNSPENT, puzzle + bytes(name))
            session.delete(CF_COIN_DELTA, singleton_puzzle_key(name))
        session.delete(CF_COIN_DELTA, key)

    async def rollback_to_block(
        self,
        block_index: int,
        *,
        created_coins: Collection[Coin] = (),
        removed_coins: Collection[Coin] = (),
    ) -> dict[bytes32, CoinRecord]:
        """Undo spends from the id list, and delete coins the caller read from the abandoned blocks.

        ``created_coins`` and ``removed_coins`` come from those stored blocks. The spent-id list
        has no coin body, so a reorg cannot invent one.
        """
        changes: dict[bytes32, CoinRecord] = {}
        removed_by_name = {coin.name(): coin for coin in removed_coins}
        async with self.db.writer_maybe_transaction() as session:
            floor_raw = await session.get(CF_META, META_UNDO_FLOOR)
            floor: int | None = None
            if floor_raw is not None:
                floor = int.from_bytes(floor_raw, "big", signed=False)
                if block_index < floor:
                    raise UndoWindowExceeded(block_index, floor)
            unspent_delta = 0
            start = b"\x00\x00\x00\x00" if block_index < 0 else u32_be(block_index + 1)
            # Stop before singleton side keys. Those are not spent-id lists.
            rows = await session.scan(CF_COIN_DELTA, start, spent_id_scan_end())
            spent_lists = [(key, value) for key, value in rows if len(key) == 4]
            born_lists = [(key, value) for key, value in rows if is_ff_born_key(key) and ff_born_height(key) > block_index]
            spent_names = [name for _key, value in spent_lists for name, _puzzle, _lineage in decode_spent_ids(value)]
            coin_rows = (
                await session.get_many(CF_COINS, [bytes(name) for name in spent_names]) if len(spent_names) != 0 else {}
            )
            restored_for_caller: list[tuple[bytes32, Coin, int]] = []
            for key, value in reversed(spent_lists):
                for name, puzzle, lineage in decode_spent_ids(value):
                    raw = coin_rows.get(bytes(name))
                    if raw is None or len(raw) != SPENT_MARK_LEN:
                        continue
                    created, _spent_at = decode_spent_mark(raw)
                    restored = encode_height(created)
                    session.put(CF_COINS, bytes(name), restored)
                    coin_rows[bytes(name)] = restored
                    session.delete(CF_CLOSED_SPEND, bytes(name))
                    # A puzzle without the 64 lineage bytes is not a fast-forward row.
                    # Writing an empty value here would hide the coin from lookup and
                    # would never expire. The birth list is what deletes a restored
                    # row. Once that list has aged out, putting the row back would
                    # leave it in the table for as long as the coin stays unspent.
                    if (
                        puzzle is not None
                        and lineage is not None
                        and len(lineage) == FF_LINEAGE_LEN
                        and (floor is None or created > floor)
                    ):
                        session.put(CF_FF_UNSPENT, bytes(puzzle) + bytes(name), lineage)
                        session.put(CF_COIN_DELTA, singleton_puzzle_key(name), bytes(puzzle))
                    unspent_delta += 1
                    coin = removed_by_name.get(name)
                    if coin is None:
                        continue
                    restored_for_caller.append((name, coin, created))
                session.delete(CF_COIN_DELTA, key)
            if len(restored_for_caller) != 0:
                time_keys = [height_time_key(created) for _name, _coin, created in restored_for_caller]
                time_rows = await session.get_many(CF_HEIGHT_TIME, list(dict.fromkeys(time_keys)))
                for name, coin, created in restored_for_caller:
                    raw_time = time_rows.get(height_time_key(created))
                    listed = None if raw_time is None else decode_block_time(raw_time)
                    changes[name] = CoinRecord(
                        coin,
                        uint32(created),
                        uint32(0),
                        self._reward_parent_height(coin.parent_coin_info) is not None,
                        uint64(0 if listed is None else listed),
                    )
            born_names = [name for _key, value in born_lists for name in decode_ff_born(value)]
            born_side_keys = [singleton_puzzle_key(name) for name in born_names]
            born_puzzles = (
                await session.get_many(CF_COIN_DELTA, born_side_keys) if len(born_side_keys) != 0 else {}
            )
            if len(born_names) != 0:
                coin_rows.update(await session.get_many(CF_COINS, [bytes(name) for name in born_names]))
            for key, value in born_lists:
                # These coins were created above the fork. The birth list is the
                # record of that, so the coin row goes with the fast-forward row.
                for name in decode_ff_born(value):
                    puzzle = born_puzzles.get(singleton_puzzle_key(name))
                    if puzzle is not None:
                        session.delete(CF_FF_UNSPENT, puzzle + bytes(name))
                    session.delete(CF_COIN_DELTA, singleton_puzzle_key(name))
                    raw = coin_rows.get(bytes(name))
                    if raw is not None:
                        session.delete(CF_COINS, bytes(name))
                        session.delete(CF_CLOSED_SPEND, bytes(name))
                        coin_rows[bytes(name)] = None
                        if len(raw) == HEIGHT_LEN:
                            unspent_delta -= 1
                session.delete(CF_COIN_DELTA, key)
            created_keys = [bytes(coin.name()) for coin in created_coins]
            created_rows = (
                await session.get_many(CF_COINS, created_keys) if len(created_keys) != 0 else {}
            )
            for coin in created_coins:
                name = coin.name()
                raw = created_rows.get(bytes(name))
                session.delete(CF_COINS, bytes(name))
                session.delete(CF_CLOSED_SPEND, bytes(name))
                session.delete(CF_FF_UNSPENT, bytes(coin.puzzle_hash) + bytes(name))
                session.delete(CF_COIN_DELTA, singleton_puzzle_key(name))
                created_rows[bytes(name)] = None
                if raw is not None and len(raw) == HEIGHT_LEN:
                    unspent_delta -= 1
                changes[name] = CoinRecord(
                    coin,
                    uint32(0),
                    uint32(0),
                    self._reward_parent_height(coin.parent_coin_info) is not None,
                    uint64(0),
                )
            if unspent_delta != 0:
                await self._add_unspent(session, unspent_delta)
            abandoned = b"\xff\xff\xff\xff\x00"
            session.delete_range(CF_HEIGHT_TIME, start, abandoned)
            session.delete_range(CF_BLOCK_FILTER, start, abandoned)
            session.delete_range(CF_HINT_FILTER, start, abandoned)
        return changes

    async def _pending_journals(self, view: _View) -> list[tuple[list[StoredCoin], list[tuple[bytes32, int]]]]:
        """No full-coin journal is written, so puzzle-hash lookup stays empty.

        The one-day fast-forward birth list is a different key. It names coin ids,
        not coin bodies, and this function does not read it.
        """
        del view
        return []

    async def _pending_coins(
        self,
        view: _View,
        journals: list[tuple[list[StoredCoin], list[tuple[bytes32, int]]]],
        *,
        puzzle_hash: bytes32 | None = None,
        parent: bytes32 | None = None,
        include_removals: bool = False,
    ) -> list[StoredCoin]:
        """Current records for coins whose lookup keys are not written yet."""
        ordered: list[bytes32] = []
        seen: set[bytes32] = set()
        for added, removed in journals:
            for record in added:
                if puzzle_hash is not None and record.puzzle_hash != puzzle_hash:
                    continue
                if parent is not None and record.parent != parent:
                    continue
                if record.coin_name in seen:
                    continue
                seen.add(record.coin_name)
                ordered.append(record.coin_name)
            if not include_removals or parent is not None:
                continue
            for name, _previous in removed:
                if name in seen:
                    continue
                seen.add(name)
                ordered.append(name)
        loaded = await self._load_many(view, ordered)
        found: list[StoredCoin] = []
        for name in ordered:
            stored = loaded.get(name)
            if stored is None:
                continue
            if puzzle_hash is not None and stored.puzzle_hash != puzzle_hash:
                continue
            if parent is not None and stored.parent != parent:
                continue
            found.append(stored)
        return found

    async def get_coin_records_by_puzzle_hash(
        self,
        include_spent_coins: bool,
        puzzle_hash: bytes32,
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2**32) - 1),
    ) -> list[CoinRecord]:
        return await self.get_coin_records_by_puzzle_hashes(
            include_spent_coins, [puzzle_hash], start_height, end_height
        )

    async def get_coin_records_by_puzzle_hashes(
        self,
        include_spent_coins: bool,
        puzzle_hashes: list[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2**32) - 1),
    ) -> list[CoinRecord]:
        found: dict[bytes32, CoinRecord] = {}
        async with self.db.reader_no_transaction() as view:
            journals = await self._pending_journals(view)
            for puzzle_hash in puzzle_hashes:
                pending = await self._pending_coins(view, journals, puzzle_hash=puzzle_hash)
                for stored in pending:
                    if stored.confirmed_index < int(start_height) or stored.confirmed_index >= int(end_height):
                        continue
                    if not include_spent_coins and stored.spent_index > 0:
                        continue
                    found[stored.coin_name] = _to_record(stored)
        return list(found.values())

    async def get_coin_records_by_parent_ids(
        self,
        include_spent_coins: bool,
        parent_ids: list[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2**32) - 1),
        *,
        max_items: int = 50000,
    ) -> list[CoinRecord]:
        if max_items <= 0 or len(parent_ids) == 0:
            return []
        # A child is created in the block that spends its parent. A farmer or pool
        # reward is not. Its parent id names the block that earned it, and a later
        # transaction block is the one that adds the coin.
        found: list[CoinRecord] = []
        seen: set[bytes32] = set()
        added_at: dict[int, list[StoredCoin]] = {}
        async with self.db.reader_no_transaction() as view:
            parents = await view.get_many(CF_COINS, [bytes(parent_id) for parent_id in parent_ids])
            for parent_id in parent_ids:
                raw = parents.get(bytes(parent_id))
                if raw is None:
                    earned = self._reward_parent_height(parent_id)
                    if earned is None:
                        continue
                    for stored in await self._reward_coins_included_after(view, parent_id, earned):
                        if stored.coin_name in seen:
                            continue
                        if stored.confirmed_index < int(start_height) or stored.confirmed_index >= int(end_height):
                            continue
                        if not include_spent_coins and stored.spent_index > 0:
                            continue
                        seen.add(stored.coin_name)
                        found.append(_to_record(stored))
                        if len(found) >= max_items:
                            return found
                    continue
                elif len(raw) == HEIGHT_LEN:
                    continue
                elif len(raw) == SPENT_MARK_LEN:
                    _created, height = decode_spent_mark(raw)
                else:
                    parent = decode_coin(parent_id, raw)
                    if parent.spent_index <= 0:
                        continue
                    height = parent.spent_index
                if height < int(start_height) or height >= int(end_height):
                    continue
                if height not in added_at:
                    added_at[height] = (await self._stable_height_coins(height))[0]
                matches = [stored for stored in added_at[height] if stored.parent == parent_id]
                matches.sort(key=lambda stored: (stored.confirmed_index, bytes(stored.coin_name)))
                for stored in matches:
                    if stored.coin_name in seen:
                        continue
                    if stored.confirmed_index < int(start_height) or stored.confirmed_index >= int(end_height):
                        continue
                    if not include_spent_coins and stored.spent_index > 0:
                        continue
                    seen.add(stored.coin_name)
                    found.append(_to_record(stored))
                    if len(found) >= max_items:
                        return found
        return found

    async def _reward_coins_included_after(
        self, view: _View, parent_id: bytes32, earned_height: int
    ) -> list[StoredCoin]:
        """Reward coins are in the block that added them. This table does not keep a second copy."""
        del view, parent_id, earned_height
        return []

    async def _append_capped_states(
        self,
        view: _View,
        names: list[bytes32],
        states: set[CoinState],
        min_height: int,
        include_spent_coins: bool,
        max_items: int,
    ) -> None:
        """Load coin records in cap-sized windows and stop once the cap is full."""
        cursor = 0
        while cursor < len(names) and len(states) < max_items:
            room = max_items - len(states)
            window = names[cursor : cursor + room]
            cursor += len(window)
            loaded = await self._load_many(view, window)
            for name in window:
                stored = loaded.get(name)
                if stored is None:
                    continue
                if not self._state_visible(stored, min_height, include_spent_coins):
                    continue
                states.add(_to_state(stored))
                if len(states) >= max_items:
                    return

    async def get_coin_states_by_puzzle_hashes(
        self,
        include_spent_coins: bool,
        puzzle_hashes: set[bytes32],
        min_height: uint32 = uint32(0),
        *,
        max_items: int = 50000,
    ) -> set[CoinState]:
        if max_items <= 0 or len(puzzle_hashes) == 0:
            return set()
        states: set[CoinState] = set()
        async with self.db.reader_no_transaction() as view:
            journals = await self._pending_journals(view)
            for puzzle_hash in puzzle_hashes:
                if not journals:
                    continue
                pending = await self._pending_coins(
                    view, journals, puzzle_hash=puzzle_hash, include_removals=include_spent_coins
                )
                seen: set[bytes32] = set()
                for stored in pending:
                    if stored.coin_name in seen:
                        continue
                    seen.add(stored.coin_name)
                    if not self._state_visible(stored, int(min_height), include_spent_coins):
                        continue
                    states.add(_to_state(stored))
                    if len(states) >= max_items:
                        return states
        return states

    async def get_coin_states_by_ids(
        self,
        include_spent_coins: bool,
        coin_ids: Collection[bytes32],
        min_height: uint32 = uint32(0),
        *,
        max_height: uint32 = uint32.MAXIMUM,
        max_items: int = 50000,
    ) -> list[CoinState]:
        """Coin states for these ids. A short row is rebuilt from the block that created it.

        The old reader skipped 4-byte and 8-byte rows, so a wallet asking by coin id
        was told the coin did not exist unless the caller tried a second lookup.
        """
        if max_items <= 0 or len(coin_ids) == 0:
            return []
        states: list[CoinState] = []
        limit_height = max_height != uint32.MAXIMUM
        for record in await self.get_coin_records(list(coin_ids)):
            created = int(record.confirmed_block_index)
            spent = int(record.spent_block_index)
            if created < int(min_height) and spent < int(min_height):
                continue
            if limit_height and (created > int(max_height) or spent > int(max_height)):
                continue
            if not include_spent_coins and record.spent:
                continue
            states.append(record.coin_state)
            if len(states) >= max_items:
                break
        return states

    async def _lineage_once(self, puzzle_hash: bytes32) -> tuple[UnspentLineageInfo | None, bool]:
        """The current singleton for this puzzle, and whether the rows still match that answer."""
        end = prefix_end(bytes(puzzle_hash))
        async with self.db.reader_no_transaction() as view:
            rows = await view.scan(CF_FF_UNSPENT, bytes(puzzle_hash), end)
            names = _coin_names(rows)
            raws = await view.get_many(CF_COINS, [bytes(name) for name in names]) if len(names) != 0 else {}
        observed: dict[tuple[str, bytes], bytes | None] = {
            (CF_COINS, bytes(name)): raws.get(bytes(name)) for name in names
        }
        matches: dict[bytes32, UnspentLineageInfo] = {}
        for key, value in rows:
            parsed = decode_ff_lineage(value)
            if parsed is None or len(key) < 32:
                continue
            name = bytes32(key[-32:])
            raw = raws.get(bytes(name))
            # A spent mark or a missing row is not the current coin. An empty legacy value is ignored.
            if raw is None or len(raw) != HEIGHT_LEN:
                continue
            parent_id, parent_parent_id = parsed
            matches[name] = UnspentLineageInfo(
                coin_id=name,
                parent_id=parent_id,
                parent_parent_id=parent_parent_id,
            )
        async with self.db.reader_no_transaction() as view:
            rows_again = await view.scan(CF_FF_UNSPENT, bytes(puzzle_hash), end)
        if set(_coin_names(rows_again)) != set(names):
            return None, False
        stable = await self._observed_rows_held(observed)
        if not stable:
            return None, False
        found = list(matches.values())
        if len(found) != 1:
            log.debug("Expected 1 unspent with puzzle hash %s, but found %s", puzzle_hash.hex(), len(found))
            return None, True
        return found[0], True

    async def get_unspent_lineage_info_for_puzzle_hash(self, puzzle_hash: bytes32) -> UnspentLineageInfo | None:
        """The one unspent singleton with this puzzle. None when there is not exactly one.

        The fast-forward value holds the parent ids written when the coin was created.
        The coin row is read again so a spend that landed in between is not reported.
        A row older than the one-day window has been deleted. This does not open a block.
        """
        for _attempt in range(_CONSISTENT_READS):
            answer, stable = await self._lineage_once(puzzle_hash)
            if stable:
                return answer
        return None

    @staticmethod
    def _state_visible(stored: StoredCoin, min_height: int, include_spent_coins: bool) -> bool:
        if stored.confirmed_index < min_height and stored.spent_index < min_height:
            return False
        return include_spent_coins or stored.spent_index <= 0

    async def is_empty(self) -> bool:
        """True when the coin table has no row.

        One key is enough. Loading the table is what a corrupt database (coins
        present, no peak) would do on startup, and that table is the whole
        unspent set.
        """
        async with self.db.reader_no_transaction() as view:
            rows = await view.scan(CF_COINS, b"", None, limit=1)
        return len(rows) == 0

    async def batch_coin_states_by_puzzle_hashes(
        self,
        puzzle_hashes: list[bytes32],
        *,
        min_height: uint32 = uint32(0),
        include_spent: bool = True,
        include_unspent: bool = True,
        include_hinted: bool = True,
        min_amount: uint64 = uint64(0),
        max_items: int = 50000,
    ) -> tuple[list[CoinState], uint32 | None]:
        if len(puzzle_hashes) == 0 or (not include_spent and not include_unspent):
            return [], None
        # Address and hint lists are not stored. A wallet reads the block, or the
        # peak update, for the coins it watches. This query stays empty.
        del include_hinted
        by_name: dict[bytes32, CoinState] = {}
        async with self.db.reader_no_transaction() as view:
            journals = await self._pending_journals(view)
            if journals:
                for puzzle_hash in puzzle_hashes:
                    pending = await self._pending_coins(
                        view, journals, puzzle_hash=puzzle_hash, include_removals=include_spent
                    )
                    _keep_batch_coins(
                        pending,
                        by_name,
                        int(min_height),
                        include_spent,
                        include_unspent,
                        int(min_amount),
                        None,
                    )
        ordered = sorted(by_name.values(), key=_order_height)
        return _paginate(ordered, max_items)


def _keep_batch_coins(
    records: list[StoredCoin],
    by_name: dict[bytes32, CoinState],
    min_height: int,
    include_spent: bool,
    include_unspent: bool,
    min_amount: int,
    closed_before: int | None,
) -> None:
    for stored in records:
        if stored.coin_name in by_name:
            continue
        if not _batch_accepts(stored, min_height, include_spent, include_unspent, min_amount):
            continue
        state = _to_state(stored)
        if closed_before is not None and _order_height(state) >= closed_before:
            continue
        by_name[stored.coin_name] = state


def _order_height(state: CoinState) -> int:
    created = int(state.created_height or 0)
    spent = int(state.spent_height or 0)
    return max(created, spent)


def _batch_accepts(
    stored: StoredCoin, min_height: int, include_spent: bool, include_unspent: bool, min_amount: int
) -> bool:
    if stored.amount < min_amount:
        return False
    spent = stored.spent_index > 0
    if spent and not include_spent:
        return False
    if not spent and not include_unspent:
        return False
    if stored.confirmed_index < min_height and stored.spent_index < min_height:
        return False
    return True


def _coin_names(rows: list[tuple[bytes, bytes]]) -> list[bytes32]:
    return [bytes32(key[-32:]) for key, _value in rows]


def _unique_names(groups: list[list[tuple[bytes, bytes]]]) -> list[bytes32]:
    names: list[bytes32] = []
    seen: set[bytes32] = set()
    for rows in groups:
        for name in _coin_names(rows):
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
    return names


def _paginate(ordered: list[CoinState], max_items: int) -> tuple[list[CoinState], uint32 | None]:
    if len(ordered) <= max_items:
        return ordered, None
    ordered = ordered[: max_items + 1]
    next_state = ordered.pop()
    next_height = uint32(_order_height(next_state))
    while ordered:
        if uint32(_order_height(ordered[-1])) != next_height:
            break
        ordered.pop()
    return ordered, next_height


def _with_live_spend(record: StoredCoin, raw: bytes | None) -> StoredCoin:
    """The journal remembers the coin at birth. The row knows whether it was spent later."""
    if raw is None or len(raw) == HEIGHT_LEN:
        return record
    if len(raw) == SPENT_MARK_LEN:
        _created, spent = decode_spent_mark(raw)
        if spent == record.spent_index:
            return record
        return replace(record, spent_index=spent)
    current = decode_coin(record.coin_name, raw)
    if current.spent_index == record.spent_index:
        return record
    return replace(record, spent_index=current.spent_index)


def _to_record(stored: StoredCoin, listed_time: int | None = None) -> CoinRecord:
    when = stored.timestamp if listed_time is None else listed_time
    spent = uint32(0) if stored.spent_index <= 0 else uint32(stored.spent_index)
    return CoinRecord(stored.coin(), uint32(stored.confirmed_index), spent, stored.coinbase, uint64(when))


def _to_state(stored: StoredCoin) -> CoinState:
    spent_h = None if stored.spent_index <= 0 else uint32(stored.spent_index)
    return CoinState(stored.coin(), spent_h, uint32(stored.confirmed_index))
