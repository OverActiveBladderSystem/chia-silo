from __future__ import annotations

from dataclasses import dataclass

from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64

from chia.full_node.db.keys import CF_COINS
from chia.types.blockchain_format.coin import Coin

_RECORD_LEN = 4 + 8 + 1 + 32 + 32 + 8 + 8
HEIGHT_LEN = 4
# A spent coin is these 8 bytes for one day: creation height, then spend height. Then the row is deleted.
SPENT_MARK_LEN = 8


@dataclass(frozen=True)
class StoredCoin:
    coin_name: bytes32
    confirmed_index: int
    spent_index: int
    coinbase: bool
    puzzle_hash: bytes32
    parent: bytes32
    amount: uint64
    timestamp: int

    def coin(self) -> Coin:
        return Coin(self.parent, self.puzzle_hash, self.amount)


def encode_height(height: int) -> bytes:
    if height < 0 or height > 0xFFFFFFFF:
        raise ValueError(f"coin height {height} does not fit in 4 bytes")
    return int(height).to_bytes(HEIGHT_LEN, "little", signed=False)


def decode_height(raw: bytes) -> int:
    if len(raw) != HEIGHT_LEN:
        raise ValueError(f"unspent coin row is {len(raw)} bytes")
    return int.from_bytes(raw, "little", signed=False)


def encode_spent_mark(created_height: int, spent_height: int) -> bytes:
    """Creation height and spend height. The coin body stays in the block."""
    return encode_height(created_height) + encode_height(spent_height)


def decode_spent_mark(raw: bytes) -> tuple[int, int]:
    if len(raw) != SPENT_MARK_LEN:
        raise ValueError(f"spent mark is {len(raw)} bytes")
    return decode_height(raw[:HEIGHT_LEN]), decode_height(raw[HEIGHT_LEN:])


_SPENT_IDS_MAGIC = b"CS1"
# Sorts after every 4-byte height key the chain will use. A height scan stops here
# so it does not read singleton side keys.
_SINGLETON_MARK = b"\xff"


def singleton_puzzle_key(name: bytes32) -> bytes:
    """Side key for a singleton's puzzle hash. Not a birth list and not a height key."""
    return _SINGLETON_MARK + bytes(name)


def spent_id_scan_end() -> bytes:
    """Exclusive end of the 4-byte spent-id keys. Singleton side keys start here."""
    return _SINGLETON_MARK


# parent coin id, then that parent's parent. Written on a fast-forward row so a
# later spend does not open the birth block.
FF_LINEAGE_LEN = 64
# One-day list of fast-forward coin ids created at a height. Sorts before singleton
# side keys and is not a 4-byte spent-id key.
_FF_BORN_MARK = b"\xfe"


def encode_ff_lineage(parent_id: bytes32, parent_parent_id: bytes32) -> bytes:
    return bytes(parent_id) + bytes(parent_parent_id)


def decode_ff_lineage(raw: bytes) -> tuple[bytes32, bytes32] | None:
    if len(raw) != FF_LINEAGE_LEN:
        return None
    return bytes32(raw[:32]), bytes32(raw[32:])


def ff_born_key(height: int) -> bytes:
    if height < 0 or height > 0xFFFFFFFF:
        raise ValueError(f"fast-forward birth height {height} does not fit")
    return _FF_BORN_MARK + int(height).to_bytes(4, "big", signed=False)


def encode_ff_born(names: list[bytes32]) -> bytes:
    return b"".join(bytes(name) for name in names)


def decode_ff_born(raw: bytes) -> list[bytes32]:
    if len(raw) % 32 != 0:
        raise ValueError("fast-forward birth list is truncated")
    return [bytes32(raw[index : index + 32]) for index in range(0, len(raw), 32)]


def is_ff_born_key(key: bytes) -> bool:
    return len(key) == 5 and key[:1] == _FF_BORN_MARK


def ff_born_height(key: bytes) -> int:
    if not is_ff_born_key(key):
        raise ValueError("not a fast-forward birth key")
    return int.from_bytes(key[1:], "big", signed=False)


def spent_lineage_entry(
    name: bytes32, puzzle: bytes | None, lineage: bytes | None
) -> tuple[bytes32, bytes32 | None, bytes | None]:
    """The spent-id record a reorg can restore.

    The puzzle and the 64 lineage bytes are kept together. A puzzle without those
    bytes is not a fast-forward row, so it is stored as the coin id alone.
    """
    if puzzle is not None and lineage is not None and len(lineage) == FF_LINEAGE_LEN:
        return name, bytes32(puzzle), lineage
    return name, None, None


def encode_spent_ids(entries: list[tuple[bytes32, bytes32 | None, bytes | None]]) -> bytes:
    """Coin ids spent at one height, so those marks can be deleted a day later.

    A puzzle hash is stored only together with the 64 lineage bytes, so a reorg
    can put that fast-forward row back. A puzzle without those bytes is stored
    as the coin id alone. Older lists may still contain a puzzle-only entry.
    """
    parts = [_SPENT_IDS_MAGIC, len(entries).to_bytes(4, "big", signed=False)]
    for name, puzzle, lineage in entries:
        kept_name, kept_puzzle, kept_lineage = spent_lineage_entry(name, puzzle, lineage)
        parts.append(bytes(kept_name))
        if kept_puzzle is None or kept_lineage is None:
            parts.append(b"\x00")
            continue
        parts.append(b"\x02" + bytes(kept_puzzle) + kept_lineage)
    return b"".join(parts)


def decode_spent_ids(raw: bytes) -> list[tuple[bytes32, bytes32 | None, bytes | None]]:
    if len(raw) < 7 or raw[:3] != _SPENT_IDS_MAGIC:
        raise ValueError("spent-id list has the wrong header")
    count = int.from_bytes(raw[3:7], "big", signed=False)
    offset = 7
    found: list[tuple[bytes32, bytes32 | None, bytes | None]] = []
    for _ in range(count):
        if offset + 33 > len(raw):
            raise ValueError("spent-id list is truncated")
        name = bytes32(raw[offset : offset + 32])
        offset += 32
        flag = raw[offset]
        offset += 1
        if flag == 0:
            found.append((name, None, None))
            continue
        if flag == 1:
            if offset + 32 > len(raw):
                raise ValueError("spent-id list has a bad singleton mark")
            found.append((name, bytes32(raw[offset : offset + 32]), None))
            offset += 32
            continue
        if flag != 2 or offset + 32 + FF_LINEAGE_LEN > len(raw):
            raise ValueError("spent-id list has a bad singleton mark")
        puzzle = bytes32(raw[offset : offset + 32])
        offset += 32
        lineage = raw[offset : offset + FF_LINEAGE_LEN]
        offset += FF_LINEAGE_LEN
        found.append((name, puzzle, lineage))
    if offset != len(raw):
        raise ValueError("spent-id list has trailing bytes")
    return found


def encode_row(record: StoredCoin) -> bytes:
    """An unspent coin is its creation height. A spent coin is the 8-byte mark."""
    if record.spent_index <= 0:
        return encode_height(record.confirmed_index)
    return encode_spent_mark(record.confirmed_index, record.spent_index)


def encode_coin(record: StoredCoin) -> bytes:
    return (
        int(record.confirmed_index).to_bytes(4, "little", signed=False)
        + int(record.spent_index).to_bytes(8, "little", signed=True)
        + bytes([1 if record.coinbase else 0])
        + bytes(record.puzzle_hash)
        + bytes(record.parent)
        + int(record.amount).to_bytes(8, "big", signed=False)
        + int(record.timestamp).to_bytes(8, "little", signed=False)
    )


def decode_coin(coin_name: bytes32, raw: bytes) -> StoredCoin:
    if len(raw) != _RECORD_LEN:
        raise ValueError(f"coin record for {coin_name.hex()} is {len(raw)} bytes")
    confirmed = int.from_bytes(raw[0:4], "little", signed=False)
    spent = int.from_bytes(raw[4:12], "little", signed=True)
    coinbase = raw[12] == 1
    puzzle_hash = bytes32(raw[13:45])
    parent = bytes32(raw[45:77])
    amount = uint64(int.from_bytes(raw[77:85], "big", signed=False))
    timestamp = int.from_bytes(raw[85:93], "little", signed=False)
    return StoredCoin(coin_name, confirmed, spent, coinbase, puzzle_hash, parent, amount, timestamp)


def index_entries(record: StoredCoin) -> list[tuple[str, bytes]]:
    """The coin row only.

    A fast-forward row is not derived from this record. It needs the parent's
    parent, which is not stored here. An empty key would never expire.
    """
    return [(CF_COINS, bytes(record.coin_name))]


def lookup_entries(record: StoredCoin) -> list[tuple[str, bytes]]:
    """No lookup key is derived from a stored coin.

    Address keys are not stored. A fast-forward row is written with its lineage
    when the coin is created, not as an empty key from this record.
    """
    del record
    return []


_DELTA_MAGIC = b"CD1"


def encode_delta(added: list[StoredCoin], removed: list[tuple[bytes32, int]]) -> bytes:
    parts = [_DELTA_MAGIC, len(added).to_bytes(4, "big", signed=False)]
    for record in added:
        parts.append(bytes(record.coin_name))
        parts.append(encode_coin(record))
    parts.append(len(removed).to_bytes(4, "big", signed=False))
    for name, previous_spent in removed:
        parts.append(bytes(name))
        parts.append(int(previous_spent).to_bytes(8, "big", signed=True))
    return b"".join(parts)


_NAME_LEN = 32
_REMOVAL_LEN = _NAME_LEN + 8
_ADDED_STRIDE = _NAME_LEN + _RECORD_LEN


def _delta_layout(raw: bytes) -> tuple[int, int]:
    """Return (added_end, removed_count) for a CD1 journal."""
    if len(raw) < 7 or raw[:3] != _DELTA_MAGIC:
        raise ValueError("coin journal is not a CD1 record")
    added_count = int.from_bytes(raw[3:7], "big", signed=False)
    added_end = 7 + added_count * _ADDED_STRIDE
    if added_end + 4 > len(raw):
        raise ValueError("coin journal additions are truncated")
    removed_count = int.from_bytes(raw[added_end : added_end + 4], "big", signed=False)
    if added_end + 4 + removed_count * _REMOVAL_LEN != len(raw):
        raise ValueError("coin journal removals are truncated")
    return added_end, removed_count


def added_lookup_keys(raw: bytes) -> tuple[int, list[tuple[str, bytes]]]:
    """How many coins a journal names. It does not produce fast-forward keys.

    An empty fast-forward key has no parent ids and does not expire. The block
    writer stores the lineage when it creates the coin.
    """
    added_end, _removed_count = _delta_layout(raw)
    return (added_end - 7) // _ADDED_STRIDE, []


def delta_removals(raw: bytes) -> list[tuple[bytes32, int]]:
    """Names and previous spent heights for coins spent in a journal."""
    added_end, removed_count = _delta_layout(raw)
    cursor = added_end + 4
    end = cursor + removed_count * _REMOVAL_LEN
    removed: list[tuple[bytes32, int]] = []
    while cursor < end:
        name = bytes32(raw[cursor : cursor + _NAME_LEN])
        previous = int.from_bytes(raw[cursor + _NAME_LEN : cursor + _REMOVAL_LEN], "big", signed=True)
        removed.append((name, previous))
        cursor += _REMOVAL_LEN
    return removed


def decode_delta(raw: bytes) -> tuple[list[StoredCoin], list[tuple[bytes32, int]]]:
    added_end, _removed_count = _delta_layout(raw)
    cursor = 7
    added: list[StoredCoin] = []
    while cursor < added_end:
        name = bytes32(raw[cursor : cursor + _NAME_LEN])
        cursor += _NAME_LEN
        added.append(decode_coin(name, raw[cursor : cursor + _RECORD_LEN]))
        cursor += _RECORD_LEN
    return added, delta_removals(raw)
