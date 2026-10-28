from __future__ import annotations

import pytest
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64

from chia.full_node.db.coin_codec import (
    HEIGHT_LEN,
    SPENT_MARK_LEN,
    StoredCoin,
    added_lookup_keys,
    decode_coin,
    decode_delta,
    decode_height,
    decode_spent_ids,
    decode_spent_mark,
    delta_removals,
    encode_delta,
    encode_height,
    encode_row,
    encode_spent_ids,
    encode_spent_mark,
    index_entries,
    lookup_entries,
)
from chia.full_node.db.keys import CF_COINS, CF_FF_UNSPENT

_NAME = bytes32(b"\x11" * 32)
_PUZZLE = bytes32(b"\x22" * 32)
_PARENT = bytes32(b"\x33" * 32)


def _record(name: bytes, spent: int, *, coinbase: bool = False) -> StoredCoin:
    return StoredCoin(bytes32(name), 9_141_759, spent, coinbase, _PUZZLE, _PARENT, uint64(100), 1_700_000_000)


def test_unspent_row_is_the_creation_height() -> None:
    record = _record(b"\x01" * 32, 0)
    raw = encode_row(record)
    assert raw == encode_height(record.confirmed_index)
    assert len(raw) == HEIGHT_LEN
    assert decode_height(raw) == record.confirmed_index
    spent = _record(b"\x01" * 32, 12)
    assert len(encode_row(spent)) != HEIGHT_LEN


def test_a_full_record_is_not_a_height() -> None:
    with pytest.raises(ValueError, match="unspent coin row"):
        decode_height(b"\x00" * 93)


def test_a_spent_mark_is_birth_then_spend_and_is_not_a_coin() -> None:
    raw = encode_spent_mark(1_000, 1_000 + 4_608)
    assert len(raw) == SPENT_MARK_LEN
    assert decode_spent_mark(raw) == (1_000, 5_608)
    with pytest.raises(ValueError, match="spent mark"):
        decode_spent_mark(encode_height(1_000))
    with pytest.raises(ValueError, match="coin record"):
        decode_coin(_NAME, raw)
    with pytest.raises(ValueError, match="coin height"):
        encode_height(-1)
    with pytest.raises(ValueError, match="coin height"):
        encode_spent_mark(0, 2**32)


def test_journal_lookup_keys_match_the_decoded_coins() -> None:
    added = [
        _record(b"\x01" * 32, 0),
        _record(b"\x02" * 32, -1),
        _record(b"\x03" * 32, 9_352_167),
        _record(b"\x04" * 32, -2, coinbase=True),
    ]
    removed = [(_NAME, 0), (bytes32(b"\x44" * 32), -1)]
    raw = encode_delta(added, removed)
    count, keys = added_lookup_keys(raw)
    expected: list[tuple[str, bytes]] = []
    for record in added:
        expected.extend(lookup_entries(record))
    assert count == len(added)
    assert keys == expected
    assert delta_removals(raw) == removed
    assert decode_delta(raw) == (added, removed)


def test_empty_journal_has_no_lookup_keys() -> None:
    raw = encode_delta([], [])
    assert added_lookup_keys(raw) == (0, [])
    assert delta_removals(raw) == []
    assert decode_delta(raw) == ([], [])


def test_truncated_journal_is_rejected() -> None:
    raw = encode_delta([_record(b"\x05" * 32, 0)], [])
    with pytest.raises(ValueError, match="truncated"):
        added_lookup_keys(raw[:-1])
    with pytest.raises(ValueError, match="not a CD1"):
        delta_removals(b"not-a-journal")


def test_a_stored_coin_does_not_grow_a_fast_forward_key() -> None:
    record = _record(b"\x02" * 32, -1)
    assert index_entries(record) == [(CF_COINS, bytes(record.coin_name))]
    assert lookup_entries(record) == []
    _count, keys = added_lookup_keys(encode_delta([record], []))
    assert keys == []
    assert all(family != CF_FF_UNSPENT for family, _key in keys)


def test_spent_ids_keep_lineage_only_when_it_is_complete() -> None:
    puzzle = bytes32(b"\x22" * 32)
    lineage = bytes(bytes32(b"\x33" * 32)) + bytes(bytes32(b"\x44" * 32))
    assert encode_spent_ids([(_NAME, puzzle, None)]) == encode_spent_ids([(_NAME, None, None)])
    assert encode_spent_ids([(_NAME, puzzle, b"short")]) == encode_spent_ids([(_NAME, None, None)])
    restored = decode_spent_ids(encode_spent_ids([(_NAME, puzzle, lineage)]))
    assert restored == [(_NAME, puzzle, lineage)]
    legacy = b"CS1" + (1).to_bytes(4, "big") + bytes(_NAME) + b"\x01" + bytes(puzzle)
    assert decode_spent_ids(legacy) == [(_NAME, puzzle, None)]
