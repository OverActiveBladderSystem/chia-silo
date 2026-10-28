from __future__ import annotations

from chia.full_node.db.rocks import (
    _BLOCK_CACHE_CAP,
    _BLOCK_CACHE_FLOOR,
    _MEMTABLE_CAP,
    _MEMTABLE_FLOOR,
    block_cache_bytes,
    clamp_share,
    memtable_budget_bytes,
)


def test_cache_stays_between_a_floor_and_a_cap() -> None:
    small = 2 * 1024 * 1024 * 1024
    large = 256 * 1024 * 1024 * 1024
    assert block_cache_bytes(small) == _BLOCK_CACHE_FLOOR
    assert memtable_budget_bytes(small) == _MEMTABLE_FLOOR
    assert block_cache_bytes(large) == _BLOCK_CACHE_CAP
    assert memtable_budget_bytes(large) == _MEMTABLE_CAP
    assert clamp_share(None, divisor=64, floor=_BLOCK_CACHE_FLOOR, cap=_BLOCK_CACHE_CAP) == _BLOCK_CACHE_FLOOR

    middle = 16 * 1024 * 1024 * 1024
    cache = block_cache_bytes(middle)
    memtables = memtable_budget_bytes(middle)
    assert _BLOCK_CACHE_FLOOR < cache < _BLOCK_CACHE_CAP
    assert _MEMTABLE_FLOOR < memtables < _MEMTABLE_CAP
    assert _BLOCK_CACHE_FLOOR <= block_cache_bytes() <= _BLOCK_CACHE_CAP
    assert _MEMTABLE_FLOOR <= memtable_budget_bytes() <= _MEMTABLE_CAP
