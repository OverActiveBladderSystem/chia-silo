from __future__ import annotations

import pytest
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.full_node.coin_store import CoinStore
from chia.full_node.db.block_filter import (
    ADDRESS_INDEX_REFUSED,
    FILTERS_NOT_STORED,
    FILTERS_OUTSIDE_CHAIN,
    FILTERS_TOO_WIDE,
    HINTS_NOT_READ,
    address_hints,
    address_lookup_refusal,
    committed_filter,
    filter_matches,
    filter_request_rejection,
    hint_filter,
    one_height_filter,
)
from chia.full_node.db.chain_db import ChainDB
from chia.full_node.db.coin_store import RocksCoinStore
from chia.full_node.db.memory import MemoryBackend
from chia.protocols.wallet_protocol import (
    HeightFilter,
    HintedCoin,
    RejectBlockFilters,
    RejectBlockHints,
    RequestBlockFilters,
    RequestBlockHints,
    RespondBlockFilters,
    RespondBlockHints,
)
from chia.types.blockchain_format.coin import Coin

PARENT = bytes32(b"\x11" * 32)
PUZZLE = bytes32(b"\x22" * 32)
OTHER = bytes32(b"\x33" * 32)


def _coin(parent: bytes32, puzzle: bytes32, amount: int) -> Coin:
    return Coin(parent, puzzle, uint64(amount))


def test_a_filter_page_says_when_a_height_has_no_transaction_block() -> None:
    assert filter_request_rejection(0, 31, 100) is None
    assert filter_request_rejection(0, 31, 31) is None
    assert filter_request_rejection(10, 10, 10) is None
    assert filter_request_rejection(0, 32, 100) == FILTERS_TOO_WIDE
    assert filter_request_rejection(5, 4, 100) == FILTERS_OUTSIDE_CHAIN
    assert filter_request_rejection(-1, 0, 10) == FILTERS_OUTSIDE_CHAIN
    assert filter_request_rejection(0, 1, None) == FILTERS_OUTSIDE_CHAIN
    assert filter_request_rejection(0, 10, 9) == FILTERS_OUTSIDE_CHAIN
    assert one_height_filter(False, b"\x02", b"\x03") == (None, None)
    assert one_height_filter(True, None, b"\x00") == FILTERS_NOT_STORED
    assert one_height_filter(True, b"\x02", None) == FILTERS_NOT_STORED
    assert one_height_filter(True, b"", b"") == (b"", b"")
    assert one_height_filter(True, b"\x02", b"\x00") == (b"\x02", b"\x00")

    page = RespondBlockFilters(
        uint32(1),
        uint32(2),
        [HeightFilter(uint32(1), b"\x02", b"\x00"), HeightFilter(uint32(2), None, None)],
    )
    assert RespondBlockFilters.from_bytes(bytes(page)) == page
    refusal = RejectBlockFilters(uint32(1), uint32(2), FILTERS_NOT_STORED)
    assert RejectBlockFilters.from_bytes(bytes(refusal)) == refusal
    request = RequestBlockFilters(uint32(1), uint32(2))
    assert RequestBlockFilters.from_bytes(bytes(request)) == request


def test_a_filter_node_refuses_an_address_lookup_and_an_index_node_answers() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    assert store.has_address_index() is False
    assert address_lookup_refusal(store) == ADDRESS_INDEX_REFUSED
    assert ADDRESS_INDEX_REFUSED != ""


@pytest.mark.anyio
async def test_sqlite_keeps_an_address_index_and_does_not_serve_filters() -> None:
    from chia._tests.util.db_connection import DBConnection

    async with DBConnection(2) as wrapper:
        store = await CoinStore.create(wrapper)
        assert store.has_address_index() is True
        assert address_lookup_refusal(store) is None
        assert await store.get_block_filter(1) is None
        assert await store.get_hint_filter(1) is None


def test_only_a_32_byte_memo_is_an_address_hint() -> None:
    coin = _coin(PARENT, PUZZLE, 5)
    short = _coin(PARENT, OTHER, 6)
    found = address_hints([(coin, PUZZLE), (short, b"\x01"), (coin, None)])
    assert found == [(coin, PUZZLE)]
    hinted = RespondBlockHints(uint32(4), PARENT, [HintedCoin(coin, PUZZLE)])
    assert RespondBlockHints.from_bytes(bytes(hinted)) == hinted
    refusal = RejectBlockHints(uint32(4), PARENT, HINTS_NOT_READ)
    assert RejectBlockHints.from_bytes(bytes(refusal)) == refusal
    request = RequestBlockHints(uint32(4), PARENT)
    assert RequestBlockHints.from_bytes(bytes(request)) == request


def test_committed_filter_bytes_do_not_depend_on_item_order() -> None:
    forward = committed_filter([PUZZLE, OTHER], [PARENT])
    backward = committed_filter([OTHER, PUZZLE], [PARENT])
    assert forward == backward
    assert filter_matches(forward, PUZZLE)
    assert filter_matches(forward, PARENT)


@pytest.mark.anyio
async def test_a_transaction_block_keeps_both_filters_until_a_reorg() -> None:
    store = RocksCoinStore(ChainDB(MemoryBackend()))
    parent = _coin(bytes32(b"\x01" * 32), PUZZLE, 100)
    child = _coin(parent.name(), OTHER, 40)
    reward = _coin(bytes32(b"\x02" * 32), PUZZLE, 50)
    hint = b"\xee" * 32

    await store.new_block(uint32(1), uint64(10), [reward], [(parent.name(), parent, False)], [])
    await store.new_block(
        uint32(4),
        uint64(40),
        [],
        [(child.name(), child, False)],
        [parent.name()],
        hints=[hint],
    )

    first = await store.get_block_filter(1)
    assert first == committed_filter([parent.puzzle_hash, reward.puzzle_hash], [])
    assert filter_matches(first, parent.puzzle_hash)
    assert filter_matches(first, reward.puzzle_hash)
    assert await store.get_hint_filter(1) == hint_filter([])
    assert await store.get_block_filter(2) is None
    assert await store.get_block_filter(3) is None

    spent = await store.get_block_filter(4)
    assert spent == committed_filter([child.puzzle_hash], [parent.name()])
    assert filter_matches(spent, child.puzzle_hash)
    assert filter_matches(spent, parent.name())
    hinted = await store.get_hint_filter(4)
    assert hinted == hint_filter([hint])
    assert filter_matches(hinted, hint)

    await store.rollback_to_block(1)
    assert await store.get_block_filter(4) is None
    assert await store.get_hint_filter(4) is None
    assert await store.get_block_filter(1) == first
