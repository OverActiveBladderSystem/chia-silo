from __future__ import annotations

import pytest
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import int16, uint64

from chia.protocols.shared_protocol import Error
from chia.types.blockchain_format.coin import Coin
from chia.wallet.util.wallet_sync_utils import _coin_states_from_subscribe_response, include_same_block_spends


def _coin(parent: bytes, puzzle: bytes, amount: int) -> Coin:
    return Coin(bytes32(parent * 32), bytes32(puzzle * 32), uint64(amount))


@pytest.mark.parametrize(
    "response, match",
    [
        (None, "None response from peer peer.example for register_for_ph_updates"),
        (
            Error(int16(1), "rejected", None),
            "Error response from peer peer.example for register_for_ph_updates",
        ),
        (object(), "Unexpected response from peer peer.example for register_for_ph_updates: object"),
    ],
    ids=["none", "protocol_error", "unexpected_type"],
)
def test_coin_states_from_subscribe_response_rejects_invalid(response: object, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _coin_states_from_subscribe_response(response, "peer.example", "register_for_ph_updates")


@pytest.mark.anyio
async def test_a_memo_coin_spent_in_the_same_block_is_read_with_its_children() -> None:
    """The first removal request happened before the memo coin was known."""
    parent = _coin(b"\x01", b"\x02", 50)
    memo = Coin(parent.name(), bytes32(b"\x09" * 32), uint64(40))
    payment = Coin(memo.name(), bytes32(b"\x08" * 32), uint64(39))
    grandchild = Coin(payment.name(), bytes32(b"\x07" * 32), uint64(39))
    created = [memo]
    spent = [parent]
    verified: list[tuple[bytes32, bytes]] = [(memo.name(), bytes32(b"\x22" * 32))]
    asked: list[list[bytes32]] = []

    async def fetch_removals(names: list[bytes32]) -> list[Coin]:
        asked.append(list(names))
        return [coin for coin in (memo, payment) if coin.name() in names]

    async def fetch_spend(coin: Coin) -> tuple[list[Coin], list[tuple[bytes32, bytes]]]:
        if coin.name() == memo.name():
            return [payment], []
        if coin.name() == payment.name():
            return [grandchild], [(grandchild.name(), bytes32(b"\x22" * 32))]
        raise AssertionError(coin.name().hex())

    await include_same_block_spends(
        created, spent, verified, fetch_removals=fetch_removals, fetch_spend=fetch_spend
    )

    assert [coin.name() for coin in spent] == [parent.name(), memo.name(), payment.name()]
    assert [coin.name() for coin in created] == [memo.name(), payment.name(), grandchild.name()]
    assert grandchild.name() not in {coin.name() for coin in spent}
    assert verified[-1] == (grandchild.name(), bytes32(b"\x22" * 32))
    assert asked == [[memo.name()], [payment.name()], [grandchild.name()]]


@pytest.mark.anyio
async def test_an_unspent_memo_coin_is_asked_once_and_not_marked_spent() -> None:
    parent = _coin(b"\x01", b"\x02", 50)
    memo = Coin(parent.name(), bytes32(b"\x09" * 32), uint64(40))
    created = [memo]
    spent = [parent]
    verified: list[tuple[bytes32, bytes]] = []
    asked: list[list[bytes32]] = []

    async def fetch_removals(names: list[bytes32]) -> list[Coin]:
        asked.append(list(names))
        return []

    async def fetch_spend(_coin: Coin) -> tuple[list[Coin], list[tuple[bytes32, bytes]]]:
        raise AssertionError("an unspent coin has no spend to read")

    await include_same_block_spends(
        created, spent, verified, fetch_removals=fetch_removals, fetch_spend=fetch_spend
    )

    assert spent == [parent]
    assert created == [memo]
    assert asked == [[memo.name()]]


@pytest.mark.anyio
async def test_a_coin_already_known_spent_is_not_asked_again() -> None:
    parent = _coin(b"\x01", b"\x02", 50)
    memo = Coin(parent.name(), bytes32(b"\x09" * 32), uint64(40))
    created = [memo]
    spent = [parent, memo]

    async def fetch_removals(_names: list[bytes32]) -> list[Coin]:
        raise AssertionError("the spend was already read")

    async def fetch_spend(_coin: Coin) -> tuple[list[Coin], list[tuple[bytes32, bytes]]]:
        raise AssertionError("the spend was already read")

    await include_same_block_spends(
        created, spent, [], fetch_removals=fetch_removals, fetch_spend=fetch_spend
    )
    assert spent == [parent, memo]
