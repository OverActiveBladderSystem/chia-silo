from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.wallet.wallet_node import WalletNode


class _Lock:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _Node:
    def __init__(self) -> None:
        self.log = logging.getLogger("test.watch_coin_ids")
        self.wallet_state_manager = SimpleNamespace(lock=_Lock())
        self.applied: list[CoinState] = []
        self.filtered: list[list[bytes32]] = []

    async def add_states_from_peer(
        self,
        states: list[CoinState],
        peer: object,
        fork_height: uint32 | None = None,
        known_children: object = None,
    ) -> bool:
        del peer, fork_height, known_children
        self.applied.extend(states)
        return True

    async def _read_filters_for_coin_ids(self, peer: object, coin_ids: list[bytes32]) -> None:
        del peer
        self.filtered.append(list(coin_ids))


def _coin() -> Coin:
    return Coin(bytes32(b"\x21" * 32), bytes32(b"\x22" * 32), uint64(5))


@pytest.mark.anyio
async def test_a_watched_coin_id_is_read_from_the_node_before_the_blocks() -> None:
    coin = _coin()
    state = CoinState(coin, None, uint32(4))
    node = _Node()

    async def lookup(coin_ids: list[bytes32], peer: object, min_height: int, priority: int = 0) -> list[CoinState]:
        del peer, min_height, priority
        assert coin_ids == [coin.name()]
        return [state]

    with patch("chia.wallet.wallet_node.subscribe_to_coin_updates", lookup):
        await WalletNode._watch_coin_ids_on_filter_peer(node, object(), [coin.name()])  # type: ignore[arg-type]

    assert node.applied == [state]
    assert node.filtered == [[coin.name()]]


@pytest.mark.anyio
async def test_an_empty_coin_id_lookup_does_not_invent_an_unspent_coin() -> None:
    coin = _coin()
    node = _Node()

    async def lookup(coin_ids: list[bytes32], peer: object, min_height: int, priority: int = 0) -> list[CoinState]:
        del coin_ids, peer, min_height, priority
        return []

    with patch("chia.wallet.wallet_node.subscribe_to_coin_updates", lookup):
        await WalletNode._watch_coin_ids_on_filter_peer(node, object(), [coin.name()])  # type: ignore[arg-type]

    assert node.applied == []
    assert node.filtered == [[coin.name()]]
