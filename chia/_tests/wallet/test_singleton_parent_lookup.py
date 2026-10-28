from __future__ import annotations

from unittest.mock import patch

import pytest
from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.wallet.wallet_state_manager import WalletStateManager


def _coin(marker: bytes, parent: bytes32 | None = None) -> Coin:
    return Coin(bytes32(b"\x10" * 32) if parent is None else parent, bytes32(marker * 32), uint64(1))


@pytest.mark.anyio
async def test_a_lost_singleton_uses_the_creation_block_when_the_parent_row_is_gone() -> None:
    parent = _coin(b"\x21")
    singleton = Coin(parent.name(), bytes32(b"\x22" * 32), uint64(1))
    singleton_state = CoinState(singleton, None, uint32(4))
    parent_state = CoinState(parent, uint32(4), uint32(0))
    spend = make_spend(parent, Program.to(1), Program.to(0))
    asked: list[list[bytes32]] = []

    class _Node:
        async def get_coin_state(
            self, names: list[bytes32], peer: object, fork_height: uint32 | None = None
        ) -> list[CoinState]:
            del peer, fork_height
            asked.append(list(names))
            if names == [singleton.name()]:
                return [singleton_state]
            return []

    class _Manager:
        wallet_node = _Node()

        async def _parent_spent_in_creation_block(self, peer: object, coin_state: CoinState) -> list[CoinState]:
            del peer
            assert coin_state.coin.name() == singleton.name()
            return [parent_state]

    async def fetch(coin_state: CoinState, peer: object):
        del peer
        assert coin_state.spent_height == 4
        assert coin_state.coin.name() == parent.name()
        return spend

    with patch("chia.wallet.wallet_state_manager.fetch_coin_spend_for_coin_state", fetch):
        found, state = await WalletStateManager.get_latest_singleton_coin_spend(
            _Manager(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            singleton.name(),
            latest=False,
        )

    assert found == spend
    assert state == singleton_state
    assert asked == [[singleton.name()], [parent.name()]]
