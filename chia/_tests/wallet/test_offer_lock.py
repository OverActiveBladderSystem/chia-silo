from __future__ import annotations

from types import SimpleNamespace

import pytest
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64

from chia.data_layer.data_layer_wallet import DataLayerWallet
from chia.types.blockchain_format.coin import Coin
from chia.wallet.nft_wallet.nft_wallet import NFTWallet


def _coin(marker: bytes) -> Coin:
    return Coin(bytes32(b"\x11" * 32), bytes32(marker * 32), uint64(1))


class _Trades:
    def __init__(self, locked: set[bytes32]) -> None:
        self.locked = locked

    async def assert_coin_not_locked(self, coin_id: bytes32) -> None:
        if coin_id in self.locked:
            raise ValueError(f"Coin {coin_id.hex()} is locked by an offer")


@pytest.mark.anyio
async def test_an_offered_nft_cannot_be_transferred() -> None:
    coin = _coin(b"\x21")
    wallet = object.__new__(NFTWallet)
    wallet.wallet_state_manager = SimpleNamespace(trade_manager=_Trades({coin.name()}))  # type: ignore[attr-defined]
    nft = SimpleNamespace(coin=coin)

    with pytest.raises(ValueError, match=f"Coin {coin.name().hex()} is locked by an offer"):
        await wallet.generate_unsigned_spendbundle(
            [],
            object(),  # type: ignore[arg-type]
            nft_coin=nft,  # type: ignore[arg-type]
        )


@pytest.mark.anyio
async def test_an_offered_data_layer_singleton_cannot_be_updated() -> None:
    coin_id = bytes32(b"\x31" * 32)
    wallet = object.__new__(DataLayerWallet)
    wallet.wallet_state_manager = SimpleNamespace(trade_manager=_Trades({coin_id}))  # type: ignore[attr-defined]

    async def info(_launcher_id: bytes32) -> tuple[SimpleNamespace, None]:
        return SimpleNamespace(coin_id=coin_id), None

    wallet.get_spendable_singleton_info = info  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=f"Coin {coin_id.hex()} is locked by an offer"):
        await wallet.create_update_state_spend(
            bytes32(b"\x32" * 32),
            bytes32(b"\x33" * 32),
            object(),  # type: ignore[arg-type]
        )
