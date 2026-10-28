from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.wallet.nft_wallet.nft_info import NFTWalletInfo
from chia.wallet.nft_wallet.nft_wallet import NFTWallet
from chia.wallet.util.wallet_types import WalletType

DID = bytes32(b"\x41" * 32)
OLD_PH = Program.to(b"old-owner").get_tree_hash()
NEW_PH = Program.to(b"new-owner").get_tree_hash()


class _Puzzle:
    def __init__(self, puzzle_hash: bytes32) -> None:
        self._puzzle_hash = puzzle_hash

    def get_tree_hash(self) -> bytes32:
        return self._puzzle_hash


@pytest.mark.anyio
async def test_sending_the_last_nft_back_to_yourself_keeps_the_new_coin() -> None:
    parent = Coin(bytes32(b"\x11" * 32), OLD_PH, uint64(1))
    created: list[bytes32 | None] = []
    deleted: list[uint32] = []
    wallets: dict[uint32, NFTWallet] = {}
    old_wallet = object.__new__(NFTWallet)
    old_wallet.wallet_info = SimpleNamespace(id=uint32(7))  # type: ignore[assignment]
    old_wallet.nft_wallet_info = NFTWalletInfo(DID)
    wallets[uint32(7)] = old_wallet

    async def remove_coin(coin: Coin, height: uint32) -> None:
        del coin, height
        deleted.append(old_wallet.wallet_info.id)
        wallets.pop(old_wallet.wallet_info.id)

    async def is_empty() -> bool:
        return True

    old_wallet.remove_coin = remove_coin  # type: ignore[method-assign]
    old_wallet.is_empty = is_empty  # type: ignore[method-assign]

    class _Created:
        def id(self) -> uint32:
            return uint32(11)

        def type(self) -> WalletType:
            return WalletType.NFT

    async def create_new_nft_wallet(
        wallet_state_manager: object,
        main_wallet: object,
        did_id: bytes32 | None = None,
        name: str = "NFT Wallet",
    ) -> _Created:
        del wallet_state_manager, main_wallet, name
        created.append(did_id)
        return _Created()

    manager = SimpleNamespace(
        log=logging.getLogger("test.nft.last"),
        wallets=wallets,
        config={},
        main_wallet=object(),
    )

    async def get_derivation_record_for_puzzle_hash(puzzle_hash: bytes32) -> object | None:
        if puzzle_hash in {OLD_PH, NEW_PH}:
            return object()
        return None

    async def delete_wallet(wallet_id: uint32) -> None:
        deleted.append(wallet_id)

    async def get_all_wallet_info_entries(wallet_type: object = None) -> list[object]:
        del wallet_type
        return []

    manager.puzzle_store = SimpleNamespace(get_derivation_record_for_puzzle_hash=get_derivation_record_for_puzzle_hash)
    manager.delete_wallet = delete_wallet
    manager.get_all_wallet_info_entries = get_all_wallet_info_entries

    nft = SimpleNamespace(
        supports_did=True,
        p2_puzzle=_Puzzle(OLD_PH),
        owner_did=DID,
        singleton_launcher_id=bytes32(b"\x51" * 32),
    )
    nft_data = SimpleNamespace(
        uncurried_nft=nft,
        parent_coin_spend=make_spend(parent, Program.to(0), Program.to(0)),
        parent_coin_state=CoinState(parent, uint32(5), uint32(4)),
    )

    with (
        patch(
            "chia.wallet.nft_wallet.nft_wallet.get_metadata_and_phs",
            return_value=(Program.to(0), NEW_PH),
        ),
        patch("chia.wallet.nft_wallet.nft_wallet.get_new_owner_did", return_value=None),
        patch.object(NFTWallet, "create_new_nft_wallet", create_new_nft_wallet),
    ):
        found = await NFTWallet.identify(manager, nft_data)  # type: ignore[arg-type]

    assert found is not None
    assert found.id == uint32(11)
    assert found.type == WalletType.NFT
    assert created == [DID]
    assert deleted == [uint32(7)]
    assert uint32(7) not in wallets
