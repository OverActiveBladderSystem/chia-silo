from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from chia_rs import AugSchemeMPL, CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.wallet.cat_wallet.cat_info import CATCoinData, CATInfo
from chia.wallet.cat_wallet.cat_utils import CAT_MOD, construct_cat_puzzle
from chia.wallet.cat_wallet.cat_wallet import CATWallet
from chia.wallet.cat_wallet.r_cat_wallet import RCATWallet
from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import puzzle_for_pk
from chia.wallet.util.wallet_types import WalletType
from chia.wallet.vc_wallet.vc_drivers import create_revocation_layer

TAIL = bytes32(b"\x71" * 32)
HIDDEN = bytes32(b"\x72" * 32)


@pytest.mark.anyio
async def test_an_incoming_revocable_cat_is_kept_when_the_plain_cat_already_has_coins() -> None:
    key = AugSchemeMPL.key_gen(b"\x02" * 32).get_g1()
    inner = puzzle_for_pk(key)
    revocable = create_revocation_layer(HIDDEN, inner.get_tree_hash())
    cat_puzzle = construct_cat_puzzle(CAT_MOD, TAIL, revocable)
    parent = Coin(bytes32(b"\x31" * 32), bytes32(b"\x32" * 32), uint64(100))
    child = Coin(parent.name(), cat_puzzle.get_tree_hash(), uint64(100))
    created: list[tuple[bytes32, bytes32]] = []

    plain = object.__new__(CATWallet)
    plain.log = logging.getLogger("test.rcat")
    plain.lineage_store = SimpleNamespace()

    async def is_empty() -> bool:
        return False

    plain.lineage_store.is_empty = is_empty  # type: ignore[attr-defined, method-assign]

    async def get_or_create(
        wallet_state_manager: object,
        wallet: object,
        limitations_program_hash: bytes32,
        hidden_puzzle_hash: bytes32,
        name: str | None = None,
    ) -> SimpleNamespace:
        del wallet_state_manager, wallet, name
        created.append((limitations_program_hash, hidden_puzzle_hash))
        return SimpleNamespace(id=lambda: uint32(9), type=lambda: WalletType.RCAT)

    info = bytes(CATInfo(TAIL, None)).hex()

    async def get_entries(wallet_type: WalletType | None = None) -> list[SimpleNamespace]:
        if wallet_type == WalletType.CAT:
            return [SimpleNamespace(id=uint32(3), data=info, type=WalletType.CAT)]
        return []

    manager = SimpleNamespace(
        log=logging.getLogger("test.rcat.identify"),
        wallets={uint32(3): plain},
        config={"automatically_add_unknown_cats": True},
        default_cats=set(),
        main_wallet=SimpleNamespace(puzzle_for_pk=lambda pubkey: puzzle_for_pk(pubkey)),
        puzzle_store=SimpleNamespace(
            get_derivation_record_for_puzzle_hash=lambda puzzle_hash: _record(puzzle_hash, key, inner.get_tree_hash())
        ),
        get_all_wallet_info_entries=get_entries,
        state_changed=lambda *args: None,
    )

    def hinted(coin_spend: object) -> tuple[dict[bytes32, SimpleNamespace], list[object]]:
        del coin_spend
        return {child.name(): SimpleNamespace(hint=inner.get_tree_hash(), coin=child)}, []

    parent_data = CATCoinData(
        mod_hash=bytes32.zeros,
        tail_program_hash=TAIL,
        inner_puzzle=inner,
        parent_coin_id=parent.parent_coin_info,
        amount=uint64(100),
    )
    with (
        patch("chia.wallet.cat_wallet.cat_wallet.compute_spend_hints_and_additions", hinted),
        patch.object(RCATWallet, "get_or_create_wallet_for_cat", get_or_create),
    ):
        found = await CATWallet.identify(
            manager,  # type: ignore[arg-type]
            parent_data,
            CoinState(parent, uint32(4), uint32(3)),
            CoinState(child, None, uint32(4)),
            make_spend(parent, cat_puzzle, Program.to(0)),
            uint32(4),
        )

    assert found is not None
    assert found.id == uint32(9)
    assert found.type == WalletType.RCAT
    assert created == [(TAIL, HIDDEN)]


async def _record(puzzle_hash: bytes32, key: object, hint: bytes32) -> SimpleNamespace | None:
    if puzzle_hash != hint:
        return None
    return SimpleNamespace(pubkey=key)
