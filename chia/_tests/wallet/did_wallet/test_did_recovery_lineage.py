from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint16, uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.wallet.did_wallet.did_info import DIDCoinData, DIDInfo
from chia.wallet.did_wallet.did_wallet import (
    DIDWallet,
    did_data_with_parent_coin,
    lineage_proof_for_parent,
)
from chia.wallet.util.wallet_sync_utils import PeerRequestException


def _coin(parent: bytes32, puzzle: bytes, amount: int = 1) -> Coin:
    return Coin(parent, bytes32(puzzle * 32), uint64(amount))


def _info(origin: Coin) -> DIDInfo:
    return DIDInfo(
        origin_coin=origin,
        backup_ids=[],
        num_of_backup_ids_needed=uint64(0),
        parent_info=[],
        current_inner=Program.to(1),
        temp_coin=None,
        temp_puzhash=None,
        temp_pubkey=None,
        sent_recovery_transaction=False,
        metadata=json.dumps({}),
    )


def test_recovered_did_lineage_names_the_parents_parent() -> None:
    grandparent = bytes32(b"\x10" * 32)
    parent = _coin(grandparent, b"\x21")
    child = _coin(parent.name(), b"\x22")
    proof = lineage_proof_for_parent(parent, bytes32(b"\x33" * 32))
    data = DIDCoinData(
        Program.to(1),
        None,
        uint16(0),
        Program.to(1),
        Program.to([]),
        Program.to(b"inner"),
        CoinState(child, None, uint32(4)),
    )

    fixed = did_data_with_parent_coin(data, parent, uint32(4))

    assert proof.parent_name == grandparent
    assert proof.amount == parent.amount
    assert fixed.coin_state.coin.name() == parent.name()
    assert fixed.coin_state.coin.parent_coin_info == grandparent


@pytest.mark.anyio
async def test_adding_a_recovered_did_stores_the_parents_lineage() -> None:
    grandparent = bytes32(b"\x10" * 32)
    parent = _coin(grandparent, b"\x21")
    child = _coin(parent.name(), b"\x22")
    wallet = object.__new__(DIDWallet)
    wallet.log = logging.getLogger("test.did.lineage")
    wallet.did_info = _info(_coin(bytes32(b"\x01" * 32), b"\x02"))
    wallet.wallet_state_manager = SimpleNamespace(add_interested_coin_ids=_async_noop)

    async def save_info(info: DIDInfo) -> None:
        wallet.did_info = info

    async def remember(coin_id: bytes32) -> None:
        del coin_id

    async def inner_for(_puzzle_hash: bytes32) -> Program:
        return Program.to(b"child-inner")

    wallet.save_info = save_info  # type: ignore[method-assign]
    wallet._remember_info_for_coin = remember  # type: ignore[method-assign]
    wallet.inner_puzzle_for_did_puzzle = inner_for  # type: ignore[method-assign]
    wallet._coin_is_first_singleton = lambda _coin: True  # type: ignore[method-assign]

    data = did_data_with_parent_coin(
        DIDCoinData(
            Program.to(1),
            None,
            uint16(0),
            Program.to(1),
            Program.to([]),
            Program.to(b"inner"),
            CoinState(child, None, uint32(4)),
        ),
        parent,
        uint32(4),
    )

    await wallet.coin_added(child, uint32(4), object(), data)  # type: ignore[arg-type]

    stored = wallet.get_parent_for_coin(child)
    assert stored is not None
    assert stored.parent_name == grandparent
    assert stored.amount == parent.amount


@pytest.mark.anyio
async def test_import_reads_the_launcher_block_when_child_lookup_fails() -> None:
    launcher_parent = bytes32(b"\x44" * 32)
    launcher = _coin(launcher_parent, b"\x45", amount=1)
    eve = _coin(launcher.name(), b"\x46", amount=1)
    launcher_spend = make_spend(launcher, Program.to(1), Program.to([eve.puzzle_hash, eve.amount, 0]))

    class _Node:
        def get_full_node_peer(self) -> object:
            return object()

        async def fetch_children(self, coin_id: bytes32, peer: object, fork_height: uint32 | None = None) -> list:
            del peer, fork_height
            if coin_id == launcher.name():
                raise PeerRequestException("spent row is gone")
            return []

        async def get_coin_state(self, names: list[bytes32], peer: object, fork_height: uint32 | None = None) -> list:
            del peer, fork_height
            if names == [eve.name()]:
                return [CoinState(eve, None, uint32(9))]
            return []

    class _Use:
        async def __aenter__(self) -> object:
            async def puzzle_hash(_manager: object, override_reuse_puzhash_with: bool | None = None) -> bytes32:
                del override_reuse_puzhash_with
                return bytes32(b"\x77" * 32)

            return SimpleNamespace(get_puzzle_hash=puzzle_hash)

        async def __aexit__(self, *_args: object) -> bool:
            return False

    wallet = object.__new__(DIDWallet)
    wallet.log = logging.getLogger("test.did.import")
    wallet.did_info = _info(launcher)
    wallet.wallet_state_manager = SimpleNamespace(
        wallet_node=_Node(),
        new_action_scope=lambda *_args, **_kwargs: _Use(),
        get_public_key=_async_pubkey,
    )

    async def save_info(info: DIDInfo) -> None:
        wallet.did_info = info

    wallet.save_info = save_info  # type: ignore[method-assign]

    async def fetch(height: uint32, coin: Coin, peer: object):
        del peer
        assert height == 9
        assert coin.name() == launcher.name()
        return launcher_spend

    from chia.wallet.did_wallet import did_wallet as did_module

    original_fetch = did_module.fetch_coin_spend
    original_additions = did_module.compute_additions
    did_module.fetch_coin_spend = fetch
    did_module.compute_additions = lambda _spend: [eve]
    try:
        await wallet.load_parent(wallet.did_info, uint32(9))
    finally:
        did_module.fetch_coin_spend = original_fetch
        did_module.compute_additions = original_additions

    stored = wallet.get_parent_for_coin(eve)
    assert stored is not None
    assert stored.parent_name == launcher_parent
    assert stored.inner_puzzle_hash == launcher.puzzle_hash
    assert stored.amount == launcher.amount


async def _async_noop(*_args: object, **_kwargs: object) -> None:
    return None


async def _async_pubkey(_puzzle_hash: bytes32) -> bytes:
    return b"\x01" * 48
