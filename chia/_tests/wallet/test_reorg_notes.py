from __future__ import annotations

import dataclasses

import pytest
from chia_rs import CoinState, G2Element
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia._tests.util.db_connection import DBConnection
from chia._tests.wallet.test_transaction_store import tr1
from chia.types.blockchain_format.coin import Coin
from chia.wallet.clawback_manager import clawback_confirmed_height
from chia.wallet.notification_store import Notification, NotificationStore
from chia.wallet.transaction_record import transaction_returns_after_reorg
from chia.wallet.util.transaction_type import TransactionType
from chia.wallet.wallet_retry_store import WalletRetryStore
from chia.wallet.wallet_spend_bundle import WalletSpendBundle

PEER = bytes32(b"\x11" * 32)


def _coin(marker: bytes) -> Coin:
    return Coin(bytes32(marker * 32), bytes32(b"\x22" * 32), uint64(5))


@pytest.mark.anyio
async def test_a_notification_from_a_dropped_block_can_be_received_again() -> None:
    dropped = bytes32(b"\x31" * 32)
    kept = bytes32(b"\x32" * 32)
    async with DBConnection(1) as db_wrapper:
        store = await NotificationStore.create(db_wrapper)
        await store.add_notification(Notification(dropped, b"gone", uint64(1), uint32(10)))
        await store.add_notification(Notification(kept, b"stay", uint64(1), uint32(4)))

        await store.rollback_to_block(6)

        assert [note.id for note in await store.get_notifications()] == [kept]
        assert await store.notification_exists(dropped) is False
        assert await store.notification_exists(kept) is True


@pytest.mark.anyio
async def test_a_retry_above_the_fork_is_dropped_even_when_the_sync_fork_was_missing() -> None:
    above = CoinState(_coin(b"\x41"), None, uint32(11))
    spent_above = CoinState(_coin(b"\x42"), uint32(12), uint32(5))
    kept = CoinState(_coin(b"\x43"), None, uint32(4))
    async with DBConnection(1) as db_wrapper:
        store = await WalletRetryStore.create(db_wrapper)
        await store.add_state(above, PEER, None)
        await store.add_state(spent_above, PEER, uint32(0))
        await store.add_state(kept, PEER, None)

        await store.rollback_to_block(10)

        left = {state.coin.name() for state, _peer, _height in await store.get_all_states_to_retry()}
        assert left == {kept.coin.name()}


def test_a_note_of_someone_elses_spend_is_not_put_back_as_a_pending_send() -> None:
    synthetic = dataclasses.replace(
        tr1,
        type=uint32(TransactionType.OUTGOING_TX),
        spend_bundle=None,
        confirmed=True,
        confirmed_at_height=uint32(10),
    )
    real_send = dataclasses.replace(synthetic, spend_bundle=WalletSpendBundle([], G2Element()))
    clawback = dataclasses.replace(
        tr1,
        type=uint32(TransactionType.INCOMING_CLAWBACK_RECEIVE),
        spend_bundle=None,
        confirmed=True,
    )

    assert transaction_returns_after_reorg(synthetic, coin_still_present=True) is False
    assert transaction_returns_after_reorg(real_send, coin_still_present=True) is True
    assert transaction_returns_after_reorg(clawback, coin_still_present=False) is False
    assert transaction_returns_after_reorg(clawback, coin_still_present=True) is True


def test_a_spent_clawback_is_tied_to_the_spend_height() -> None:
    assert clawback_confirmed_height(uint32(5), uint32(0)) == uint32(5)
    assert clawback_confirmed_height(uint32(5), uint32(10)) == uint32(10)
