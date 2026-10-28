from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint16, uint32, uint64

from chia.types.blockchain_format.coin import Coin
from chia.wallet.fungibility_manager import FungibilityManager
from chia.wallet.util.tx_config import CoinSelectionConfig
from chia.wallet.util.wallet_types import WalletType
from chia.wallet.wallet_coin_record import WalletCoinRecord

PUZZLE = bytes32(b"\x21" * 32)


def _coin(marker: bytes, amount: int) -> Coin:
    return Coin(bytes32(marker * 32), PUZZLE, uint64(amount))


def _record(coin: Coin, *, spent: bool = False) -> WalletCoinRecord:
    return WalletCoinRecord(
        coin=coin,
        confirmed_block_height=uint32(3),
        spent_block_height=uint32(4 if spent else 0),
        spent=spent,
        coinbase=False,
        wallet_type=WalletType.STANDARD_WALLET,
        wallet_id=1,
    )


@dataclass
class _CoinStore:
    by_id: dict[bytes32, WalletCoinRecord]
    raw_reads: int = 0

    async def get_coin_record(self, coin_id: bytes32) -> WalletCoinRecord | None:
        return self.by_id.get(coin_id)

    async def get_coin_records(self, **_kwargs: object) -> "_Records":
        self.raw_reads += 1
        return _Records(list(self.by_id.values()))


@dataclass
class _Records:
    records: list[WalletCoinRecord]


@dataclass
class _State:
    spendable: list[WalletCoinRecord]
    asked_for_wallet: list[int] = field(default_factory=list)

    async def get_spendable_coins_for_wallet(self, wallet_id: int) -> list[WalletCoinRecord]:
        self.asked_for_wallet.append(wallet_id)
        return list(self.spendable)


@dataclass
class _Side:
    selected_coins: list[Coin] = field(default_factory=list)


@dataclass
class _Interface:
    side_effects: _Side = field(default_factory=_Side)


class _Use:
    def __init__(self, interface: _Interface) -> None:
        self._interface = interface

    async def __aenter__(self) -> _Interface:
        return self._interface

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _Scope:
    def __init__(self, selection: CoinSelectionConfig) -> None:
        self.config = self
        self.tx_config = self
        self.coin_selection_config = selection
        self.interface = _Interface()
        self.used = False

    def use(self) -> _Use:
        self.used = True
        return _Use(self.interface)

    async def get_puzzle_hash(self, _manager: object, override_reuse_puzhash_with: bool | None = None) -> bytes32:
        return PUZZLE


@dataclass
class _Wallet:
    seen: set[Coin] | None = None
    wallet_type: WalletType = WalletType.CAT

    def id(self) -> int:
        return 1

    def type(self) -> WalletType:
        return self.wallet_type

    async def generate_signed_transaction(
        self,
        amounts: list[uint64],
        puzzle_hashes: list[bytes32],
        _action_scope: object,
        _fee: uint64,
        coins: set[Coin] | None = None,
        extra_conditions: tuple[object, ...] = tuple(),
    ) -> None:
        self.seen = coins
        self.amounts = amounts
        self.puzzle_hashes = puzzle_hashes


def _selection(**overrides: object) -> CoinSelectionConfig:
    values: dict[str, object] = {
        "min_coin_amount": uint64(2),
        "max_coin_amount": uint64(1000),
        "excluded_coin_amounts": [],
        "excluded_coin_ids": [],
        "included_coin_ids": [],
        "primary_coin": None,
    }
    values.update(overrides)
    return CoinSelectionConfig(**values)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_a_spent_coin_cannot_be_split() -> None:
    coin = _coin(b"\x01", 100)
    store = _CoinStore({coin.name(): _record(coin, spent=True)})
    state = _State([])
    wallet = _Wallet()
    scope = _Scope(_selection())

    with pytest.raises(ValueError, match=f"Cannot split already-spent coin {coin.name().hex()}"):
        await FungibilityManager(coin_store=store, wallet_state_manager=state).split_coins(  # type: ignore[arg-type]
            action_scope=scope,  # type: ignore[arg-type]
            wallet=wallet,  # type: ignore[arg-type]
            target_coin_id=coin.name(),
            amount_per_coin=uint64(40),
            number_of_coins=uint16(2),
            fee=uint64(0),
        )

    assert state.asked_for_wallet == []
    assert wallet.seen is None
    assert scope.used is False


@pytest.mark.anyio
async def test_a_coin_locked_by_an_offer_cannot_be_split() -> None:
    coin = _coin(b"\x02", 100)
    store = _CoinStore({coin.name(): _record(coin)})
    state = _State([])
    wallet = _Wallet()
    scope = _Scope(_selection())

    with pytest.raises(ValueError, match=f"Coin {coin.name().hex()} is not spendable"):
        await FungibilityManager(coin_store=store, wallet_state_manager=state).split_coins(  # type: ignore[arg-type]
            action_scope=scope,  # type: ignore[arg-type]
            wallet=wallet,  # type: ignore[arg-type]
            target_coin_id=coin.name(),
            amount_per_coin=uint64(40),
            number_of_coins=uint16(2),
            fee=uint64(0),
        )

    assert wallet.seen is None
    assert scope.used is False


@pytest.mark.anyio
async def test_a_spendable_coin_can_be_split() -> None:
    coin = _coin(b"\x03", 100)
    record = _record(coin)
    store = _CoinStore({coin.name(): record})
    state = _State([record])
    wallet = _Wallet()
    scope = _Scope(_selection())

    await FungibilityManager(coin_store=store, wallet_state_manager=state).split_coins(  # type: ignore[arg-type]
        action_scope=scope,  # type: ignore[arg-type]
        wallet=wallet,  # type: ignore[arg-type]
        target_coin_id=coin.name(),
        amount_per_coin=uint64(40),
        number_of_coins=uint16(2),
        fee=uint64(0),
    )

    assert wallet.seen == {coin}
    assert wallet.amounts == [uint64(40), uint64(40)]


@pytest.mark.anyio
async def test_coins_that_are_not_spendable_cannot_be_combined() -> None:
    locked = _coin(b"\x04", 30)
    store = _CoinStore({locked.name(): _record(locked)})
    state = _State([])
    wallet = _Wallet()
    scope = _Scope(_selection())

    with pytest.raises(ValueError, match="Cannot combine coins that are not spendable"):
        await FungibilityManager(coin_store=store, wallet_state_manager=state).combine_coins(  # type: ignore[arg-type]
            action_scope=scope,  # type: ignore[arg-type]
            wallet=wallet,  # type: ignore[arg-type]
            number_of_coins=uint16(1),
            largest_first=False,
            fee=uint64(0),
            target_coin_ids=[locked.name()],
        )

    assert wallet.seen is None
    assert scope.used is False


@pytest.mark.anyio
async def test_a_fill_up_combine_uses_only_spendable_coins() -> None:
    small = _coin(b"\x05", 3)
    large = _coin(b"\x06", 9)
    excluded_amount = _coin(b"\x07", 7)
    excluded_id = _coin(b"\x08", 4)
    too_small = _coin(b"\x09", 1)
    spent = _coin(b"\x0a", 1)
    spendable = [
        _record(small),
        _record(large),
        _record(excluded_amount),
        _record(excluded_id),
        _record(too_small),
    ]
    store = _CoinStore({record.coin.name(): record for record in [*spendable, _record(spent, spent=True)]})
    state = _State(spendable)
    wallet = _Wallet()
    scope = _Scope(
        _selection(
            excluded_coin_amounts=[uint64(7)],
            excluded_coin_ids=[excluded_id.name()],
        )
    )

    await FungibilityManager(coin_store=store, wallet_state_manager=state).combine_coins(  # type: ignore[arg-type]
        action_scope=scope,  # type: ignore[arg-type]
        wallet=wallet,  # type: ignore[arg-type]
        number_of_coins=uint16(2),
        largest_first=False,
        fee=uint64(0),
    )

    assert store.raw_reads == 0
    assert wallet.seen == {small, large}
    assert wallet.amounts == [uint64(12)]
