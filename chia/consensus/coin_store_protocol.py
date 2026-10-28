from __future__ import annotations

from collections.abc import Collection
from typing import Protocol

from chia_rs import CoinRecord
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin


class CoinStoreProtocol(Protocol):
    """
    The coin store interface used by `chia.consensus`.
    This is a substitute for importing from chia.full_node.coin_store directly.

    The concrete `CoinStore` has a much larger surface (puzzle hash queries,
    coin states, lineage lookups, etc.), but those methods serve RPCs and the
    wallet protocol, not consensus, so they are not part of this protocol.
    """

    async def new_block(
        self,
        height: uint32,
        timestamp: uint64,
        included_reward_coins: Collection[Coin],
        tx_additions: Collection[tuple[bytes32, Coin, bool]],
        tx_removals: list[bytes32],
        *,
        assume_additions_are_new: bool = False,
        hints: Collection[bytes] = (),
        removed_coins: Collection[Coin] = (),
    ) -> None:
        """
        Add a new block to the coin store.

        `assume_additions_are_new` is set by block acceptance. Validation has already
        rejected a repeated spend and a repeated output in the block, so the new coin
        ids are not looked up in the historical coin table on the way in.
        """

    async def get_coin_records(self, coin_ids: Collection[bytes32]) -> list[CoinRecord]:
        """
        Returns the coin records for the specified coin ids
        """

    async def get_coins_added_at_height(self, height: uint32) -> list[CoinRecord]:
        """
        Returns the coins added at a specific height
        """

    async def get_coins_removed_at_height(self, height: uint32) -> list[CoinRecord]:
        """
        Returns the coins removed at a specific height
        """

    async def get_additions_and_removals_at_height(
        self, height: uint32
    ) -> tuple[list[CoinRecord], list[CoinRecord]]:
        """
        Returns the coins added and removed at one height from the same read.
        """

    async def rollback_to_block(self, block_index: int) -> dict[bytes32, CoinRecord]:
        """
        Rolls back the blockchain to the specified block index
        """

    async def undo_floor(self) -> int | None:
        """Highest height whose spent row was shortened. None when every spend is kept."""

    async def spent_mark(self, coin_name: bytes32) -> tuple[int, int] | None:
        """Creation and spend height from an older 8-byte row. New databases delete that row."""

    async def creation_height(self, coin_name: bytes32) -> int | None:
        """Creation height of an unspent coin whose body is no longer in the journal."""

    async def parent_spend_height(self, parent_id: bytes32) -> int | None:
        """Height of the block that spent this parent. None when it is unspent or unknown."""

    async def block_timestamp(self, height: int) -> int | None:
        """Timestamp stored for a transaction-block height."""

    def has_address_index(self) -> bool:
        """True when puzzle-hash and hint lists are stored. A filter node returns False."""

    async def get_block_filter(self, height: int) -> bytes | None:
        """Committed filter for one transaction block, or None when this store does not keep filters."""

    async def get_hint_filter(self, height: int) -> bytes | None:
        """Hint filter for one transaction block, or None when this store does not keep filters."""
