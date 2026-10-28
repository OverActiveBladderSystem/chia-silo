from __future__ import annotations

from collections.abc import Collection

from chia_rs.sized_bytes import bytes32
from chiabip158 import PyBIP158

from chia.consensus.coin_store_protocol import CoinStoreProtocol
from chia.types.blockchain_format.coin import Coin

# One reply stays small enough to stream. The wallet asks for the next page after it drops this one.
FILTER_PAGE = 32
FILTERS_OUTSIDE_CHAIN = "block filter request is outside the chain"
FILTERS_TOO_WIDE = "block filter request is too wide"
FILTERS_NOT_STORED = "block filters are not stored for that height"
HINTS_OUTSIDE_CHAIN = "block hint request is outside the chain"
HINTS_NOT_READ = "block hints could not be read"
# A node that serves block filters sends this. An empty coin list is a real answer
# from a node that still has the address index, and is not this refusal.
ADDRESS_INDEX_REFUSED = "Silo node has no address index. Sync from block filters."


def address_lookup_refusal(coin_store: CoinStoreProtocol) -> str | None:
    """Refusal text when this store has no address index. None means the lookup should be answered."""
    if coin_store.has_address_index():
        return None
    return ADDRESS_INDEX_REFUSED


def encode_bip158(items: Collection[bytes]) -> bytes:
    """BIP158 bytes for these items. Item order does not change the bytes."""
    return bytes(PyBIP158([bytearray(item) for item in items]).GetEncoded())


def filter_matches(encoded: bytes, item: bytes) -> bool:
    """True when `item` may be in the filter. A hit still has to be checked in the block."""
    return bool(PyBIP158(bytearray(encoded)).Match(bytearray(item)))


def committed_filter(puzzle_hashes: Collection[bytes], spent_names: Collection[bytes]) -> bytes:
    """The filter a transaction block already commits.

    Created coins are listed by puzzle hash, including reward coins. Spent coins are
    listed by coin name. The block stores the hash of these bytes.
    """
    return encode_bip158([*puzzle_hashes, *spent_names])


def hint_filter(hints: Collection[bytes]) -> bytes:
    """Hints are not in the committed filter. A wallet checks this second filter for them."""
    return encode_bip158([hint for hint in hints if len(hint) > 0])


def filter_request_rejection(start: int, end: int, peak_height: int | None) -> str | None:
    """None when the inclusive range can be answered. A bad range is refused, not filled with gaps."""
    if peak_height is None or start < 0 or end < start or end > peak_height:
        return FILTERS_OUTSIDE_CHAIN
    if end - start + 1 > FILTER_PAGE:
        return FILTERS_TOO_WIDE
    return None


def one_height_filter(
    is_transaction_block: bool, committed: bytes | None, hints: bytes | None
) -> tuple[bytes | None, bytes | None] | str:
    """A non-transaction block has no filters. A transaction block with no stored bytes is a refusal.

    Returning two empty values for a transaction block would look like a block with no coins.
    """
    if not is_transaction_block:
        return (None, None)
    if committed is None or hints is None:
        return FILTERS_NOT_STORED
    return (committed, hints)


def address_hints(additions: Collection[tuple[Coin, bytes | None]]) -> list[tuple[Coin, bytes]]:
    """Create-coin memos that are 32 bytes. Those are the addresses a hint filter can match.

    A missing or shorter memo is not an address. The wallet still has to read the spend
    before one of these pairs becomes a coin it keeps.
    """
    found: list[tuple[Coin, bytes]] = []
    for coin, hint in additions:
        if hint is not None and len(hint) == 32:
            found.append((coin, bytes32(hint)))
    return found
