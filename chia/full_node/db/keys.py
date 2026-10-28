from __future__ import annotations

# Column families for the full-node chain database. "default" is required by RocksDB.
CF_DEFAULT = "default"
CF_META = "meta"
CF_COINS = "coins"
# Names kept only so an older database can drop these families on open.
# A Silo database does not store an address index, a height index of every coin,
# or a hint list. Those answers come from the block and the filters.
CF_COINS_BY_CONFIRMED = "coins_by_confirmed"
CF_COINS_BY_SPENT = "coins_by_spent"
CF_COINS_BY_PUZZLE_CONFIRMED = "coins_by_puzzle_confirmed"
CF_COINS_BY_PUZZLE_SPENT = "coins_by_puzzle_spent"
CF_COINS_BY_PARENT = "coins_by_parent"
CF_COIN_DELTA = "coin_delta"
CF_FF_UNSPENT = "ff_unspent"
# No longer written. Older databases may still contain these rows until cleanup.
# A question about a block older than the one-day mark does not use them.
CF_CLOSED_SPEND = "closed_spend"
CF_BLOCK_BLOBS = "block_blobs"
CF_BLOCK_META = "block_meta"
CF_BLOCKS_AT_HEIGHT = "blocks_at_height"
CF_MAIN_CHAIN = "main_chain"
CF_UNCOMPACTIFIED = "uncompactified"
CF_SES = "ses"
CF_HINTS_BY_COIN = "hints_by_coin"
CF_HINTS_BY_HINT = "hints_by_hint"
# Exact timestamp of each transaction block, keyed by height.
CF_HEIGHT_TIME = "height_time"
# Committed transaction-block filter, and the hint filter the node builds beside it.
CF_BLOCK_FILTER = "block_filter"
CF_HINT_FILTER = "hint_filter"

COLUMN_FAMILIES: tuple[str, ...] = (
    CF_DEFAULT,
    CF_META,
    CF_COINS,
    CF_COIN_DELTA,
    CF_FF_UNSPENT,
    CF_CLOSED_SPEND,
    CF_BLOCK_BLOBS,
    CF_BLOCK_META,
    CF_BLOCKS_AT_HEIGHT,
    CF_MAIN_CHAIN,
    CF_UNCOMPACTIFIED,
    CF_SES,
    CF_HEIGHT_TIME,
    CF_BLOCK_FILTER,
    CF_HINT_FILTER,
)

# Opened only to delete them. A new database never creates these.
RETIRED_FAMILIES: tuple[str, ...] = (
    CF_COINS_BY_PARENT,
    CF_COINS_BY_CONFIRMED,
    CF_COINS_BY_SPENT,
    CF_COINS_BY_PUZZLE_CONFIRMED,
    CF_COINS_BY_PUZZLE_SPENT,
    CF_HINTS_BY_COIN,
    CF_HINTS_BY_HINT,
)

# Families whose values are already compressed, or are tiny flags.
UNCOMPRESSED_FAMILIES = frozenset(
    {
        CF_DEFAULT,
        CF_BLOCK_BLOBS,
        CF_META,
        CF_MAIN_CHAIN,
        CF_UNCOMPACTIFIED,
        CF_HEIGHT_TIME,
        # Live coin rows are 4-byte heights. Compressing them costs CPU and saves nothing.
        CF_COINS,
        # Closed spends are the same 8-byte height pair. Compression saves nothing.
        CF_CLOSED_SPEND,
    }
)

META_FORMAT = b"db_format"
META_FORMAT_VALUE = b"silo-v4"
META_SCHEMA_VERSION = b"schema_version"
# Unspent rows are creation heights. Spent rows are 8-byte marks. 4 refuses an older Silo folder.
SHORT_COIN_SCHEMA = 4
META_UNSPENT = b"unspent_count"
# Obsolete. Fast-forward rows are written in the creating block. index_pending
# does not write this key. A leftover value is ignored.
META_COIN_INDEXED = b"coin_indexed_height"
# Spent marks are kept for this many blocks, then deleted. A fork older than that is
# rebuilt from the stored blocks. 4608 blocks is one day at the 18.75 second target.
REORG_WINDOW = 4608
META_UNDO_FLOOR = b"undo_floor"
META_COMPLETE = b"complete"
META_PEAK = b"peak"
META_COMPACT_COUNT = b"compact_count"
META_UNCOMPACT_COUNT = b"uncompact_count"
META_HINT_COUNT = b"hint_count"
# Left by the removed SQLite copy. A phase other than missing or "complete"
# means that folder was never finished and must not be opened.
META_MIGRATE_PHASE = b"migrate_phase"
META_MIGRATE_SOURCE = b"migrate_source"
# Set after the chain snapshot has stored every main-chain sub-epoch summary.
# Missing means step 8 must walk block records instead of this short list.
META_SES_INDEX_READY = b"ses_index_ready"
SES_INDEX_PREFIX = b"sesh"

SQLITE_SUFFIX = ".sqlite"
ROCKS_SUFFIX = ".rocksdb"


def needs_silo_format(existing: bytes | None) -> bool:
    """True when the caller should write the Silo format. An older store is refused.

    The folder itself comes from config.yaml. This only rejects a file whose marker
    says it still holds the full coin record.
    """
    if existing is None:
        return True
    if existing == META_FORMAT_VALUE:
        return False
    raise RuntimeError(
        "This database is an older Silo coin store. "
        "A live coin is a 4-byte creation height, and a spent coin is an 8-byte mark kept for one day. "
        "Set database_path in config.yaml to a new folder."
    )


def schema_version_bytes() -> bytes:
    return SHORT_COIN_SCHEMA.to_bytes(4, "little", signed=False)


def height_time_key(height: int) -> bytes:
    return u32_be(height)


def encode_block_time(timestamp: int) -> bytes:
    """Exact seconds. Time locks compare this number, so it is not rounded."""
    if timestamp < 0 or timestamp > 0xFFFFFFFFFFFFFFFF:
        raise ValueError(f"block time {timestamp} does not fit in 8 bytes")
    return int(timestamp).to_bytes(8, "little", signed=False)


def decode_block_time(raw: bytes) -> int:
    if len(raw) != 8:
        raise ValueError(f"block time is {len(raw)} bytes")
    return int.from_bytes(raw, "little", signed=False)


def ses_index_key(height: int) -> bytes:
    return SES_INDEX_PREFIX + u32_be(height)


def u32_be(value: int) -> bytes:
    return int(value).to_bytes(4, "big", signed=False)


def u32_from_be(raw: bytes) -> int:
    return int.from_bytes(raw, "big", signed=False)


def prefix_end(prefix: bytes) -> bytes | None:
    """Exclusive end key for every key that starts with `prefix`. None means unbounded."""
    if not prefix:
        return None
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        return None
    return stripped[:-1] + bytes([stripped[-1] + 1])


def key_after(key: bytes) -> bytes:
    """First bytewise key strictly after `key`."""
    return key + b"\x00"


# Wallet index walks return after this many keys so a block save can use the
# database thread before the next piece of the same request.
INDEX_SCAN_CHUNK = 4096
