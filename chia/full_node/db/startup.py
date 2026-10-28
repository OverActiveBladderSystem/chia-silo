from __future__ import annotations

from pathlib import Path

from chia.full_node.db.chain_db import ChainDB
from chia.full_node.db.keys import (
    CF_META,
    META_COMPLETE,
    META_MIGRATE_PHASE,
    META_MIGRATE_SOURCE,
)
from chia.full_node.db.rocks import RocksBackend
from chia.util.config import lock_and_load_config, save_config


async def migration_block_reason(database: ChainDB, path: Path) -> str | None:
    """Return an error string when an already-open RocksDB folder is an unfinished copy."""
    async with database.reader_no_transaction() as view:
        phase = await view.get(CF_META, META_MIGRATE_PHASE)
        complete = await view.get(CF_META, META_COMPLETE)
        source = await view.get(CF_META, META_MIGRATE_SOURCE)
    if complete == b"1" or phase in {None, b"complete"}:
        return None
    phase_name = phase.decode() if phase is not None else "unknown"
    copied_from = ""
    if source:
        try:
            text = source.decode()
        except UnicodeDecodeError:
            text = ""
        if text:
            copied_from = f" It was copied from {text}."
    return (
        f"{path} is an unfinished copy (phase {phase_name}). "
        f"It cannot be opened as a Silo database.{copied_from} "
        "Point database_path at a new folder and sync from the network."
    )


async def unfinished_migration_reason(path: Path) -> str | None:
    """Return an error string when `path` is a RocksDB copy that has not finished."""
    if not (path / "CURRENT").exists():
        return None
    database = ChainDB(RocksBackend(path, sync="OFF"))
    try:
        return await migration_block_reason(database, path)
    finally:
        await database.close()


def persist_full_node_database_path(root_path: Path, database_path: str) -> None:
    with lock_and_load_config(root_path, "config.yaml") as config:
        config["full_node"]["database_path"] = database_path
        save_config(root_path, "config.yaml", config)
