from __future__ import annotations

from pathlib import Path

import pytest
from rocksdict import Options, Rdict

from chia.full_node.db.chain_db import ChainDB
from chia.full_node.db.keys import CF_HINTS_BY_COIN, CF_HINTS_BY_HINT, COLUMN_FAMILIES
from chia.full_node.db.rocks import RocksBackend


def test_the_chain_database_does_not_keep_a_hint_list() -> None:
    assert CF_HINTS_BY_COIN not in COLUMN_FAMILIES
    assert CF_HINTS_BY_HINT not in COLUMN_FAMILIES


@pytest.mark.anyio
async def test_an_old_hint_list_is_dropped_and_a_new_database_does_not_grow_one(tmp_path: Path) -> None:
    old = tmp_path / "old.rocksdb"
    options = Options(raw_mode=True)
    options.create_if_missing(True)
    options.create_missing_column_families(True)
    family = Options(raw_mode=True)
    db = Rdict(
        str(old),
        options=options,
        column_families={"default": family, CF_HINTS_BY_COIN: family, CF_HINTS_BY_HINT: family},
    )
    db.close()
    chain = ChainDB(RocksBackend(old, sync="OFF"))
    await chain.close()
    names = set(Rdict.list_cf(str(old)))
    assert CF_HINTS_BY_COIN not in names
    assert CF_HINTS_BY_HINT not in names

    fresh = tmp_path / "fresh.rocksdb"
    opened = ChainDB(RocksBackend(fresh, sync="OFF"))
    await opened.close()
    created = set(Rdict.list_cf(str(fresh)))
    assert created.isdisjoint({CF_HINTS_BY_COIN, CF_HINTS_BY_HINT})
    assert set(COLUMN_FAMILIES).issubset(created)
