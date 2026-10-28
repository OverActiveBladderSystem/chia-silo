from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from chia_rs.sized_ints import uint32, uint64

from chia._tests.blockchain.blockchain_test_utils import _validate_and_add_block
from chia._tests.blockchain.test_rocks_mempool_flow import _reward
from chia._tests.blockchain.test_rocks_reorg import _assert_coin_table_matches_peak
from chia._tests.conftest import ConsensusMode
from chia._tests.util.blockchain import create_blockchain
from chia.consensus.blockchain import Blockchain, coin_spent_at_height
from chia.consensus.generator_tools import get_block_header
from chia.full_node.db.block_filter import one_height_filter
from chia.full_node.db.coin_store import RocksCoinStore
from chia.full_node.full_node_api import FullNodeAPI
from chia.full_node.full_node_rpc_api import FullNodeRpcApi
from chia.rpc.rpc_errors import RpcError, RpcErrorCodes
from chia.protocols.protocol_message_types import ProtocolMessageTypes
from chia.protocols.wallet_protocol import RequestBlockFilters, RespondBlockFilters
from chia.simulator.block_tools import BlockTools
from chia.simulator.wallet_tools import WalletTool
from chia.util.hash import std_hash


def _store(blockchain: Blockchain) -> RocksCoinStore:
    store = blockchain.coin_store
    assert isinstance(store, RocksCoinStore)
    return store


@pytest.mark.anyio
@pytest.mark.limit_consensus_modes(reason="the sqlite suite already covers every consensus mode")
async def test_filters_and_puzzle_lookup_follow_every_block(
    bt: BlockTools,
    consensus_mode: ConsensusMode,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every transaction block keeps a filter. After the window, the coin-height RPC refuses."""
    del consensus_mode
    monkeypatch.setattr("chia.full_node.db.coin_store.REORG_WINDOW", 3)
    async with create_blockchain(bt.constants, 2, engine="rocksdb", root_path=tmp_path) as (blockchain, wrapper):
        assert wrapper is None
        wallet = WalletTool(blockchain.constants)
        farmer = wallet.get_new_puzzlehash()
        blocks = bt.get_consecutive_blocks(12, farmer_reward_puzzle_hash=farmer)
        for block in blocks:
            await _validate_and_add_block(blockchain, block)
        store = _store(blockchain)
        saw_tx = False
        for block in blocks:
            height = int(block.height)
            if not block.is_transaction_block():
                assert one_height_filter(False, None, None) == (None, None)
                continue
            saw_tx = True
            foliage = block.foliage_transaction_block
            assert foliage is not None
            committed = await store.get_block_filter(height)
            hints = await store.get_hint_filter(height)
            stored = one_height_filter(True, committed, hints)
            assert isinstance(stored, tuple)
            assert stored[0] is not None and std_hash(stored[0]) == foliage.filter_hash
            tx_additions, removal_names = await blockchain.tx_additions_and_removals(block)
            header = get_block_header(block, (removal_names, tx_additions))
            assert std_hash(header.transactions_filter) == foliage.filter_hash
        assert saw_tx
        peak = blockchain.get_peak()
        assert peak is not None
        api = FullNodeAPI(SimpleNamespace(blockchain=blockchain, coin_store=store))  # type: ignore[arg-type]
        end = min(2, int(peak.height))
        msg = await api.request_block_filters(RequestBlockFilters(uint32(0), uint32(end)))
        assert msg is not None
        assert int(msg.type) != int(ProtocolMessageTypes.reject_block_filters.value)
        page = RespondBlockFilters.from_bytes(msg.data)
        assert len(page.filters) == end + 1

        reward, _timestamp = _reward(blocks, farmer)
        created = await store.creation_height(reward.name())
        assert created is not None
        bundle = wallet.generate_signed_transaction(uint64(1_000), wallet.get_new_puzzlehash(), reward)
        child = bundle.additions()[0]
        spent_blocks = bt.get_consecutive_blocks(
            1,
            blocks,
            farmer_reward_puzzle_hash=farmer,
            transaction_data=bundle,
            guarantee_transaction_block=True,
        )
        await _validate_and_add_block(blockchain, spent_blocks[-1])
        spend_block = spent_blocks[-1]
        record = await store.get_coin_record(reward.name())
        assert record is not None and record.spent
        assert coin_spent_at_height(record, None, spend_block.height) == reward
        states = await store.get_coin_states_by_ids(True, [reward.name()])
        assert states == [record.coin_state]

        chain = spent_blocks
        floor = await store.undo_floor()
        while floor is None or floor < int(spend_block.height):
            chain = bt.get_consecutive_blocks(
                1, chain, farmer_reward_puzzle_hash=farmer, guarantee_transaction_block=True
            )
            await _validate_and_add_block(blockchain, chain[-1])
            floor = await store.undo_floor()
            assert int(chain[-1].height) < 40
        assert await store.get_coin_record(reward.name()) is None
        assert await store.closed_spend(reward.name()) is None
        api = FullNodeRpcApi(
            SimpleNamespace(
                blockchain=blockchain,
                block_store=blockchain.block_store,
                coin_store=blockchain.coin_store,
                constants=blockchain.constants,
            )
        )
        with pytest.raises(RpcError) as refused:
            await api.get_additions_and_removals({"header_hash": spend_block.header_hash.hex()})
        assert refused.value.error_code == RpcErrorCodes.COIN_HISTORY_NOT_KEPT.value
        assert str(int(spend_block.height)) in refused.value.message
        assert "get_block" in refused.value.message
        assert refused.value.data["undo_floor"] == int(floor)
        recent = await api.get_additions_and_removals({"header_hash": chain[-1].header_hash.hex()})
        assert "additions" in recent and "removals" in recent
        removed = await blockchain.coin_removed_in_block(reward.name(), int(spend_block.height))
        assert removed == reward
        assert coin_spent_at_height(None, removed, spend_block.height) == reward
        added, removed_map = await blockchain.coins_in_block(spend_block)
        assert child in added
        assert removed_map[reward.name()] == reward
        await _assert_coin_table_matches_peak(blockchain)
