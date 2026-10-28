from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from chia_rs import (
    MEMPOOL_MODE,
    BlockRecord,
    CoinRecord,
    CoinSpend,
    FullBlock,
    PlotParam,
    SpendBundle,
    SpendBundleConditions,
    get_spends_for_trusted_block,
    get_spends_for_trusted_block_with_conditions,
    run_block_generator2,
)
from chia_rs import get_puzzle_and_solution_for_coin2 as get_puzzle_and_solution_for_coin
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64, uint128

from chia.consensus.block_generator_info import block_has_transactions_generator
from chia.consensus.blockchain import Blockchain, BlockchainMutexPriority, coin_spent_at_height
from chia.consensus.coinbase import is_reward_parent
from chia.consensus.get_block_generator import get_block_generator
from chia.consensus.pos_quality import UI_ACTUAL_SPACE_CONSTANT_FACTOR
from chia.full_node.db.block_filter import address_lookup_refusal
from chia.full_node.db.coin_store import RocksCoinStore
from chia.full_node.db.keys import REORG_WINDOW
from chia.full_node.fee_estimator_interface import FeeEstimatorInterface
from chia.full_node.full_node import FullNode
from chia.full_node.hard_fork_utils import get_flags
from chia.protocols.outbound_message import NodeType
from chia.rpc.rpc_errors import RpcError, RpcErrorCodes
from chia.rpc.rpc_server import Endpoint, EndpointResult
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.proof_of_space import calculate_prefix_bits
from chia.types.generator_types import BlockGenerator, NewBlockGenerator
from chia.types.mempool_inclusion_status import MempoolInclusionStatus
from chia.types.unfinished_header_block import UnfinishedHeaderBlock
from chia.util.byte_types import hexstr_to_bytes
from chia.util.log_exceptions import log_exceptions
from chia.util.math import make_monotonically_decreasing
from chia.util.ws_message import WsRpcMessage, create_payload_dict


def coin_record_dict_backwards_compat(coin_record: dict[str, Any]) -> dict[str, bool]:
    coin_record["spent"] = coin_record["spent_block_index"] > 0
    return coin_record


def coin_record_for_listed_coin(
    coin: Coin,
    *,
    block_height: uint32,
    block_timestamp: uint64,
    added_here: bool,
    removed_here: bool,
    stored: CoinRecord | None,
    closed: tuple[int, int] | None,
    birth_timestamp: int | None,
    genesis_challenge: bytes32,
) -> CoinRecord:
    """Record for one coin this block added or removed.

    The stored row is used while it still has both heights. A leftover closed-spend
    row is used the same way. New blocks do not write that row. A coin created and
    spent in this same block has both heights from the block. Any other missing row
    is not given a guessed birth height or a guessed spent flag.
    """
    if stored is not None and (not removed_here or int(stored.spent_block_index) == int(block_height)):
        return stored
    coinbase = is_reward_parent(coin.parent_coin_info, genesis_challenge)
    if closed is not None:
        created, spent = closed
        matches_birth = added_here and created == int(block_height)
        matches_spend = removed_here and spent == int(block_height)
        if matches_birth or matches_spend:
            if created == int(block_height):
                when = int(block_timestamp)
            else:
                when = 0 if birth_timestamp is None else birth_timestamp
            return CoinRecord(coin, uint32(created), uint32(spent), coinbase, uint64(when))
    if added_here and removed_here:
        return CoinRecord(coin, block_height, block_height, coinbase, block_timestamp)
    raise ValueError(f"coin {coin.name().hex()} in block {int(block_height)} has no stored heights")


async def get_nearest_transaction_block(blockchain: Blockchain, block: BlockRecord) -> BlockRecord:
    if block.is_transaction_block:
        return block

    prev_hash = blockchain.height_to_hash(block.prev_transaction_block_height)
    # Genesis block is a transaction block, so theoretically `prev_hash` of all blocks
    # other than genesis block cannot be `None`.
    assert prev_hash

    tb = await blockchain.get_block_record_from_db(prev_hash)
    assert tb

    return tb


async def get_average_block_time(
    blockchain: Blockchain,
    base_block: BlockRecord,
    height_distance: int,
) -> uint32 | None:
    newer_block = await get_nearest_transaction_block(blockchain, base_block)
    if newer_block.height < 1:
        return None

    prev_height = uint32(max(1, newer_block.height - height_distance))
    prev_hash = blockchain.height_to_hash(prev_height)
    assert prev_hash
    prev_block = await blockchain.get_block_record_from_db(prev_hash)
    assert prev_block

    older_block = await get_nearest_transaction_block(blockchain, prev_block)

    assert newer_block.timestamp is not None and older_block.timestamp is not None

    if newer_block.height == older_block.height:  # small chain not long enough to have a block in between
        return None

    average_block_time = uint32(
        (newer_block.timestamp - older_block.timestamp) / (newer_block.height - older_block.height)
    )
    return average_block_time


class FullNodeRpcApi:
    if TYPE_CHECKING:
        from chia.rpc.rpc_server import RpcApiProtocol

        _protocol_check: ClassVar[RpcApiProtocol] = cast("FullNodeRpcApi", None)

    def __init__(self, service: FullNode) -> None:
        self.service = service
        self.service_name = "chia_full_node"
        self.cached_blockchain_state: dict[str, Any] | None = None

    def get_routes(self) -> dict[str, Endpoint]:
        return {
            # Blockchain
            "/get_blockchain_state": self.get_blockchain_state,
            "/get_block": self.get_block,
            "/get_blocks": self.get_blocks,
            "/get_block_count_metrics": self.get_block_count_metrics,
            "/get_block_record_by_height": self.get_block_record_by_height,
            "/get_block_record": self.get_block_record,
            "/get_block_records": self.get_block_records,
            "/get_block_spends": self.get_block_spends,
            "/get_block_spends_with_conditions": self.get_block_spends_with_conditions,
            "/get_unfinished_block_headers": self.get_unfinished_block_headers,
            "/get_network_space": self.get_network_space,
            "/get_additions_and_removals": self.get_additions_and_removals,
            "/get_aggsig_additional_data": self.get_aggsig_additional_data,
            "/get_recent_signage_point_or_eos": self.get_recent_signage_point_or_eos,
            "/get_constants": self.get_constants,
            # Coins
            "/get_coin_records_by_puzzle_hash": self.get_coin_records_by_puzzle_hash,
            "/get_coin_records_by_puzzle_hashes": self.get_coin_records_by_puzzle_hashes,
            "/get_coin_record_by_name": self.get_coin_record_by_name,
            "/get_coin_records_by_names": self.get_coin_records_by_names,
            "/get_coin_records_by_parent_ids": self.get_coin_records_by_parent_ids,
            "/get_coin_records_by_hint": self.get_coin_records_by_hint,
            "/push_tx": self.push_tx,
            "/get_puzzle_and_solution": self.get_puzzle_and_solution,
            # Mempool
            "/get_all_mempool_tx_ids": self.get_all_mempool_tx_ids,
            "/get_all_mempool_items": self.get_all_mempool_items,
            "/get_mempool_item_by_tx_id": self.get_mempool_item_by_tx_id,
            "/get_mempool_items_by_coin_name": self.get_mempool_items_by_coin_name,
            "/create_block_generator": self.create_block_generator,
            # Fee estimation
            "/get_fee_estimate": self.get_fee_estimate,
        }

    async def _state_changed(self, change: str, change_data: dict[str, Any] | None = None) -> list[WsRpcMessage]:
        if change_data is None:
            change_data = {}

        payloads = []
        if change in {"new_peak", "sync_mode"}:
            data = await self.get_blockchain_state({})
            assert data is not None
            payloads.append(
                create_payload_dict(
                    "get_blockchain_state",
                    data,
                    self.service_name,
                    "wallet_ui",
                )
            )
            payloads.append(
                create_payload_dict(
                    "get_blockchain_state",
                    data,
                    self.service_name,
                    "metrics",
                )
            )

        if change in {"block", "signage_point"}:
            payloads.append(create_payload_dict(change, change_data, self.service_name, "metrics"))

        if change == "unfinished_block":
            payloads.append(create_payload_dict(change, change_data, self.service_name, "unfinished_block_info"))

        return payloads

    async def get_blockchain_state(self, _: dict[str, Any]) -> EndpointResult:
        """
        Returns a summary of the node's view of the blockchain.
        """
        node_id = self.service.server.node_id.hex()
        if self.service.initialized is False:
            res = {
                "blockchain_state": {
                    "peak": None,
                    "genesis_challenge_initialized": self.service.initialized,
                    "sync": {
                        "sync_mode": False,
                        "synced": False,
                        "sync_tip_height": 0,
                        "sync_progress_height": 0,
                    },
                    "difficulty": 0,
                    "sub_slot_iters": 0,
                    "space": 0,
                    "average_block_time": None,
                    "mempool_size": 0,
                    "mempool_cost": 0,
                    "mempool_min_fees": {
                        "cost_5000000": 0,
                    },
                    "mempool_max_total_cost": 0,
                    "block_max_cost": 0,
                    "node_id": node_id,
                },
            }
            return res
        peak: BlockRecord | None = self.service.blockchain.get_peak()

        if peak is not None and peak.height > 0:
            difficulty = uint64(peak.weight - self.service.blockchain.block_record(peak.prev_hash).weight)
            sub_slot_iters = peak.sub_slot_iters
        else:
            difficulty = self.service.constants.DIFFICULTY_STARTING
            sub_slot_iters = self.service.constants.SUB_SLOT_ITERS_STARTING

        sync_mode: bool = self.service.sync_store.get_sync_mode() or self.service.sync_store.get_long_sync()

        sync_tip_height: uint32 | None = uint32(0)
        if sync_mode:
            target_peak = self.service.sync_store.target_peak
            if target_peak is not None:
                sync_tip_height = target_peak.height
            if peak is not None:
                sync_progress_height: uint32 = peak.height
                # Don't display we're syncing towards 0, instead show 'Syncing height/height'
                # until sync_store retrieves the correct number.
                if sync_tip_height == uint32(0):
                    sync_tip_height = peak.height
            else:
                sync_progress_height = uint32(0)
        else:
            sync_progress_height = uint32(0)

        average_block_time: uint32 | None = None
        if peak is not None and peak.height > 1:
            newer_block_hex = peak.header_hash.hex()
            # Average over the last day
            header_hash = self.service.blockchain.height_to_hash(uint32(max(1, peak.height - 4608)))
            assert header_hash is not None
            older_block_hex = header_hash.hex()
            space = await self.get_network_space(
                {"newer_block_header_hash": newer_block_hex, "older_block_header_hash": older_block_hex}
            )
            average_block_time = await get_average_block_time(self.service.blockchain, peak, 4608)
        else:
            space = {"space": uint128(0)}

        if self.service.mempool_manager is not None:
            mempool_size = self.service.mempool_manager.mempool.size()
            mempool_cost = self.service.mempool_manager.mempool.total_mempool_cost()
            mempool_fees = self.service.mempool_manager.mempool.total_mempool_fees()
            mempool_min_fee_5m = self.service.mempool_manager.mempool.get_min_fee_rate(5000000)
            mempool_max_total_cost = self.service.mempool_manager.mempool_max_total_cost
        else:
            mempool_size = 0
            mempool_cost = 0
            mempool_fees = 0
            mempool_min_fee_5m = 0
            mempool_max_total_cost = 0
        if self.service.server is not None:
            is_connected = len(self.service.server.get_connections(NodeType.FULL_NODE)) > 0 or "simulator" in str(
                self.service.config.get("selected_network")
            )
        else:
            is_connected = False
        synced = await self.service.synced() and is_connected

        assert space is not None
        response = {
            "blockchain_state": {
                "peak": peak,
                "genesis_challenge_initialized": self.service.initialized,
                "sync": {
                    "sync_mode": sync_mode,
                    "synced": synced,
                    "sync_tip_height": sync_tip_height,
                    "sync_progress_height": sync_progress_height,
                },
                "difficulty": difficulty,
                "sub_slot_iters": sub_slot_iters,
                "space": space["space"],
                "average_block_time": average_block_time,
                "mempool_size": mempool_size,
                "mempool_cost": mempool_cost,
                "mempool_fees": mempool_fees,
                "mempool_min_fees": {
                    # We may give estimates for varying costs in the future
                    # This Dict sets us up for that in the future
                    "cost_5000000": mempool_min_fee_5m,
                },
                "mempool_max_total_cost": mempool_max_total_cost,
                "block_max_cost": self.service.constants.MAX_BLOCK_COST_CLVM,
                "node_id": node_id,
            },
        }
        self.cached_blockchain_state = dict(response["blockchain_state"])
        return response

    async def get_recent_signage_point_or_eos(self, request: dict[str, Any]) -> EndpointResult:
        if "sp_hash" not in request:
            challenge_hash: bytes32 = bytes32.from_hexstr(request["challenge_hash"])
            # This is the case of getting an end of slot
            eos_tuple = self.service.full_node_store.recent_eos.get(challenge_hash)
            if not eos_tuple:
                raise RpcError(
                    RpcErrorCodes.EOS_NOT_IN_CACHE,
                    f"Did not find eos {challenge_hash.hex()} in cache",
                    data={"challenge_hash": challenge_hash.hex()},
                    structured_message="Did not find eos in cache",
                )
            eos, time_received = eos_tuple

            # If it's still in the full node store, it's not reverted
            if self.service.full_node_store.get_sub_slot(eos.challenge_chain.get_hash()):
                return {"eos": eos, "time_received": time_received, "reverted": False}

            # Otherwise we can backtrack from peak to find it in the blockchain
            curr: BlockRecord | None = self.service.blockchain.get_peak()
            if curr is None:
                raise RpcError.simple(RpcErrorCodes.NO_BLOCKS_IN_CHAIN, "No blocks in the chain")

            number_of_slots_searched = 0
            while number_of_slots_searched < 10:
                if curr.first_in_sub_slot:
                    assert curr.finished_challenge_slot_hashes is not None
                    if curr.finished_challenge_slot_hashes[-1] == eos.challenge_chain.get_hash():
                        # Found this slot in the blockchain
                        return {"eos": eos, "time_received": time_received, "reverted": False}
                    number_of_slots_searched += len(curr.finished_challenge_slot_hashes)
                curr = self.service.blockchain.try_block_record(curr.prev_hash)
                if curr is None:
                    # Got to the beginning of the blockchain without finding the slot
                    return {"eos": eos, "time_received": time_received, "reverted": True}

            # Backtracked through 10 slots but still did not find it
            return {"eos": eos, "time_received": time_received, "reverted": True}

        # Now we handle the case of getting a signage point
        sp_hash: bytes32 = bytes32.from_hexstr(request["sp_hash"])
        sp_tuple = self.service.full_node_store.recent_signage_points.get(sp_hash)
        if sp_tuple is None:
            raise RpcError(
                RpcErrorCodes.SP_NOT_IN_CACHE,
                f"Did not find sp {sp_hash.hex()} in cache",
                data={"sp_hash": sp_hash.hex()},
                structured_message="Did not find sp in cache",
            )

        sp, time_received = sp_tuple
        assert sp.rc_vdf is not None, "Not an EOS, the signage point rewards chain VDF must not be None"
        assert sp.cc_vdf is not None, "Not an EOS, the signage point challenge chain VDF must not be None"

        # If it's still in the full node store, it's not reverted
        if self.service.full_node_store.get_signage_point(sp_hash):
            return {"signage_point": sp, "time_received": time_received, "reverted": False}

        # Otherwise we can backtrack from peak to find it in the blockchain
        rc_challenge: bytes32 = sp.rc_vdf.challenge
        next_b: BlockRecord | None = None
        curr_b_optional: BlockRecord | None = self.service.blockchain.get_peak()
        assert curr_b_optional is not None
        curr_b: BlockRecord = curr_b_optional

        for _ in range(200):
            sp_total_iters = sp.cc_vdf.number_of_iterations + curr_b.ip_sub_slot_total_iters(self.service.constants)
            if curr_b.reward_infusion_new_challenge == rc_challenge:
                if next_b is None:
                    return {"signage_point": sp, "time_received": time_received, "reverted": False}
                next_b_total_iters = next_b.ip_sub_slot_total_iters(self.service.constants) + next_b.ip_iters(
                    self.service.constants
                )

                return {
                    "signage_point": sp,
                    "time_received": time_received,
                    "reverted": sp_total_iters > next_b_total_iters,
                }
            if curr_b.finished_reward_slot_hashes is not None:
                assert curr_b.finished_challenge_slot_hashes is not None
                for eos_rc in curr_b.finished_challenge_slot_hashes:
                    if eos_rc == rc_challenge:
                        if next_b is None:
                            return {"signage_point": sp, "time_received": time_received, "reverted": False}
                        next_b_total_iters = next_b.ip_sub_slot_total_iters(self.service.constants) + next_b.ip_iters(
                            self.service.constants
                        )
                        return {
                            "signage_point": sp,
                            "time_received": time_received,
                            "reverted": sp_total_iters > next_b_total_iters,
                        }
            next_b = curr_b
            curr_b_optional = self.service.blockchain.try_block_record(curr_b.prev_hash)
            if curr_b_optional is None:
                break
            curr_b = curr_b_optional

        return {"signage_point": sp, "time_received": time_received, "reverted": True}

    async def get_block(self, request: dict[str, Any]) -> EndpointResult:
        if "header_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_HEADER_HASH_IN_REQUEST, "No header_hash in request")
        header_hash = bytes32.from_hexstr(request["header_hash"])

        block: FullBlock | None = await self.service.block_store.get_full_block(header_hash)
        if block is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_NOT_FOUND,
                f"Block {header_hash.hex()} not found",
                data={"header_hash": header_hash.hex()},
                structured_message="Block not found",
            )

        return {"block": block}

    async def get_blocks(self, request: dict[str, Any]) -> EndpointResult:
        if "start" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_START_IN_REQUEST, "No start in request")
        if "end" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_END_IN_REQUEST, "No end in request")
        exclude_hh = False
        if "exclude_header_hash" in request:
            exclude_hh = request["exclude_header_hash"]
        exclude_reorged = False
        if "exclude_reorged" in request:
            exclude_reorged = request["exclude_reorged"]

        start = int(request["start"])
        end = int(request["end"])
        block_range = []
        for a in range(start, end):
            block_range.append(uint32(a))
        blocks: list[FullBlock] = await self.service.block_store.get_full_blocks_at(block_range)
        json_blocks = []
        for block in blocks:
            hh: bytes32 = block.header_hash
            if exclude_reorged and self.service.blockchain.height_to_hash(block.height) != hh:
                # Don't include forked (reorged) blocks
                continue
            json = block.to_json_dict()
            if not exclude_hh:
                json["header_hash"] = hh.hex()
            json_blocks.append(json)
        return {"blocks": json_blocks}

    async def get_block_count_metrics(self, _: dict[str, Any]) -> EndpointResult:
        compact_blocks = 0
        uncompact_blocks = 0
        with log_exceptions(self.service.log, consume=True):
            compact_blocks = await self.service.block_store.count_compactified_blocks()
            uncompact_blocks = await self.service.block_store.count_uncompactified_blocks()

        hint_count = 0
        if self.service.hint_store is not None:
            with log_exceptions(self.service.log, consume=True):
                hint_count = await self.service.hint_store.count_hints()

        return {
            "metrics": {
                "compact_blocks": compact_blocks,
                "uncompact_blocks": uncompact_blocks,
                "hint_count": hint_count,
            }
        }

    async def get_block_records(self, request: dict[str, Any]) -> EndpointResult:
        if "start" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_START_IN_REQUEST, "No start in request")
        if "end" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_END_IN_REQUEST, "No end in request")

        start = int(request["start"])
        end = int(request["end"])
        records = []
        peak_height = self.service.blockchain.get_peak_height()
        if peak_height is None:
            raise RpcError.simple(RpcErrorCodes.PEAK_IS_NONE, "Peak is None")

        for a in range(start, end):
            if peak_height < uint32(a):
                self.service.log.warning("requested block is higher than known peak ")
                break
            header_hash: bytes32 | None = self.service.blockchain.height_to_hash(uint32(a))
            if header_hash is None:
                raise RpcError(
                    RpcErrorCodes.HEIGHT_NOT_IN_BLOCKCHAIN,
                    f"Height not in blockchain: {a}",
                    data={"height": a},
                    structured_message="Height not in blockchain",
                )
            record: BlockRecord | None = self.service.blockchain.try_block_record(header_hash)
            if record is None:
                # Fetch from DB
                record = await self.service.blockchain.block_store.get_block_record(header_hash)
            if record is None:
                raise RpcError(
                    RpcErrorCodes.BLOCK_DOES_NOT_EXIST,
                    f"Block {header_hash.hex()} does not exist",
                    data={"header_hash": header_hash.hex()},
                    structured_message="Block does not exist",
                )

            records.append(record)
        return {"block_records": records}

    async def get_block_spends(self, request: dict[str, Any]) -> EndpointResult:
        if "header_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_HEADER_HASH_IN_REQUEST, "No header_hash in request")
        header_hash = bytes32.from_hexstr(request["header_hash"])
        full_block: FullBlock | None = await self.service.block_store.get_full_block(header_hash)
        if full_block is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_NOT_FOUND,
                f"Block {header_hash.hex()} not found",
                data={"header_hash": header_hash.hex()},
                structured_message="Block not found",
            )

        block_generator = await get_block_generator(self.service.blockchain.lookup_block_generators, full_block)
        if block_generator is None:  # if block is not a transaction block.
            return {"block_spends": []}

        flags = await get_flags(constants=self.service.constants, blocks=self.service.blockchain, block=full_block)

        spends = await self.service.pool.run_in_loop(
            get_spends_for_trusted_block,
            self.service.constants,
            block_generator.program,
            block_generator.generator_refs,
            flags,
            nice=(5,),
            dedicated=True,
        )

        return cast(EndpointResult, spends)

    async def get_block_spends_with_conditions(self, request: dict[str, Any]) -> EndpointResult:
        if "header_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_HEADER_HASH_IN_REQUEST, "No header_hash in request")
        header_hash = bytes32.from_hexstr(request["header_hash"])
        full_block: FullBlock | None = await self.service.block_store.get_full_block(header_hash)
        if full_block is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_NOT_FOUND,
                f"Block {header_hash.hex()} not found",
                data={"header_hash": header_hash.hex()},
                structured_message="Block not found",
            )

        block_generator = await get_block_generator(self.service.blockchain.lookup_block_generators, full_block)
        if block_generator is None:  # if block is not a transaction block.
            return {"block_spends_with_conditions": []}

        flags = await get_flags(constants=self.service.constants, blocks=self.service.blockchain, block=full_block)
        spends_with_conditions = await self.service.pool.run_in_loop(
            get_spends_for_trusted_block_with_conditions,
            self.service.constants,
            block_generator.program,
            block_generator.generator_refs,
            flags,
            nice=(5,),
            dedicated=True,
        )
        return {"block_spends_with_conditions": spends_with_conditions}

    async def get_block_record_by_height(self, request: dict[str, Any]) -> EndpointResult:
        if "height" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_HEIGHT_IN_REQUEST, "No height in request")
        height = request["height"]
        header_height = uint32(height)
        peak_height = self.service.blockchain.get_peak_height()
        if peak_height is None or header_height > peak_height:
            raise RpcError(
                RpcErrorCodes.BLOCK_HEIGHT_NOT_FOUND,
                f"Block height {height} not found in chain",
                data={"height": height},
                structured_message="Block height not found in chain",
            )
        header_hash: bytes32 | None = self.service.blockchain.height_to_hash(header_height)
        if header_hash is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_HASH_NOT_FOUND,
                f"Block hash {height} not found in chain",
                data={"height": height},
                structured_message="Block hash not found in chain",
            )
        record: BlockRecord | None = self.service.blockchain.try_block_record(header_hash)
        if record is None:
            # Fetch from DB
            record = await self.service.blockchain.block_store.get_block_record(header_hash)
        if record is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_DOES_NOT_EXIST,
                f"Block {header_hash} does not exist",
                data={"header_hash": str(header_hash)},
                structured_message="Block does not exist",
            )
        return {"block_record": record}

    async def get_block_record(self, request: dict[str, Any]) -> EndpointResult:
        if "header_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.HEADER_HASH_NOT_IN_REQUEST, "header_hash not in request")
        header_hash_str = request["header_hash"]
        header_hash = bytes32.from_hexstr(header_hash_str)
        record: BlockRecord | None = self.service.blockchain.try_block_record(header_hash)
        if record is None:
            # Fetch from DB
            record = await self.service.blockchain.block_store.get_block_record(header_hash)
        if record is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_DOES_NOT_EXIST,
                f"Block {header_hash.hex()} does not exist",
                data={"header_hash": header_hash.hex()},
                structured_message="Block does not exist",
            )

        return {"block_record": record}

    async def get_unfinished_block_headers(self, _request: dict[str, Any]) -> EndpointResult:
        peak: BlockRecord | None = self.service.blockchain.get_peak()
        if peak is None:
            return {"headers": []}

        response_headers: list[UnfinishedHeaderBlock] = []
        for block in self.service.full_node_store.get_unfinished_blocks(peak.height):
            unfinished_header_block = UnfinishedHeaderBlock(
                block.finished_sub_slots,
                block.reward_chain_block,
                block.challenge_chain_sp_proof,
                block.reward_chain_sp_proof,
                block.foliage,
                block.foliage_transaction_block,
                b"",
            )
            response_headers.append(unfinished_header_block)
        return {"headers": response_headers}

    async def get_network_space(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves an estimate of total space validating the chain
        between two block header hashes.
        """
        if "newer_block_header_hash" not in request or "older_block_header_hash" not in request:
            raise RpcError.simple(
                RpcErrorCodes.INVALID_NETWORK_SPACE_REQUEST,
                "Invalid request. newer_block_header_hash and older_block_header_hash required",
            )
        newer_block_hex = request["newer_block_header_hash"]
        older_block_hex = request["older_block_header_hash"]

        if newer_block_hex == older_block_hex:
            raise RpcError.simple(RpcErrorCodes.NEW_AND_OLD_MUST_DIFFER, "New and old must not be the same")

        newer_block_bytes = bytes32.from_hexstr(newer_block_hex)
        older_block_bytes = bytes32.from_hexstr(older_block_hex)

        newer_block = await self.service.block_store.get_block_record(newer_block_bytes)
        if newer_block is None:
            # It's possible that the peak block has not yet been committed to the DB, so as a fallback, check memory
            try:
                newer_block = self.service.blockchain.block_record(newer_block_bytes)
            except KeyError:
                raise RpcError(
                    RpcErrorCodes.NEWER_BLOCK_NOT_FOUND,
                    f"Newer block {newer_block_hex} not found",
                    data={"header_hash": newer_block_hex},
                    structured_message="Newer block not found",
                )
        older_block = await self.service.block_store.get_block_record(older_block_bytes)
        if older_block is None:
            raise RpcError(
                RpcErrorCodes.OLDER_BLOCK_NOT_FOUND,
                f"Older block {older_block_hex} not found",
                data={"header_hash": older_block_hex},
                structured_message="Older block not found",
            )
        delta_weight = newer_block.weight - older_block.weight

        # TODO: todo_v2_plots Update this calculation to take v2 plots into
        # account
        plot_filter_size = calculate_prefix_bits(self.service.constants, newer_block.height, PlotParam.make_v1(32))
        delta_iters = newer_block.total_iters - older_block.total_iters
        weight_div_iters = delta_weight / delta_iters
        additional_difficulty_constant = self.service.constants.DIFFICULTY_CONSTANT_FACTOR
        eligible_plots_filter_multiplier = 2**plot_filter_size
        network_space_bytes_estimate = (
            UI_ACTUAL_SPACE_CONSTANT_FACTOR
            * weight_div_iters
            * additional_difficulty_constant
            * eligible_plots_filter_multiplier
        )
        return {"space": uint128(network_space_bytes_estimate)}

    async def get_constants(self, request: dict[str, Any]) -> EndpointResult:
        return {"constants": self.service.constants.to_json_dict()}

    def _refuse_address_index(self) -> None:
        reason = address_lookup_refusal(self.service.coin_store)
        if reason is not None:
            raise RpcError(RpcErrorCodes.UNKNOWN, reason, structured_message=reason)

    async def get_coin_records_by_puzzle_hash(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves the coins for a given puzzlehash, by default returns unspent coins.
        """
        self._refuse_address_index()
        if "puzzle_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.PUZZLE_HASH_NOT_IN_REQUEST, "Puzzle hash not in request")
        kwargs: dict[str, Any] = {"include_spent_coins": False, "puzzle_hash": hexstr_to_bytes(request["puzzle_hash"])}
        if "start_height" in request:
            kwargs["start_height"] = uint32(request["start_height"])
        if "end_height" in request:
            kwargs["end_height"] = uint32(request["end_height"])

        if "include_spent_coins" in request:
            kwargs["include_spent_coins"] = request["include_spent_coins"]

        coin_records = await self.service.coin_store.get_coin_records_by_puzzle_hash(**kwargs)

        return {"coin_records": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in coin_records]}

    async def get_coin_records_by_puzzle_hashes(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves the coins for a given puzzlehash, by default returns unspent coins.
        """
        self._refuse_address_index()
        if "puzzle_hashes" not in request:
            raise RpcError.simple(RpcErrorCodes.PUZZLE_HASHES_NOT_IN_REQUEST, "Puzzle hashes not in request")
        kwargs: dict[str, Any] = {
            "include_spent_coins": False,
            "puzzle_hashes": [hexstr_to_bytes(ph) for ph in request["puzzle_hashes"]],
        }
        if "start_height" in request:
            kwargs["start_height"] = uint32(request["start_height"])
        if "end_height" in request:
            kwargs["end_height"] = uint32(request["end_height"])

        if "include_spent_coins" in request:
            kwargs["include_spent_coins"] = request["include_spent_coins"]

        coin_records = await self.service.coin_store.get_coin_records_by_puzzle_hashes(**kwargs)

        return {"coin_records": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in coin_records]}

    async def get_coin_record_by_name(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves a coin record by its name.
        """
        if "name" not in request:
            raise RpcError.simple(RpcErrorCodes.NAME_NOT_IN_REQUEST, "Name not in request")
        name = bytes32.from_hexstr(request["name"])

        coin_record: CoinRecord | None = await self.service.blockchain.coin_record_for_name(name)
        if coin_record is None:
            raise RpcError(
                RpcErrorCodes.COIN_RECORD_NOT_FOUND,
                f"Coin record 0x{name.hex()} not found",
                data={"name": name.hex()},
                structured_message="Coin record not found",
            )

        return {"coin_record": coin_record_dict_backwards_compat(coin_record.to_json_dict())}

    async def get_coin_records_by_names(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves the coins for given coin IDs, by default returns unspent coins.
        """
        if "names" not in request:
            raise RpcError.simple(RpcErrorCodes.NAMES_NOT_IN_REQUEST, "Names not in request")
        kwargs: dict[str, Any] = {
            "include_spent_coins": False,
            "names": [hexstr_to_bytes(name) for name in request["names"]],
        }
        if "start_height" in request:
            kwargs["start_height"] = uint32(request["start_height"])
        if "end_height" in request:
            kwargs["end_height"] = uint32(request["end_height"])

        if "include_spent_coins" in request:
            kwargs["include_spent_coins"] = request["include_spent_coins"]

        coin_records = await self.service.coin_store.get_coin_records_by_names(**kwargs)
        # An unspent coin older than a day is only a creation height in the table.
        # The block still has the coin. A spent coin older than a day is gone.
        found = {record.name for record in coin_records}
        for raw_name in kwargs["names"]:
            name = bytes32(raw_name)
            if name in found:
                continue
            restored = await self.service.blockchain.coin_record_for_name(name)
            if restored is None:
                continue
            if not kwargs["include_spent_coins"] and restored.spent:
                continue
            if "start_height" in kwargs and restored.confirmed_block_index < kwargs["start_height"]:
                continue
            if "end_height" in kwargs and restored.confirmed_block_index >= kwargs["end_height"]:
                continue
            coin_records.append(restored)

        return {"coin_records": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in coin_records]}

    async def get_coin_records_by_parent_ids(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves the coins for given parent coin IDs, by default returns unspent coins.
        """
        if "parent_ids" not in request:
            raise RpcError.simple(RpcErrorCodes.PARENT_IDS_NOT_IN_REQUEST, "Parent IDs not in request")
        kwargs: dict[str, Any] = {
            "include_spent_coins": False,
            "parent_ids": [hexstr_to_bytes(ph) for ph in request["parent_ids"]],
        }
        if "start_height" in request:
            kwargs["start_height"] = uint32(request["start_height"])
        if "end_height" in request:
            kwargs["end_height"] = uint32(request["end_height"])

        if "include_spent_coins" in request:
            kwargs["include_spent_coins"] = request["include_spent_coins"]

        from chia.consensus.blockchain import ParentHistoryUnavailable

        coin_records = []
        try:
            for parent_id in kwargs["parent_ids"]:
                coin_records.extend(
                    await self.service.blockchain.child_records(
                        bytes32(parent_id),
                        include_spent_coins=kwargs["include_spent_coins"],
                        start_height=kwargs.get("start_height", uint32(0)),
                        end_height=kwargs.get("end_height", uint32((2**32) - 1)),
                    )
                )
        except ParentHistoryUnavailable as exc:
            raise RpcError.simple(
                RpcErrorCodes.COIN_HISTORY_NOT_KEPT,
                str(exc),
                data={"parent_id": exc.parent_id.hex()},
            ) from exc

        return {"coin_records": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in coin_records]}

    async def get_coin_records_by_hint(self, request: dict[str, Any]) -> EndpointResult:
        """
        Retrieves coins by hint, by default returns unspent coins.
        """
        if "hint" not in request:
            raise RpcError.simple(RpcErrorCodes.HINT_NOT_IN_REQUEST, "Hint not in request")

        if address_lookup_refusal(self.service.coin_store) is not None:
            raise RpcError(
                RpcErrorCodes.UNKNOWN,
                "Silo node has no hint list. Hints are read from the block.",
                structured_message="Silo node has no hint list. Hints are read from the block.",
            )
        if self.service.hint_store is None:
            return {"coin_records": []}

        names: list[bytes32] = await self.service.hint_store.get_coin_ids(bytes32.from_hexstr(request["hint"]))

        kwargs: dict[str, Any] = {
            "include_spent_coins": False,
            "names": names,
        }

        if "start_height" in request:
            kwargs["start_height"] = uint32(request["start_height"])
        if "end_height" in request:
            kwargs["end_height"] = uint32(request["end_height"])

        if "include_spent_coins" in request:
            kwargs["include_spent_coins"] = request["include_spent_coins"]

        coin_records = await self.service.coin_store.get_coin_records_by_names(**kwargs)

        return {"coin_records": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in coin_records]}

    async def push_tx(self, request: dict[str, Any]) -> EndpointResult:
        if "spend_bundle" not in request:
            raise RpcError.simple(RpcErrorCodes.SPEND_BUNDLE_NOT_IN_REQUEST, "Spend bundle not in request")

        spend_bundle: SpendBundle = SpendBundle.from_json_dict(request["spend_bundle"])
        spend_name = spend_bundle.name()

        if self.service.mempool_manager.get_spendbundle(spend_name) is not None:
            status = MempoolInclusionStatus.SUCCESS
            error = None
        else:
            status, error = await self.service.add_transaction(spend_bundle, spend_name)
            if status != MempoolInclusionStatus.SUCCESS:
                if self.service.mempool_manager.get_spendbundle(spend_name) is not None:
                    # Already in mempool
                    status = MempoolInclusionStatus.SUCCESS
                    error = None

        if status == MempoolInclusionStatus.FAILED:
            assert error is not None
            raise RpcError(
                RpcErrorCodes.TRANSACTION_FAILED,
                f"Failed to include transaction {spend_name}, error {error.name}",
                data={"spend_name": str(spend_name), "error": error.name},
                structured_message="Failed to include transaction",
            )
        return {
            "status": status.name,
        }

    async def get_puzzle_and_solution(self, request: dict[str, Any]) -> EndpointResult:
        coin_name: bytes32 = bytes32.from_hexstr(request["coin_id"])
        height = uint32(request["height"])
        coin_record = await self.service.blockchain.coin_record_for_name(coin_name)
        removed_here = None
        if coin_record is None:
            removed_here = await self.service.blockchain.coin_removed_in_block(coin_name, int(height))
        spent_coin = coin_spent_at_height(coin_record, removed_here, height)
        if spent_coin is None:
            raise RpcError(
                RpcErrorCodes.INVALID_HEIGHT_FOR_COIN,
                f"Invalid height {height}. coin record {coin_record}",
                data={"height": height},
                structured_message="Invalid height for coin record",
            )

        header_hash = self.service.blockchain.height_to_hash(height)
        assert header_hash is not None
        block: FullBlock | None = await self.service.block_store.get_full_block(header_hash)

        if block is None or not block_has_transactions_generator(block):
            raise RpcError.simple(RpcErrorCodes.INVALID_BLOCK_OR_GENERATOR, "Invalid block or block generator")

        block_generator: BlockGenerator | None = await get_block_generator(
            self.service.blockchain.lookup_block_generators, block
        )
        assert block_generator is not None

        try:
            flags = await get_flags(constants=self.service.constants, blocks=self.service.blockchain, block=block)
            puzzle, solution = get_puzzle_and_solution_for_coin(
                block_generator.program,
                block_generator.generator_refs,
                self.service.constants.MAX_BLOCK_COST_CLVM,
                spent_coin,
                flags,
            )
            return {"coin_solution": CoinSpend(spent_coin, puzzle, solution)}
        except Exception as e:
            raise RpcError(
                RpcErrorCodes.PUZZLE_SOLUTION_FAILED,
                f"Failed to get puzzle and solution for coin {spent_coin}, error: {e}",
                data={"coin": str(spent_coin), "error": str(e)},
                structured_message="Failed to get puzzle and solution for coin",
            ) from e

    async def get_additions_and_removals(self, request: dict[str, Any]) -> EndpointResult:
        if "header_hash" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_HEADER_HASH_IN_REQUEST, "No header_hash in request")
        header_hash = bytes32.from_hexstr(request["header_hash"])

        block: FullBlock | None = await self.service.block_store.get_full_block(header_hash)
        if block is None:
            raise RpcError(
                RpcErrorCodes.BLOCK_NOT_FOUND,
                f"Block {header_hash.hex()} not found",
                data={"header_hash": header_hash.hex()},
                structured_message="Block not found",
            )

        async with self.service.blockchain.priority_mutex.acquire(priority=BlockchainMutexPriority.low):
            if self.service.blockchain.height_to_hash(block.height) != header_hash:
                raise RpcError(
                    RpcErrorCodes.BLOCK_IN_FORK,
                    f"Block at {header_hash.hex()} is no longer in the blockchain (it's in a fork)",
                    data={"header_hash": header_hash.hex()},
                    structured_message="Block is no longer in the blockchain (in a fork)",
                )
            # Past the one-day window the spent mark is gone. Building a coin
            # record from the block alone would report the wrong spent height
            # or the wrong birth height. Refuse instead of returning that.
            floor = await self.service.coin_store.undo_floor()
            if floor is not None and int(block.height) <= int(floor):
                raise RpcError.simple(
                    RpcErrorCodes.COIN_HISTORY_NOT_KEPT,
                    (
                        f"Coin additions and removals for height {int(block.height)} are not kept. "
                        f"This node drops them after {REORG_WINDOW} blocks "
                        f"(dropped through height {int(floor)}). "
                        "An answer from here would show the wrong spent height or the wrong birth height. "
                        "The block itself is still available from get_block."
                    ),
                    data={
                        "header_hash": header_hash.hex(),
                        "height": int(block.height),
                        "undo_floor": int(floor),
                        "window": REORG_WINDOW,
                    },
                )
            if await self.service.blockchain.coin_list_is_in_the_block(int(block.height)):
                added_coins, removed_coins = await self.service.blockchain.coins_in_block(block)
                timestamp = (
                    uint64(0) if block.foliage_transaction_block is None else block.foliage_transaction_block.timestamp
                )
                removed_names = set(removed_coins)
                added_names = {coin.name() for coin in added_coins}
                rocks = self.service.coin_store if isinstance(self.service.coin_store, RocksCoinStore) else None
                genesis = self.service.constants.GENESIS_CHALLENGE
                # One read for every coin in the block. A per-coin lookup runs the
                # birth block again for each output.
                wanted = list(dict.fromkeys([*added_names, *removed_names]))
                stored_by_name = {
                    record.coin.name(): record
                    for record in await self.service.coin_store.get_coin_records(wanted)
                }

                async def listed(coin: Coin, *, added_here: bool, removed_here: bool) -> CoinRecord:
                    stored = stored_by_name.get(coin.name())
                    closed: tuple[int, int] | None = None
                    birth_timestamp: int | None = None
                    needs_closed = stored is None
                    if stored is not None:
                        needs_closed = removed_here and int(stored.spent_block_index) != int(block.height)
                    if rocks is not None and needs_closed:
                        closed = await rocks.closed_spend(coin.name())
                        if closed is not None and closed[0] != int(block.height):
                            birth_timestamp = await rocks.block_timestamp(closed[0])
                    try:
                        return coin_record_for_listed_coin(
                            coin,
                            block_height=block.height,
                            block_timestamp=timestamp,
                            added_here=added_here,
                            removed_here=removed_here,
                            stored=stored,
                            closed=closed,
                            birth_timestamp=birth_timestamp,
                            genesis_challenge=genesis,
                        )
                    except ValueError as exc:
                        raise RpcError.simple(
                            RpcErrorCodes.COIN_HISTORY_NOT_KEPT,
                            str(exc),
                            data={"header_hash": header_hash.hex(), "height": int(block.height)},
                        ) from exc

                additions = [
                    await listed(coin, added_here=True, removed_here=coin.name() in removed_names)
                    for coin in added_coins
                ]
                removals = [
                    await listed(coin, added_here=coin.name() in added_names, removed_here=True)
                    for coin in removed_coins.values()
                ]
            else:
                additions = await self.service.coin_store.get_coins_added_at_height(block.height)
                removals = await self.service.coin_store.get_coins_removed_at_height(block.height)

        return {
            "additions": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in additions],
            "removals": [coin_record_dict_backwards_compat(cr.to_json_dict()) for cr in removals],
        }

    async def get_aggsig_additional_data(self, _: dict[str, Any]) -> EndpointResult:
        return {"additional_data": self.service.constants.AGG_SIG_ME_ADDITIONAL_DATA.hex()}

    async def get_all_mempool_tx_ids(self, _: dict[str, Any]) -> EndpointResult:
        ids = list(self.service.mempool_manager.mempool.all_item_ids())
        return {"tx_ids": ids}

    async def get_all_mempool_items(self, _: dict[str, Any]) -> EndpointResult:
        spends = {}
        for item in self.service.mempool_manager.mempool.all_items():
            spends[item.name.hex()] = item.to_json_dict()
        return {"mempool_items": spends}

    async def get_mempool_item_by_tx_id(self, request: dict[str, Any]) -> EndpointResult:
        if "tx_id" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_TX_ID_IN_REQUEST, "No tx_id in request")
        include_pending: bool = request.get("include_pending", False)
        tx_id: bytes32 = bytes32.from_hexstr(request["tx_id"])

        item = self.service.mempool_manager.get_mempool_item(tx_id, include_pending)
        if item is None:
            raise RpcError(
                RpcErrorCodes.TX_NOT_IN_MEMPOOL,
                f"Tx id 0x{tx_id.hex()} not in the mempool",
                data={"tx_id": tx_id.hex()},
                structured_message="Tx id not in the mempool",
            )

        return {"mempool_item": item.to_json_dict()}

    async def get_mempool_items_by_coin_name(self, request: dict[str, Any]) -> EndpointResult:
        if "coin_name" not in request:
            raise RpcError.simple(RpcErrorCodes.NO_COIN_NAME_IN_REQUEST, "No coin_name in request")

        coin_name: bytes32 = bytes32.from_hexstr(request["coin_name"])
        items = self.service.mempool_manager.mempool.get_items_by_coin_id(coin_name)

        return {"mempool_items": [item.to_json_dict() for item in items]}

    async def create_block_generator(self, _: dict[str, Any]) -> EndpointResult:
        gen = NewBlockGenerator()

        # Grab best transactions from Mempool for given tip target
        async with self.service.blockchain.priority_mutex.acquire(priority=BlockchainMutexPriority.low):
            peak: BlockRecord | None = self.service.blockchain.get_peak()

            if peak is None:
                return {
                    "generator": gen.program,
                    "refs": gen.block_refs,
                    "additions": gen.additions,
                    "removals": gen.removals,
                    "sig": gen.signature,
                    "cost": gen.cost,
                }

            # Finds the last transaction block before this one
            curr_l_tb: BlockRecord = peak
            while not curr_l_tb.is_transaction_block:
                curr_l_tb = self.service.blockchain.block_record(curr_l_tb.prev_hash)

            self.service.log.info("Beginning simulated block construction from mempool")
            start_time = time.monotonic()

            try:
                maybe_gen = self.service.mempool_manager.create_block_generator2(curr_l_tb.header_hash, 2.0)
                if maybe_gen is None:
                    self.service.log.error(f"failed to create block generator, peak: {peak}")
                else:
                    gen = maybe_gen
            except Exception:
                self.service.log.exception(f"Error creating block generator, peak: {peak}")
            self.service.log.info(f"Simulated block constructed in {time.monotonic() - start_time:0.2f} seconds")

            if maybe_gen is not None:
                # this also validates the signature
                err, err_msg, conds = await self.service.pool.run_in_loop(
                    run_block_generator2,
                    bytes(gen.program),
                    gen.generator_refs,
                    self.service.constants.MAX_BLOCK_COST_CLVM,
                    MEMPOOL_MODE,
                    gen.signature,
                    None,
                    self.service.constants,
                    nice=(5,),
                    dedicated=True,
                )
                if err is not None:
                    self.service.log.error(f"failed to validate block: {err} {err_msg}")
                else:
                    assert conds is not None
                    if conds.cost != gen.cost:
                        self.service.log.error(
                            f"invalid cost of generated block: {conds.cost} expected {gen.cost}"
                            f" exe-cost: {conds.execution_cost}"
                            f" cond-cost: {conds.condition_cost}"
                        )
                    # TODO: maybe validate additions and removals too

        return {
            "generator": gen.program,
            "refs": gen.block_refs,
            "additions": gen.additions,
            "removals": gen.removals,
            "sig": gen.signature,
            "cost": gen.cost,
        }

    def _get_spendbundle_type_cost(self, name: str) -> uint64:
        """
        This is a stopgap until we modify the wallet RPCs to get exact costs for created SpendBundles
        before we send them to the Mempool.
        """

        tx_cost_estimates = {
            "send_xch_transaction": 9_401_710,
            "cat_spend": 36_382_111,
            "take_offer": 721_393_265,
            "cancel_offer": 212_443_993,
            "nft_set_nft_did": 115_540_006,
            "nft_transfer_nft": 74_385_541,  # burn or transfer
            "create_new_pool_wallet": 18_055_407,
            "pw_absorb_rewards": 82_668_466,
            "create_new_did_wallet": 57_360_396,
        }
        return uint64(tx_cost_estimates[name])

    async def _validate_fee_estimate_cost(self, request: dict[str, Any]) -> uint64:
        c = 0
        ns = ["spend_bundle", "cost", "spend_type"]
        for n in ns:
            if n in request:
                c += 1
        if c != 1:
            raise RpcError(
                RpcErrorCodes.REQUEST_MUST_CONTAIN_EXACTLY_ONE,
                f"Request must contain exactly one of {ns}",
                data={"options": ns},
                structured_message="Request must contain exactly one of the required options",
            )

        if "spend_bundle" in request:
            spend_bundle = SpendBundle.from_json_dict(request["spend_bundle"])
            conds: SpendBundleConditions = await self.service.mempool_manager.pre_validate_spendbundle(spend_bundle)
            cost = conds.cost
        elif "cost" in request:
            cost = request["cost"]
        else:
            cost = self._get_spendbundle_type_cost(request["spend_type"])
            cost *= request.get("spend_count", 1)
        try:
            return uint64(cost)
        except (ValueError, TypeError) as e:
            raise RpcError(
                RpcErrorCodes.INVALID_COST,
                f"Invalid cost value: {cost!r}",
                data={"cost": str(cost)},
                structured_message="Invalid cost value",
            ) from e

    def _validate_target_times(self, request: dict[str, Any]) -> None:
        if "target_times" not in request:
            raise RpcError.simple(RpcErrorCodes.TARGET_TIMES_REQUIRED, "Request must contain 'target_times' array")
        if any(t < 0 for t in request["target_times"]):
            raise RpcError.simple(
                RpcErrorCodes.TARGET_TIMES_NON_NEGATIVE, "'target_times' array members must be non-negative"
            )

    async def get_fee_estimate(self, request: dict[str, Any]) -> dict[str, Any]:
        self._validate_target_times(request)
        spend_cost = await self._validate_fee_estimate_cost(request)

        target_times: list[int] = request["target_times"]
        estimator: FeeEstimatorInterface = self.service.mempool_manager.mempool.fee_estimator
        target_times.sort()
        estimates = [
            estimator.estimate_fee_rate(time_offset_seconds=time).mojos_per_clvm_cost * spend_cost
            for time in target_times
        ]
        # The Bitcoin Fee Estimator works by observing the most common fee rates that appear
        # at set times into the future. This can lead to situations that users do not expect,
        # such as estimating a higher fee for a longer transaction time.
        estimates = make_monotonically_decreasing(estimates)
        estimates = [uint64(e) for e in estimates]
        current_fee_rate = estimator.estimate_fee_rate(time_offset_seconds=1)
        mempool_size = self.service.mempool_manager.mempool.total_mempool_cost()
        mempool_fees = self.service.mempool_manager.mempool.total_mempool_fees()
        num_mempool_spends = self.service.mempool_manager.mempool.size()
        mempool_max_size = estimator.mempool_max_size()
        blockchain_state = await self.get_blockchain_state({})
        synced = blockchain_state["blockchain_state"]["sync"]["synced"]
        peak = blockchain_state["blockchain_state"]["peak"]

        if peak is None:
            peak_height = uint32(0)
            last_peak_timestamp: uint64 | None = uint64(0)
            last_block_cost = 0
            fee_rate_last_block = 0.0
            last_tx_block_fees = uint64(0)
            last_tx_block_height = 0
        else:
            peak_height = peak.height
            last_peak_timestamp = peak.timestamp
            peak_with_timestamp = peak_height  # Last transaction block height
            last_tx_block = self.service.blockchain.height_to_block_record(peak_with_timestamp)
            while last_peak_timestamp is None:
                peak_with_timestamp -= 1
                last_tx_block = self.service.blockchain.height_to_block_record(peak_with_timestamp)
                last_peak_timestamp = last_tx_block.timestamp

            assert last_tx_block is not None  # mypy
            assert last_peak_timestamp is not None  # mypy
            assert last_tx_block.fees is not None  # mypy

            record = await self.service.blockchain.block_store.get_full_block(last_tx_block.header_hash)

            last_block_cost = 0
            fee_rate_last_block = 0.0
            if record and record.transactions_info and record.transactions_info.cost > 0:
                last_block_cost = record.transactions_info.cost
                fee_rate_last_block = record.transactions_info.fees / record.transactions_info.cost
            last_tx_block_fees = last_tx_block.fees
            last_tx_block_height = last_tx_block.height

        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        utc_timestamp = utc_time.timestamp()

        return {
            "estimates": estimates,
            "target_times": target_times,
            "current_fee_rate": current_fee_rate.mojos_per_clvm_cost,
            "mempool_size": mempool_size,
            "mempool_fees": mempool_fees,
            "num_spends": num_mempool_spends,
            "mempool_max_size": mempool_max_size,
            "full_node_synced": synced,
            "peak_height": peak_height,
            "last_peak_timestamp": last_peak_timestamp,
            "node_time_utc": int(utc_timestamp),
            "last_block_cost": last_block_cost,
            "fees_last_block": last_tx_block_fees,
            "fee_rate_last_block": fee_rate_last_block,
            "last_tx_block_height": last_tx_block_height,
        }
