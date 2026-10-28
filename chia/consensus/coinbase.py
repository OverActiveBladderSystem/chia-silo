from __future__ import annotations

from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.types.blockchain_format.coin import Coin


def pool_parent_id(block_height: uint32, genesis_challenge: bytes32) -> bytes32:
    return bytes32(genesis_challenge[:16] + block_height.to_bytes(16, "big"))


def farmer_parent_id(block_height: uint32, genesis_challenge: bytes32) -> bytes32:
    return bytes32(genesis_challenge[16:] + block_height.to_bytes(16, "big"))


def is_reward_parent(parent_id: bytes32, genesis_challenge: bytes32) -> bool:
    """True for a farmer or pool reward parent. The height inside it is the earned block, not the inclusion block."""
    raw = bytes(parent_id)
    return raw[:16] == bytes(genesis_challenge[:16]) or raw[:16] == bytes(genesis_challenge[16:])


def create_pool_coin(block_height: uint32, puzzle_hash: bytes32, reward: uint64, genesis_challenge: bytes32) -> Coin:
    parent_id = pool_parent_id(block_height, genesis_challenge)
    return Coin(parent_id, puzzle_hash, reward)


def create_farmer_coin(block_height: uint32, puzzle_hash: bytes32, reward: uint64, genesis_challenge: bytes32) -> Coin:
    parent_id = farmer_parent_id(block_height, genesis_challenge)
    return Coin(parent_id, puzzle_hash, reward)
