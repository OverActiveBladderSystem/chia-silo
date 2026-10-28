from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pytest
from chia_rs import CoinState, G2Element
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint8, uint32, uint64

from chia.consensus.blockchain import AddBlockResult
from chia.types.blockchain_format.coin import Coin
from chia.types.mempool_inclusion_status import MempoolInclusionStatus
from chia.util.bech32m import encode_puzzle_hash
from chia.util.errors import Err
from chia.wallet.conditions import ConditionValidTimes
from chia.wallet.transaction_record import TransactionRecord
from chia.wallet.util.wallet_sync_utils import PeerRequestException
from chia.wallet.wallet_node import WalletNode
from chia.wallet.wallet_spend_bundle import WalletSpendBundle

PUZZLE = bytes32(b"\x31" * 32)
PEER_ID = bytes32(b"\x41" * 32)
OTHER_ID = bytes32(b"\x42" * 32)


def _coin(marker: bytes) -> Coin:
    return Coin(bytes32(marker * 32), PUZZLE, uint64(5))


def _state(marker: bytes) -> CoinState:
    return CoinState(_coin(marker), None, uint32(4))


class _Peer:
    def __init__(self, peer_node_id: bytes32) -> None:
        self.peer_node_id = peer_node_id
        self.closed = False
        self.sent: list[object] = []

    async def send_message(self, msg: object) -> None:
        self.sent.append(msg)


@dataclass
class _ChildrenNode:
    from_peer: list[CoinState] | Exception
    saved: list[CoinState]
    others: list[tuple[_Peer, list[CoinState] | Exception]]
    saved_calls: int = 0
    other_calls: int = 0

    async def _children_from_peer(
        self, coin_name: bytes32, peer: _Peer, fork_height: uint32 | None
    ) -> list[CoinState]:
        if peer.peer_node_id == PEER_ID:
            if isinstance(self.from_peer, Exception):
                raise self.from_peer
            return self.from_peer
        self.other_calls += 1
        for other, answer in self.others:
            if other.peer_node_id == peer.peer_node_id:
                if isinstance(answer, Exception):
                    raise answer
                return answer
        raise AssertionError(f"unexpected peer {peer.peer_node_id.hex()}")

    async def _children_saved_locally(self, coin_name: bytes32) -> list[CoinState]:
        self.saved_calls += 1
        return self.saved

    def _peers_that_keep_coin_lists(self, peer: _Peer) -> list[_Peer]:
        return [other for other, _answer in self.others]


@pytest.mark.anyio
async def test_an_error_asking_for_children_uses_children_saved_from_the_block() -> None:
    child = _state(b"\x01")
    node = _ChildrenNode(PeerRequestException("forgotten"), [child], [])

    found = await WalletNode.fetch_children(node, _coin(b"\x09").name(), _Peer(PEER_ID), None)  # type: ignore[arg-type]

    assert found == [child]
    assert node.saved_calls == 1
    assert node.other_calls == 0


@pytest.mark.anyio
async def test_an_error_with_no_saved_children_is_not_an_empty_child_list() -> None:
    other = _Peer(OTHER_ID)
    node = _ChildrenNode(PeerRequestException("forgotten"), [], [(other, PeerRequestException("also forgotten"))])

    with pytest.raises(PeerRequestException, match="Was not able to obtain children"):
        await WalletNode.fetch_children(node, _coin(b"\x09").name(), _Peer(PEER_ID), None)  # type: ignore[arg-type]

    assert node.other_calls == 1


@pytest.mark.anyio
async def test_a_peer_that_answers_with_no_children_stays_an_empty_list() -> None:
    node = _ChildrenNode([], [], [])

    found = await WalletNode.fetch_children(node, _coin(b"\x09").name(), _Peer(PEER_ID), None)  # type: ignore[arg-type]

    assert found == []
    assert node.saved_calls == 1


@pytest.mark.anyio
async def test_another_node_supplies_children_when_the_first_one_errors() -> None:
    child = _state(b"\x02")
    other = _Peer(OTHER_ID)
    node = _ChildrenNode(PeerRequestException("forgotten"), [], [(other, [child])])

    found = await WalletNode.fetch_children(node, _coin(b"\x09").name(), _Peer(PEER_ID), None)  # type: ignore[arg-type]

    assert found == [child]


def _record(sent_to: list[tuple[str, uint8, str | None]], *, with_bundle: bool) -> TransactionRecord:
    bundle = WalletSpendBundle([], G2Element()) if with_bundle else None
    puzzle_hash = bytes32.zeros
    return TransactionRecord(
        confirmed_at_height=uint32(0),
        created_at_time=uint64(1),
        to_puzzle_hash=puzzle_hash,
        to_address=encode_puzzle_hash(puzzle_hash, "txch"),
        amount=uint64(1),
        fee_amount=uint64(0),
        confirmed=False,
        sent=uint32(len(sent_to)),
        spend_bundle=bundle,
        additions=[],
        removals=[],
        wallet_id=uint32(1),
        sent_to=sent_to,
        trade_id=None,
        type=uint32(1),
        name=bytes32(b"\x71" * 32),
        memos={},
        valid_times=ConditionValidTimes(),
    )


class _Connections:
    def __init__(self, peers: list[_Peer]) -> None:
        self._peers = peers

    def get_connections(self, _node_type: object) -> list[_Peer]:
        return self._peers


class _TxStore:
    def __init__(self, records: list[TransactionRecord]) -> None:
        self.records = records

    async def get_not_sent(self) -> list[TransactionRecord]:
        return self.records


class _RetryNode:
    _shut_down = False

    def __init__(self, peers: list[_Peer], records: list[TransactionRecord]) -> None:
        self._server = object()
        self.server = _Connections(peers)
        manager = type("Manager", (), {"tx_store": _TxStore(records)})()
        self._wallet_state_manager = manager
        self.wallet_state_manager = manager
        self._tx_messages_in_progress: dict[bytes32, list[bytes32]] = {}
        self._tx_message_in_flight = WalletNode._tx_message_in_flight.__get__(self, WalletNode)
        self._send_transaction_message = WalletNode._send_transaction_message.__get__(self, WalletNode)


@pytest.mark.anyio
async def test_a_missing_coin_row_is_sent_again_and_a_double_spend_is_not() -> None:
    peer = _Peer(PEER_ID)
    unknown = (
        PEER_ID.hex(),
        uint8(MempoolInclusionStatus.FAILED),
        Err.UNKNOWN_UNSPENT.name,
    )
    double_spend = (
        PEER_ID.hex(),
        uint8(MempoolInclusionStatus.FAILED),
        Err.DOUBLE_SPEND.name,
    )
    pending = (PEER_ID.hex(), uint8(MempoolInclusionStatus.PENDING), Err.UNKNOWN_UNSPENT.name)
    node = _RetryNode(
        [peer],
        [
            _record([unknown], with_bundle=True),
            _record([double_spend], with_bundle=True),
            _record([pending], with_bundle=True),
            _record([unknown], with_bundle=False),
        ],
    )

    await WalletNode._retry_fee_failed_transactions(node)  # type: ignore[arg-type]

    assert len(peer.sent) == 1


class Continued(Exception):
    pass


@dataclass
class _Header:
    height: int
    prev_header_hash: bytes32
    header_hash: bytes32
    weight: int = 0


@dataclass
class _Chain:
    stored: set[bytes32] = field(default_factory=set)
    peak: _Header | None = None
    added: list[_Header] = field(default_factory=list)
    finished_height: int = 0

    def contains_block(self, header_hash: bytes32) -> bool:
        return header_hash in self.stored

    async def get_peak_block(self) -> _Header | None:
        return self.peak

    async def get_finished_sync_up_to(self) -> uint32:
        return uint32(self.finished_height)

    async def add_block(self, block: _Header) -> tuple[AddBlockResult, None]:
        self.added.append(block)
        return AddBlockResult.NEW_PEAK, None

    async def get_birthday_height(self) -> int:
        raise Continued()


class _Lock:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _SyncNode:
    _shut_down = False
    LONG_SYNC_THRESHOLD = 300
    config = {"use_delta_sync": False}

    def __init__(self, chain: _Chain) -> None:
        self.log = logging.getLogger("test.wallet.short_sync")
        self.wallet_state_manager = type(
            "Manager",
            (),
            {"blockchain": chain, "lock": _Lock()},
        )()
        self.rolled_back: list[int] = []

    async def perform_atomic_rollback(self, fork_height: int) -> None:
        self.rolled_back.append(fork_height)

    async def update_ui(self) -> None:
        raise AssertionError("the wallet was updated")

    async def wallet_short_sync_backtrack(self, *_args: object) -> int | None:
        raise AssertionError("equal weight must not walk backward")


@pytest.mark.anyio
async def test_a_short_sync_that_reaches_an_unfamiliar_genesis_does_not_roll_back() -> None:
    peak = _Header(height=4, prev_header_hash=bytes32(b"\x01" * 32), header_hash=bytes32(b"\x02" * 32))
    chain = _Chain(peak=peak, finished_height=4)
    stranger = _Header(height=0, prev_header_hash=bytes32(b"\x03" * 32), header_hash=bytes32(b"\x04" * 32))
    node = _SyncNode(chain)

    fork = await WalletNode.wallet_short_sync_backtrack(node, peak, stranger, None)  # type: ignore[arg-type]

    assert fork is None
    assert node.rolled_back == []
    assert chain.added == []


@pytest.mark.anyio
async def test_the_first_sync_can_still_start_at_genesis() -> None:
    chain = _Chain(finished_height=0)
    genesis = _Header(height=0, prev_header_hash=bytes32(b"\x05" * 32), header_hash=bytes32(b"\x06" * 32))
    node = _SyncNode(chain)

    fork = await WalletNode.wallet_short_sync_backtrack(node, None, genesis, None)  # type: ignore[arg-type]

    assert fork == 0
    assert node.rolled_back == []
    assert chain.added == [genesis]


@pytest.mark.anyio
async def test_a_block_that_extends_the_saved_peak_is_accepted_after_restart() -> None:
    peak_hash = bytes32(b"\x07" * 32)
    peak = _Header(height=4, prev_header_hash=bytes32(b"\x08" * 32), header_hash=peak_hash)
    chain = _Chain(peak=peak, finished_height=4)
    extension = _Header(height=5, prev_header_hash=peak_hash, header_hash=bytes32(b"\x09" * 32))
    node = _SyncNode(chain)

    fork = await WalletNode.wallet_short_sync_backtrack(node, peak, extension, None)  # type: ignore[arg-type]

    assert fork == 4
    assert node.rolled_back == []
    assert chain.added == [extension]


@pytest.mark.anyio
@pytest.mark.parametrize("weight", [10, 9])
async def test_an_equal_or_lower_peak_must_connect_to_a_stored_header(weight: int) -> None:
    peak_hash = bytes32(b"\x0a" * 32)
    peak = _Header(height=8, prev_header_hash=bytes32(b"\x0b" * 32), header_hash=peak_hash, weight=10)
    chain = _Chain(peak=peak)
    stranger = _Header(height=8, prev_header_hash=bytes32(b"\x0c" * 32), header_hash=bytes32(b"\x0d" * 32), weight=weight)
    node = _SyncNode(chain)

    accepted = await WalletNode.sync_from_untrusted_close_to_peak(node, stranger, None)  # type: ignore[arg-type]

    assert accepted is False
    assert chain.added == []


@pytest.mark.anyio
async def test_an_equal_weight_peak_that_extends_the_saved_peak_is_read() -> None:
    peak_hash = bytes32(b"\x0e" * 32)
    peak = _Header(height=8, prev_header_hash=bytes32(b"\x0f" * 32), header_hash=peak_hash, weight=10)
    chain = _Chain(peak=peak)
    extension = _Header(height=9, prev_header_hash=peak_hash, header_hash=bytes32(b"\x10" * 32), weight=10)
    node = _SyncNode(chain)

    with pytest.raises(Continued):
        await WalletNode.sync_from_untrusted_close_to_peak(node, extension, None)  # type: ignore[arg-type]
