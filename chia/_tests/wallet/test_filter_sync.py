from __future__ import annotations

import pytest
from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32, uint64

from chia.full_node.db.block_filter import committed_filter, hint_filter
from chia.types.blockchain_format.coin import Coin
from chia.util.hash import std_hash
from chia.wallet.filter_sync import (
    ADDRESS_INDEX_REFUSED,
    FilterCoinChange,
    FilterLook,
    OpenedBlock,
    apply_checked_block,
    children_of_spent_coins,
    coin_state_for_known_child,
    committed_filter_matches,
    launcher_coin_if_known,
    nft_mint_height,
    remembered_spent_state,
    settlement_coins_accounted_for,
    singleton_spent_in_this_block,
    confirmed_hinted_coins,
    continuing_filter_sync,
    examine_filters,
    filter_heights_matching_coin,
    filter_page_bounds,
    filter_reorg_rewind_height,
    header_is_known,
    unfamiliar_genesis_must_not_roll_back,
    names_still_held,
    looks_for_filter_page,
    next_filter_height,
    nodes_that_keep_coin_lists,
    removal_names_to_request,
    replaced_height_rolls_back,
    run_filter_walk,
    step_filter_height,
    subscription_error_uses_filters,
    subscription_min_height,
    walk_filters,
)

PUZZLE = bytes32(b"\x22" * 32)
OTHER = bytes32(b"\x33" * 32)
HELD = bytes32(b"\x44" * 32)
OTHER_NAME = bytes32(b"\x55" * 32)


def test_an_empty_filter_node_answer_asks_a_node_that_keeps_the_list() -> None:
    silo = bytes32(b"\x01" * 32)
    regular = bytes32(b"\x02" * 32)
    other_silo = bytes32(b"\x03" * 32)
    filters = {silo, other_silo}
    assert nodes_that_keep_coin_lists(silo, [silo, other_silo, regular], filters) == [regular]
    assert nodes_that_keep_coin_lists(regular, [silo, regular], filters) == []
    assert nodes_that_keep_coin_lists(silo, [silo, other_silo], filters) == []


def test_birthday_and_saved_height_choose_where_reading_starts() -> None:
    assert next_filter_height(synced_to=0, birthday=None) == 0
    assert next_filter_height(synced_to=0, birthday=100) == 100
    assert next_filter_height(synced_to=-1, birthday=10) == 10
    assert next_filter_height(synced_to=500, birthday=100) == 501
    assert next_filter_height(synced_to=50, birthday=100) == 100
    assert next_filter_height(synced_to=100, birthday=100) == 101
    assert subscription_min_height(fork_height=0, use_delta_sync=False, birthday=None) == 0
    assert subscription_min_height(fork_height=0, use_delta_sync=False, birthday=80) == 80
    assert subscription_min_height(fork_height=90, use_delta_sync=True, birthday=80) == 90


def test_an_empty_note_is_not_an_address_index_refusal() -> None:
    assert subscription_error_uses_filters("") is False
    assert subscription_error_uses_filters("peer returned no coin states") is False
    assert subscription_error_uses_filters(f"Error response: {ADDRESS_INDEX_REFUSED}") is True


def test_filters_are_not_kept_and_a_hint_is_not_a_coin_until_the_block_is_read() -> None:
    committed = committed_filter([PUZZLE], [HELD])
    hints = hint_filter([PUZZLE])
    look = examine_filters([PUZZLE, OTHER], [HELD, OTHER_NAME], committed, hints)
    assert look.read_block is True
    assert look.matched_puzzle_hashes == frozenset({PUZZLE})
    assert look.matched_coin_names == frozenset({HELD})
    assert look.untrusted_hints == frozenset({PUZZLE})

    quiet = examine_filters([PUZZLE], [HELD], None, None)
    assert quiet.read_block is False
    assert quiet.matched_puzzle_hashes == frozenset()
    assert quiet.untrusted_hints == frozenset()

    hinted = Coin(bytes32(b"\x01" * 32), OTHER, uint64(1))
    # The hint filter matched, but the block we read did not contain that hint.
    ignored = apply_checked_block([PUZZLE], [], [hinted], [], [])
    assert hinted.name() not in ignored.coin_names
    assert hinted.name() not in ignored.noticed

    verified = apply_checked_block([PUZZLE], [], [hinted], [], [(hinted.name(), PUZZLE)])
    assert hinted.name() in verified.coin_names
    assert hinted.name() in verified.noticed


def test_a_coin_created_and_spent_in_one_block_is_seen_and_not_kept() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    # The coin is not on the watch list until the block is opened, so the spend
    # request has to include the name that the additions just returned.
    assert removal_names_to_request([], [ours]) == [ours.name()]
    checked = apply_checked_block([PUZZLE], [], [ours], removal_names_to_request([], [ours]), [])
    assert ours.name() in checked.noticed
    assert ours.name() not in checked.coin_names


def test_a_synced_filter_node_still_reads_the_next_block() -> None:
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=100, current_height=100) is True
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=0, current_height=0) is True
    # The saved tip was replaced, even by one block. That is a rollback, not a short read.
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=100, current_height=101) is False
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=0, current_height=1) is False
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=99, current_height=101) is False
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=-1, current_height=1) is False
    assert continuing_filter_sync(already_synced=True, filter_peer=True, fork_point=50, current_height=101) is False
    assert continuing_filter_sync(already_synced=False, filter_peer=True, fork_point=100, current_height=101) is False
    assert continuing_filter_sync(already_synced=True, filter_peer=False, fork_point=100, current_height=101) is False
    assert replaced_height_rolls_back(filter_peer=True, fork_point=100, current_height=101) is True
    assert replaced_height_rolls_back(filter_peer=True, fork_point=50, current_height=101) is True
    assert replaced_height_rolls_back(filter_peer=True, fork_point=100, current_height=100) is False
    assert replaced_height_rolls_back(filter_peer=False, fork_point=100, current_height=101) is False

    still_held = apply_checked_block([PUZZLE], [HELD], [], [HELD], [])
    assert HELD not in still_held.coin_names


def test_a_filter_page_is_read_and_the_bytes_are_not_part_of_the_result() -> None:
    committed = committed_filter([PUZZLE], [HELD])
    hints = hint_filter([PUZZLE])
    looks = looks_for_filter_page(
        [PUZZLE],
        [HELD],
        [(4, committed, hints), (5, None, None)],
    )
    assert [height for height, _look in looks] == [4, 5]
    assert looks[0][1].read_block is True
    assert looks[0][1].matched_puzzle_hashes == frozenset({PUZZLE})
    assert looks[0][1].matched_coin_names == frozenset({HELD})
    assert looks[0][1].untrusted_hints == frozenset({PUZZLE})
    assert looks[1][1].read_block is False
    assert not any(isinstance(item, (bytes, bytearray)) for _height, look in looks for item in look.__dict__.values())


def test_a_verified_hint_for_a_coin_the_block_did_not_create_is_ignored() -> None:
    missing = bytes32(b"\x09" * 32)
    checked = apply_checked_block([PUZZLE], [], [], [], [(missing, PUZZLE)])
    assert missing not in checked.coin_names
    assert missing not in checked.noticed


def test_filter_pages_run_from_the_birthday_through_the_target() -> None:
    # 100 through 131 is 32 heights. 132 through 140 is the last 9.
    assert filter_page_bounds(100, 140) == [(100, 131), (132, 140)]
    assert filter_page_bounds(0, 31) == [(0, 31)]
    assert filter_page_bounds(5, 5) == [(5, 5)]
    assert filter_page_bounds(5, 4) == []


def test_the_committed_filter_is_checked_against_the_header_hash() -> None:
    raw = committed_filter([PUZZLE], [HELD])
    assert committed_filter_matches(raw, std_hash(raw))
    assert committed_filter_matches(raw, std_hash(b"other")) is False


def test_a_walk_opens_a_block_only_on_a_hit_and_does_not_keep_the_filter() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    raw = committed_filter([PUZZLE], [])
    quiet_block = OpenedBlock(created=(ours,), spent=(), verified_hints=())
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(1, None, None), (2, raw, hint_filter([]))],
        {1: quiet_block, 2: OpenedBlock(created=(ours,), spent=(), verified_hints=())},
    )
    assert result.opened_heights == (2,)
    assert ours.name() in result.coin_names
    assert ours.name() in result.noticed
    assert result.changes == (FilterCoinChange(ours, 2, None),)
    assert raw.hex() not in repr(result)


def test_a_coin_from_one_height_is_watched_at_the_next_and_a_spend_drops_it() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    created = committed_filter([PUZZLE], [])
    spent = committed_filter([], [ours.name()])
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(4, created, hint_filter([])), (5, spent, hint_filter([])), (6, spent, hint_filter([]))],
        {
            4: OpenedBlock(created=(ours,), spent=(), verified_hints=()),
            5: OpenedBlock(created=(), spent=(ours,), verified_hints=()),
        },
    )
    assert result.opened_heights == (4, 5)
    assert ours.name() not in result.coin_names
    assert ours.name() in result.noticed
    assert result.changes == (
        FilterCoinChange(ours, 4, None),
        FilterCoinChange(ours, 4, 5),
    )


def test_a_spend_the_block_does_not_contain_keeps_the_coin_name() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    raw = committed_filter([], [ours.name()])
    result = walk_filters(
        [PUZZLE],
        [ours.name()],
        {ours.name(): (ours, 1)},
        [(8, raw, hint_filter([]))],
        {8: OpenedBlock(created=(), spent=(), verified_hints=())},
    )
    assert result.opened_heights == (8,)
    assert ours.name() in result.coin_names
    assert result.changes == ()


def test_a_listed_hint_is_kept_only_when_the_spend_created_that_coin() -> None:
    parent = Coin(bytes32(b"\x07" * 32), OTHER, uint64(10))
    child = Coin(parent.name(), bytes32(b"\x66" * 32), uint64(4))
    stranger = Coin(parent.name(), bytes32(b"\x67" * 32), uint64(1))
    produced = [(child.name(), PUZZLE)]
    assert confirmed_hinted_coins([PUZZLE], [(child, PUZZLE), (stranger, PUZZLE)], produced) == ((child, PUZZLE),)
    assert confirmed_hinted_coins([PUZZLE], [(child, PUZZLE)], []) == ()
    assert confirmed_hinted_coins([PUZZLE], [(child, OTHER)], produced) == ()

    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(9, committed_filter([], []), hint_filter([PUZZLE]))],
        {9: OpenedBlock(created=(child,), spent=(), verified_hints=((child.name(), PUZZLE),))},
    )
    assert result.opened_heights == (9,)
    assert child.name() in result.coin_names
    assert child.name() in result.noticed
    assert result.changes == (FilterCoinChange(child, 9, None),)


def test_a_hint_is_not_a_coin_unless_the_opened_block_carries_that_hint() -> None:
    hinted = Coin(bytes32(b"\x01" * 32), OTHER, uint64(1))
    hints = hint_filter([PUZZLE])
    committed = committed_filter([], [])
    untrusted = walk_filters(
        [PUZZLE],
        [],
        {},
        [(3, committed, hints)],
        {3: OpenedBlock(created=(hinted,), spent=(), verified_hints=())},
    )
    assert untrusted.opened_heights == (3,)
    assert hinted.name() not in untrusted.coin_names
    assert hinted.name() not in untrusted.noticed

    trusted = walk_filters(
        [PUZZLE],
        [],
        {},
        [(3, committed, hints)],
        {3: OpenedBlock(created=(hinted,), spent=(), verified_hints=((hinted.name(), PUZZLE),))},
    )
    assert hinted.name() in trusted.coin_names
    assert hinted.name() in trusted.noticed
    assert trusted.changes == (FilterCoinChange(hinted, 3, None),)


def test_a_hinted_coin_created_and_spent_in_one_block_is_not_kept() -> None:
    """A memo coin is learned after the first removal request. Once its spend is known, it is not left unspent."""
    parent = Coin(bytes32(b"\x02" * 32), OTHER, uint64(20))
    hinted = Coin(parent.name(), OTHER, uint64(7))
    payment = Coin(hinted.name(), bytes32(b"\x77" * 32), uint64(7))
    opened = OpenedBlock(
        created=(hinted, payment),
        spent=(parent, hinted),
        verified_hints=((hinted.name(), PUZZLE),),
    )
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(4, committed_filter([], []), hint_filter([PUZZLE]))],
        {4: opened},
    )
    assert hinted.name() in result.noticed
    assert hinted.name() not in result.coin_names
    assert result.changes == (
        FilterCoinChange(hinted, 4, 4),
        FilterCoinChange(payment, 4, None),
    )
    assert dict(children_of_spent_coins(opened))[hinted.name()] == (payment,)


def test_a_pool_singleton_spent_again_in_the_same_block_stays_spent() -> None:
    """The node no longer has the spent row. The block already listed the next spend."""
    parent = Coin(bytes32(b"\x02" * 32), OTHER, uint64(1))
    middle = Coin(parent.name(), OTHER, uint64(1))
    tip = Coin(middle.name(), OTHER, uint64(1))
    known = {parent.name(): (middle,), middle.name(): (tip,)}
    spent = singleton_spent_in_this_block(middle, 8, known)
    assert spent == CoinState(middle, uint32(8), uint32(8))
    assert singleton_spent_in_this_block(tip, 8, known) is None
    assert singleton_spent_in_this_block(middle, 8, None) is None


def test_a_did_launcher_already_read_from_the_block_is_kept() -> None:
    launcher = Coin(bytes32(b"\x03" * 32), OTHER, uint64(1))
    eve = Coin(launcher.name(), PUZZLE, uint64(1))
    parent = CoinState(launcher, uint32(12), uint32(0))
    assert launcher_coin_if_known(parent, launcher.name(), []) == parent
    later = CoinState(eve, None, uint32(20))
    assert launcher_coin_if_known(later, launcher.name(), []) is None
    from_node = CoinState(launcher, None, uint32(12))
    assert launcher_coin_if_known(later, launcher.name(), [from_node]) is None
    spent_on_node = CoinState(launcher, uint32(12), uint32(11))
    assert launcher_coin_if_known(later, launcher.name(), [spent_on_node]) == spent_on_node


def test_an_nft_transfer_keeps_going_when_the_launcher_row_is_gone() -> None:
    launcher = Coin(bytes32(b"\x04" * 32), OTHER, uint64(1))
    previous = Coin(launcher.name(), PUZZLE, uint64(1))
    parent = CoinState(previous, uint32(30), uint32(12))
    assert nft_mint_height(parent, launcher.name(), []) is None
    eve_parent = CoinState(launcher, uint32(12), uint32(0))
    assert nft_mint_height(eve_parent, launcher.name(), []) == 12


def test_a_child_spent_in_the_same_block_keeps_that_spent_height() -> None:
    child = Coin(bytes32(b"\x02" * 32), OTHER, uint64(1))
    still_there = coin_state_for_known_child(child, 4, set())
    assert still_there.created_height == 4
    assert still_there.spent_height is None
    spent = coin_state_for_known_child(child, 4, {child.name()})
    assert spent.created_height == 4
    assert spent.spent_height == 4


def test_a_clawback_child_of_a_same_block_payment_is_kept() -> None:
    """The payment is created and spent here, so it was not on the previous watch list."""
    ephemeral = Coin(bytes32(b"\x11" * 32), PUZZLE, uint64(20))
    clawback = Coin(ephemeral.name(), OTHER, uint64(20))
    stranger = Coin(bytes32(b"\x12" * 32), OTHER, uint64(5))
    grandchild = Coin(stranger.name(), OTHER, uint64(5))
    raw = committed_filter([PUZZLE], [ephemeral.name()])
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(4, raw, hint_filter([]))],
        {
            4: OpenedBlock(
                created=(ephemeral, clawback, stranger, grandchild),
                spent=(ephemeral, stranger),
                verified_hints=(),
            )
        },
    )
    examined = {change.coin.name(): change for change in result.changes}
    assert examined[ephemeral.name()] == FilterCoinChange(ephemeral, 4, 4)
    assert examined[clawback.name()] == FilterCoinChange(clawback, 4, None)
    assert grandchild.name() not in examined


def test_a_coin_created_and_spent_in_one_block_is_not_watched_afterward() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    both = committed_filter([PUZZLE], [ours.name()])
    later = committed_filter([], [ours.name()])
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(2, both, hint_filter([])), (3, later, hint_filter([]))],
        {2: OpenedBlock(created=(ours,), spent=(ours,), verified_hints=())},
    )
    assert result.opened_heights == (2,)
    assert ours.name() in result.noticed
    assert ours.name() not in result.coin_names
    assert result.changes == (FilterCoinChange(ours, 2, 2),)


def test_a_spend_that_creates_nothing_is_still_recorded() -> None:
    parent = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(20))
    opened = OpenedBlock(created=(), spent=(parent,), verified_hints=())
    assert dict(children_of_spent_coins(opened))[parent.name()] == ()
    raw = committed_filter([], [parent.name()])
    step = step_filter_height(
        [PUZZLE],
        [parent.name()],
        {parent.name(): (parent, 1)},
        4,
        raw,
        hint_filter([]),
        opened,
    )
    assert dict(step.spend_children)[parent.name()] == ()
    examined = {change.coin.name(): change for change in step.changes}
    assert examined[parent.name()].spent_height == 4


def test_a_spend_keeps_coins_paid_to_someone_else() -> None:
    parent = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(20))
    ours = Coin(parent.name(), PUZZLE, uint64(5))
    stranger = Coin(parent.name(), bytes32(b"\x33" * 32), uint64(14))
    opened = OpenedBlock(created=(ours, stranger, ours), spent=(parent,), verified_hints=())
    grouped = dict(children_of_spent_coins(opened))
    assert [coin.name() for coin in grouped[parent.name()]] == [ours.name(), stranger.name()]

    raw = committed_filter([PUZZLE], [parent.name()])
    step = step_filter_height(
        [PUZZLE],
        [parent.name()],
        {parent.name(): (parent, 1)},
        4,
        raw,
        hint_filter([]),
        opened,
    )
    assert dict(step.spend_children)[parent.name()] == (ours, stranger)
    examined = {change.coin.name(): change for change in step.changes}
    assert examined[stranger.name()].created_height == 4
    assert examined[stranger.name()].spent_height is None


def test_a_child_paid_to_someone_else_is_watched_when_it_is_spent_later() -> None:
    """The child is not our puzzle hash. Its name still has to be tested on the next block."""
    parent = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(20))
    child = Coin(parent.name(), OTHER, uint64(20))
    created = committed_filter([PUZZLE], [parent.name()])
    spent = committed_filter([], [child.name()])
    result = walk_filters(
        [PUZZLE],
        [parent.name()],
        {parent.name(): (parent, 1)},
        [
            (4, created, hint_filter([])),
            (5, spent, hint_filter([])),
        ],
        {
            4: OpenedBlock(created=(child,), spent=(parent,), verified_hints=()),
            5: OpenedBlock(created=(), spent=(child,), verified_hints=()),
        },
    )
    assert result.opened_heights == (4, 5)
    examined = {change.coin.name(): change for change in result.changes}
    assert examined[child.name()].created_height == 4
    assert examined[child.name()].spent_height == 5
    assert child.name() not in result.coin_names


def test_a_missing_node_row_is_not_filled_from_a_local_unspent_coin() -> None:
    coin = Coin(bytes32(b"\x05" * 32), PUZZLE, uint64(7))
    assert remembered_spent_state(coin, 4, 0) is None
    remembered = remembered_spent_state(coin, 4, 9)
    assert remembered is not None
    assert remembered.created_height == 4
    assert remembered.spent_height == 9


def test_a_settlement_coin_created_in_the_spend_block_still_confirms_the_offer() -> None:
    payment = bytes32(b"\x11" * 32)
    settlement = bytes32(b"\x22" * 32)
    assert settlement_coins_accounted_for([payment, settlement], [payment], [settlement])
    assert settlement_coins_accounted_for([payment, settlement], [payment, settlement], [])
    assert not settlement_coins_accounted_for([payment, settlement], [payment], [])
    assert settlement_coins_accounted_for([], [], [])


def test_a_launcher_name_is_found_in_the_block_that_spent_it() -> None:
    launcher = bytes32(b"\x41" * 32)
    other = bytes32(b"\x42" * 32)
    pages = [
        (1, committed_filter([], []), None),
        (2, committed_filter([], [other]), None),
        (3, committed_filter([], [launcher]), None),
    ]
    assert filter_heights_matching_coin(pages, launcher) == [3]


def test_a_saved_peak_is_a_known_header_after_restart() -> None:
    assert header_is_known(stored=False, matches_saved_peak=True)
    assert header_is_known(stored=True, matches_saved_peak=False)
    assert not header_is_known(stored=False, matches_saved_peak=False)


def test_an_unfamiliar_genesis_does_not_wipe_a_wallet_that_already_has_a_peak() -> None:
    assert unfamiliar_genesis_must_not_roll_back(wallet_has_peak=True)
    assert not unfamiliar_genesis_must_not_roll_back(wallet_has_peak=False)


def test_a_reorg_rewind_never_goes_below_zero() -> None:
    assert filter_reorg_rewind_height(fork_point=9, current_height=10) == 0
    assert filter_reorg_rewind_height(fork_point=100, current_height=1000) == 100
    assert filter_reorg_rewind_height(fork_point=990, current_height=1000) == 744
    assert names_still_held([PUZZLE], {}) == {PUZZLE}


def test_a_filter_hit_does_not_keep_a_coin_the_block_did_not_create() -> None:
    stranger = Coin(bytes32(b"\x08" * 32), OTHER, uint64(9))
    raw = committed_filter([PUZZLE], [])
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(4, raw, hint_filter([]))],
        {4: OpenedBlock(created=(stranger,), spent=(), verified_hints=())},
    )
    assert result.opened_heights == (4,)
    assert stranger.name() not in result.coin_names
    assert stranger.name() not in result.noticed
    assert result.changes == ()


def test_one_coin_matched_by_puzzle_and_hint_is_recorded_once() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    raw = committed_filter([PUZZLE], [])
    result = walk_filters(
        [PUZZLE],
        [],
        {},
        [(4, raw, hint_filter([PUZZLE]))],
        {4: OpenedBlock(created=(ours,), spent=(), verified_hints=((ours.name(), PUZZLE), (ours.name(), PUZZLE)))},
    )
    assert result.changes == (FilterCoinChange(ours, 4, None),)
    assert result.coin_names == frozenset({ours.name()})


def test_a_filter_hit_without_the_block_is_not_an_empty_wallet() -> None:
    raw = committed_filter([PUZZLE], [])
    with pytest.raises(ValueError, match="block was not read"):
        walk_filters([PUZZLE], [], {}, [(4, raw, hint_filter([]))], {})


@pytest.mark.anyio
async def test_the_walk_pages_from_the_saved_height_and_drops_each_page() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    raw = committed_filter([PUZZLE], [])
    pages: list[tuple[int, int]] = []
    opened: list[int] = []

    async def fetch_page(start: int, end: int) -> list[tuple[int, bytes | None, bytes | None]]:
        pages.append((start, end))
        rows: list[tuple[int, bytes | None, bytes | None]] = []
        for height in range(start, end + 1):
            if height == 10:
                rows.append((height, raw, hint_filter([])))
            else:
                rows.append((height, None, None))
        return rows

    async def open_block(height: int, look: FilterLook, committed: bytes) -> OpenedBlock:
        opened.append(height)
        assert committed is raw
        assert look.read_block is True
        return OpenedBlock(created=(ours,), spent=(), verified_hints=())

    result = await run_filter_walk(
        start=0,
        target=40,
        puzzle_hashes=[PUZZLE],
        coin_names=[],
        held={},
        fetch_page=fetch_page,
        open_block=open_block,
    )
    # 0 through 31 is one page of 32. 32 through 40 is the rest.
    assert pages == [(0, 31), (32, 40)]
    assert opened == [10]
    assert result.completed is True
    assert result.synced_through == 40
    assert ours.name() in result.coin_names
    assert raw.hex() not in repr(result)


@pytest.mark.anyio
async def test_a_stopped_walk_does_not_report_the_target_as_finished() -> None:
    stop = False

    async def fetch_page(start: int, end: int) -> list[tuple[int, bytes | None, bytes | None]]:
        nonlocal stop
        stop = True
        return [(height, None, None) for height in range(start, end + 1)]

    async def open_block(height: int, look: FilterLook, committed: bytes) -> OpenedBlock:
        del height, look, committed
        raise AssertionError("a quiet page has no block to open")

    result = await run_filter_walk(
        start=0,
        target=40,
        puzzle_hashes=[PUZZLE],
        coin_names=[],
        held={},
        fetch_page=fetch_page,
        open_block=open_block,
        should_stop=lambda: stop,
    )
    assert result.completed is False
    assert result.synced_through == 31
    assert result.opened_heights == ()


@pytest.mark.anyio
async def test_a_short_filter_page_is_refused() -> None:
    async def fetch_page(start: int, end: int) -> list[tuple[int, bytes | None, bytes | None]]:
        del end
        return [(start, None, None)]

    async def open_block(height: int, look: FilterLook, committed: bytes) -> OpenedBlock:
        del height, look, committed
        raise AssertionError("the short page is refused before a block is opened")

    with pytest.raises(ValueError, match="missing heights"):
        await run_filter_walk(
            start=0,
            target=5,
            puzzle_hashes=[],
            coin_names=[],
            held={},
            fetch_page=fetch_page,
            open_block=open_block,
        )


@pytest.mark.anyio
async def test_a_coin_stored_while_reading_a_block_is_watched_for_a_later_spend() -> None:
    ours = Coin(bytes32(b"\x02" * 32), PUZZLE, uint64(5))
    eve = Coin(bytes32(b"\x61" * 32), bytes32(b"\x62" * 32), uint64(1))
    created = committed_filter([PUZZLE], [])
    spent = committed_filter([], [eve.name()])
    opened: list[int] = []

    async def fetch_page(start: int, end: int) -> list[tuple[int, bytes | None, bytes | None]]:
        rows: list[tuple[int, bytes | None, bytes | None]] = []
        for height in range(start, end + 1):
            if height == 4:
                rows.append((height, created, hint_filter([])))
            elif height == 8:
                rows.append((height, spent, hint_filter([])))
            else:
                rows.append((height, None, None))
        return rows

    async def open_block(height: int, look: FilterLook, committed: bytes) -> OpenedBlock:
        opened.append(height)
        if height == 4:
            return OpenedBlock(created=(ours,), spent=(), verified_hints=())
        assert height == 8
        assert eve.name() in look.matched_coin_names
        return OpenedBlock(created=(), spent=(eve,), verified_hints=())

    async def on_height(
        height: int,
        changes: tuple[FilterCoinChange, ...],
        spend_children: tuple[tuple[bytes32, tuple[Coin, ...]], ...],
    ) -> tuple[bool, list[bytes32], list[bytes32]]:
        del changes, spend_children
        if height == 4:
            return True, [], [eve.name()]
        return True, [], []

    result = await run_filter_walk(
        start=1,
        target=8,
        puzzle_hashes=[PUZZLE],
        coin_names=[],
        held={},
        fetch_page=fetch_page,
        open_block=open_block,
        on_height=on_height,
    )
    assert opened == [4, 8]
    assert FilterCoinChange(eve, None, 8) in result.changes
