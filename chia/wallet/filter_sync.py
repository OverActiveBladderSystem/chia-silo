from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass

from chia_rs import CoinState
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32

from chia.full_node.db.block_filter import ADDRESS_INDEX_REFUSED, FILTER_PAGE, filter_matches
from chia.types.blockchain_format.coin import Coin
from chia.util.hash import std_hash


def subscription_error_uses_filters(message: str) -> bool:
    return ADDRESS_INDEX_REFUSED in message


def subscription_min_height(*, fork_height: int, use_delta_sync: bool, birthday: int | None) -> int:
    """First height to ask an address-index node for.

    A birthday is the earliest block this key could have been paid. Coins before it
    are not requested. An unset birthday leaves the old start unchanged.
    """
    base = fork_height if use_delta_sync else 0
    floor = 0 if birthday is None else birthday
    return max(base, floor)


def removal_names_to_request(known: Collection[bytes32], created: Collection[Coin]) -> list[bytes32]:
    """Coin names to check for a spend. A coin created in this block may be spent here too."""
    ordered: list[bytes32] = []
    seen: set[bytes32] = set()
    for name in known:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    for coin in created:
        name = coin.name()
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def nodes_that_keep_coin_lists(
    asked: bytes32, ordered: Sequence[bytes32], filter_peers: Collection[bytes32]
) -> list[bytes32]:
    """Peers to ask after a filter node reports no coin.

    This node drops a spent note after a day, so an empty answer from it is not proof
    that the coin had no children. A node that still answers address lookups is asked
    next. An empty answer from one of those nodes is left as empty. A filter node is
    not asked again.
    """
    if asked not in filter_peers:
        return []
    skip = set(filter_peers)
    return [peer_id for peer_id in ordered if peer_id not in skip]


def continuing_filter_sync(*, already_synced: bool, filter_peer: bool, fork_point: int, current_height: int) -> bool:
    """A filter node does not push new coins. The next height still has to be read.

    This is an extension: the saved height is still on the chain. A replaced tip
    rolls back through the long sync, including a tip replaced by one block.
    """
    if not filter_peer or not already_synced:
        return False
    return fork_point >= current_height


def replaced_height_rolls_back(*, filter_peer: bool, fork_point: int, current_height: int) -> bool:
    """A filter node does not push the coins a replaced block removed.

    The fork point is the last height still shared with the old peak. Below the
    height this wallet already applied, that height was replaced and has to be
    read again. An address-index node pushes the change itself.
    """
    if not filter_peer:
        return False
    return fork_point < current_height


def header_is_known(*, stored: bool, matches_saved_peak: bool) -> bool:
    """A header we have in memory, or the saved peak after a restart."""
    return stored or matches_saved_peak


def unfamiliar_genesis_must_not_roll_back(*, wallet_has_peak: bool) -> bool:
    """Walking to block 0 without a shared header must not wipe a wallet that already has a peak."""
    return wallet_has_peak


def filter_reorg_rewind_height(*, fork_point: int, current_height: int) -> int:
    """Height to roll back to before filters are read again.

    The fork itself is kept. The rewind is also at most 256 blocks, and it never
    goes below zero. A saved height under 256 used to ask for a negative height.
    """
    return max(0, min(int(fork_point), int(current_height) - 256))


def remembered_spent_state(coin: Coin, created_height: int, spent_height: int) -> CoinState | None:
    """A local row for a coin the node has already forgotten.

    The node keeps an unspent coin. A missing node row is not filled in from a
    local unspent record, because that record is not proof the coin is unspent.
    """
    if spent_height <= 0:
        return None
    return CoinState(coin, uint32(spent_height), uint32(created_height))


def settlement_coins_accounted_for(
    wanted: Collection[bytes32],
    reported: Collection[bytes32],
    created_in_spend_block: Collection[bytes32],
) -> bool:
    """True when every settlement coin was stored, or was created in the spend block.

    A coin created and spent in that block is deleted after a day. Missing it
    from the node is not a failed offer when the block still shows it.
    """
    return set(wanted) <= (set(reported) | set(created_in_spend_block))


def names_still_held(watched_names: Collection[bytes32], held: Mapping[bytes32, tuple[Coin, int]]) -> set[bytes32]:
    """Coin ids to test on the next filter.

    A coin this wallet just stored is included even when its puzzle hash is not one
    of ours. Otherwise a later spend of that coin is never opened.
    """
    names = set(watched_names)
    names.update(held)
    return names


def next_filter_height(*, synced_to: int, birthday: int | None) -> int:
    """First filter to read.

    `synced_to` is the last height already applied. Zero means nothing is applied yet.
    A birthday is a floor: a later one skips older filters, which hides older coins.
    """
    floor = 0 if birthday is None else birthday
    if synced_to <= 0:
        return floor
    return max(synced_to + 1, floor)


@dataclass(frozen=True)
class FilterLook:
    """What one height's filters ask for. The filter bytes are not kept."""

    read_block: bool
    matched_puzzle_hashes: frozenset[bytes32]
    matched_coin_names: frozenset[bytes32]
    untrusted_hints: frozenset[bytes32]


def filter_heights_matching_coin(
    pages: Iterable[tuple[int, bytes | None, bytes | None]],
    coin_name: bytes32,
) -> list[int]:
    """Heights whose committed filter may contain this coin name.

    A launcher spent more than a day ago is not a coin row anymore. Its name is
    still in the filter of the block that spent it. A filter can false-match,
    so the caller still reads that block before trusting the height.
    """
    hits: list[int] = []
    for height, committed, _hints in pages:
        if committed is not None and filter_matches(committed, coin_name):
            hits.append(height)
    return hits


def examine_filters(
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    committed: bytes | None,
    hints: bytes | None,
) -> FilterLook:
    """Test our addresses and the coin names we still hold.

    A committed-filter hit on an address may be a new coin. A hit on a coin name may
    be a spend. A hint-filter hit is not a coin until the block is checked.
    A height with no filters is not a transaction block.
    """
    matched_puzzle_hashes: set[bytes32] = set()
    matched_coin_names: set[bytes32] = set()
    untrusted_hints: set[bytes32] = set()
    if committed is not None:
        for puzzle_hash in puzzle_hashes:
            if filter_matches(committed, puzzle_hash):
                matched_puzzle_hashes.add(puzzle_hash)
        for name in coin_names:
            if filter_matches(committed, name):
                matched_coin_names.add(name)
    if hints is not None:
        for puzzle_hash in puzzle_hashes:
            if filter_matches(hints, puzzle_hash):
                untrusted_hints.add(puzzle_hash)
    read_block = bool(matched_puzzle_hashes or matched_coin_names or untrusted_hints)
    return FilterLook(
        read_block=read_block,
        matched_puzzle_hashes=frozenset(matched_puzzle_hashes),
        matched_coin_names=frozenset(matched_coin_names),
        untrusted_hints=frozenset(untrusted_hints),
    )


def _hint_is_ours(hint: bytes, puzzle_hashes: set[bytes32]) -> bool:
    return len(hint) == 32 and bytes32(hint) in puzzle_hashes


def confirmed_hinted_coins(
    puzzle_hashes: Collection[bytes32],
    candidates: Collection[tuple[Coin, bytes]],
    produced: Collection[tuple[bytes32, bytes]],
) -> tuple[tuple[Coin, bytes], ...]:
    """Keep a listed hint only when a spend we read created that coin with that hint.

    The hint also has to be one of our addresses. A memo for someone else is ignored,
    and a list that the spend does not contain is ignored.
    """
    watched = {bytes32(puzzle_hash) for puzzle_hash in puzzle_hashes}
    created = {(name, bytes(hint)) for name, hint in produced}
    kept: list[tuple[Coin, bytes]] = []
    seen: set[bytes32] = set()
    for coin, hint in candidates:
        if not _hint_is_ours(hint, watched) or (coin.name(), bytes(hint)) not in created:
            continue
        if coin.name() in seen:
            continue
        seen.add(coin.name())
        kept.append((coin, bytes(hint)))
    return tuple(kept)


@dataclass(frozen=True)
class CheckedBlock:
    """Coins still watched, and every coin we cared about in the block.

    `noticed` includes a coin that was created and spent in this same block.
    `coin_names` does not, because that coin is not still unspent.
    """

    coin_names: frozenset[bytes32]
    noticed: frozenset[bytes32]


def looks_for_filter_page(
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    filters: Collection[tuple[int, bytes | None, bytes | None]],
) -> list[tuple[int, FilterLook]]:
    """Apply one page and return only the hits. The caller drops the filter bytes."""
    return [
        (height, examine_filters(puzzle_hashes, coin_names, committed, hints)) for height, committed, hints in filters
    ]


def apply_checked_block(
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    created: Collection[Coin],
    spent: Collection[bytes32],
    verified_hints: Collection[tuple[bytes32, bytes]],
) -> CheckedBlock:
    """Update the watch list from a block that was actually read.

    A hint adds a coin only when that hint is in `verified_hints` and the coin was
    created in this block. Filter hits alone do not add coins.
    """
    watched = set(puzzle_hashes)
    names = set(coin_names)
    noticed: set[bytes32] = set()
    created_by_name = {coin.name(): coin for coin in created}
    for coin in created:
        if coin.puzzle_hash in watched:
            names.add(coin.name())
            noticed.add(coin.name())
    for coin_id, hint in verified_hints:
        coin = created_by_name.get(coin_id)
        if coin is None or not _hint_is_ours(hint, watched):
            continue
        names.add(coin.name())
        noticed.add(coin.name())
    for name in spent:
        names.discard(name)
    return CheckedBlock(coin_names=frozenset(names), noticed=frozenset(noticed))


def filter_page_bounds(start: int, target: int) -> list[tuple[int, int]]:
    """Inclusive pages from the first filter through the target. Each page is at most 32 heights."""
    if start > target:
        return []
    pages: list[tuple[int, int]] = []
    cursor = start
    while cursor <= target:
        end = min(target, cursor + FILTER_PAGE - 1)
        pages.append((cursor, end))
        cursor = end + 1
    return pages


def committed_filter_matches(committed: bytes, filter_hash: bytes32) -> bool:
    """The header stores the hash of the committed filter. A mismatch is a bad reply, not an empty block."""
    return std_hash(committed) == filter_hash


@dataclass(frozen=True)
class OpenedBlock:
    """What a block read returned. Filter bytes are not stored here.

    `spent` contains only coins the block actually removed. A filter hit that the
    block does not contain is left out, so that name stays on the watch list.
    `verified_hints` are memos read from the block, not hits from the hint filter.
    """

    created: tuple[Coin, ...]
    spent: tuple[Coin, ...]
    verified_hints: tuple[tuple[bytes32, bytes], ...]


@dataclass(frozen=True)
class FilterCoinChange:
    """One coin the wallet should record from a block it opened.

    `created_height` is missing when a spend was confirmed and this wallet has not
    stored that coin's creation height yet. `spent_height` is missing when the coin
    is still unspent. Both are set when the coin was created and spent in this block.
    """

    coin: Coin
    created_height: int | None
    spent_height: int | None


@dataclass(frozen=True)
class FilterStep:
    """One height after its filters were tested. Filter bytes are not kept.

    `spend_children` are every coin created by a spend this block actually contained,
    including coins paid to someone else. The wallet records those while the block is open.
    """

    opened: bool
    coin_names: frozenset[bytes32]
    changes: tuple[FilterCoinChange, ...]
    spend_children: tuple[tuple[bytes32, tuple[Coin, ...]], ...] = ()


def coin_state_for_known_child(
    coin: Coin, parent_spent_height: int, spent_in_block: Collection[bytes32]
) -> CoinState:
    """A child taken from the parent's spend in this block.

    When this block also spent the child, the spent height is the parent's height.
    Leaving it unset makes a same-block launcher look like it was never spent.
    """
    spent = uint32(parent_spent_height) if coin.name() in spent_in_block else None
    return CoinState(coin, spent, uint32(parent_spent_height))


def singleton_spent_in_this_block(
    coin: Coin,
    parent_spent_height: int,
    known_children: Mapping[bytes32, Sequence[Coin]] | None,
) -> CoinState | None:
    """The next singleton, when this same block also spent it.

    A missing node row is not an unspent coin. Unspent coins stay in the node.
    The row this walk just wrote must not be read back as the live tip.
    """
    if known_children is None or coin.name() not in known_children:
        return None
    spent = uint32(parent_spent_height)
    return CoinState(coin, spent, spent)


def launcher_coin_if_known(
    parent: CoinState, launch_id: bytes32, from_node: Sequence[CoinState]
) -> CoinState | None:
    """Launcher coin from the parent spend, or from a node row that is still spent.

    An empty node answer is not proof the launcher never existed. A launcher is
    spent in the block that mints the singleton, so an unspent answer is not used.
    """
    if parent.coin.name() == launch_id:
        return parent
    for state in from_node:
        if state.coin.name() == launch_id and state.spent_height is not None:
            return state
    return None


def nft_mint_height(
    parent: CoinState, singleton_id: bytes32, from_node: Sequence[CoinState]
) -> uint32 | None:
    """Height that spent the launcher. None when that row is already gone."""
    state = launcher_coin_if_known(parent, singleton_id, from_node)
    if state is None or state.spent_height is None:
        return None
    return uint32(state.spent_height)


def children_of_spent_coins(opened: OpenedBlock) -> tuple[tuple[bytes32, tuple[Coin, ...]], ...]:
    """Coins created by the spends in this block, grouped by the spent coin.

    A coin paid to someone else is included. A spend that created nothing is included
    with an empty list, so the wallet does not ask the node for children it already
    knows are absent. The node does not keep that list after a day.
    """
    grouped: dict[bytes32, list[Coin]] = {coin.name(): [] for coin in opened.spent}
    seen: dict[bytes32, set[bytes32]] = {name: set() for name in grouped}
    for coin in opened.created:
        parent = coin.parent_coin_info
        if parent not in grouped:
            continue
        name = coin.name()
        already = seen[parent]
        if name in already:
            continue
        already.add(name)
        grouped[parent].append(coin)
    return tuple((parent, tuple(coins)) for parent, coins in grouped.items())


def _changes_from_block(
    height: int,
    previous_names: Collection[bytes32],
    held: Mapping[bytes32, tuple[Coin, int]],
    opened: OpenedBlock,
    checked: CheckedBlock,
) -> tuple[FilterCoinChange, ...]:
    created_by_name = {coin.name(): coin for coin in opened.created}
    spent_names = {coin.name() for coin in opened.spent}
    changes: list[FilterCoinChange] = []
    seen: set[bytes32] = set()
    for name in checked.noticed:
        coin = created_by_name.get(name)
        if coin is None:
            continue
        spent_height = height if name in spent_names else None
        changes.append(FilterCoinChange(coin, height, spent_height))
        seen.add(name)
    previous = set(previous_names)
    for coin in opened.spent:
        name = coin.name()
        if name in seen or name not in previous or name in checked.coin_names:
            continue
        known = held.get(name)
        created_height = None if known is None else known[1]
        changes.append(FilterCoinChange(coin, created_height, height))
        seen.add(name)
    # A new NFT, clawback coin, or fused coin often does not use our address as its
    # puzzle hash and may not carry a hint. It is still created by a coin we spent.
    held_parents = set(held)
    for coin in opened.created:
        name = coin.name()
        if name in seen or coin.parent_coin_info not in spent_names:
            continue
        # A coin created and spent in this block is not in the previous watch list.
        # Its child (a clawback, for example) still belongs to us when we noticed that coin.
        if (
            coin.parent_coin_info not in held_parents
            and coin.parent_coin_info not in previous
            and coin.parent_coin_info not in checked.noticed
        ):
            continue
        spent_height = height if name in spent_names else None
        changes.append(FilterCoinChange(coin, height, spent_height))
        seen.add(name)
    return tuple(changes)


def step_filter_height(
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    held: Mapping[bytes32, tuple[Coin, int]],
    height: int,
    committed: bytes | None,
    hints: bytes | None,
    opened: OpenedBlock | None,
) -> FilterStep:
    """Test one height and, on a hit, apply the block that was read.

    A block supplied for a height that did not hit is ignored. A hit with no block
    is a refusal: the filter is not treated as proof, and it is not treated as an
    empty wallet. A hint adds a coin only when `opened.verified_hints` names it.
    """
    look = examine_filters(puzzle_hashes, coin_names, committed, hints)
    if not look.read_block:
        return FilterStep(opened=False, coin_names=frozenset(coin_names), changes=())
    if committed is None:
        raise ValueError("a filter hit did not include the committed filter")
    if opened is None:
        raise ValueError(f"height {height} matched a filter and the block was not read")
    checked = apply_checked_block(
        puzzle_hashes,
        coin_names,
        opened.created,
        [coin.name() for coin in opened.spent],
        opened.verified_hints,
    )
    return FilterStep(
        opened=True,
        coin_names=checked.coin_names,
        changes=_changes_from_block(height, coin_names, held, opened, checked),
        spend_children=children_of_spent_coins(opened),
    )


def held_after(
    held: Mapping[bytes32, tuple[Coin, int]],
    changes: Collection[FilterCoinChange],
) -> dict[bytes32, tuple[Coin, int]]:
    """Watch list after these changes. A spent coin, including one created and spent together, is dropped."""
    updated = dict(held)
    for change in changes:
        name = change.coin.name()
        if change.spent_height is None and change.created_height is not None:
            updated[name] = (change.coin, change.created_height)
        elif change.spent_height is not None:
            updated.pop(name, None)
    return updated


@dataclass(frozen=True)
class FilterWalkResult:
    """Coins still watched, and every coin a read block asked the wallet to record.

    `opened_heights` are the only heights whose blocks were used. Filter bytes are not here.
    `completed` is false when the walk stopped before the target. `synced_through` is the
    last height whose page was finished, or one below the start when no page finished.
    """

    coin_names: frozenset[bytes32]
    noticed: frozenset[bytes32]
    opened_heights: tuple[int, ...]
    changes: tuple[FilterCoinChange, ...]
    completed: bool
    synced_through: int


def _notice_created(changes: Collection[FilterCoinChange], noticed: set[bytes32]) -> None:
    """Coins created in a block that was read, including one created and spent in that same block."""
    for change in changes:
        if change.created_height is None:
            continue
        if change.spent_height is None or change.created_height == change.spent_height:
            noticed.add(change.coin.name())


def walk_filters(
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    held: Mapping[bytes32, tuple[Coin, int]],
    heights: Iterable[tuple[int, bytes | None, bytes | None]],
    opened: Mapping[int, OpenedBlock],
) -> FilterWalkResult:
    """Walk heights in order. The coin-name list updates before the next height is tested.

    A block in `opened` is read only when that height's filters hit. After a spend, children
    in that block are applied and the spent name is not tested again.
    """
    names = set(coin_names)
    held_map = dict(held)
    noticed: set[bytes32] = set()
    opened_heights: list[int] = []
    changes: list[FilterCoinChange] = []
    last_height: int | None = None
    for height, committed, hints in heights:
        last_height = height
        block = opened.get(height)
        step = step_filter_height(puzzle_hashes, names, held_map, height, committed, hints, block)
        held_map = held_after(held_map, step.changes)
        names = names_still_held(step.coin_names, held_map)
        if step.opened:
            opened_heights.append(height)
            changes.extend(step.changes)
            _notice_created(step.changes, noticed)
    synced = -1 if last_height is None else last_height
    return FilterWalkResult(
        coin_names=frozenset(names),
        noticed=frozenset(noticed),
        opened_heights=tuple(opened_heights),
        changes=tuple(changes),
        completed=True,
        synced_through=synced,
    )


async def run_filter_walk(
    *,
    start: int,
    target: int,
    puzzle_hashes: Collection[bytes32],
    coin_names: Collection[bytes32],
    held: Mapping[bytes32, tuple[Coin, int]],
    fetch_page: Callable[[int, int], Awaitable[Collection[tuple[int, bytes | None, bytes | None]]]],
    open_block: Callable[[int, FilterLook, bytes], Awaitable[OpenedBlock]],
    on_height: Callable[
        [int, tuple[FilterCoinChange, ...], tuple[tuple[bytes32, tuple[Coin, ...]], ...]],
        Awaitable[tuple[bool, Collection[bytes32], Collection[bytes32]]],
    ]
    | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> FilterWalkResult:
    """Page filters from `start` through `target`. Drop each page after it is tested.

    A block is requested only when that height hits. `on_height` records the coins and may
    report puzzle hashes learned from them; those hashes are used on later heights.
    """
    if start > target:
        return FilterWalkResult(frozenset(coin_names), frozenset(), (), (), True, target)
    names = set(coin_names)
    held_map = dict(held)
    watched = set(puzzle_hashes)
    noticed: set[bytes32] = set()
    opened_heights: list[int] = []
    changes: list[FilterCoinChange] = []
    synced_through = start - 1
    for page_start, page_end in filter_page_bounds(start, target):
        if should_stop is not None and should_stop():
            return FilterWalkResult(
                frozenset(names), frozenset(noticed), tuple(opened_heights), tuple(changes), False, synced_through
            )
        page = await fetch_page(page_start, page_end)
        rows = list(page)
        del page
        expected = page_start
        page_changes: list[FilterCoinChange] = []
        page_opened: list[int] = []
        for height, committed, hints in rows:
            if height != expected:
                raise ValueError("block filter page is missing heights")
            expected += 1
            look = examine_filters(watched, names, committed, hints)
            opened: OpenedBlock | None = None
            if look.read_block:
                if committed is None:
                    raise ValueError("a filter hit did not include the committed filter")
                opened = await open_block(height, look, committed)
            step = step_filter_height(watched, names, held_map, height, committed, hints, opened)
            held_map = held_after(held_map, step.changes)
            names = names_still_held(step.coin_names, held_map)
            if step.opened:
                page_opened.append(height)
                page_changes.extend(step.changes)
                _notice_created(step.changes, noticed)
                if on_height is not None:
                    accepted, extra_hashes, extra_coin_ids = await on_height(
                        height, step.changes, step.spend_children
                    )
                    if not accepted:
                        return FilterWalkResult(
                            frozenset(names),
                            frozenset(noticed),
                            tuple(opened_heights),
                            tuple(changes),
                            False,
                            synced_through,
                        )
                    watched.update(extra_hashes)
                    # A pool singleton (and any other coin stored while this block was
                    # applied) is not one of the filter changes. Later heights in this
                    # same walk still have to see it spent.
                    names.update(extra_coin_ids)
        if expected != page_end + 1:
            raise ValueError("block filter page is missing heights")
        opened_heights.extend(page_opened)
        changes.extend(page_changes)
        synced_through = page_end
    return FilterWalkResult(
        frozenset(names),
        frozenset(noticed),
        tuple(opened_heights),
        tuple(changes),
        True,
        synced_through,
    )
