from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from coco_harness.stable_hash import HASH_BITS
from coco_harness.tools.reduce import (
    Group,
    GroupItem,
    HashExhausted,
    ItemTooLarge,
    OutputTooLarge,
    bisect_groups,
    group_item,
    hierarchical_reduce,
    leaf_item,
)


def _items(n: int, size: int = 1) -> list[GroupItem[str]]:
    return [leaf_item(f"k{i}", size, f"k{i}") for i in range(n)]


def _run(coro: Coroutine[Any, Any, str]) -> str:
    return asyncio.run(coro)


def _recording_reducer(
    calls: list[Group[str]],
) -> Callable[[Group[str]], Coroutine[Any, Any, str]]:
    async def reduce_group(group: Group[str]) -> str:
        calls.append(group)
        return "(" + "+".join(m.payload for m in group.members) + ")"

    return reduce_group


def test_every_multi_member_group_fits_the_threshold() -> None:
    items = [leaf_item(f"k{i}", 1 + (i * 7) % 10, f"k{i}") for i in range(300)]
    groups = bisect_groups(items, 25)
    assert sum(len(g.members) for g in groups) == 300
    for g in groups:
        if len(g.members) > 1:
            assert g.size <= 25
        idents = [m.ident for m in g.members]
        assert idents == sorted(idents)


def test_an_oversize_single_item_is_its_own_group() -> None:
    items = [leaf_item("big", 100, "big"), *_items(3)]
    groups = bisect_groups(items, 10)
    big = [g for g in groups if any(m.payload == "big" for m in g.members)]
    assert len(big) == 1 and len(big[0].members) == 1


def test_adding_one_item_changes_at_most_one_leaf() -> None:
    base = _items(200)
    before = {frozenset(m.payload for m in g.members) for g in bisect_groups(base, 16)}
    after = {
        frozenset(m.payload for m in g.members)
        for g in bisect_groups([*base, leaf_item("new", 1, "new")], 16)
    }
    assert len(before - after) <= 1  # one leaf absorbed the item or split
    assert len(after - before) <= 2


def test_a_size_change_is_confined_to_its_groups_subtree() -> None:
    """Groups outside the changed item's former group prefix are untouched (DESIGN §6)."""
    base = _items(300, size=2)
    before = bisect_groups(base, 24)
    changed = "k17"
    (old,) = [g for g in before if any(m.payload == changed for m in g.members)]
    grown = [
        leaf_item(k.payload, 23 if k.payload == changed else 2, k.payload) for k in base
    ]
    after = bisect_groups(grown, 24)

    def outside(groups: list[Group[str]]) -> set[tuple[str, frozenset[str]]]:
        return {
            (g.prefix, frozenset(m.payload for m in g.members))
            for g in groups
            if not g.prefix.startswith(old.prefix)
        }

    assert outside(before) == outside(after)
    # Inside the subtree the former group may become several; the size bound holds.
    inside = [g for g in after if g.prefix.startswith(old.prefix)]
    assert all(g.size <= 24 for g in inside if len(g.members) > 1)


def test_group_items_form_a_consistent_trie() -> None:
    leaves = bisect_groups(_items(50), 8)
    level1 = [group_item(g, 2, g.prefix) for g in leaves]
    groups = bisect_groups(level1, 7)
    assert sum(len(g.members) for g in groups) == len(level1)


def test_shallow_item_in_a_multi_member_group_is_rejected() -> None:
    bad = GroupItem(ident=0, depth=0, size=5, payload="root")
    with pytest.raises(ValueError):
        bisect_groups([bad, leaf_item("a", 5, "a"), leaf_item("b", 5, "b")], 8)


def test_hash_collision_that_does_not_fit_fails_loudly() -> None:
    twin = GroupItem(ident=12345, depth=HASH_BITS, size=6, payload="twin")
    assert len(bisect_groups([twin, twin], 20)) == 1  # fits: one group
    with pytest.raises(HashExhausted):
        bisect_groups([twin, twin], 10)


def test_hierarchical_reduce_calls_once_per_group_and_prefixes_are_unique() -> None:
    calls: list[Group[str]] = []
    result = _run(
        hierarchical_reduce(
            _items(40, size=3), 10, _recording_reducer(calls), lambda s: 2
        )
    )
    for i in range(40):
        assert f"k{i}" in result
    assert all(len(g.members) > 1 for g in calls)
    assert all(g.size <= 10 for g in calls)
    prefixes = [g.prefix for g in calls]
    assert len(prefixes) == len(set(prefixes))  # usable as component slots on their own


def test_single_input_gets_exactly_one_call() -> None:
    calls: list[Group[str]] = []
    assert (
        _run(hierarchical_reduce(_items(1), 10, _recording_reducer(calls), len))
        == "(k0)"
    )
    assert len(calls) == 1


def test_raw_items_above_half_the_budget_are_condensed_first() -> None:
    """Two items of size 6 at T=10 cannot be grouped; each is condensed, then combined."""
    calls: list[Group[str]] = []
    items = [leaf_item("a", 6, "a"), leaf_item("b", 6, "b")]
    result = _run(
        hierarchical_reduce(items, 10, _recording_reducer(calls), lambda s: 2)
    )
    assert result == "((a)+(b))"
    assert [len(g.members) for g in calls] == [1, 1, 2]


def test_an_item_above_the_budget_is_an_error() -> None:
    with pytest.raises(ItemTooLarge):
        _run(
            hierarchical_reduce(
                [leaf_item("a", 11, "a"), *_items(2)], 10, _recording_reducer([]), len
            )
        )


def test_an_oversize_reducer_output_is_an_error_for_the_caller_to_retry() -> None:
    async def too_big(group: Group[str]) -> str:
        return "x" * 100

    with pytest.raises(OutputTooLarge):
        _run(hierarchical_reduce(_items(8, size=4), 10, too_big, len))


def test_empty_input_is_rejected_as_a_reduce_error() -> None:
    from coco_harness.tools.reduce import EmptyInput

    with pytest.raises(EmptyInput):
        _run(hierarchical_reduce([], 10, _recording_reducer([]), len))
