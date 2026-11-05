"""Hierarchical reduce: size-bounded hash bisection at every level (DESIGN.md §6).

Every item has an identity (64 bits, MSB first) and a size. Raw items use the
stable hash of their key; a reducer's output uses its group's trie prefix.
``bisect_groups`` splits a level on successive bits until each group fits the
threshold; ``hierarchical_reduce`` applies it level after level until one
item remains.

Frontier invariant: every item on the frontier has ``size <= threshold // 2``.
Raw items above that are condensed by a single-item reducer call first; an
item above the threshold cannot be processed at all and is an error. With the
invariant, any two items fit together, so every level with two or more items
has a multi-member group and the item count strictly decreases: that is the
termination argument. Reducer outputs are checked against the same bound.

Pure, apart from awaiting the caller's ``reduce_group``; the CocoIndex glue
(one memoized component per group, retries) lives in ``tools/agents.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

from ..stable_hash import HASH_BITS, bit_at, ident_from_prefix, stable_hash64

T = TypeVar("T")


class ReduceError(Exception):
    """A reduce could not proceed within its budget."""


class ItemTooLarge(ReduceError):
    """A single input exceeds the threshold; no call can process it."""


class OutputTooLarge(ReduceError):
    """A reducer output exceeds half the threshold; the caller should re-ask."""


class HashExhausted(ReduceError):
    """Distinct keys share all 64 identity bits and do not fit together."""


class EmptyInput(ReduceError):
    """There is nothing to reduce."""


@dataclass(frozen=True)
class GroupItem(Generic[T]):
    ident: int  # 64-bit identity, MSB first
    depth: int  # how many leading bits of ``ident`` are meaningful
    size: int  # estimated tokens (or any additive cost)
    payload: T

    @property
    def prefix(self) -> str:
        return "".join(str(bit_at(self.ident, i)) for i in range(self.depth))


@dataclass(frozen=True)
class Group(Generic[T]):
    prefix: str  # bit string; "" at the root
    members: tuple[GroupItem[T], ...]  # sorted by identity: deterministic prompts

    @property
    def size(self) -> int:
        return sum(m.size for m in self.members)


def leaf_item(key: str, size: int, payload: T) -> GroupItem[T]:
    return GroupItem(
        ident=stable_hash64(key), depth=HASH_BITS, size=size, payload=payload
    )


def group_item(group: Group[T], size: int, payload: T) -> GroupItem[T]:
    """A reducer's output as an item of the next level: identity = the group's prefix."""
    return GroupItem(
        ident=ident_from_prefix(group.prefix),
        depth=len(group.prefix),
        size=size,
        payload=payload,
    )


def bisect_groups(items: Sequence[GroupItem[T]], threshold: int) -> list[Group[T]]:
    """Split ``items`` on successive identity bits until every group fits ``threshold``.

    Invariant: a multi-member set under prefix ``p`` has every member strictly
    deeper than ``len(p)``, so the next bit is defined for all of them. A
    single item is always its own group, whatever its size; the caller holds
    the frontier invariant that makes that fine. Distinct keys whose 64-bit
    identities collide and do not fit together raise ``HashExhausted`` rather
    than producing an over-budget group.
    """
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    out: list[Group[T]] = []

    def rec(members: list[GroupItem[T]], bit: int, prefix: str) -> None:
        if len(members) <= 1 or sum(m.size for m in members) <= threshold:
            out.append(
                Group(prefix, tuple(sorted(members, key=lambda m: (m.ident, m.depth))))
            )
            return
        if bit >= HASH_BITS:
            raise HashExhausted(
                f"{len(members)} items share all {HASH_BITS} identity bits and exceed the threshold"
            )
        shallow = [m for m in members if m.depth <= bit]
        if shallow:
            raise ValueError(
                f"item at depth {shallow[0].depth} inside a multi-member group at "
                f"prefix {prefix!r}: identities are not a consistent trie"
            )
        zeros = [m for m in members if bit_at(m.ident, bit) == 0]
        ones = [m for m in members if bit_at(m.ident, bit) == 1]
        if not zeros or not ones:
            # Every member shares this bit: the prefix grows, the group does not split.
            rec(members, bit + 1, prefix + ("0" if zeros else "1"))
            return
        rec(zeros, bit + 1, prefix + "0")
        rec(ones, bit + 1, prefix + "1")

    rec(list(items), 0, "")
    return out


async def hierarchical_reduce(
    items: Sequence[GroupItem[T]],
    threshold: int,
    reduce_group: Callable[[Group[T]], Awaitable[T]],
    size_of: Callable[[T], int],
    *,
    max_levels: int = HASH_BITS + 1,
) -> T:
    """Reduce ``items`` to one payload; ``reduce_group`` runs once per group it is given.

    1. Admission: an item above ``threshold`` raises ``ItemTooLarge``; an item
       above ``threshold // 2`` is condensed by a single-item call.
    2. A single input gets exactly one call, so the task is applied to it.
    3. Levels: bisect, reduce every multi-member group (concurrently; levels
       are a barrier), pass singletons through, repeat until one item remains.

    Every output is checked against ``threshold // 2``; a larger one raises
    ``OutputTooLarge`` for the caller to re-ask. ``max_levels`` is a guard only.
    """
    if not items:
        raise EmptyInput("the collection is empty; nothing to reduce")
    half = threshold // 2

    async def call(group: Group[T]) -> GroupItem[T]:
        out = await reduce_group(group)
        size = size_of(out)
        if size > half:
            raise OutputTooLarge(
                f"reducer output of size {size} exceeds {half} at prefix {group.prefix!r}"
            )
        return group_item(group, size, out)

    for it in items:
        if it.size > threshold:
            raise ItemTooLarge(
                f"item of size {it.size} exceeds the threshold {threshold}"
            )
    if len(items) == 1:
        return (await call(Group(items[0].prefix, (items[0],)))).payload

    # Admission: condense raw items that would not fit together with anything.
    level: list[GroupItem[T]] = list(items)
    oversize = [i for i, it in enumerate(level) if it.size > half]
    condensed = await asyncio.gather(
        *(call(Group(level[i].prefix, (level[i],))) for i in oversize)
    )
    for i, item in zip(oversize, condensed):
        level[i] = item

    for _ in range(max_levels):
        if len(level) == 1:
            return level[0].payload
        groups = bisect_groups(level, threshold)
        multi = [g for g in groups if len(g.members) > 1]
        outputs = await asyncio.gather(*(call(g) for g in multi))
        reduced = {id(g): out for g, out in zip(multi, outputs)}
        level = [
            g.members[0] if len(g.members) == 1 else reduced[id(g)] for g in groups
        ]
    raise RuntimeError(
        "hierarchical_reduce did not converge; the frontier invariant was violated"
    )
