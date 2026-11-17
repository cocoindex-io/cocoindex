"""Split a batch of writes into requests under a server's size limit."""

from __future__ import annotations

from typing import Callable, Iterable, TypeVar

_T = TypeVar("_T")


def split_by_size(
    items: Iterable[_T], budget: int, size: Callable[[_T], int]
) -> list[list[_T]]:
    """Split ``items``, in order, into groups whose sizes add up to at most
    ``budget`` each, so each group fits one request. An item larger than
    ``budget`` makes a group of its own."""
    groups: list[list[_T]] = []
    group: list[_T] = []
    group_size = 0
    for item in items:
        item_size = size(item)
        if group and group_size + item_size > budget:
            groups.append(group)
            group = []
            group_size = 0
        group.append(item)
        group_size += item_size
    if group:
        groups.append(group)
    return groups
