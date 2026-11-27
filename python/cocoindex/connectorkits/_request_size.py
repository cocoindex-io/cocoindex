"""Split a batch of writes into requests under a server's size limit."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator, TypeVar

import msgspec

_T = TypeVar("_T")

# The most bytes one number of a vector takes in a JSON request, with the
# comma after it: ``-2.2250738585072014e-308,``. A vector is sized from its
# length with it, never by encoding it.
MAX_JSON_NUMBER_BYTES = 25

# Sizes a value as JSON; a value msgspec cannot encode is sized by its text.
_json_encoder = msgspec.json.Encoder(enc_hook=str)


def json_size(value: Any) -> int:
    """The bytes of ``value`` as JSON. For the small parts of a write (ids,
    payloads, attributes): size a vector with `MAX_JSON_NUMBER_BYTES`."""
    return len(_json_encoder.encode(value))


def split_by_size(
    items: Iterable[_T], budget: int, size: Callable[[_T], int]
) -> Iterator[list[_T]]:
    """Split ``items``, in order, into groups whose sizes add up to at most
    ``budget`` each, so each group fits one request. An item larger than
    ``budget`` makes a group of its own. Each group is yielded once it is
    full, so it can be sent before the next one is sized."""
    group: list[_T] = []
    group_size = 0
    for item in items:
        item_size = size(item)
        if group and group_size + item_size > budget:
            yield group
            group = []
            group_size = 0
        group.append(item)
        group_size += item_size
    if group:
        yield group
