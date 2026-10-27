"""Relationship ids the built-in graph connectors derive from an edge's endpoints.

Internal: shared by the Neo4j and FalkorDB connectors.
"""

from __future__ import annotations

import re
from typing import Any

# A string id that would read as an integer, or that starts with this marker,
# gets the marker prepended, so the integer 1 and the string "1" never share
# an id.
_STR_ESCAPE = "s:"
_INT_LIKE_RE = re.compile(r"[+-]?[0-9]+")


def _id_part(value: Any) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        if value.startswith(_STR_ESCAPE) or _INT_LIKE_RE.fullmatch(value):
            return _STR_ESCAPE + value
        return value
    raise TypeError(
        f"A relationship id is derived from its endpoint ids only when they are "
        f"int or str, got {type(value).__name__}: {value!r}. Pass a record "
        f"carrying the relationship's primary key."
    )


def _length_prefixed(part: str) -> str:
    return f"{len(part.encode())}:{part};"


def relationship_id(from_label: str, from_id: Any, to_label: str, to_id: Any) -> str:
    """The id of a relationship declared without one: each endpoint's label
    and id, length-prefixed, so distinct endpoints never share an id. Same
    encoding as the Rust SDK."""
    return "".join(
        _length_prefixed(part)
        for part in (from_label, _id_part(from_id), to_label, _id_part(to_id))
    )


def legacy_relation_id(from_label: str, from_id: Any, to_label: str, to_id: Any) -> str:
    """The id the deprecated relation API derives: an ``_``-joined string,
    ambiguous (``"a_b", "c"`` and ``"a", "b_c"`` share one, and so do ``1``
    and ``"1"``). Kept so existing edges keep their ids."""
    return f"{from_label}_{from_id}_{to_label}_{to_id}"
