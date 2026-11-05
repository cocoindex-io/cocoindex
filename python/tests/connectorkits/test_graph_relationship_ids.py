"""The relationship ids the graph connectors derive from an edge's endpoints."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from cocoindex.connectorkits._graph_relationship_ids import (
    legacy_relation_id,
    relationship_id,
)


def test_plain_ids_are_length_prefixed_like_the_rust_sdk() -> None:
    assert relationship_id("Person", "alice", "Task", 42) == (
        "6:Person;5:alice;4:Task;2:42;"
    )
    # Lengths count UTF-8 bytes, as the Rust SDK does.
    assert relationship_id("P", "é", "T", "x") == "1:P;2:é;1:T;1:x;"


@pytest.mark.parametrize(
    "a, b",
    [
        # Integer vs the string that reads as it.
        (("N", 1, "N", 2), ("N", "1", "N", 2)),
        (("N", 1, "N", 2), ("N", 1, "N", "2")),
        # A string that already looks escaped.
        (("N", "1", "N", 2), ("N", "s:1", "N", 2)),
        # Integers too large for the Rust SDK's i64 still differ from strings.
        (("N", 10**30, "N", 2), ("N", str(10**30), "N", 2)),
        # Joins the legacy `_` form can't tell apart.
        (("N", "a_N_b", "N", "c"), ("N", "a", "N", "b_N_c")),
        (("ab", "c", "d", "e"), ("a", "bc", "d", "e")),
    ],
)
def test_distinct_endpoints_get_distinct_ids(
    a: tuple[str, Any, str, Any], b: tuple[str, Any, str, Any]
) -> None:
    assert relationship_id(*a) != relationship_id(*b)


def test_escaped_strings() -> None:
    assert relationship_id("N", "1", "N", "-7") == "1:N;3:s:1;1:N;4:s:-7;"
    assert relationship_id("N", "s:x", "N", "x") == "1:N;5:s:s:x;1:N;1:x;"


@pytest.mark.parametrize("value", [True, 1.5, None, b"x", uuid.UUID(int=1), (1,)])
def test_other_id_types_are_rejected(value: Any) -> None:
    with pytest.raises(TypeError, match="int or str"):
        relationship_id("N", value, "N", "x")


def test_legacy_ids_keep_their_form() -> None:
    assert legacy_relation_id("Entity", "alice", "Entity", "bob") == (
        "Entity_alice_Entity_bob"
    )
    # The ambiguity the relationship API's ids remove.
    assert legacy_relation_id("N", 1, "N", 2) == legacy_relation_id("N", "1", "N", "2")
