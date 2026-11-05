from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from examples.faststream_sink.projection import Projection
from examples.faststream_sink.protocol import (
    MAX_MESSAGE_BYTES,
    Event,
    MessageKey,
    canonical_json,
    decode_event,
)

KEY = MessageKey("test-destination", "test-cluster", "test-topic", "tea / + ☕")


def upsert(revision: int, value: object) -> Event:
    return Event(KEY, revision, "upsert", canonical_json(value))


@pytest.mark.parametrize(
    "value", [None, False, 42, "tea / + ☕", {"z": [1, 2], "a": "A"}]
)
def test_roundtrip_and_content_independent_identity(value: object) -> None:
    event = upsert(1, value)
    assert decode_event(event.encode()) == event
    assert event.event_id == upsert(1, {"different": True}).event_id
    assert event.event_id != upsert(2, value).event_id
    assert event.event_id != Event(KEY, 1, "delete", None).event_id


@pytest.mark.parametrize("revision", [True, False, 0, -1, 2**63, 1.5, "1"])
def test_invalid_revision(revision: int) -> None:
    with pytest.raises(ValueError, match="Revision"):
        upsert(revision, "A")


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", True),
        ("event_id", "wrong"),
        ("value_sha256", "wrong"),
        ("key", ""),
        ("topic", "../bad"),
        ("operation", "patch"),
    ],
)
def test_invalid_envelope(field: str, value: object) -> None:
    envelope = json.loads(upsert(1, "A").encode())
    envelope[field] = value
    with pytest.raises(ValueError):
        decode_event(canonical_json(envelope))


def test_noncanonical_duplicate_nonfinite_and_oversized_values() -> None:
    with pytest.raises(ValueError, match="canonical"):
        Event(KEY, 1, "upsert", b'{"b": 1, "a": 2}')
    with pytest.raises(ValueError, match="Duplicate"):
        decode_event(b'{"schema":1,"schema":1}')
    with pytest.raises(ValueError):
        upsert(1, float("nan"))
    with pytest.raises(ValueError, match="256 KiB"):
        upsert(1, "x" * MAX_MESSAGE_BYTES).encode()
    with pytest.raises(ValueError, match="256 KiB"):
        decode_event(b"x" * (MAX_MESSAGE_BYTES + 1))


def test_projection_reversion_delete_and_recreation_survive_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "projection.sqlite3"
    route = KEY[:3]
    first = Projection(path, route)
    group_id = first.group_id
    events = [
        upsert(1, "A"),
        upsert(2, "B"),
        upsert(3, "A"),
        Event(KEY, 3, "delete", None),
    ]
    for event in events:
        assert first.apply(event) == "applied"
    first.close()

    second = Projection(path, route)
    try:
        assert second.group_id == group_id
        for event in events[:-1]:
            assert second.apply(event) == "stale"
        assert second.apply(events[-1]) == "duplicate"
        assert second.apply(upsert(3, "A")) == "stale"
        assert second.apply(upsert(4, "A")) == "applied"
        assert second.apply(events[-1]) == "stale"
        with sqlite3.connect(path) as db:
            assert db.execute(
                "SELECT revision, phase, value_json FROM records"
            ).fetchone() == (4, 0, b'"A"')
    finally:
        second.close()

    fresh = Projection(tmp_path / "new.sqlite3", route)
    try:
        assert fresh.group_id != group_id
    finally:
        fresh.close()


def test_projection_conflict_corruption_and_destination_mismatch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "projection.sqlite3"
    projection = Projection(path, KEY[:3])
    try:
        assert projection.apply(upsert(1, "A")) == "applied"
        with pytest.raises(ValueError, match="Conflicting"):
            projection.apply(upsert(1, "B"))
        wrong = Event(MessageKey("other", *KEY[1:]), 2, "upsert", b'"B"')
        with pytest.raises(ValueError, match="different destination"):
            projection.apply(wrong)
        with sqlite3.connect(path) as db:
            db.execute("UPDATE records SET value_json = ?", (b'"lost"',))
        with pytest.raises(ValueError, match="inconsistent"):
            projection.apply(upsert(1, "A"))
    finally:
        projection.close()
    with pytest.raises(ValueError, match="different destination"):
        Projection(path, ("other", *KEY[1:3]))
