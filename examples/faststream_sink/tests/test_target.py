from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike
import pytest
from watchdog.observers.polling import PollingObserver

from examples.faststream_sink.projection import Projection
from examples.faststream_sink.protocol import (
    Event,
    MessageKey,
    canonical_json,
    decode_event,
)
from examples.faststream_sink.target import (
    KafkaDestination,
    KafkaTarget,
    MessageHandler,
    Record,
    TrackingRecord,
    _publish,
)
from tests import common

DESTINATION = coco.ContextKey[KafkaDestination]("faststream-example-test-destination")
KEY = MessageKey(DESTINATION.key, "cluster", "topic", "record")


def tracking(revision: int, value: object) -> TrackingRecord:
    return TrackingRecord(revision, hashlib.sha256(canonical_json(value)).hexdigest())


def test_reconcile_all_possible_records_and_uncertainty() -> None:
    handler = MessageHandler()
    desired = Record(3, canonical_json("A"))
    assert handler.reconcile(KEY, desired, [tracking(3, "A")], False) is None
    assert handler.reconcile(KEY, desired, [tracking(3, "A")], True) is not None
    assert (
        handler.reconcile(KEY, desired, [tracking(1, "A"), tracking(2, "B")], True)
        is not None
    )
    with pytest.raises(ValueError, match="decrease"):
        handler.reconcile(KEY, desired, [tracking(4, "A")], False)
    with pytest.raises(ValueError, match="new revision"):
        handler.reconcile(KEY, desired, [tracking(3, "B")], False)
    with pytest.raises(ValueError, match="conflict"):
        handler.reconcile(KEY, desired, [tracking(1, "A"), tracking(1, "B")], True)
    with pytest.raises(ValueError, match="without a previous revision"):
        handler.reconcile(KEY, coco.NON_EXISTENCE, [], True)
    assert handler.reconcile(KEY, coco.NON_EXISTENCE, [], False) is None
    deletion = handler.reconcile(
        KEY, coco.NON_EXISTENCE, [tracking(1, "A"), tracking(3, "A")], True
    )
    assert deletion is not None
    assert deletion.action == Event(KEY, 3, "delete", None)


@dataclass
class RecordingPublisher:
    events: list[Event] = field(default_factory=list)
    confirmed: list[Event] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event | None = None
    fail_after_accept: bool = False

    async def publish(
        self,
        message: bytes,
        topic: str,
        *,
        key: bytes,
        headers: dict[str, str],
        correlation_id: str,
        no_confirm: Literal[False],
    ) -> object:
        event = decode_event(message)
        assert topic == event.key.topic
        assert key == canonical_json([event.key.destination, event.key.key])
        assert headers["cocoindex-event-id"] == correlation_id == event.event_id
        assert no_confirm is False
        self.events.append(event)
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.fail_after_accept:
            self.fail_after_accept = False
            raise OSError("simulated lost confirmation after broker acceptance")
        self.confirmed.append(event)
        return object()


async def test_provider_repoint_rejected_before_batch_effects() -> None:
    publisher = RecordingPublisher()
    provider = coco.ContextProvider()
    provider.provide(DESTINATION, KafkaDestination(publisher, "cluster"))
    actions = [
        Event(KEY, 1, "upsert", b'"A"'),
        Event(
            MessageKey(DESTINATION.key, "other", "topic", "second"), 1, "upsert", b'"B"'
        ),
    ]
    with pytest.raises(ValueError, match="different Kafka cluster"):
        await _publish(provider, actions)
    assert publisher.events == []


async def test_independent_snapshot_failure_stats_and_unchanged_retry(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("watchdog.observers.Observer", PollingObserver)
    env = common.create_test_env(__file__, suffix=request.node.name)
    publisher = RecordingPublisher(fail_after_accept=True)
    env.context_provider.provide(DESTINATION, KafkaDestination(publisher, "cluster"))
    source = tmp_path / "source"
    source.mkdir()
    snapshot = source / "snapshot.json"
    snapshot.write_text("A")
    original = snapshot.stat()

    @coco.fn
    async def process_snapshot(file: FileLike) -> None:
        KafkaTarget(DESTINATION, "cluster", "topic").declare(
            "record", 1, await file.read_text()
        )

    @coco.fn
    async def root() -> str:
        files = localfs.walk_dir(source, live=True)
        await coco.mount_each(process_snapshot, files.items())
        return "root returned"

    app = coco.App(
        coco.AppConfig(name="IndependentSnapshotRetry", environment=env), root
    )
    try:
        failed = app.update()
        assert await failed == "root returned"
        stats = failed.stats()
        assert stats is not None
        assert stats.total.num_errors == 1
        assert stats.total.num_in_progress == 0
        processor = next(
            name for name in stats.by_component if name.endswith("process_snapshot")
        )
        child = stats.by_component[processor]
        assert child.num_errors == 1
        assert child.num_processed == 0
        assert "simulated lost confirmation" in caplog.text
        assert len(publisher.events) == 1
        assert publisher.confirmed == []
        unconfirmed = publisher.events[0]

        recovered = app.update()
        await recovered
        stats = recovered.stats()
        assert stats is not None
        assert stats.total.num_errors == 0
        assert stats.total.num_in_progress == 0
        assert stats.by_component[processor].num_processed == 1
        assert publisher.events == [unconfirmed, unconfirmed]
        assert publisher.confirmed == [unconfirmed]

        unchanged = app.update()
        await unchanged
        stats = unchanged.stats()
        assert stats is not None and stats.total.num_errors == 0
        assert publisher.events == [unconfirmed, unconfirmed]
        assert publisher.confirmed == [unconfirmed]
        assert snapshot.read_text() == "A"
        assert snapshot.stat().st_mtime_ns == original.st_mtime_ns
    finally:
        env.close()


async def test_native_ack_boundary_retry_noop_and_fresh_environment_drop(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    release = asyncio.Event()
    publisher = RecordingPublisher(release=release)
    env.context_provider.provide(DESTINATION, KafkaDestination(publisher, "cluster"))
    rows: list[tuple[int, object]] = [(1, "A")]

    @coco.fn
    async def declare() -> None:
        target = KafkaTarget(DESTINATION, "cluster", "topic")
        for revision, value in rows:
            target.declare("record", revision, value)

    app = coco.App(coco.AppConfig(name="AckRetryAndDrop", environment=env), declare)
    projection = Projection(tmp_path / "projection.sqlite3", KEY[:3])
    try:
        pending = asyncio.ensure_future(app.update())
        await asyncio.wait_for(publisher.entered.wait(), timeout=5)
        assert not pending.done()
        release.set()
        await pending
        await app.update()
        assert len(publisher.events) == 1

        publisher.fail_after_accept = True
        rows[:] = [(2, "B")]
        with pytest.raises(Exception, match="lost confirmation"):
            await app.update()
        accepted = publisher.events[-1]
        await app.update()
        assert publisher.events[-1] == accepted
        assert publisher.events[-1].event_id == accepted.event_id

        rows[:] = [(3, "A")]
        await app.update()
        rows.clear()
        await app.update()
        assert publisher.events[-1].order == (3, 1)
        rows[:] = [(4, "A")]
        await app.update()
        for event in publisher.events:
            projection.apply(event)
    finally:
        env.close()
        projection.close()

    fresh = coco.Environment(env.settings)
    replacement = RecordingPublisher()
    fresh.context_provider.provide(
        DESTINATION, KafkaDestination(replacement, "wrong-cluster")
    )
    fresh_app = coco.App(
        coco.AppConfig(name="AckRetryAndDrop", environment=fresh), declare
    )
    try:
        with pytest.raises(Exception, match="different Kafka cluster"):
            await fresh_app.drop()
        assert replacement.events == []
        fresh.context_provider.provide(
            DESTINATION, KafkaDestination(replacement, "cluster")
        )
        await fresh_app.drop()
        assert replacement.events == [Event(KEY, 4, "delete", None)]
    finally:
        fresh.close()
