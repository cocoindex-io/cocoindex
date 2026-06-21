"""Tests for the Pulsar source connector: receive loop, ack-after-readiness,
payloads/map views.

These tests mock the pulsar client/consumer to verify behavior without a real
Pulsar broker.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

# --- Mock pulsar before importing the connector ---


class MockTimeout(Exception):
    """Stand-in for pulsar.Timeout."""


class MockConsumerType:
    Exclusive = "Exclusive"
    Shared = "Shared"
    Failover = "Failover"


class MockMessage:
    def __init__(
        self,
        *,
        data: bytes = b"",
        partition_key: str | None = None,
        message_id: str = "mid",
    ) -> None:
        self._data = data
        self._partition_key = partition_key
        self._message_id = message_id

    def data(self) -> bytes:
        return self._data

    def partition_key(self) -> str | None:
        return self._partition_key

    def message_id(self) -> str:
        return self._message_id


# A sentinel scripted into the receive queue to raise Timeout.
_TIMEOUT = object()


class MockConsumer:
    def __init__(self, script: list[Any]) -> None:
        self._script: deque[Any] = deque(script)
        self.acked: list[str] = []
        self.closed = False

    def receive(self, timeout_millis: int) -> MockMessage:
        if self._script:
            item = self._script.popleft()
        else:
            item = _TIMEOUT
        if item is _TIMEOUT:
            raise MockTimeout()
        return cast(MockMessage, item)

    def acknowledge(self, msg: MockMessage) -> None:
        self.acked.append(msg.message_id())

    def close(self) -> None:
        self.closed = True


class MockClient:
    def __init__(self, consumer: MockConsumer) -> None:
        self._consumer = consumer
        self.subscribe_calls: list[tuple[Any, str, Any]] = []

    def subscribe(
        self, topics: Any, subscription_name: str, *, consumer_type: Any = None
    ) -> MockConsumer:
        self.subscribe_calls.append((topics, subscription_name, consumer_type))
        return self._consumer


_mock_pulsar = MagicMock()
_mock_pulsar.Client = MockClient
_mock_pulsar.Consumer = MockConsumer
_mock_pulsar.Message = MockMessage
_mock_pulsar.ConsumerType = MockConsumerType
_mock_pulsar.Timeout = MockTimeout
sys.modules.setdefault("pulsar", _mock_pulsar)

from cocoindex.connectors.pulsar._source import (  # noqa: E402
    TopicStream,
    topic_as_map,
    topic_as_stream,
)
from cocoindex._internal.live_component import _IMMEDIATE_READY  # noqa: E402


# --- Subscribers that record and signal readiness ------------------------------


class RecordingStreamSubscriber:
    """LiveStreamSubscriber that records sends and signals on mark_ready."""

    def __init__(self) -> None:
        self.sent: list[Any] = []
        self.ready = asyncio.Event()

    async def send(self, item: Any) -> Any:
        self.sent.append(item)
        return _IMMEDIATE_READY

    async def mark_ready(self) -> None:
        self.ready.set()


class RecordingMapSubscriber:
    def __init__(self) -> None:
        self.updates: list[tuple[Any, Any]] = []
        self.deletes: list[Any] = []
        self.ready = asyncio.Event()

    async def update(self, key: Any, value: Any) -> Any:
        self.updates.append((key, value))
        return _IMMEDIATE_READY

    async def delete(self, key: Any) -> Any:
        self.deletes.append(key)
        return _IMMEDIATE_READY

    async def mark_ready(self) -> None:
        self.ready.set()


async def _run_until_ready(coro_factory: Any, sub: Any, timeout: float = 2.0) -> None:
    """Run a watch() coroutine until the subscriber is marked ready, then cancel."""
    task = asyncio.create_task(coro_factory())
    try:
        await asyncio.wait_for(sub.ready.wait(), timeout=timeout)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --- Tests ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_payloads_stream_delivers_and_acks() -> None:
    consumer = MockConsumer(
        [
            MockMessage(data=b"v1", message_id="m1"),
            MockMessage(data=b"v2", message_id="m2"),
        ]
    )
    client = MockClient(consumer)
    stream = topic_as_stream(client, ["t"], "sub").payloads()  # type: ignore[arg-type]
    sub = RecordingStreamSubscriber()

    await _run_until_ready(lambda: stream.watch(sub), sub)

    assert sub.sent == [b"v1", b"v2"]
    assert consumer.acked == ["m1", "m2"]  # acked after delivery (immediate-ready)
    assert sub.ready.is_set()  # mark_ready fired on the first receive timeout
    assert consumer.closed  # consumer closed on watch exit


@pytest.mark.asyncio
async def test_subscribe_uses_subscription_and_default_exclusive() -> None:
    consumer = MockConsumer([])
    client = MockClient(consumer)
    stream = topic_as_stream(client, ["t"], "my-sub")  # type: ignore[arg-type]
    sub = RecordingStreamSubscriber()

    await _run_until_ready(lambda: stream.watch(sub), sub)

    assert len(client.subscribe_calls) == 1
    topics, subscription, ctype = client.subscribe_calls[0]
    assert topics == ["t"]
    assert subscription == "my-sub"
    assert ctype == MockConsumerType.Exclusive


@pytest.mark.asyncio
async def test_payloads_skips_empty_payload() -> None:
    consumer = MockConsumer(
        [
            MockMessage(data=b"", message_id="m1"),
            MockMessage(data=b"v2", message_id="m2"),
        ]
    )
    client = MockClient(consumer)
    stream = topic_as_stream(client, ["t"], "sub").payloads()  # type: ignore[arg-type]
    sub = RecordingStreamSubscriber()

    await _run_until_ready(lambda: stream.watch(sub), sub)

    # Empty payload is filtered from the bytes view but still acknowledged.
    assert sub.sent == [b"v2"]
    assert consumer.acked == ["m1", "m2"]


@pytest.mark.asyncio
async def test_map_feed_update_delete_skip() -> None:
    consumer = MockConsumer(
        [
            MockMessage(data=b"v1", partition_key="k1", message_id="m1"),  # update
            MockMessage(
                data=b"", partition_key="k2", message_id="m2"
            ),  # delete (empty)
            MockMessage(
                data=b"v3", partition_key=None, message_id="m3"
            ),  # skip (no key)
        ]
    )
    client = MockClient(consumer)
    feed = topic_as_map(client, ["t"], "sub")  # type: ignore[arg-type]
    sub = RecordingMapSubscriber()

    await _run_until_ready(lambda: feed.watch(sub), sub)  # type: ignore[arg-type]

    assert len(sub.updates) == 1 and sub.updates[0][0] == "k1"
    assert sub.deletes == ["k2"]
    assert consumer.acked == ["m1", "m2", "m3"]  # all acked, including the skipped one


@pytest.mark.asyncio
async def test_is_deletion_predicate() -> None:
    consumer = MockConsumer(
        [MockMessage(data=b"tombstone", partition_key="k1", message_id="m1")]
    )
    client = MockClient(consumer)
    feed = topic_as_map(
        client,  # type: ignore[arg-type]
        ["t"],
        "sub",
        is_deletion=lambda m: m.data() == b"tombstone",
    )
    sub = RecordingMapSubscriber()

    await _run_until_ready(lambda: feed.watch(sub), sub)  # type: ignore[arg-type]

    assert sub.deletes == ["k1"]
    assert sub.updates == []


@pytest.mark.asyncio
async def test_ack_only_after_readiness() -> None:
    gate = asyncio.Event()

    class GatedHandle:
        async def ready(self) -> None:
            await gate.wait()

    class GatedSubscriber(RecordingStreamSubscriber):
        async def send(self, item: Any) -> Any:
            self.sent.append(item)
            return GatedHandle()

    consumer = MockConsumer([MockMessage(data=b"v1", message_id="m1")])
    client = MockClient(consumer)
    stream = topic_as_stream(client, ["t"], "sub").payloads()  # type: ignore[arg-type]
    sub = GatedSubscriber()

    task = asyncio.create_task(stream.watch(sub))
    try:
        await asyncio.wait_for(sub.ready.wait(), timeout=2.0)
        # Message delivered, but its readiness handle hasn't completed → not acked.
        await asyncio.sleep(0)
        assert consumer.acked == []
        gate.set()  # complete readiness → ack should now happen
        await asyncio.sleep(0.05)
        assert consumer.acked == ["m1"]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_single_watcher_guard() -> None:
    consumer = MockConsumer([])
    client = MockClient(consumer)
    stream = topic_as_stream(client, ["t"], "sub")  # type: ignore[arg-type]
    sub1 = RecordingStreamSubscriber()
    sub2 = RecordingStreamSubscriber()

    task = asyncio.create_task(stream.watch(sub1))
    try:
        await asyncio.wait_for(sub1.ready.wait(), timeout=2.0)
        with pytest.raises(RuntimeError):
            await stream.watch(sub2)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def test_topic_stream_type() -> None:
    consumer = MockConsumer([])
    client = MockClient(consumer)
    assert isinstance(topic_as_stream(client, ["t"], "sub"), TopicStream)  # type: ignore[arg-type]
