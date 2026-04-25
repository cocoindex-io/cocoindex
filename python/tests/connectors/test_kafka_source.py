"""Tests for Kafka source connector: offset tracking and watch behavior.

These tests mock the AIOConsumer to verify behavior without a real Kafka broker.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest

# --- Mock confluent_kafka before importing the connector ---


class MockTopicPartition:
    """Mock confluent_kafka.TopicPartition."""

    def __init__(self, topic: str, partition: int, offset: int = -1) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class MockMessage:
    """Mock confluent_kafka.Message."""

    def __init__(
        self,
        *,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 0,
        key: bytes | str | None = None,
        value: bytes | str | None = None,
        error_val: object = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._error_val = error_val

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | str | None:
        return self._key

    def value(self) -> bytes | str | None:
        return self._value

    def error(self) -> object:
        return self._error_val


class MockComponentMountHandle:
    """Mock ComponentMountHandle with controllable readiness."""

    def __init__(self) -> None:
        self._ready_event = asyncio.Event()

    async def ready(self) -> None:
        await self._ready_event.wait()

    def set_ready(self) -> None:
        self._ready_event.set()


class MockAIOConsumer:
    """Mock AIOConsumer with controllable message delivery."""

    def __init__(self) -> None:
        self._messages: deque[MockMessage | None] = deque()
        self._committed: list[MockTopicPartition] = []
        self._subscribed_topics: list[str] = []
        self._on_assign: Any = None
        self._on_revoke: Any = None
        self._watermarks: dict[tuple[str, int], tuple[int, int]] = {}

    def enqueue(self, *messages: MockMessage | None) -> None:
        """Add messages to the poll queue."""
        self._messages.extend(messages)

    async def subscribe(
        self,
        topics: list[str],
        *,
        on_assign: Any = None,
        on_revoke: Any = None,
    ) -> None:
        self._subscribed_topics = topics
        self._on_assign = on_assign
        self._on_revoke = on_revoke

    async def trigger_assign(self, partitions: list[MockTopicPartition]) -> None:
        """Simulate partition assignment."""
        if self._on_assign is not None:
            await self._on_assign(self, partitions)

    async def trigger_revoke(self, partitions: list[MockTopicPartition]) -> None:
        """Simulate partition revocation."""
        if self._on_revoke is not None:
            await self._on_revoke(self, partitions)

    def set_watermarks(self, topic: str, partition: int, low: int, high: int) -> None:
        """Set watermark offsets for a partition."""
        self._watermarks[(topic, partition)] = (low, high)

    async def unsubscribe(self) -> None:
        self._subscribed_topics = []
        self._on_assign = None
        self._on_revoke = None

    async def committed(self, partitions: list[Any]) -> list[MockTopicPartition]:
        """Return committed offsets (defaults to -1001 — no commit yet)."""
        return [MockTopicPartition(tp.topic, tp.partition, -1001) for tp in partitions]

    async def get_watermark_offsets(self, tp: Any) -> tuple[int, int]:
        key = (tp.topic, tp.partition)
        return self._watermarks.get(key, (0, 0))

    async def poll(self, timeout: float = 1.0) -> MockMessage | None:
        if self._messages:
            return self._messages.popleft()
        # Signal end of messages by raising CancelledError after delivering all
        raise asyncio.CancelledError

    async def commit(self, *, offsets: list[Any], asynchronous: bool = False) -> None:
        self._committed.extend(offsets)


# Install mocks before importing the source module
_mock_tp = MagicMock()
_mock_tp.TopicPartition = MockTopicPartition

_mock_aio = MagicMock()
_mock_aio.AIOConsumer = MockAIOConsumer

_mock_module = MagicMock()
_mock_module.aio = _mock_aio
_mock_module.TopicPartition = MockTopicPartition

sys.modules.setdefault("confluent_kafka", _mock_module)
sys.modules.setdefault("confluent_kafka.aio", _mock_aio)

from cocoindex.connectors.kafka._source import (  # noqa: E402
    _OffsetTracker,
    _PartitionState,
    _TopicMapFeed,
    topic_as_map,
)


# ============================================================================
# Unit tests: _PartitionState offset tracking
# ============================================================================


class TestPartitionStateOffsetTracking:
    """Tests for per-partition offset tracking and commit logic."""

    @pytest.mark.asyncio
    async def test_in_order_completion(self) -> None:
        """Offsets completing in consumption order are committed immediately."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(3)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete in order: 0, 1, 2
        for h in handles:
            h.set_ready()
            await asyncio.sleep(0)  # let task run

        await asyncio.sleep(0.05)  # let commit propagate

        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3  # offset 2 + 1

    @pytest.mark.asyncio
    async def test_out_of_order_completion(self) -> None:
        """Later offsets completing first don't trigger commit until front drains."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(3)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete offset 2 first
        handles[2].set_ready()
        await asyncio.sleep(0.05)
        assert len(consumer._committed) == 0  # nothing committable yet

        # Complete offset 1
        handles[1].set_ready()
        await asyncio.sleep(0.05)
        assert len(consumer._committed) == 0  # still blocked on offset 0

        # Complete offset 0 — all three drain
        handles[0].set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3

    @pytest.mark.asyncio
    async def test_partial_drain(self) -> None:
        """Only contiguous completed offsets from the front drain."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(4)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete 0 and 1, leave 2 pending, complete 3
        handles[0].set_ready()
        handles[1].set_ready()
        handles[3].set_ready()
        await asyncio.sleep(0.05)

        # Should commit offset 2 (after draining 0, 1)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 2

        # Now complete 2 — should drain 2 and 3, commit 4
        handles[2].set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 4

    @pytest.mark.asyncio
    async def test_skip_null_key(self) -> None:
        """Skipped offsets (null key) are immediately completed."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        h0 = MockComponentMountHandle()
        state.track(0, h0)
        state.skip(1)  # null key at offset 1

        # Complete offset 0 — should drain 0 and 1
        h0.set_ready()
        await asyncio.sleep(0.05)

        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 2

        # Add and complete offset 2
        h2 = MockComponentMountHandle()
        state.track(2, h2)
        h2.set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3


# ============================================================================
# E2E tests: watch behavior
# ============================================================================


class MockSubscriber:
    """Mock LiveMapSubscriber that records calls and returns controllable handles."""

    def __init__(self, *, auto_ready: bool = True) -> None:
        self.updates: list[tuple[bytes | str, MockMessage]] = []
        self.deletes: list[bytes | str] = []
        self.ready_called = False
        self.update_all_called = False
        self._auto_ready = auto_ready

    async def update(
        self, key: bytes | str, value: MockMessage
    ) -> MockComponentMountHandle:
        self.updates.append((key, value))
        h = MockComponentMountHandle()
        if self._auto_ready:
            h.set_ready()
        return h

    async def delete(self, key: bytes | str) -> MockComponentMountHandle:
        self.deletes.append(key)
        h = MockComponentMountHandle()
        if self._auto_ready:
            h.set_ready()
        return h

    async def update_all(self) -> None:
        self.update_all_called = True

    async def mark_ready(self) -> None:
        self.ready_called = True


async def _watch_until_done(feed: _TopicMapFeed, sub: MockSubscriber) -> None:
    """Run watch() and suppress CancelledError (raised by mock when messages are exhausted)."""
    try:
        await feed.watch(sub)  # type: ignore[arg-type]
    except asyncio.CancelledError:
        pass


class TestWatchBehavior:
    """E2E tests for _TopicMapFeed.watch()."""

    @pytest.mark.asyncio
    async def test_basic_consumption(self) -> None:
        """Messages are delivered as subscriber.update() calls."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 3)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"v1", offset=0),
            MockMessage(key=b"k2", value=b"v2", offset=1),
            MockMessage(key=b"k3", value=b"v3", offset=2),
        )

        feed = _TopicMapFeed(consumer, ["test-topic"], None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        # Trigger assign manually since subscribe is called inside watch
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
        ]
        assert sub.ready_called

    @pytest.mark.asyncio
    async def test_tombstone_deletion(self) -> None:
        """Messages with None value trigger subscriber.delete()."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"v1", offset=0),
            MockMessage(key=b"k1", value=None, offset=1),
        )

        feed = _TopicMapFeed(consumer, ["test-topic"], None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [(b"k1", b"v1")]
        assert sub.deletes == [b"k1"]

    @pytest.mark.asyncio
    async def test_custom_is_deletion(self) -> None:
        """Custom is_deletion predicate triggers subscriber.delete()."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"DELETED", offset=0),
            MockMessage(key=b"k2", value=b"normal", offset=1),
        )

        feed = _TopicMapFeed(
            consumer,  # type: ignore[arg-type]
            ["test-topic"],
            is_deletion=lambda msg: msg.value() == b"DELETED",
        )
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.deletes == [b"k1"]
        assert [(k, m.value()) for k, m in sub.updates] == [(b"k2", b"normal")]

    @pytest.mark.asyncio
    async def test_null_key_skipped(self) -> None:
        """Messages with None key are skipped."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=None, value=b"v1", offset=0),
            MockMessage(key=b"k2", value=b"v2", offset=1),
        )

        feed = _TopicMapFeed(consumer, ["test-topic"], None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [(b"k2", b"v2")]
        assert len(sub.deletes) == 0

    @pytest.mark.asyncio
    async def test_readiness_after_watermark(self) -> None:
        """mark_ready() called only after all partitions reach watermarks."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.set_watermarks("test-topic", 1, 0, 1)

        ready_at_offset: list[int] = []

        class TrackingSubscriber(MockSubscriber):
            async def update(
                self, key: bytes | str, value: MockMessage
            ) -> MockComponentMountHandle:
                h = await super().update(key, value)
                return h

            async def mark_ready(self) -> None:
                ready_at_offset.append(len(self.updates) + len(self.deletes))
                await super().mark_ready()

        consumer.enqueue(
            MockMessage(
                topic="test-topic", partition=0, key=b"k1", value=b"v1", offset=0
            ),
            MockMessage(
                topic="test-topic", partition=1, key=b"k2", value=b"v2", offset=0
            ),
            MockMessage(
                topic="test-topic", partition=0, key=b"k3", value=b"v3", offset=1
            ),
        )

        feed = _TopicMapFeed(consumer, ["test-topic"], None)  # type: ignore[arg-type]
        sub = TrackingSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign(
                [
                    MockTopicPartition("test-topic", 0),
                    MockTopicPartition("test-topic", 1),
                ]
            )

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.ready_called
        # Ready should be called after all 3 messages (both partitions caught up)
        assert ready_at_offset == [3]

    @pytest.mark.asyncio
    async def test_partition_rebalance_discards_state(self) -> None:
        """Partition revoke discards tracking state."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 0)

        feed = _TopicMapFeed(consumer, ["test-topic"], None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        # We need to test rebalance during consumption.
        # Inject messages that trigger a rebalance mid-stream.
        msg_count = 0

        async def patched_poll(timeout: float = 1.0) -> MockMessage | None:
            nonlocal msg_count
            msg_count += 1
            if msg_count == 1:
                # First: assign partition 0
                await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])
                return MockMessage(key=b"k1", value=b"v1", partition=0, offset=0)
            elif msg_count == 2:
                # Revoke partition 0, assign partition 1
                await consumer.trigger_revoke([MockTopicPartition("test-topic", 0)])
                consumer.set_watermarks("test-topic", 1, 0, 1)
                await consumer.trigger_assign([MockTopicPartition("test-topic", 1)])
                return MockMessage(key=b"k2", value=b"v2", partition=1, offset=0)
            else:
                raise asyncio.CancelledError

        consumer.poll = patched_poll  # type: ignore[assignment]

        # Don't auto-trigger assign from subscribe
        await _watch_until_done(feed, sub)

        update_kvs = [(k, m.value()) for k, m in sub.updates]
        assert (b"k1", b"v1") in update_kvs
        assert (b"k2", b"v2") in update_kvs

    @pytest.mark.asyncio
    async def test_tombstone_always_deletion_even_with_custom_predicate(self) -> None:
        """None value is always deletion even when is_deletion returns False."""
        consumer = MockAIOConsumer()
        consumer.set_watermarks("test-topic", 0, 0, 1)
        consumer.enqueue(
            MockMessage(key=b"k1", value=None, offset=0),
        )

        # is_deletion always returns False, but None value should still be deletion
        feed = _TopicMapFeed(
            consumer,  # type: ignore[arg-type]
            ["test-topic"],
            is_deletion=lambda msg: False,
        )
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.deletes == [b"k1"]
        assert len(sub.updates) == 0
