"""Tests for the Iggy source connector.

These tests mock the Python Iggy SDK to verify CocoIndex source semantics
without a real Iggy server.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.common.iggy_stub import MockIggyClient, MockIggyConsumer, MockReceiveMessage

# ``tests.common.iggy_stub`` must be imported before the connector so that the
# connector binds the stub SDK.
from cocoindex._internal.live_component import _IMMEDIATE_READY
from cocoindex.connectors.iggy._source import (
    TopicStream,
    _PartitionState,
    topic_as_map,
    topic_as_stream,
)


class MockReadyHandle:
    def __init__(self) -> None:
        self._ready_event = asyncio.Event()

    async def ready(self) -> None:
        await self._ready_event.wait()

    def set_ready(self) -> None:
        self._ready_event.set()


class MockStreamSubscriber:
    def __init__(self, *, auto_ready: bool = True) -> None:
        self.messages: list[MockReceiveMessage] = []
        self.ready_called = False
        self._auto_ready = auto_ready

    async def send(self, message: MockReceiveMessage) -> MockReadyHandle:
        self.messages.append(message)
        handle = MockReadyHandle()
        if self._auto_ready:
            handle.set_ready()
        return handle

    async def mark_ready(self) -> None:
        self.ready_called = True


class MockMapSubscriber:
    def __init__(self) -> None:
        self.updates: list[tuple[bytes | str, MockReceiveMessage]] = []
        self.deletes: list[bytes | str] = []
        self.ready_called = False

    async def update(
        self, key: bytes | str, value: MockReceiveMessage
    ) -> MockReadyHandle:
        self.updates.append((key, value))
        handle = MockReadyHandle()
        handle.set_ready()
        return handle

    async def delete(self, key: bytes | str) -> MockReadyHandle:
        self.deletes.append(key)
        handle = MockReadyHandle()
        handle.set_ready()
        return handle

    async def update_all(self) -> None:
        pass

    async def mark_ready(self) -> None:
        self.ready_called = True


class TestPartitionState:
    @pytest.mark.asyncio
    async def test_stores_last_consumed_offset_after_contiguous_completion(
        self,
    ) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=3,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockReadyHandle() for _ in range(3)]
        for offset, handle in enumerate(handles):
            state.track(offset, handle)

        handles[2].set_ready()
        await asyncio.sleep(0.01)
        assert consumer.stored_offsets == []

        handles[0].set_ready()
        handles[1].set_ready()
        await asyncio.sleep(0.05)

        assert consumer.stored_offsets[-1] == (2, 0)

    @pytest.mark.asyncio
    async def test_immediate_ready_fast_path(self) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.sleep(0.05)

        assert consumer.stored_offsets == [(0, 0)]


class TestTopicStream:
    @pytest.mark.asyncio
    async def test_stream_consumes_and_stores_offsets_after_ready(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"v1", offset=0),
                MockReceiveMessage(payload=b"v2", offset=1),
            ]
        )
        client = MockIggyClient(consumer, messages_count=2)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber()

        await stream.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [m.payload() for m in subscriber.messages] == [b"v1", b"v2"]
        assert consumer.stored_offsets[-1] == (1, 0)
        assert subscriber.ready_called
        assert client.consumer_group_calls[0]["auto_commit"].__class__.__name__ == (
            "Disabled"
        )

    @pytest.mark.asyncio
    async def test_stream_skips_duplicate_offsets_from_live_consumer(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"v1", offset=0),
                MockReceiveMessage(payload=b"v2", offset=1),
                MockReceiveMessage(payload=b"v2-duplicate", offset=1),
                MockReceiveMessage(payload=b"v3", offset=2),
            ]
        )
        client = MockIggyClient(consumer, messages_count=3)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber()

        await stream.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [(m.offset(), m.payload()) for m in subscriber.messages] == [
            (0, b"v1"),
            (1, b"v2"),
            (2, b"v3"),
        ]
        assert consumer.stored_offsets[-1] == (2, 0)
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_payloads_view_forwards_bytes(self) -> None:
        consumer = MockIggyConsumer([MockReceiveMessage(payload=b"payload", offset=0)])
        client = MockIggyClient(consumer, messages_count=1)
        payloads = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        ).payloads()

        class BytesSubscriber:
            def __init__(self) -> None:
                self.payloads: list[bytes] = []
                self.ready_called = False

            async def send(self, payload: bytes) -> MockReadyHandle:
                self.payloads.append(payload)
                handle = MockReadyHandle()
                handle.set_ready()
                return handle

            async def mark_ready(self) -> None:
                self.ready_called = True

        subscriber = BytesSubscriber()
        await payloads.watch(subscriber)
        await asyncio.sleep(0.05)

        assert subscriber.payloads == [b"payload"]
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_multi_partition_requires_explicit_watermark(self) -> None:
        consumer = MockIggyConsumer([])
        client = MockIggyClient(consumer, messages_count=10, partitions_count=2)
        stream = TopicStream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )

        with pytest.raises(RuntimeError, match="per-partition high watermarks"):
            await stream.watch(MockStreamSubscriber())  # type: ignore[arg-type]


class TestTopicMap:
    @pytest.mark.asyncio
    async def test_map_uses_application_key_and_deletion_predicate(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"k1:v1", offset=0),
                MockReceiveMessage(payload=b"k1:DELETE", offset=1),
            ]
        )
        client = MockIggyClient(consumer, messages_count=2)
        feed = topic_as_map(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
            key=lambda msg: msg.payload().split(b":", 1)[0],
            is_deletion=lambda msg: msg.payload().endswith(b"DELETE"),
        )
        subscriber = MockMapSubscriber()

        await feed.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [(k, m.payload()) for k, m in subscriber.updates] == [(b"k1", b"k1:v1")]
        assert subscriber.deletes == [b"k1"]
        assert subscriber.ready_called
