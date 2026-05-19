"""
Iggy source for CocoIndex.

Exposes an Iggy stream/topic/partition as a :class:`LiveStream` of raw
``ReceiveMessage`` objects. It also provides a keyed-map adapter for payloads
that carry an application-level key.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import timedelta
from typing import Any, Callable

try:
    from apache_iggy import AutoCommit  # type: ignore[import-not-found]
    from apache_iggy import IggyClient  # type: ignore[import-not-found]
    from apache_iggy import IggyConsumer  # type: ignore[import-not-found]
    from apache_iggy import PollingStrategy  # type: ignore[import-not-found]
    from apache_iggy import ReceiveMessage  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "apache-iggy is required to use the Iggy connector. "
        "Please install cocoindex[iggy]."
    ) from e

from cocoindex._internal.live_component import (
    _IMMEDIATE_READY,
    LiveMapSubscriber,
    LiveStream,
    LiveStreamSubscriber,
    ReadyAwaitable,
)
from cocoindex._internal.typing import StableKey

_logger = logging.getLogger(__name__)


# --- Public type aliases ---

IsDeleteFn = Callable[[ReceiveMessage], bool]
KeyFn = Callable[[ReceiveMessage], StableKey | None]


# --- Per-partition state ---


class _PartitionState:
    """Tracks inflight offsets and stores offsets when safe.

    Iggy stores the last consumed offset, while Kafka commits the next offset.
    Internally this class tracks ``_committed_next_offset`` so readiness uses the
    same comparison as Kafka: caught up when the next offset to consume has
    reached the initial high watermark.
    """

    __slots__ = (
        "_consumer",
        "_stream",
        "_topic",
        "_partition",
        "_inflight",
        "_completed",
        "_tasks",
        "_high_watermark",
        "_committed_next_offset",
        "_on_commit",
    )

    def __init__(
        self,
        consumer: IggyConsumer,
        stream: str,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_next_offset: int,
        on_commit: Callable[[], None],
    ) -> None:
        self._consumer = consumer
        self._stream = stream
        self._topic = topic
        self._partition = partition
        self._inflight: deque[int] = deque()
        self._completed: set[int] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._high_watermark = high_watermark
        self._committed_next_offset = committed_next_offset
        self._on_commit = on_commit

    def is_caught_up(self) -> bool:
        """Whether this partition has consumed up to its initial high watermark."""
        return self._committed_next_offset >= self._high_watermark

    def track(self, offset: int, handle: ReadyAwaitable) -> None:
        """Register an inflight offset with its readiness handle."""
        self._inflight.append(offset)
        if handle is _IMMEDIATE_READY:
            self._completed.add(offset)
            self._try_drain_and_store()
            return

        task = asyncio.create_task(self._await_handle(offset, handle))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _await_handle(self, offset: int, handle: ReadyAwaitable) -> None:
        """Await a readiness handle and mark the offset as completed."""
        try:
            await handle.ready()
        except asyncio.CancelledError:
            return
        self._completed.add(offset)
        self._try_drain_and_store()

    def _try_drain_and_store(self) -> None:
        """Drain contiguous completed offsets from the front and store the last."""
        last_drained: int | None = None
        while self._inflight and self._inflight[0] in self._completed:
            offset = self._inflight.popleft()
            self._completed.discard(offset)
            last_drained = offset

        if last_drained is not None:
            self._committed_next_offset = last_drained + 1
            self._on_commit()
            asyncio.ensure_future(self._store_offset(last_drained))

    async def _store_offset(self, offset: int) -> None:
        """Store the given last-consumed offset in Iggy."""
        try:
            await self._consumer.store_offset(offset, self._partition)
        except Exception:
            _logger.exception(
                "Failed to store offset %d for Iggy %s/%s partition %d",
                offset,
                self._stream,
                self._topic,
                self._partition,
            )

    def discard(self) -> None:
        """Cancel background readiness tasks and clear state."""
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        self._inflight.clear()
        self._completed.clear()


# --- Offset tracker ---


class _OffsetTracker:
    """Tracks all partitions this stream is consuming.

    The current Python Iggy SDK does not expose Kafka-style assignment callbacks
    or per-partition high watermarks. This connector therefore requires a known
    initial high watermark for each consumed partition before watching starts.
    """

    __slots__ = ("_partitions", "_initialized", "ready_event")

    def __init__(self) -> None:
        self._partitions: dict[int, _PartitionState] = {}
        self._initialized = False
        self.ready_event = asyncio.Event()

    def _check_ready(self) -> None:
        if self._initialized and all(
            state.is_caught_up() for state in self._partitions.values()
        ):
            self.ready_event.set()

    def add(
        self,
        consumer: IggyConsumer,
        stream: str,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_next_offset: int,
    ) -> _PartitionState:
        """Create and register a partition state."""
        state = _PartitionState(
            consumer=consumer,
            stream=stream,
            topic=topic,
            partition=partition,
            high_watermark=high_watermark,
            committed_next_offset=committed_next_offset,
            on_commit=self._check_ready,
        )
        self._partitions[partition] = state
        return state

    def get(self, partition: int) -> _PartitionState:
        """Get an initialized partition state."""
        try:
            return self._partitions[partition]
        except KeyError as e:
            raise RuntimeError(
                "Received an Iggy message for an untracked partition. "
                "Use an explicit partition_id per TopicStream instance."
            ) from e

    def mark_initialized(self) -> None:
        """Mark initial partition state loaded and check readiness."""
        self._initialized = True
        self._check_ready()

    def discard_all(self) -> None:
        """Discard all partition states."""
        for state in self._partitions.values():
            state.discard()
        self._partitions.clear()


def _committed_next_offset(stored_offset: int | None) -> int:
    """Convert Iggy's last-stored offset into the next offset to consume."""
    return 0 if stored_offset is None else stored_offset + 1


# --- TopicStream: LiveStream[ReceiveMessage] primitive ---


class TopicStream:
    """A :class:`LiveStream` of raw Iggy ``ReceiveMessage`` objects.

    The stream creates an Iggy consumer group with auto-commit disabled, sends
    messages to CocoIndex, and stores offsets only after the returned
    ``ReadyAwaitable`` is ready. This mirrors the Kafka connector's at-least-once
    processing contract.
    """

    __slots__ = (
        "_client",
        "_consumer_group",
        "_stream",
        "_topic",
        "_partition_id",
        "_batch_length",
        "_poll_interval",
        "_polling_retry_interval",
        "_init_retries",
        "_init_retry_interval",
        "_allow_replay",
        "_initial_high_watermark",
    )

    def __init__(
        self,
        client: IggyClient,
        consumer_group: str,
        stream: str,
        topic: str,
        *,
        partition_id: int = 0,
        batch_length: int = 100,
        poll_interval: timedelta | None = None,
        polling_retry_interval: timedelta | None = None,
        init_retries: int | None = None,
        init_retry_interval: timedelta | None = None,
        allow_replay: bool = False,
        initial_high_watermark: int | None = None,
    ) -> None:
        self._client = client
        self._consumer_group = consumer_group
        self._stream = stream
        self._topic = topic
        self._partition_id = partition_id
        self._batch_length = batch_length
        self._poll_interval = poll_interval
        self._polling_retry_interval = polling_retry_interval
        self._init_retries = init_retries
        self._init_retry_interval = init_retry_interval
        self._allow_replay = allow_replay
        self._initial_high_watermark = initial_high_watermark

    def payloads(self) -> LiveStream[bytes]:
        """View of this stream yielding each message payload as bytes."""
        return _TopicPayloadsStream(self)

    async def _resolve_initial_high_watermark(self) -> int:
        """Resolve the initial next-offset watermark for readiness."""
        if self._initial_high_watermark is not None:
            return self._initial_high_watermark

        topic = await self._client.get_topic(self._stream, self._topic)
        if topic is None:
            raise RuntimeError(
                f"Iggy topic {self._stream}/{self._topic} does not exist."
            )
        if topic.partitions_count != 1:
            raise RuntimeError(
                "The Python Iggy SDK does not expose per-partition high watermarks. "
                "Pass initial_high_watermark for multi-partition topics, or consume "
                "a single-partition topic."
            )
        return int(topic.messages_count)

    async def _create_consumer(self) -> IggyConsumer:
        """Create an Iggy consumer group configured for manual offset storage."""
        return await self._client.consumer_group(
            name=self._consumer_group,
            stream=self._stream,
            topic=self._topic,
            partition_id=self._partition_id,
            polling_strategy=PollingStrategy.Next(),
            batch_length=self._batch_length,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=self._poll_interval,
            polling_retry_interval=self._polling_retry_interval,
            init_retries=self._init_retries,
            init_retry_interval=self._init_retry_interval,
            allow_replay=self._allow_replay,
        )

    async def watch(self, subscriber: LiveStreamSubscriber[ReceiveMessage]) -> None:
        """Consume messages and deliver them to the subscriber."""
        high_watermark = await self._resolve_initial_high_watermark()
        consumer = await self._create_consumer()
        tracker = _OffsetTracker()
        tracker.add(
            consumer=consumer,
            stream=self._stream,
            topic=self._topic,
            partition=self._partition_id,
            high_watermark=high_watermark,
            committed_next_offset=_committed_next_offset(
                consumer.get_last_stored_offset(self._partition_id)
            ),
        )
        tracker.mark_initialized()

        ready_signaled = False
        active_next_task: asyncio.Future[ReceiveMessage] | None = None
        last_delivered_offsets: dict[int, int] = {}
        iterator = consumer.iter_messages().__aiter__()

        async def _process_message(message: ReceiveMessage) -> None:
            partition = message.partition_id()
            offset = message.offset()
            last_delivered_offset = last_delivered_offsets.get(partition)
            if last_delivered_offset is not None and offset <= last_delivered_offset:
                _logger.debug(
                    "Skipping duplicate Iggy message for %s/%s partition %d "
                    "offset %d; last delivered offset is %d",
                    self._stream,
                    self._topic,
                    partition,
                    offset,
                    last_delivered_offset,
                )
                return
            last_delivered_offsets[partition] = offset
            part_state = tracker.get(partition)
            handle = await subscriber.send(message)
            part_state.track(offset, handle)

        try:
            while True:
                if not ready_signaled:
                    if active_next_task is None:
                        active_next_task = asyncio.ensure_future(anext(iterator))
                    ready_task: asyncio.Future[bool] = asyncio.ensure_future(
                        tracker.ready_event.wait()
                    )
                    wait_set: set[asyncio.Future[Any]] = {
                        active_next_task,
                        ready_task,
                    }
                    done, _ = await asyncio.wait(
                        wait_set,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if ready_task in done:
                        await subscriber.mark_ready()
                        ready_signaled = True
                    else:
                        ready_task.cancel()

                    if active_next_task in done:
                        message = await active_next_task
                        active_next_task = None
                        await _process_message(message)
                    continue

                message = await anext(iterator)
                await _process_message(message)
        except StopAsyncIteration:
            return
        finally:
            if active_next_task is not None:
                active_next_task.cancel()
            tracker.discard_all()


class _TopicPayloadsStream:
    """``LiveStream[bytes]`` view over a :class:`TopicStream`."""

    __slots__ = ("_source",)

    def __init__(self, source: TopicStream) -> None:
        self._source = source

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        await self._source.watch(_PayloadsAdapter(subscriber))


class _PayloadsAdapter:
    """Adapts a ``LiveStreamSubscriber[bytes]`` to receive Iggy messages."""

    __slots__ = ("_downstream",)

    def __init__(self, downstream: LiveStreamSubscriber[bytes]) -> None:
        self._downstream = downstream

    async def send(self, message: ReceiveMessage) -> ReadyAwaitable:
        return await self._downstream.send(message.payload())

    async def mark_ready(self) -> None:
        await self._downstream.mark_ready()


# --- LiveMapFeed implementation ---


class _StreamToMapSubscriber:
    """Adapts a :class:`LiveMapSubscriber` to consume a ``LiveStream``."""

    __slots__ = ("_map_sub", "_key", "_is_deletion")

    def __init__(
        self,
        map_sub: LiveMapSubscriber[StableKey, ReceiveMessage],
        key: KeyFn,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._map_sub = map_sub
        self._key = key
        self._is_deletion = is_deletion

    async def send(self, message: ReceiveMessage) -> ReadyAwaitable:
        key = self._key(message)
        if key is None:
            _logger.error(
                "Skipping Iggy message without application key at partition %d "
                "offset %d",
                message.partition_id(),
                message.offset(),
            )
            return _IMMEDIATE_READY
        if self._is_deletion is not None and self._is_deletion(message):
            return await self._map_sub.delete(key)
        return await self._map_sub.update(key, message)

    async def mark_ready(self) -> None:
        await self._map_sub.mark_ready()


class _TopicMapFeed:
    """``LiveMapFeed`` view over a :class:`TopicStream`."""

    __slots__ = ("_stream", "_key", "_is_deletion")

    def __init__(
        self,
        stream: TopicStream,
        key: KeyFn,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._stream = stream
        self._key = key
        self._is_deletion = is_deletion

    async def watch(
        self, subscriber: LiveMapSubscriber[StableKey, ReceiveMessage]
    ) -> None:
        await self._stream.watch(
            _StreamToMapSubscriber(subscriber, self._key, self._is_deletion)
        )


# --- Public API ---


def topic_as_stream(
    client: IggyClient,
    consumer_group: str,
    stream: str,
    topic: str,
    *,
    partition_id: int = 0,
    batch_length: int = 100,
    poll_interval: timedelta | None = None,
    polling_retry_interval: timedelta | None = None,
    init_retries: int | None = None,
    init_retry_interval: timedelta | None = None,
    allow_replay: bool = False,
    initial_high_watermark: int | None = None,
) -> TopicStream:
    """
    Treat an Iggy stream/topic/partition as a :class:`LiveStream`.

    ``initial_high_watermark`` is the initial next offset for readiness. It is
    optional for single-partition topics because the connector can use
    ``TopicDetails.messages_count``. For multi-partition topics the Python SDK
    does not currently expose per-partition watermarks, so callers must provide
    the exact partition watermark to preserve Kafka-strength readiness semantics.
    """
    return TopicStream(
        client,
        consumer_group,
        stream,
        topic,
        partition_id=partition_id,
        batch_length=batch_length,
        poll_interval=poll_interval,
        polling_retry_interval=polling_retry_interval,
        init_retries=init_retries,
        init_retry_interval=init_retry_interval,
        allow_replay=allow_replay,
        initial_high_watermark=initial_high_watermark,
    )


def topic_as_map(
    client: IggyClient,
    consumer_group: str,
    stream: str,
    topic: str,
    *,
    key: KeyFn,
    partition_id: int = 0,
    batch_length: int = 100,
    poll_interval: timedelta | None = None,
    polling_retry_interval: timedelta | None = None,
    init_retries: int | None = None,
    init_retry_interval: timedelta | None = None,
    allow_replay: bool = False,
    initial_high_watermark: int | None = None,
    is_deletion: IsDeleteFn | None = None,
) -> _TopicMapFeed:
    """
    Treat an Iggy stream/topic/partition as a live keyed map.

    Iggy Python messages do not expose Kafka-style message keys or tombstones,
    so callers must provide ``key`` to extract an application-level key from the
    message payload or metadata. Use ``is_deletion`` for application-level
    delete events.
    """
    return _TopicMapFeed(
        topic_as_stream(
            client,
            consumer_group,
            stream,
            topic,
            partition_id=partition_id,
            batch_length=batch_length,
            poll_interval=poll_interval,
            polling_retry_interval=polling_retry_interval,
            init_retries=init_retries,
            init_retry_interval=init_retry_interval,
            allow_replay=allow_replay,
            initial_high_watermark=initial_high_watermark,
        ),
        key,
        is_deletion,
    )


__all__ = [
    "IsDeleteFn",
    "KeyFn",
    "TopicStream",
    "topic_as_map",
    "topic_as_stream",
]
