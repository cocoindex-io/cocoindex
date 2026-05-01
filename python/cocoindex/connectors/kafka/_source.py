"""
Kafka source for CocoIndex.

Exposes a Kafka topic as a :class:`LiveStream` of raw messages and as a
:class:`LiveMapFeed` of keyed change events. ``topic_as_stream`` returns the
primitive stream (with a ``payloads()`` view yielding bytes), and
``topic_as_map`` interprets messages as a keyed map for use with ``mount_each``.

User-facing docs and worked examples:
https://cocoindex.io/docs/connectors/kafka
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Callable

try:
    from confluent_kafka import Message, TopicPartition  # type: ignore[import-not-found]
    from confluent_kafka.aio import AIOConsumer  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "confluent_kafka is required to use the Kafka connector. "
        "Please install cocoindex[kafka]."
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

IsDeleteFn = Callable[[Message], bool]


# --- Per-partition state ---


class _PartitionState:
    """Tracks inflight offsets for a single partition and commits when safe.

    Stores the high watermark and committed offset at assignment time for
    readiness tracking. Notifies the parent tracker when committed offset advances.
    """

    __slots__ = (
        "_consumer",
        "_topic",
        "_partition",
        "_inflight",
        "_completed",
        "_tasks",
        "_high_watermark",
        "_committed_offset",
        "_on_commit",
    )

    def __init__(
        self,
        consumer: AIOConsumer,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_offset: int,
        on_commit: Callable[[], None],
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._partition = partition
        self._inflight: deque[int] = deque()
        self._completed: set[int] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._high_watermark = high_watermark
        self._committed_offset = committed_offset
        self._on_commit = on_commit

    def is_caught_up(self) -> bool:
        """Whether this partition has consumed up to its initial high watermark."""
        return self._committed_offset >= self._high_watermark

    def track(self, offset: int, handle: ReadyAwaitable) -> None:
        """Register an inflight offset with its readiness handle.

        Fast path: if ``handle is _IMMEDIATE_READY``, record completion
        synchronously without spawning a task.
        """
        if handle is _IMMEDIATE_READY:
            self._inflight.append(offset)
            self._completed.add(offset)
            self._try_drain_and_commit()
            return
        self._inflight.append(offset)
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
        self._try_drain_and_commit()

    def _try_drain_and_commit(self) -> None:
        """Drain contiguous completed offsets from the front and commit."""
        last_drained: int | None = None
        while self._inflight and self._inflight[0] in self._completed:
            offset = self._inflight.popleft()
            self._completed.discard(offset)
            last_drained = offset

        if last_drained is not None:
            commit_offset = last_drained + 1
            self._committed_offset = commit_offset
            self._on_commit()
            asyncio.ensure_future(self._commit(commit_offset))

    async def _commit(self, offset: int) -> None:
        """Commit the given offset to the broker."""
        try:
            await self._consumer.commit(
                offsets=[TopicPartition(self._topic, self._partition, offset)],
                asynchronous=False,
            )
        except Exception:
            _logger.exception(
                "Failed to commit offset %d for %s/%d",
                offset,
                self._topic,
                self._partition,
            )

    def discard(self) -> None:
        """Cancel all background tasks and clear state."""
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        self._inflight.clear()
        self._completed.clear()


# --- Offset tracker ---


class _OffsetTracker:
    """Manages _PartitionState objects across partitions with rebalance support.

    Sets ``ready_event`` when all partitions have consumed up to their initial
    high watermarks.
    """

    __slots__ = ("_consumer", "_partitions", "_assigned", "ready_event")

    def __init__(self, consumer: AIOConsumer) -> None:
        self._consumer = consumer
        self._partitions: dict[tuple[str, int], _PartitionState] = {}
        self._assigned = False
        self.ready_event = asyncio.Event()

    def _check_ready(self) -> None:
        """Set ready_event if all partitions are caught up."""
        if self._assigned and all(s.is_caught_up() for s in self._partitions.values()):
            self.ready_event.set()

    def get_or_create(self, topic: str, partition: int) -> _PartitionState:
        """Get a partition state, creating one (already caught up) if not found."""
        key = (topic, partition)
        state = self._partitions.get(key)
        if state is None:
            state = _PartitionState(
                self._consumer,
                topic,
                partition,
                high_watermark=0,
                committed_offset=0,
                on_commit=self._check_ready,
            )
            self._partitions[key] = state
        return state

    def add(
        self,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_offset: int,
    ) -> _PartitionState:
        """Create and register a partition state."""
        state = _PartitionState(
            self._consumer,
            topic,
            partition,
            high_watermark=high_watermark,
            committed_offset=committed_offset,
            on_commit=self._check_ready,
        )
        self._partitions[(topic, partition)] = state
        return state

    def mark_assigned(self) -> None:
        """Mark that at least one assignment has been received."""
        self._assigned = True

    def on_revoke(self, partitions: list[TopicPartition]) -> None:
        """Handle partition revocation (discard state)."""
        for tp in partitions:
            key = (tp.topic, tp.partition)
            state = self._partitions.pop(key, None)
            if state is not None:
                state.discard()

    def is_assigned(self) -> bool:
        """Whether at least one partition assignment has been received."""
        return self._assigned

    def discard_all(self) -> None:
        """Discard all partition states."""
        for state in self._partitions.values():
            state.discard()
        self._partitions.clear()


# --- TopicStream: LiveStream[Message] primitive ---


class TopicStream:
    """A :class:`LiveStream` of raw Kafka :class:`Message` objects.

    Owns the consumer subscription and delivers every valid polled message to
    the subscriber's ``send()``. ``mark_ready()`` is called once per
    ``watch()`` invocation, when all initially-assigned partitions have been
    consumed up to their initial high watermarks.

    The underlying ``AIOConsumer`` can only be subscribed once at a time, so
    ``watch()`` is single-shot per :class:`TopicStream` instance: at most one
    of ``watch()`` and ``payloads().watch()`` (across all ``payloads()``
    views) may be active concurrently. Documented contract; not runtime-checked.
    """

    __slots__ = ("_consumer", "_topics")

    def __init__(self, consumer: AIOConsumer, topics: list[str]) -> None:
        self._consumer = consumer
        self._topics = topics

    def payloads(self) -> LiveStream[bytes]:
        """View of this stream yielding each message's payload as bytes.

        Null-valued messages (Kafka tombstones) are filtered out of the bytes
        view; consumers that need tombstone semantics should subscribe at the
        ``Message`` level.
        """
        return _TopicPayloadsStream(self)

    async def watch(self, subscriber: LiveStreamSubscriber[Message]) -> None:
        """Consume messages from the topics and deliver them to the subscriber."""
        tracker = _OffsetTracker(self._consumer)
        ready_signaled = False

        async def _on_assign(
            _consumer: AIOConsumer, partitions: list[TopicPartition]
        ) -> None:
            tracker.mark_assigned()
            if not ready_signaled and partitions:
                committed = await self._consumer.committed(partitions)
                for tp, committed_tp in zip(partitions, committed):
                    try:
                        _, high = await self._consumer.get_watermark_offsets(tp)
                    except Exception:
                        _logger.exception(
                            "Failed to get watermark offsets for %s/%d",
                            tp.topic,
                            tp.partition,
                        )
                        high = 0
                    tracker.add(
                        tp.topic,
                        tp.partition,
                        high_watermark=high,
                        committed_offset=max(committed_tp.offset, 0),
                    )
            # Check if already caught up (e.g. empty topic, or fully consumed)
            tracker._check_ready()

        async def _on_revoke(
            _consumer: AIOConsumer, partitions: list[TopicPartition]
        ) -> None:
            tracker.on_revoke(partitions)

        await self._consumer.subscribe(
            self._topics,
            on_assign=_on_assign,
            on_revoke=_on_revoke,
        )

        async def _process_message(msg: Message | None) -> None:
            """Forward a polled message to the subscriber and track its offset."""
            if msg is None or msg.error() is not None:
                if msg is not None:
                    _logger.error("Consumer error: %s", msg.error())
                return

            topic: str = msg.topic()  # type: ignore[assignment]
            partition: int = msg.partition()  # type: ignore[assignment]
            offset: int = msg.offset()  # type: ignore[assignment]

            part_state = tracker.get_or_create(topic, partition)
            handle = await subscriber.send(msg)
            part_state.track(offset, handle)

        # AIOConsumer.poll() runs Consumer.poll() in a ThreadPoolExecutor.
        # Keep the timeout short so the blocked thread returns promptly on
        # cancellation — the executor waits for threads during shutdown.
        _POLL_TIMEOUT = 1.0

        active_poll_task: asyncio.Task[Message | None] | None = None
        try:
            # Phase 1: Wait for initial partition assignment.
            while not tracker.is_assigned():
                await _process_message(await self._consumer.poll(timeout=_POLL_TIMEOUT))

            # Phase 2: Consume messages, racing poll against the readiness event.
            while True:
                if not ready_signaled:
                    active_poll_task = asyncio.ensure_future(
                        self._consumer.poll(timeout=_POLL_TIMEOUT)
                    )
                    ready_task = asyncio.ensure_future(tracker.ready_event.wait())
                    done, _ = await asyncio.wait(
                        {active_poll_task, ready_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if ready_task in done:
                        await subscriber.mark_ready()
                        ready_signaled = True
                    else:
                        ready_task.cancel()
                    await _process_message(await active_poll_task)
                    active_poll_task = None
                else:
                    await _process_message(
                        await self._consumer.poll(timeout=_POLL_TIMEOUT)
                    )

        finally:
            if active_poll_task is not None:
                active_poll_task.cancel()
            tracker.discard_all()
            await self._consumer.unsubscribe()


class _TopicPayloadsStream:
    """``LiveStream[bytes]`` view over a :class:`TopicStream`."""

    __slots__ = ("_source",)

    def __init__(self, source: TopicStream) -> None:
        self._source = source

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        await self._source.watch(_PayloadsAdapter(subscriber))


class _PayloadsAdapter:
    """Adapts a ``LiveStreamSubscriber[bytes]`` to receive Kafka ``Message`` objects."""

    __slots__ = ("_downstream",)

    def __init__(self, downstream: LiveStreamSubscriber[bytes]) -> None:
        self._downstream = downstream

    async def send(self, message: Message) -> ReadyAwaitable:
        value = message.value()
        if value is None:
            return _IMMEDIATE_READY
        return await self._downstream.send(value)

    async def mark_ready(self) -> None:
        await self._downstream.mark_ready()


# --- LiveMapFeed implementation ---


class _StreamToMapSubscriber:
    """Adapts a :class:`LiveMapSubscriber` to consume a ``LiveStream[Message]``."""

    __slots__ = ("_map_sub", "_is_deletion")

    def __init__(
        self,
        map_sub: LiveMapSubscriber[StableKey, Message],
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._map_sub = map_sub
        self._is_deletion = is_deletion

    async def send(self, message: Message) -> ReadyAwaitable:
        msg = message
        key = msg.key()
        if key is None:
            _logger.error(
                "Skipping message with null key at %s/%d offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            return _IMMEDIATE_READY
        if msg.value() is None or (
            self._is_deletion is not None and self._is_deletion(msg)
        ):
            return await self._map_sub.delete(key)
        return await self._map_sub.update(key, msg)

    async def mark_ready(self) -> None:
        await self._map_sub.mark_ready()


class _TopicMapFeed:
    """``LiveMapFeed`` view over a :class:`TopicStream`."""

    __slots__ = ("_stream", "_is_deletion")

    def __init__(
        self,
        stream: TopicStream,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._stream = stream
        self._is_deletion = is_deletion

    async def watch(self, subscriber: LiveMapSubscriber[StableKey, Message]) -> None:
        await self._stream.watch(_StreamToMapSubscriber(subscriber, self._is_deletion))


# --- Public API ---


def topic_as_stream(consumer: AIOConsumer, topics: list[str]) -> TopicStream:
    """
    Treat a Kafka topic as a :class:`LiveStream` of raw messages.

    The returned :class:`TopicStream` implements ``LiveStream[Message]`` and
    exposes ``.payloads()`` for a ``LiveStream[bytes]`` view of message values
    — the typical input for sources that consume opaque event payloads (e.g.
    the OCI Object Storage source's live mode; see
    https://cocoindex.io/docs/connectors/oci_object_storage#live-bucket-watching).

    The consumer must be **unsubscribed** — ``topic_as_stream()`` handles
    subscription internally to register partition rebalance callbacks.

    Args:
        consumer: An unsubscribed ``AIOConsumer``. Auto-commit should be disabled.
        topics: Topics to subscribe to.

    Returns:
        A :class:`TopicStream` (single-watcher; bind to one consumer).
    """
    return TopicStream(consumer, topics)


def topic_as_map(
    consumer: AIOConsumer,
    topics: list[str],
    *,
    is_deletion: IsDeleteFn | None = None,
) -> _TopicMapFeed:
    """
    Treat a Kafka topic as a live keyed map.

    Returns a ``LiveMapFeed`` that streams change events (updates/deletes) from the
    given topics. Each item is keyed by the message key, and the value is the full
    ``confluent_kafka.Message`` object. Suitable for passing to ``mount_each()`` for
    parallel processing with automatic offset management.

    The consumer must be **unsubscribed** — ``topic_as_map()`` handles subscription
    internally to register partition rebalance callbacks.

    Args:
        consumer: An unsubscribed ``AIOConsumer``. Auto-commit should be disabled.
        topics: Topics to subscribe to.
        is_deletion: Optional predicate ``(message) -> bool`` for custom deletion
            detection on non-tombstone messages. Messages with ``None`` value (Kafka
            tombstones) are always treated as deletions regardless of this predicate.

    Returns:
        A ``LiveMapFeed[bytes | str, Message]`` for use with ``mount_each()``.
    """
    return _TopicMapFeed(topic_as_stream(consumer, topics), is_deletion)


__all__ = [
    "IsDeleteFn",
    "TopicStream",
    "topic_as_map",
    "topic_as_stream",
]
