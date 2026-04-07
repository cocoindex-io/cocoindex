"""
Kafka source for CocoIndex.

Treats a Kafka topic as a live keyed map — each message is a create/update/delete
event for a key. Returns a ``LiveMapFeed`` for use with ``mount_each()``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Callable, Protocol

try:
    from confluent_kafka import Message, TopicPartition  # type: ignore[import-not-found]
    from confluent_kafka.aio import AIOConsumer  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "confluent_kafka is required to use the Kafka connector. "
        "Please install cocoindex[kafka]."
    ) from e
from cocoindex._internal.live_component import LiveMapSubscriber
from cocoindex._internal.typing import StableKey

_logger = logging.getLogger(__name__)

# --- Public type aliases ---

IsDeleteFn = Callable[[Message], bool]


class _ReadyAwaitable(Protocol):
    """Protocol for objects with an async ready() method (e.g. ComponentMountHandle)."""

    async def ready(self) -> None: ...


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

    def track(self, offset: int, handle: _ReadyAwaitable) -> None:
        """Register an inflight offset with its ComponentMountHandle."""
        self._inflight.append(offset)
        task = asyncio.create_task(self._await_handle(offset, handle))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def skip(self, offset: int) -> None:
        """Mark an offset as immediately completed (e.g. null-key message)."""
        self._inflight.append(offset)
        self._completed.add(offset)
        self._try_drain_and_commit()

    async def _await_handle(self, offset: int, handle: _ReadyAwaitable) -> None:
        """Await a ComponentMountHandle and mark the offset as completed."""
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


# --- LiveMapFeed implementation ---


class _TopicMapFeed:
    """LiveMapFeed implementation for Kafka topics."""

    __slots__ = ("_consumer", "_topics", "_is_deletion")

    def __init__(
        self,
        consumer: AIOConsumer,
        topics: list[str],
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._consumer = consumer
        self._topics = topics
        self._is_deletion = is_deletion

    async def watch(self, subscriber: LiveMapSubscriber[StableKey, Message]) -> None:
        """Consume messages and deliver them as map changes."""
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
            """Dispatch a consumed message to the subscriber."""
            if msg is None or msg.error() is not None:
                if msg is not None:
                    _logger.error("Consumer error: %s", msg.error())
                return

            key = msg.key()
            topic: str = msg.topic()  # type: ignore[assignment]
            partition: int = msg.partition()  # type: ignore[assignment]
            offset: int = msg.offset()  # type: ignore[assignment]

            part_state = tracker.get_or_create(topic, partition)

            if key is None:
                _logger.error(
                    "Skipping message with null key at %s/%d offset %d",
                    topic,
                    partition,
                    offset,
                )
                part_state.skip(offset)
            elif msg.value() is None or (
                self._is_deletion is not None and self._is_deletion(msg)
            ):
                handle = await subscriber.delete(key)
                part_state.track(offset, handle)
            else:
                handle = await subscriber.update(key, msg)
                part_state.track(offset, handle)

        try:
            # Phase 1: Wait for initial partition assignment.
            # on_assign fires during poll(), which sets watermarks on partition states.
            while not tracker.is_assigned():
                await _process_message(await self._consumer.poll(timeout=1.0))

            # Phase 2: Consume messages, racing poll against the readiness event.
            while True:
                if not ready_signaled:
                    poll_task = asyncio.ensure_future(self._consumer.poll(timeout=60.0))
                    ready_task = asyncio.ensure_future(tracker.ready_event.wait())
                    done, _ = await asyncio.wait(
                        {poll_task, ready_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if ready_task in done:
                        await subscriber.mark_ready()
                        ready_signaled = True
                    else:
                        ready_task.cancel()
                    # Always process the poll result
                    await _process_message(await poll_task)
                else:
                    await _process_message(await self._consumer.poll(timeout=60.0))

        finally:
            tracker.discard_all()
            await self._consumer.unsubscribe()


# --- Public API ---


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
    return _TopicMapFeed(consumer, topics, is_deletion)


__all__ = [
    "IsDeleteFn",
    "topic_as_map",
]
