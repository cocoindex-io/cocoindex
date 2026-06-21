"""
Apache Pulsar source for CocoIndex.

Exposes a Pulsar topic as a :class:`LiveStream` of raw messages and as a
:class:`LiveMapFeed` of keyed change events. ``topic_as_stream`` returns the
primitive stream (with a ``payloads()`` view yielding bytes), and
``topic_as_map`` interprets messages as a keyed map (keyed by the message
partition key) for use with ``mount_each``.

Durability is the Pulsar subscription cursor: the connector consumes with manual
acknowledgement and acks a message only after the downstream readiness handle
completes, so an unprocessed message is redelivered on restart (at-least-once),
mirroring the Kafka source's commit-after-readiness back-pressure.

The pulsar-client Python client is synchronous, so blocking ``receive`` /
``acknowledge`` / ``close`` calls run off the event loop via ``asyncio.to_thread``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable

try:
    import pulsar  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "pulsar-client is required to use the Pulsar connector. "
        "Please install cocoindex[pulsar]."
    ) from e

from cocoindex._internal.live_component import (
    _IMMEDIATE_READY,
    LiveMapSubscriber,
    LiveStream,
    LiveStreamSubscriber,
    ReadyAwaitable,
)
from cocoindex._internal.typing import StableKey
from cocoindex.connectorkits import SingleWatcherGuard

_logger = logging.getLogger(__name__)


# --- Public type aliases ---

IsDeleteFn = Callable[["pulsar.Message"], bool]

# Default per-receive timeout. Kept short so the blocking receive thread returns
# promptly (for shutdown) and so an idle gap is detected as "backlog drained".
_DEFAULT_RECEIVE_TIMEOUT_MILLIS = 1000


# --- TopicStream: LiveStream[Message] primitive ---


class TopicStream:
    """A :class:`LiveStream` of raw Pulsar :class:`pulsar.Message` objects.

    Owns the consumer subscription and delivers every received message to the
    subscriber's ``send()``, acknowledging each message only once its readiness
    handle completes. ``mark_ready()`` is called once per ``watch()`` invocation,
    when the initial backlog appears drained.

    Readiness signal. Pulsar's client does not expose a Kafka-style per-partition
    high watermark, so "initial backlog drained" is detected as the first
    ``receive`` that times out (no message available within
    ``receive_timeout_millis``). This is a pragmatic heuristic: on a continuously
    saturated topic readiness may be delayed until the first lull.

    The underlying consumer is single-subscription, so ``watch()`` is single-shot
    per :class:`TopicStream` instance (runtime-guarded; a second concurrent call
    raises ``RuntimeError``).
    """

    __slots__ = (
        "_client",
        "_topics",
        "_subscription_name",
        "_consumer_type",
        "_receive_timeout_millis",
        "_watch_guard",
    )

    def __init__(
        self,
        client: pulsar.Client,
        topics: list[str],
        subscription_name: str,
        *,
        consumer_type: "pulsar.ConsumerType | None" = None,
        receive_timeout_millis: int = _DEFAULT_RECEIVE_TIMEOUT_MILLIS,
    ) -> None:
        self._client = client
        self._topics = topics
        self._subscription_name = subscription_name
        # Default to Exclusive: a durable, single-consumer cursor for the source.
        self._consumer_type = (
            consumer_type
            if consumer_type is not None
            else pulsar.ConsumerType.Exclusive
        )
        self._receive_timeout_millis = receive_timeout_millis
        self._watch_guard = SingleWatcherGuard("Pulsar TopicStream")

    def payloads(self) -> LiveStream[bytes]:
        """View of this stream yielding each message's payload as bytes."""
        return _TopicPayloadsStream(self)

    async def watch(self, subscriber: LiveStreamSubscriber["pulsar.Message"]) -> None:
        """Consume messages from the topics and deliver them to the subscriber."""
        with self._watch_guard:
            await self._watch(subscriber)

    async def _watch(self, subscriber: LiveStreamSubscriber["pulsar.Message"]) -> None:
        consumer = await asyncio.to_thread(
            self._client.subscribe,
            self._topics,
            self._subscription_name,
            consumer_type=self._consumer_type,
        )
        ready_signaled = False
        ack_tasks: set[asyncio.Task[None]] = set()

        async def _ack_when_ready(
            msg: "pulsar.Message", handle: ReadyAwaitable
        ) -> None:
            try:
                await handle.ready()
            except asyncio.CancelledError:
                return
            await asyncio.to_thread(consumer.acknowledge, msg)

        try:
            while True:
                try:
                    msg = await asyncio.to_thread(
                        consumer.receive, self._receive_timeout_millis
                    )
                except pulsar.Timeout:
                    if not ready_signaled:
                        await subscriber.mark_ready()
                        ready_signaled = True
                    continue

                handle = await subscriber.send(msg)
                if handle is _IMMEDIATE_READY:
                    await asyncio.to_thread(consumer.acknowledge, msg)
                else:
                    task = asyncio.create_task(_ack_when_ready(msg, handle))
                    ack_tasks.add(task)
                    task.add_done_callback(ack_tasks.discard)
        finally:
            for task in ack_tasks:
                task.cancel()
            await asyncio.to_thread(consumer.close)


class _TopicPayloadsStream:
    """``LiveStream[bytes]`` view over a :class:`TopicStream`."""

    __slots__ = ("_source",)

    def __init__(self, source: TopicStream) -> None:
        self._source = source

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        await self._source.watch(_PayloadsAdapter(subscriber))


class _PayloadsAdapter:
    """Adapts a ``LiveStreamSubscriber[bytes]`` to receive Pulsar ``Message`` objects."""

    __slots__ = ("_downstream",)

    def __init__(self, downstream: LiveStreamSubscriber[bytes]) -> None:
        self._downstream = downstream

    async def send(self, message: "pulsar.Message") -> ReadyAwaitable:
        value = message.data()
        # Empty payloads (tombstone markers) are filtered out of the bytes view;
        # consumers needing tombstone semantics should subscribe at Message level.
        if not value:
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
        map_sub: LiveMapSubscriber[StableKey, "pulsar.Message"],
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._map_sub = map_sub
        self._is_deletion = is_deletion

    async def send(self, message: "pulsar.Message") -> ReadyAwaitable:
        key = message.partition_key()
        if not key:
            _logger.error(
                "Skipping Pulsar message with no partition key (message id %s)",
                message.message_id(),
            )
            return _IMMEDIATE_READY
        if not message.data() or (
            self._is_deletion is not None and self._is_deletion(message)
        ):
            return await self._map_sub.delete(key)
        return await self._map_sub.update(key, message)

    async def mark_ready(self) -> None:
        await self._map_sub.mark_ready()


class _TopicMapFeed:
    """``LiveMapFeed`` view over a :class:`TopicStream`."""

    __slots__ = ("_stream", "_is_deletion")

    def __init__(self, stream: TopicStream, is_deletion: IsDeleteFn | None) -> None:
        self._stream = stream
        self._is_deletion = is_deletion

    async def watch(
        self, subscriber: LiveMapSubscriber[StableKey, "pulsar.Message"]
    ) -> None:
        await self._stream.watch(_StreamToMapSubscriber(subscriber, self._is_deletion))


# --- Public API ---


def topic_as_stream(
    client: pulsar.Client,
    topics: list[str],
    subscription_name: str,
    *,
    consumer_type: "pulsar.ConsumerType | None" = None,
    receive_timeout_millis: int = _DEFAULT_RECEIVE_TIMEOUT_MILLIS,
) -> TopicStream:
    """
    Treat a Pulsar topic as a :class:`LiveStream` of raw messages.

    The returned :class:`TopicStream` implements ``LiveStream[Message]`` and
    exposes ``.payloads()`` for a ``LiveStream[bytes]`` view of message payloads.

    Args:
        client: A connected ``pulsar.Client``.
        topics: Topics to subscribe to.
        subscription_name: Pulsar subscription name (the durable cursor identity;
            analogous to a Kafka consumer group).
        consumer_type: Pulsar consumer type. Defaults to ``Exclusive``.
        receive_timeout_millis: Per-receive timeout; also the idle gap after which
            the initial backlog is considered drained (readiness).

    Returns:
        A :class:`TopicStream` (single-watcher; bind to one subscription).
    """
    return TopicStream(
        client,
        topics,
        subscription_name,
        consumer_type=consumer_type,
        receive_timeout_millis=receive_timeout_millis,
    )


def topic_as_map(
    client: pulsar.Client,
    topics: list[str],
    subscription_name: str,
    *,
    is_deletion: IsDeleteFn | None = None,
    consumer_type: "pulsar.ConsumerType | None" = None,
    receive_timeout_millis: int = _DEFAULT_RECEIVE_TIMEOUT_MILLIS,
) -> _TopicMapFeed:
    """
    Treat a Pulsar topic as a live keyed map.

    Returns a ``LiveMapFeed`` that streams change events (updates/deletes) from the
    given topics, keyed by the message **partition key** (the same key the Pulsar
    target writes), with the full ``pulsar.Message`` as the value. Suitable for
    ``mount_each()``.

    Messages with an empty payload are treated as deletions; ``is_deletion`` adds
    custom deletion detection on non-empty messages. Messages with no partition
    key are skipped.

    Args:
        client: A connected ``pulsar.Client``.
        topics: Topics to subscribe to.
        subscription_name: Pulsar subscription name (durable cursor identity).
        is_deletion: Optional predicate ``(message) -> bool`` for custom deletion
            detection.
        consumer_type: Pulsar consumer type. Defaults to ``Exclusive``.
        receive_timeout_millis: Per-receive timeout (also the readiness idle gap).

    Returns:
        A ``LiveMapFeed[bytes | str, Message]`` for use with ``mount_each()``.
    """
    return _TopicMapFeed(
        topic_as_stream(
            client,
            topics,
            subscription_name,
            consumer_type=consumer_type,
            receive_timeout_millis=receive_timeout_millis,
        ),
        is_deletion,
    )


__all__ = [
    "IsDeleteFn",
    "TopicStream",
    "topic_as_map",
    "topic_as_stream",
]
