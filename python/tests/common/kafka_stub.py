"""Stub ``confluent_kafka`` SDK shared by the Kafka connector tests.

Importing this module registers stub ``confluent_kafka`` and
``confluent_kafka.aio`` modules in ``sys.modules`` that carry every name the
connector package imports: ``Message``, ``TopicPartition`` and ``AIOConsumer``
for ``cocoindex.connectors.kafka._source``, and ``AIOProducer`` for
``cocoindex.connectors.kafka._target``. Because the package imports both
halves, they bind these same stub classes whichever test module imports the
connector first, keeping the test modules independent of pytest's collection
order. Import this module before the connector.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any

from .module_utils import install_stub_module


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


class MockAIOProducer:
    """Mock AIOProducer that records produce calls and returns resolved futures."""

    def __init__(self) -> None:
        self.produced_messages: list[tuple[str, Any, Any]] = []

    async def produce(
        self, topic: str, *, key: Any = None, value: Any = None
    ) -> asyncio.Future[None]:
        self.produced_messages.append((topic, key, value))
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    def clear(self) -> None:
        self.produced_messages.clear()


if "cocoindex.connectors.kafka" in sys.modules:
    raise ImportError(
        "import tests.common.kafka_stub before cocoindex.connectors.kafka"
    )
_aio = install_stub_module(
    "confluent_kafka.aio", AIOConsumer=MockAIOConsumer, AIOProducer=MockAIOProducer
)
install_stub_module(
    "confluent_kafka", Message=MockMessage, TopicPartition=MockTopicPartition, aio=_aio
)
