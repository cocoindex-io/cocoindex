"""Stub ``apache_iggy`` SDK shared by the Iggy connector tests.

Importing this module registers a stub ``apache_iggy`` module in
``sys.modules`` that carries every name the connector package imports:
``AutoCommit``, ``IggyClient``, ``IggyConsumer``, ``PollingStrategy`` and
``ReceiveMessage`` for ``cocoindex.connectors.iggy._source``, and
``IggyClient`` and ``SendMessage`` for ``cocoindex.connectors.iggy._target``.
Because the package imports both halves, they bind these same stub classes
whichever test module imports the connector first, keeping the test modules
independent of pytest's collection order. Import this module before the
connector.
"""

from __future__ import annotations

import sys
from collections import deque
from typing import Any, Sequence

from .module_utils import install_stub_module


class MockAutoCommit:
    class Disabled:
        pass


class MockPollingStrategy:
    class Next:
        pass


class MockReceiveMessage:
    def __init__(
        self,
        *,
        payload: bytes,
        offset: int,
        partition_id: int = 0,
    ) -> None:
        self._payload = payload
        self._offset = offset
        self._partition_id = partition_id

    def payload(self) -> bytes:
        return self._payload

    def offset(self) -> int:
        return self._offset

    def partition_id(self) -> int:
        return self._partition_id


class MockSendMessage:
    """Mock apache_iggy.SendMessage that keeps its payload."""

    def __init__(self, payload: bytes | str) -> None:
        self.payload = payload


class MockTopicDetails:
    def __init__(self, *, messages_count: int, partitions_count: int = 1) -> None:
        self.messages_count = messages_count
        self.partitions_count = partitions_count


class MockMessageIterator:
    def __init__(self, messages: deque[MockReceiveMessage]) -> None:
        self._messages = messages

    def __aiter__(self) -> "MockMessageIterator":
        return self

    async def __anext__(self) -> MockReceiveMessage:
        if self._messages:
            return self._messages.popleft()
        raise StopAsyncIteration


class MockIggyConsumer:
    def __init__(
        self,
        messages: list[MockReceiveMessage],
        *,
        stored_offset: int | None = None,
    ) -> None:
        self._messages = deque(messages)
        self._stored_offset = stored_offset
        self.stored_offsets: list[tuple[int, int | None]] = []

    def get_last_stored_offset(self, partition_id: int) -> int | None:
        return self._stored_offset

    async def store_offset(self, offset: int, partition_id: int | None) -> None:
        self._stored_offset = offset
        self.stored_offsets.append((offset, partition_id))

    def iter_messages(self) -> MockMessageIterator:
        return MockMessageIterator(self._messages)


class MockIggyClient:
    """Mock IggyClient serving a canned topic/consumer and recording sent messages."""

    def __init__(
        self,
        consumer: MockIggyConsumer | None = None,
        *,
        messages_count: int = 0,
        partitions_count: int = 1,
    ) -> None:
        # Default to an empty topic for tests that only send messages.
        self.consumer = MockIggyConsumer([]) if consumer is None else consumer
        self.topic = MockTopicDetails(
            messages_count=messages_count,
            partitions_count=partitions_count,
        )
        self.consumer_group_calls: list[dict[str, Any]] = []
        self.sent_payloads: list[bytes | str] = []

    async def get_topic(self, stream: str, topic: str) -> MockTopicDetails:
        return self.topic

    async def consumer_group(self, **kwargs: Any) -> MockIggyConsumer:
        self.consumer_group_calls.append(kwargs)
        return self.consumer

    async def send_messages(
        self,
        *,
        stream: str,
        topic: str,
        partitioning: int,
        messages: Sequence[MockSendMessage],
    ) -> None:
        self.sent_payloads.extend(message.payload for message in messages)

    def clear(self) -> None:
        self.sent_payloads.clear()


if "cocoindex.connectors.iggy" in sys.modules:
    raise ImportError("import tests.common.iggy_stub before cocoindex.connectors.iggy")
install_stub_module(
    "apache_iggy",
    AutoCommit=MockAutoCommit,
    IggyClient=MockIggyClient,
    IggyConsumer=MockIggyConsumer,
    PollingStrategy=MockPollingStrategy,
    ReceiveMessage=MockReceiveMessage,
    SendMessage=MockSendMessage,
)
