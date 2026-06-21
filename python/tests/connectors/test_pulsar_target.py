"""Tests for Pulsar target connector handlers and reconciliation logic.

These tests mock the pulsar client to verify handler behavior without a real
Pulsar broker.
"""

from __future__ import annotations

import sys
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

# --- Mock pulsar before importing the connector ---


class MockProducer:
    """Mock pulsar.Producer that records send() calls into a shared list."""

    def __init__(self, topic: str, sink: list[tuple[str, Any, Any]]) -> None:
        self._topic = topic
        self._sink = sink

    def send(self, content: bytes, *, partition_key: Any = None) -> None:
        self._sink.append((self._topic, partition_key, content))


class MockClient:
    """Mock pulsar.Client that hands out per-topic MockProducers."""

    def __init__(self) -> None:
        self.sent_messages: list[tuple[str, Any, Any]] = []

    def create_producer(self, topic: str) -> MockProducer:
        return MockProducer(topic, self.sent_messages)

    def clear(self) -> None:
        self.sent_messages.clear()


_mock_pulsar = MagicMock()
_mock_pulsar.Client = MockClient
_mock_pulsar.Producer = MockProducer
sys.modules.setdefault("pulsar", _mock_pulsar)

import pulsar  # type: ignore[import-not-found]  # noqa: E402
from cocoindex.connectors.pulsar._target import (  # noqa: E402
    _MessageAction,
    _MessageHandler,
    _TopicAction,
    _TopicHandler,
    _TopicKey,
    _TopicSpec,
    PulsarTopicTarget,
)
import cocoindex as coco  # noqa: E402
from cocoindex._internal.context_keys import ContextProvider  # noqa: E402


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def client() -> MockClient:
    return MockClient()


def _as_client(mock: MockClient) -> pulsar.Client:
    return cast(pulsar.Client, mock)


@pytest.fixture
def message_handler(client: MockClient) -> _MessageHandler:
    return _MessageHandler(
        client=_as_client(client), topic="test-topic", deletion_value_fn=None
    )


@pytest.fixture
def message_handler_with_deletion(client: MockClient) -> _MessageHandler:
    return _MessageHandler(
        client=_as_client(client),
        topic="test-topic",
        deletion_value_fn=lambda k: (
            b"deleted:" + (k if isinstance(k, bytes) else k.encode())
        ),
    )


# =============================================================================
# _TopicHandler tests
# =============================================================================


class TestTopicHandler:
    def test_reconcile_always_returns_output(self) -> None:
        handler = _TopicHandler()
        spec = _TopicSpec(deletion_value_fn=None)

        result = handler.reconcile(("client_key", "my-topic"), spec, [], False)

        assert result is not None
        assert result.tracking_record is None

    def test_reconcile_non_existence(self) -> None:
        handler = _TopicHandler()

        result = handler.reconcile(
            ("client_key", "my-topic"), coco.NON_EXISTENCE, [], False
        )

        assert result is not None
        assert coco.is_non_existence(result.tracking_record)

    @pytest.mark.asyncio
    async def test_sink_creates_child_handler(self, client: MockClient) -> None:
        handler = _TopicHandler()
        spec = _TopicSpec(deletion_value_fn=None)
        key = _TopicKey(client_key="ck", topic="my-topic")
        action = _TopicAction(key=key, spec=spec)

        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        children = await handler._apply_actions(context_provider, [action])

        assert len(children) == 1
        child_def = children[0]
        assert child_def is not None
        assert isinstance(child_def.handler, _MessageHandler)

    @pytest.mark.asyncio
    async def test_sink_returns_none_for_deletion(self) -> None:
        handler = _TopicHandler()
        key = _TopicKey(client_key="ck", topic="my-topic")
        action = _TopicAction(key=key, spec=coco.NON_EXISTENCE)

        context_provider = MagicMock(spec=ContextProvider)
        children = await handler._apply_actions(context_provider, [action])

        assert len(children) == 1
        assert children[0] is None


# =============================================================================
# _MessageHandler reconcile tests
# =============================================================================


class TestMessageHandlerReconcile:
    def test_upsert_new_state(self, message_handler: _MessageHandler) -> None:
        result = message_handler.reconcile(b"k1", b"v1", [], True)

        assert result is not None
        assert result.action.key == b"k1"
        assert result.action.value == b"v1"
        assert isinstance(result.tracking_record, bytes)

    def test_upsert_unchanged_skips(self, message_handler: _MessageHandler) -> None:
        result1 = message_handler.reconcile(b"k1", b"v1", [], True)
        assert result1 is not None
        fp = result1.tracking_record
        assert isinstance(fp, bytes)

        result2 = message_handler.reconcile(b"k1", b"v1", [fp], False)
        assert result2 is None

    def test_upsert_changed_value(self, message_handler: _MessageHandler) -> None:
        result1 = message_handler.reconcile(b"k1", b"v1", [], True)
        assert result1 is not None
        fp = result1.tracking_record
        assert isinstance(fp, bytes)

        result2 = message_handler.reconcile(b"k1", b"v2", [fp], False)
        assert result2 is not None
        assert result2.action.value == b"v2"

    def test_upsert_with_prev_may_be_missing(
        self, message_handler: _MessageHandler
    ) -> None:
        result1 = message_handler.reconcile(b"k1", b"v1", [], True)
        assert result1 is not None
        fp = result1.tracking_record
        assert isinstance(fp, bytes)

        # Same fingerprint but prev_may_be_missing=True → still produces
        result2 = message_handler.reconcile(b"k1", b"v1", [fp], True)
        assert result2 is not None

    def test_delete_without_callback(self, message_handler: _MessageHandler) -> None:
        result = message_handler.reconcile(b"k1", coco.NON_EXISTENCE, [b"fp"], False)

        assert result is not None
        assert result.action.key == b"k1"
        assert result.action.value is None  # Tombstone
        assert coco.is_non_existence(result.tracking_record)

    def test_delete_with_callback(
        self, message_handler_with_deletion: _MessageHandler
    ) -> None:
        result = message_handler_with_deletion.reconcile(
            b"k1", coco.NON_EXISTENCE, [b"fp"], False
        )

        assert result is not None
        assert result.action.key == b"k1"
        assert result.action.value == b"deleted:k1"

    def test_delete_no_prev_no_missing_skips(
        self, message_handler: _MessageHandler
    ) -> None:
        result = message_handler.reconcile(b"k1", coco.NON_EXISTENCE, [], False)
        assert result is None

    def test_str_key_and_value(self, message_handler: _MessageHandler) -> None:
        result = message_handler.reconcile("str-key", "str-value", [], True)

        assert result is not None
        assert result.action.key == "str-key"
        assert result.action.value == "str-value"
        assert isinstance(result.tracking_record, bytes)


# =============================================================================
# _MessageHandler sink tests
# =============================================================================


class TestMessageHandlerSink:
    @pytest.mark.asyncio
    async def test_produce_messages(self, client: MockClient) -> None:
        handler = _MessageHandler(
            client=_as_client(client), topic="test-topic", deletion_value_fn=None
        )

        action1 = _MessageAction(key=b"k1", value=b"v1")
        action2 = _MessageAction(key=b"k2", value=b"v2")

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action1, action2])

        # Sends run concurrently via asyncio.to_thread, so compare unordered.
        assert set(client.sent_messages) == {
            ("test-topic", "k1", b"v1"),
            ("test-topic", "k2", b"v2"),
        }

    @pytest.mark.asyncio
    async def test_produce_tombstone(self, client: MockClient) -> None:
        handler = _MessageHandler(
            client=_as_client(client), topic="test-topic", deletion_value_fn=None
        )

        action = _MessageAction(key=b"k1", value=None)

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action])

        # Tombstone (value=None) is sent as an empty payload.
        assert client.sent_messages == [("test-topic", "k1", b"")]

    @pytest.mark.asyncio
    async def test_produce_deletion_value(self, client: MockClient) -> None:
        handler = _MessageHandler(
            client=_as_client(client),
            topic="test-topic",
            deletion_value_fn=lambda k: (
                b"del:" + (k if isinstance(k, bytes) else k.encode())
            ),
        )

        action = _MessageAction(key=b"k1", value=b"del:k1")

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action])

        assert client.sent_messages == [("test-topic", "k1", b"del:k1")]

    @pytest.mark.asyncio
    async def test_str_key_becomes_partition_key(self, client: MockClient) -> None:
        handler = _MessageHandler(
            client=_as_client(client), topic="test-topic", deletion_value_fn=None
        )

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(
            context_provider, [_MessageAction(key="strk", value="strv")]
        )

        assert client.sent_messages == [("test-topic", "strk", b"strv")]


# =============================================================================
# PulsarTopicTarget tests
# =============================================================================


class TestPulsarTopicTarget:
    def test_memo_key(self) -> None:
        provider = MagicMock()
        provider.memo_key = "test-memo-key"
        target = PulsarTopicTarget(provider)

        assert target.__coco_memo_key__() == "test-memo-key"
