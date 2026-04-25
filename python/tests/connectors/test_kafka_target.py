"""Tests for Kafka target connector handlers and reconciliation logic.

These tests mock the AIOProducer to verify handler behavior without a real Kafka broker.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

# --- Mock confluent_kafka before importing the connector ---


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


_mock_aio = MagicMock()
_mock_aio.AIOProducer = MockAIOProducer
_mock_module = MagicMock()
_mock_module.aio = _mock_aio
sys.modules.setdefault("confluent_kafka", _mock_module)
sys.modules.setdefault("confluent_kafka.aio", _mock_aio)

from confluent_kafka.aio import AIOProducer  # type: ignore[import-not-found]  # noqa: E402
from cocoindex.connectors.kafka._target import (  # noqa: E402
    _MessageAction,
    _MessageHandler,
    _TopicAction,
    _TopicHandler,
    _TopicKey,
    _TopicSpec,
    KafkaTopicTarget,
)
import cocoindex as coco  # noqa: E402
from cocoindex._internal.context_keys import ContextProvider  # noqa: E402


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def producer() -> MockAIOProducer:
    return MockAIOProducer()


def _as_producer(mock: MockAIOProducer) -> AIOProducer:
    return cast(AIOProducer, mock)


@pytest.fixture
def message_handler(producer: MockAIOProducer) -> _MessageHandler:
    return _MessageHandler(
        producer=_as_producer(producer), topic="test-topic", deletion_value_fn=None
    )


@pytest.fixture
def message_handler_with_deletion(producer: MockAIOProducer) -> _MessageHandler:
    return _MessageHandler(
        producer=_as_producer(producer),
        topic="test-topic",
        deletion_value_fn=lambda k: b"deleted:"
        + (k if isinstance(k, bytes) else k.encode()),
    )


# =============================================================================
# _TopicHandler tests
# =============================================================================


class TestTopicHandler:
    def test_reconcile_always_returns_output(self) -> None:
        handler = _TopicHandler()
        spec = _TopicSpec(deletion_value_fn=None)

        result = handler.reconcile(
            ("producer_key", "my-topic"),
            spec,
            [],
            False,
        )

        assert result is not None
        assert result.tracking_record is None

    def test_reconcile_non_existence(self) -> None:
        handler = _TopicHandler()

        result = handler.reconcile(
            ("producer_key", "my-topic"),
            coco.NON_EXISTENCE,
            [],
            False,
        )

        assert result is not None
        assert coco.is_non_existence(result.tracking_record)

    @pytest.mark.asyncio
    async def test_sink_creates_child_handler(self, producer: MockAIOProducer) -> None:
        handler = _TopicHandler()
        spec = _TopicSpec(deletion_value_fn=None)
        key = _TopicKey(producer_key="pk", topic="my-topic")
        action = _TopicAction(key=key, spec=spec)

        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = producer

        children = await handler._apply_actions(context_provider, [action])

        assert len(children) == 1
        child_def = children[0]
        assert child_def is not None
        assert isinstance(child_def.handler, _MessageHandler)

    @pytest.mark.asyncio
    async def test_sink_returns_none_for_deletion(self) -> None:
        handler = _TopicHandler()
        key = _TopicKey(producer_key="pk", topic="my-topic")
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
    async def test_produce_messages(self, producer: MockAIOProducer) -> None:
        handler = _MessageHandler(
            producer=_as_producer(producer), topic="test-topic", deletion_value_fn=None
        )

        action1 = _MessageAction(key=b"k1", value=b"v1")
        action2 = _MessageAction(key=b"k2", value=b"v2")

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action1, action2])

        assert len(producer.produced_messages) == 2
        assert producer.produced_messages[0] == ("test-topic", b"k1", b"v1")
        assert producer.produced_messages[1] == ("test-topic", b"k2", b"v2")

    @pytest.mark.asyncio
    async def test_produce_tombstone(self, producer: MockAIOProducer) -> None:
        handler = _MessageHandler(
            producer=_as_producer(producer), topic="test-topic", deletion_value_fn=None
        )

        action = _MessageAction(key=b"k1", value=None)

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action])

        assert len(producer.produced_messages) == 1
        assert producer.produced_messages[0] == ("test-topic", b"k1", None)

    @pytest.mark.asyncio
    async def test_produce_deletion_value(self, producer: MockAIOProducer) -> None:
        handler = _MessageHandler(
            producer=_as_producer(producer),
            topic="test-topic",
            deletion_value_fn=lambda k: b"del:"
            + (k if isinstance(k, bytes) else k.encode()),
        )

        action = _MessageAction(key=b"k1", value=b"del:k1")

        context_provider = MagicMock(spec=ContextProvider)
        await handler._apply_actions(context_provider, [action])

        assert producer.produced_messages[0] == ("test-topic", b"k1", b"del:k1")

    @pytest.mark.asyncio
    async def test_multiple_topics(self, producer: MockAIOProducer) -> None:
        handler1 = _MessageHandler(
            producer=_as_producer(producer), topic="topic-a", deletion_value_fn=None
        )
        handler2 = _MessageHandler(
            producer=_as_producer(producer), topic="topic-b", deletion_value_fn=None
        )

        context_provider = MagicMock(spec=ContextProvider)
        await handler1._apply_actions(
            context_provider, [_MessageAction(key=b"k1", value=b"v1")]
        )
        await handler2._apply_actions(
            context_provider, [_MessageAction(key=b"k2", value=b"v2")]
        )

        assert producer.produced_messages[0] == ("topic-a", b"k1", b"v1")
        assert producer.produced_messages[1] == ("topic-b", b"k2", b"v2")


# =============================================================================
# KafkaTopicTarget tests
# =============================================================================


class TestKafkaTopicTarget:
    def test_memo_key(self) -> None:
        provider = MagicMock()
        provider.memo_key = "test-memo-key"
        target = KafkaTopicTarget(provider)

        assert target.__coco_memo_key__() == "test-memo-key"
