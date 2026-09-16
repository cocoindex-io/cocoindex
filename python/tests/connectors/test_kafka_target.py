"""Tests for Kafka target connector handlers and reconciliation logic.

These tests mock the AIOProducer to verify handler behavior without a real Kafka broker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, NamedTuple, cast
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from tests.common.kafka_stub import MockAIOProducer

# ``tests.common.kafka_stub`` must be imported before the connector so that the
# connector binds the stub SDK.
from confluent_kafka.aio import AIOProducer  # type: ignore[import-not-found]
from cocoindex.connectors.kafka._target import (
    _MessageAction,
    _MessageHandler,
    _TopicAction,
    _TopicHandler,
    _TopicKey,
    _TopicSpec,
    KafkaTopicTarget,
)
import cocoindex as coco
from cocoindex.connectors import kafka
from tests import common
from tests.common.target_states import RecordingChildSlot
from cocoindex._internal.context_keys import ContextProvider


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

        slot = RecordingChildSlot()
        await handler._apply_actions(context_provider, [action], {0: slot})

        assert isinstance(slot.handler, _MessageHandler)

    @pytest.mark.asyncio
    async def test_sink_skips_child_for_deletion(self) -> None:
        handler = _TopicHandler()
        key = _TopicKey(producer_key="pk", topic="my-topic")
        action = _TopicAction(key=key, spec=coco.NON_EXISTENCE)

        context_provider = MagicMock(spec=ContextProvider)
        # An orphan delete has no child provider, so the engine hands over no slot.
        await handler._apply_actions(context_provider, [action], {})


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
            deletion_value_fn=lambda k: (
                b"del:" + (k if isinstance(k, bytes) else k.encode())
            ),
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


# =============================================================================
# App-level tests — container-deletion semantics
# =============================================================================
#
# The topic is user-managed, so CocoIndex reconciles its messages one by one
# only while the topic itself stays declared. Removing the topic declaration,
# or dropping the app, abandons the topic: no tombstones are produced for the
# messages it held.

_PRODUCER_KEY: coco.ContextKey[Any] = coco.ContextKey("test_kafka_target_producer")


@dataclass
class _TopicScenario:
    """What ``_app_main`` declares; tests mutate it between runs."""

    declare_topic: bool = True
    messages: dict[str, bytes] = field(default_factory=dict)


@coco.fn
async def _app_main(scenario: _TopicScenario) -> None:
    if not scenario.declare_topic:
        return
    target = await kafka.mount_kafka_topic_target(_PRODUCER_KEY, "exp-topic")
    for key, value in scenario.messages.items():
        target.declare_target_state(key=key, value=value)


class _TopicApp(NamedTuple):
    app: coco.App[Any, Any]
    producer: MockAIOProducer
    scenario: _TopicScenario


@pytest_asyncio.fixture
async def topic_app(
    request: pytest.FixtureRequest,
    producer: MockAIOProducer,
) -> _TopicApp:
    # Created inside an async fixture so the Environment binds to the test's
    # running event loop.
    env = common.create_test_env(__file__, suffix=request.node.name)
    env.context_provider.provide(_PRODUCER_KEY, producer)
    scenario = _TopicScenario()
    app = coco.App(
        coco.AppConfig(name=request.node.name, environment=env), _app_main, scenario
    )
    return _TopicApp(app=app, producer=producer, scenario=scenario)


@pytest.mark.asyncio
async def test_app_child_removal_produces_tombstone(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    assert topic_app.producer.produced_messages == [("exp-topic", "k1", b"v1")]

    topic_app.producer.clear()
    del topic_app.scenario.messages["k1"]
    await topic_app.app.update()
    assert topic_app.producer.produced_messages == [("exp-topic", "k1", None)]


@pytest.mark.asyncio
async def test_app_parent_removal_abandons_messages(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()

    topic_app.producer.clear()
    topic_app.scenario.declare_topic = False
    await topic_app.app.update()
    # Intended: un-declaring the user-managed topic abandons it, messages
    # included, so k1 gets no tombstone.
    assert topic_app.producer.produced_messages == []


@pytest.mark.asyncio
async def test_app_drop_abandons_messages(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()

    topic_app.producer.clear()
    await topic_app.app.drop()
    # Intended: dropping the app abandons the user-managed topic, messages
    # included, so k1 gets no tombstone.
    assert topic_app.producer.produced_messages == []


@pytest.mark.asyncio
async def test_app_redeclared_topic_starts_from_scratch(
    topic_app: _TopicApp,
) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    topic_app.scenario.declare_topic = False
    await topic_app.app.update()

    # Abandoning the topic pruned k1's tracking entry, so bringing the topic
    # back without k1 has nothing to tombstone...
    topic_app.producer.clear()
    topic_app.scenario.declare_topic = True
    del topic_app.scenario.messages["k1"]
    await topic_app.app.update()
    assert topic_app.producer.produced_messages == []

    # ...and re-declaring k1 with its old value republishes it as new.
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    assert topic_app.producer.produced_messages == [("exp-topic", "k1", b"v1")]
