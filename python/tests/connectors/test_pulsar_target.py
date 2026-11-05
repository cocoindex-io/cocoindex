"""Unit and public-app tests for the Apache Pulsar target connector."""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import threading
import uuid
import warnings
from dataclasses import dataclass
from multiprocessing import get_context
from multiprocessing.connection import Connection
from types import ModuleType
from typing import Any, cast
from unittest.mock import MagicMock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import cocoindex as coco
import pytest

try:
    import pulsar  # type: ignore[import-not-found]
except ImportError:
    HAS_PULSAR = False

    class _PulsarClientStub:
        def create_producer(self, topic: str) -> object:
            raise NotImplementedError

    _pulsar_stub = ModuleType("pulsar")
    _pulsar_stub.Client = _PulsarClientStub  # type: ignore[attr-defined]
    sys.modules["pulsar"] = _pulsar_stub
    import pulsar  # type: ignore[import-not-found,no-redef]
else:
    HAS_PULSAR = True

from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex.connectorkits.fingerprint import fingerprint_bytes
from cocoindex.connectors import pulsar as pulsar_connector
from cocoindex.connectors.pulsar._target import (
    DeletionValueFn,
    _MessageAction,
    _MessageHandler,
    _MessageKey,
    _MessageSpec,
    _MessageTrackingRecord,
    _TopicAction,
    _TopicHandle,
    _TopicHandler,
    _TopicKey,
    _TopicSpec,
)

from tests import common
from tests.common.target_states import RecordingChildSlot

_APP_CLIENT = ContextKey[pulsar.Client]("test_pulsar_target_app_client")
_LIVE_CLIENT = ContextKey[pulsar.Client]("test_pulsar_target_live_client")
_app_topic = "persistent://public/default/test-pulsar-app"
_app_desired: dict[str, bytes | str] = {}
_app_include_topic = True
_app_delete_fn: DeletionValueFn | None = None
_live_topic = ""

_PULSAR_SERVICE_URL = os.environ.get("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
_PULSAR_ADMIN_URL = os.environ.get("PULSAR_ADMIN_URL", "http://localhost:8080")

requires_pulsar_server = pytest.mark.skipif(
    not (HAS_PULSAR and os.environ.get("PULSAR_TEST_SERVER")),
    reason="pulsar-client is not installed or PULSAR_TEST_SERVER is not set",
)


@coco.fn
async def _declare_app_target() -> None:
    if not _app_include_topic:
        return

    target = await coco.use_mount(
        coco.component_subpath("pulsar-topic"),
        pulsar_connector.declare_pulsar_topic_target,
        _APP_CLIENT,
        _app_topic,
        deletion_value_fn=_app_delete_fn,
    )
    for key, value in _app_desired.items():
        target.declare_target_state(key=key, value=value)


@coco.fn
async def _declare_live_target() -> None:
    if not _app_include_topic:
        return

    target = await coco.use_mount(
        coco.component_subpath("pulsar-topic"),
        pulsar_connector.declare_pulsar_topic_target,
        _LIVE_CLIENT,
        _live_topic,
        deletion_value_fn=_app_delete_fn,
    )
    for key, value in _app_desired.items():
        target.declare_target_state(key=key, value=value)


def _custom_delete_value(key: str) -> str:
    return f"deleted:{key}"


@coco.fn
async def _declare_live_target_no_messages() -> None:
    await coco.use_mount(
        coco.component_subpath("pulsar-topic"),
        pulsar_connector.declare_pulsar_topic_target,
        _LIVE_CLIENT,
        _live_topic,
        deletion_value_fn=_app_delete_fn,
    )


async def _read_message(
    reader: pulsar.Reader, timeout_millis: int = 15_000
) -> pulsar.Message:
    return await asyncio.to_thread(reader.read_next, timeout_millis)


async def _assert_no_message(
    reader: pulsar.Reader, timeout_millis: int = 2_000
) -> None:
    with pytest.raises(pulsar.Timeout):
        await _read_message(reader, timeout_millis=timeout_millis)


class MockProducer:
    def __init__(
        self,
        *,
        send_error_at: int | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.messages: list[tuple[bytes | None, str]] = []
        self.thread_ids: list[int] = []
        self.close_calls = 0
        self.send_error_at = send_error_at
        self.close_error = close_error

    def send(self, content: bytes | None, *, partition_key: str) -> None:
        self.thread_ids.append(threading.get_ident())
        self.messages.append((content, partition_key))
        if self.send_error_at == len(self.messages):
            raise RuntimeError("injected send failure")

    def close(self) -> None:
        self.thread_ids.append(threading.get_ident())
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class MockClient:
    def __init__(
        self,
        *,
        create_error: Exception | None = None,
        send_error_at: int | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.topics: list[str] = []
        self.producers: list[MockProducer] = []
        self.thread_ids: list[int] = []
        self.close_calls = 0
        self.create_error = create_error
        self.send_error_at = send_error_at
        self.close_error = close_error

    def create_producer(self, topic: str) -> MockProducer:
        self.thread_ids.append(threading.get_ident())
        self.topics.append(topic)
        if self.create_error is not None:
            raise self.create_error
        producer = MockProducer(
            send_error_at=self.send_error_at,
            close_error=self.close_error,
        )
        self.producers.append(producer)
        return producer

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class _BlockingProducer(MockProducer):
    def __init__(self, *, send_error: Exception | None = None) -> None:
        super().__init__()
        self.send_started = threading.Event()
        self.release_send = threading.Event()
        self.send_error = send_error

    def send(self, content: bytes | None, *, partition_key: str) -> None:
        self.thread_ids.append(threading.get_ident())
        self.messages.append((content, partition_key))
        self.send_started.set()
        if not self.release_send.wait(timeout=5):
            raise TimeoutError("blocked producer was not released")
        if self.send_error is not None:
            raise self.send_error


class _BlockingClient(MockClient):
    def __init__(self, *, send_error: Exception | None = None) -> None:
        super().__init__()
        self.producer = _BlockingProducer(send_error=send_error)

    def create_producer(self, topic: str) -> MockProducer:
        self.thread_ids.append(threading.get_ident())
        self.topics.append(topic)
        self.producers.append(self.producer)
        return self.producer


class _ClientWithClose:
    def __init__(self) -> None:
        self.close_calls = 0
        self.producers: list[MockProducer] = []

    def create_producer(self, topic: str) -> MockProducer:
        producer = MockProducer()
        self.producers.append(producer)
        return producer

    def close(self) -> None:
        self.close_calls += 1


def _as_client(client: MockClient) -> pulsar.Client:
    return cast(pulsar.Client, client)


def _key(
    *,
    client_key: str = "client-key",
    topic: str = "topic",
    message_key: str = "message",
) -> _MessageKey:
    return _MessageKey(client_key=client_key, topic=topic, message_key=message_key)


def _spec(
    value: bytes | str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> _MessageSpec:
    return _MessageSpec(value=value, deletion_value_fn=deletion_value_fn)


def _record(
    *,
    fingerprint: bytes,
    deletion_payload: bytes | None,
) -> _MessageTrackingRecord:
    return _MessageTrackingRecord(
        value_fingerprint=fingerprint,
        deletion_payload=deletion_payload,
    )


def _as_record(
    record: _MessageTrackingRecord | coco.NonExistenceType,
) -> _MessageTrackingRecord:
    assert isinstance(record, _MessageTrackingRecord)
    return record


def _action(
    *,
    client_key: str = "client-key",
    topic: str = "topic",
    message_key: str = "message",
    value: bytes | None,
    publish: bool = True,
) -> _MessageAction:
    return _MessageAction(
        key=_key(
            client_key=client_key,
            topic=topic,
            message_key=message_key,
        ),
        value=value,
        publish=publish,
    )


class TestTopicHandler:
    def test_reconcile_existing_topic(self) -> None:
        handler = _TopicHandler()

        result = handler.reconcile(
            ("client", "topic"),
            _TopicSpec(deletion_value_fn=None),
            [],
            False,
        )

        assert result is not None
        assert result.action == _TopicAction(
            key=_TopicKey(client_key="client", topic="topic"),
            spec=_TopicSpec(deletion_value_fn=None),
        )
        assert result.tracking_record is None

    def test_reconcile_non_existence(self) -> None:
        result = _TopicHandler().reconcile(
            ("client", "topic"),
            coco.NON_EXISTENCE,
            [],
            False,
        )

        assert result is not None
        assert result.action == _TopicAction(
            key=_TopicKey(client_key="client", topic="topic"),
            spec=coco.NON_EXISTENCE,
        )
        assert coco.is_non_existence(result.tracking_record)

    @pytest.mark.asyncio
    async def test_apply_actions_fulfills_child_slot(self) -> None:
        context_provider = MagicMock(spec=ContextProvider)
        client = MockClient()
        context_provider.get.return_value = _as_client(client)
        slot = RecordingChildSlot()

        await _TopicHandler()._apply_actions(
            context_provider,
            [
                _TopicAction(
                    key=_TopicKey("client", "topic"),
                    spec=_TopicSpec(deletion_value_fn=None),
                )
            ],
            {0: slot},
        )

        assert isinstance(slot.handler, _TopicHandle)

    @pytest.mark.asyncio
    async def test_apply_actions_ignores_removed_topic(self) -> None:
        context_provider = MagicMock(spec=ContextProvider)

        await _TopicHandler()._apply_actions(
            context_provider,
            [_TopicAction(key=_TopicKey("client", "topic"), spec=coco.NON_EXISTENCE)],
            {},
        )

        context_provider.get.assert_not_called()


class TestMessageHandlerReconcile:
    def test_new_bytes_value(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(_key(), _spec(b"v1"), [], False)

        assert result is not None
        assert result.action == _action(value=b"v1", publish=True)
        assert result.tracking_record == _record(
            fingerprint=fingerprint_bytes(b"v1"),
            deletion_payload=None,
        )

    def test_new_string_value(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(_key(), _spec("v1"), [], False)

        assert result is not None
        assert result.action == _action(value=b"v1", publish=True)
        assert result.tracking_record == _record(
            fingerprint=fingerprint_bytes(b"v1"),
            deletion_payload=None,
        )

    def test_wire_equality_across_bytes_and_str(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(_key(message_key="k"), _spec("abc"), [], False)
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec(b"abc"),
            [_as_record(first.tracking_record)],
            False,
        )

        assert result is None

    def test_changed_value_publishes(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(_key(message_key="k"), _spec("abc"), [], False)
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec("def"),
            [_as_record(first.tracking_record)],
            False,
        )

        assert result is not None
        assert result.action == _action(message_key="k", value=b"def", publish=True)

    def test_unchanged_value_is_noop(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(_key(message_key="k"), _spec("abc"), [], False)
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec("abc"),
            [_as_record(first.tracking_record)],
            False,
        )

        assert result is None

    def test_no_prior_record_still_publishes(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(_key(message_key="k"), _spec("abc"), [], False)

        assert result is not None

    def test_prev_may_be_missing_forces_publish(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(_key(message_key="k"), _spec("abc"), [], False)
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec("abc"),
            [_as_record(first.tracking_record)],
            True,
        )

        assert result is not None
        assert result.action == _action(message_key="k", value=b"abc", publish=True)

    def test_multiple_prior_records_all_matching_are_noop(self) -> None:
        handler = _MessageHandler()
        records = [
            _record(fingerprint=fingerprint_bytes(b"abc"), deletion_payload=None),
            _record(fingerprint=fingerprint_bytes(b"abc"), deletion_payload=None),
        ]

        result = handler.reconcile(_key(message_key="k"), _spec("abc"), records, False)

        assert result is None

    def test_multiple_prior_records_with_mismatch_publish(self) -> None:
        handler = _MessageHandler()
        records = [
            _record(fingerprint=fingerprint_bytes(b"abc"), deletion_payload=None),
            _record(fingerprint=fingerprint_bytes(b"def"), deletion_payload=None),
        ]

        result = handler.reconcile(_key(message_key="k"), _spec("abc"), records, False)

        assert result is not None
        assert result.action == _action(message_key="k", value=b"abc", publish=True)

    def test_callback_invoked_every_reconcile(self) -> None:
        calls: list[str] = []

        def delete_fn(key: str) -> str:
            calls.append(key)
            return f"deleted:{key}"

        handler = _MessageHandler()
        first = handler.reconcile(
            _key(message_key="k"), _spec("abc", deletion_value_fn=delete_fn), [], False
        )
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec("abc", deletion_value_fn=delete_fn),
            [_as_record(first.tracking_record)],
            False,
        )

        assert result is None
        assert calls == ["k", "k"]

    def test_callback_output_normalized_and_cached(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(
            _key(message_key="k"),
            _spec("abc", deletion_value_fn=lambda key: "first"),
            [],
            False,
        )
        assert first is not None

        second = handler.reconcile(
            _key(message_key="k"),
            _spec("abc", deletion_value_fn=lambda key: "second"),
            [_as_record(first.tracking_record)],
            False,
        )
        assert second is not None
        assert second.action.publish is False
        assert _as_record(second.tracking_record).deletion_payload == b"second"

    def test_callback_invalid_result_raises(self) -> None:
        handler = _MessageHandler()

        with pytest.raises(TypeError, match="must return bytes or str"):
            handler.reconcile(
                _key(message_key="k"),
                _spec("abc", deletion_value_fn=cast(DeletionValueFn, lambda key: 123)),
                [],
                False,
            )

    def test_callback_exception_propagates(self) -> None:
        handler = _MessageHandler()

        def bad(_key: str) -> bytes:
            raise RuntimeError("bad-callback")

        with pytest.raises(RuntimeError, match="bad-callback"):
            handler.reconcile(
                _key(message_key="k"), _spec("abc", deletion_value_fn=bad), [], False
            )

    def test_deletion_policy_only_update(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(
            _key(message_key="k"),
            _spec("abc", deletion_value_fn=lambda key: "first"),
            [],
            False,
        )
        assert first is not None

        second = handler.reconcile(
            _key(message_key="k"),
            _spec("abc", deletion_value_fn=lambda key: "second"),
            [_as_record(first.tracking_record)],
            False,
        )

        assert second is not None
        assert second.action == _action(message_key="k", value=None, publish=False)
        assert _as_record(second.tracking_record).deletion_payload == b"second"

    def test_default_nonexistence_without_prior_records(self) -> None:
        handler = _MessageHandler()

        assert (
            handler.reconcile(_key(message_key="k"), coco.NON_EXISTENCE, [], False)
            is None
        )

    def test_default_delete_publishes_tombstone(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(
            _key(message_key="k"),
            coco.NON_EXISTENCE,
            [_record(fingerprint=fingerprint_bytes(b"abc"), deletion_payload=None)],
            False,
        )

        assert result is not None
        assert result.action == _action(message_key="k", value=None, publish=True)
        assert coco.is_non_existence(result.tracking_record)

    def test_custom_delete_payload(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(
            _key(message_key="k"),
            coco.NON_EXISTENCE,
            [
                _record(
                    fingerprint=fingerprint_bytes(b"abc"), deletion_payload=b"custom"
                )
            ],
            False,
        )

        assert result is not None
        assert result.action.value == b"custom"

    def test_multiple_equal_delete_policies_publish_once(self) -> None:
        handler = _MessageHandler()
        result = handler.reconcile(
            _key(message_key="k"),
            coco.NON_EXISTENCE,
            [
                _record(
                    fingerprint=fingerprint_bytes(b"abc"), deletion_payload=b"custom"
                ),
                _record(
                    fingerprint=fingerprint_bytes(b"xyz"), deletion_payload=b"custom"
                ),
            ],
            False,
        )

        assert result is not None
        assert result.action == _action(message_key="k", value=b"custom", publish=True)

    def test_conflicting_delete_payloads_fail(self) -> None:
        handler = _MessageHandler()

        with pytest.raises(ValueError, match="Conflicting prior deletion payloads"):
            handler.reconcile(
                _key(message_key="k"),
                coco.NON_EXISTENCE,
                [
                    _record(
                        fingerprint=fingerprint_bytes(b"abc"), deletion_payload=b"a"
                    ),
                    _record(
                        fingerprint=fingerprint_bytes(b"abc"), deletion_payload=b"b"
                    ),
                ],
                False,
            )

    def test_uncertain_history_tombstone(self) -> None:
        handler = _MessageHandler()

        result = handler.reconcile(
            _key(message_key="k"),
            coco.NON_EXISTENCE,
            [],
            True,
        )

        assert result is not None
        assert result.action == _action(message_key="k", value=None, publish=True)

    def test_empty_payload_is_not_tombstone(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(_key(message_key="k"), _spec(b""), [], False)
        assert first is not None

        result = handler.reconcile(
            _key(message_key="k"),
            _spec(b""),
            [_as_record(first.tracking_record)],
            False,
        )
        assert result is None

        delete = handler.reconcile(
            _key(message_key="k"),
            coco.NON_EXISTENCE,
            [_as_record(first.tracking_record)],
            False,
        )
        assert delete is not None
        assert delete.action.value is None


class TestMessageHandlerSink:
    @pytest.mark.asyncio
    async def test_empty_action_list(self) -> None:
        await _MessageHandler()._apply_actions(MagicMock(spec=ContextProvider), [])

    @pytest.mark.asyncio
    async def test_metadata_only_actions_dont_resolve_client(self) -> None:
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.side_effect = AssertionError(
            "client should not be resolved"
        )

        await _MessageHandler()._apply_actions(
            context_provider,
            [_action(value=None, publish=False)],
        )

    @pytest.mark.asyncio
    async def test_mixed_batch_processes_publish_only(self) -> None:
        client = MockClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        await _MessageHandler()._apply_actions(
            context_provider,
            [
                _action(value=None, publish=False),
                _action(message_key="k", value=b"v1"),
            ],
        )

        assert client.topics == ["topic"]
        assert client.producers[0].messages == [(b"v1", "k")]

    @pytest.mark.asyncio
    async def test_multiple_messages_one_group(self) -> None:
        client = MockClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        await _MessageHandler()._apply_actions(
            context_provider,
            [
                _action(message_key="a", value=b"one"),
                _action(message_key="b", value=b"two"),
                _action(message_key="c", value=None, publish=False),
            ],
        )

        assert len(client.producers) == 1
        assert client.producers[0].messages == [(b"one", "a"), (b"two", "b")]

    @pytest.mark.asyncio
    async def test_multiple_groups_and_order(self) -> None:
        client_a = MockClient()
        client_b = MockClient()
        context_provider = MagicMock(spec=ContextProvider)

        def resolve_client(key: str, _type: Any | None = None) -> Any:
            if key == "c1":
                return client_a
            return client_b

        context_provider.get.side_effect = resolve_client

        await _MessageHandler()._apply_actions(
            context_provider,
            [
                _action(client_key="c2", topic="t2", message_key="k1", value=b"v1"),
                _action(client_key="c1", topic="t1", message_key="k2", value=b"v2"),
                _action(client_key="c2", topic="t2", message_key="k3", value=b"v3"),
                _action(client_key="c1", topic="t1", message_key="k4", value=b"v4"),
            ],
        )

        assert client_b.topics == ["t2"]
        assert client_b.producers[0].messages == [(b"v1", "k1"), (b"v3", "k3")]
        assert client_a.topics == ["t1"]
        assert client_a.producers[0].messages == [(b"v2", "k2"), (b"v4", "k4")]

    @pytest.mark.asyncio
    async def test_apply_actions_off_event_loop(self) -> None:
        client = MockClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        event_loop_thread = threading.get_ident()
        await _MessageHandler()._apply_actions(
            context_provider,
            [_action(message_key="k", value=b"v1")],
        )

        assert all(tid != event_loop_thread for tid in client.thread_ids)
        assert all(tid != event_loop_thread for tid in client.producers[0].thread_ids)

    @pytest.mark.asyncio
    async def test_blocked_send_keeps_event_loop_responsive(self) -> None:
        client = _BlockingClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client
        apply_task = asyncio.create_task(
            _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )
        )

        try:
            assert await asyncio.to_thread(client.producer.send_started.wait, 5)
            progressed = asyncio.Event()
            asyncio.get_running_loop().call_soon(progressed.set)
            await asyncio.wait_for(progressed.wait(), timeout=0.25)
            assert not apply_task.done()
        finally:
            client.producer.release_send.set()

        await asyncio.wait_for(apply_task, timeout=5)
        assert client.producer.close_calls == 1

    @pytest.mark.asyncio
    async def test_cancellation_waits_for_worker_cleanup(self) -> None:
        client = _BlockingClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client
        apply_task = asyncio.create_task(
            _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )
        )

        try:
            assert await asyncio.to_thread(client.producer.send_started.wait, 5)
            apply_task.cancel()
            await asyncio.sleep(0)
            assert not apply_task.done()
        finally:
            client.producer.release_send.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(apply_task, timeout=5)
        assert client.producer.close_calls == 1
        assert client.close_calls == 0

    @pytest.mark.asyncio
    async def test_cancellation_keeps_worker_failure_as_diagnostic(self) -> None:
        client = _BlockingClient(send_error=RuntimeError("send failed after cancel"))
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client
        apply_task = asyncio.create_task(
            _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )
        )

        try:
            assert await asyncio.to_thread(client.producer.send_started.wait, 5)
            apply_task.cancel()
            await asyncio.sleep(0)
            assert not apply_task.done()
        finally:
            client.producer.release_send.set()

        with pytest.raises(asyncio.CancelledError) as exc_info:
            await asyncio.wait_for(apply_task, timeout=5)
        assert exc_info.value.__notes__ == [
            (
                "Pulsar publication also failed during cancellation: "
                "RuntimeError('send failed after cancel')"
            )
        ]
        assert client.producer.close_calls == 1

    @pytest.mark.asyncio
    async def test_repeated_cancellation_still_waits_for_cleanup(self) -> None:
        client = _BlockingClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client
        apply_task = asyncio.create_task(
            _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )
        )

        try:
            assert await asyncio.to_thread(client.producer.send_started.wait, 5)
            apply_task.cancel()
            await asyncio.sleep(0)
            apply_task.cancel()
            await asyncio.sleep(0)
            assert not apply_task.done()
        finally:
            client.producer.release_send.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(apply_task, timeout=5)
        assert client.producer.close_calls == 1

    @pytest.mark.asyncio
    async def test_large_batch_has_one_send_in_flight(self) -> None:
        class _ConcurrencyTrackingProducer(MockProducer):
            def __init__(self) -> None:
                super().__init__()
                self.active_sends = 0
                self.max_active_sends = 0
                self.first_send_started = threading.Event()
                self.release_first_send = threading.Event()

            def send(self, content: bytes | None, *, partition_key: str) -> None:
                self.active_sends += 1
                self.max_active_sends = max(self.max_active_sends, self.active_sends)
                try:
                    if not self.messages:
                        self.first_send_started.set()
                        if not self.release_first_send.wait(timeout=5):
                            raise TimeoutError("first send was not released")
                    super().send(content, partition_key=partition_key)
                finally:
                    self.active_sends -= 1

        class _ConcurrencyTrackingClient(MockClient):
            def __init__(self) -> None:
                super().__init__()
                self.producer = _ConcurrencyTrackingProducer()

            def create_producer(self, topic: str) -> MockProducer:
                self.topics.append(topic)
                self.producers.append(self.producer)
                return self.producer

        client = _ConcurrencyTrackingClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client
        actions = [
            _action(message_key=f"k{i:03}", value=f"v{i:03}".encode())
            for i in range(100)
        ]

        apply_task = asyncio.create_task(
            _MessageHandler()._apply_actions(context_provider, actions)
        )
        try:
            assert await asyncio.to_thread(client.producer.first_send_started.wait, 5)
            await asyncio.sleep(0)
            assert client.producer.active_sends == 1
            assert client.producer.messages == []
            assert not apply_task.done()
        finally:
            client.producer.release_first_send.set()

        await asyncio.wait_for(apply_task, timeout=5)

        assert len(client.producers) == 1
        assert client.producer.max_active_sends == 1
        assert client.producer.messages == [
            (f"v{i:03}".encode(), f"k{i:03}") for i in range(100)
        ]
        assert client.producer.close_calls == 1

    @pytest.mark.asyncio
    async def test_repeated_invocations_close_every_producer(self) -> None:
        client = MockClient()
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        for i in range(20):
            await _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key=f"k{i}", value=f"v{i}".encode())],
            )

        assert len(client.producers) == 20
        assert all(producer.close_calls == 1 for producer in client.producers)
        assert client.close_calls == 0

    @pytest.mark.asyncio
    async def test_groups_run_sequentially_in_first_seen_order(self) -> None:
        events: list[tuple[str, str, str | None]] = []

        class _OrderedProducer(MockProducer):
            def __init__(self, group: str) -> None:
                super().__init__()
                self.group = group

            def send(self, content: bytes | None, *, partition_key: str) -> None:
                events.append(("send", self.group, partition_key))
                super().send(content, partition_key=partition_key)

            def close(self) -> None:
                events.append(("close", self.group, None))
                super().close()

        class _OrderedClient(MockClient):
            def __init__(self, name: str) -> None:
                super().__init__()
                self.name = name

            def create_producer(self, topic: str) -> MockProducer:
                group = f"{self.name}/{topic}"
                events.append(("create", group, None))
                producer = _OrderedProducer(group)
                self.producers.append(producer)
                return producer

        clients = {"c1": _OrderedClient("c1"), "c2": _OrderedClient("c2")}
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.side_effect = lambda key: clients[key]

        await _MessageHandler()._apply_actions(
            context_provider,
            [
                _action(client_key="c2", topic="t2", message_key="k1", value=b"v1"),
                _action(client_key="c1", topic="t1", message_key="k2", value=b"v2"),
                _action(client_key="c2", topic="t2", message_key="k3", value=b"v3"),
                _action(client_key="c1", topic="t1", message_key="k4", value=b"v4"),
            ],
        )

        assert events == [
            ("create", "c2/t2", None),
            ("send", "c2/t2", "k1"),
            ("send", "c2/t2", "k3"),
            ("close", "c2/t2", None),
            ("create", "c1/t1", None),
            ("send", "c1/t1", "k2"),
            ("send", "c1/t1", "k4"),
            ("close", "c1/t1", None),
        ]

    @pytest.mark.asyncio
    async def test_caller_owned_client_not_closed(self) -> None:
        mock_client = _ClientWithClose()
        client = cast(pulsar.Client, mock_client)
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        await _MessageHandler()._apply_actions(
            context_provider,
            [_action(message_key="k", value=b"v1")],
        )

        assert mock_client.close_calls == 0

    @pytest.mark.asyncio
    async def test_create_failure(self) -> None:
        client = MockClient(create_error=RuntimeError("inject create"))
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        with pytest.raises(RuntimeError, match="inject create"):
            await _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )

    @pytest.mark.asyncio
    async def test_send_failure_closes_producer(self) -> None:
        client = MockClient(send_error_at=2)
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        with pytest.raises(RuntimeError, match="injected send failure"):
            await _MessageHandler()._apply_actions(
                context_provider,
                [
                    _action(message_key="a", value=b"one"),
                    _action(message_key="b", value=b"two"),
                ],
            )

        assert client.producers[0].messages == [(b"one", "a"), (b"two", "b")]
        assert client.producers[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_close_failure_after_publish(self) -> None:
        client = MockClient(close_error=RuntimeError("inject close"))
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        with pytest.raises(RuntimeError, match="inject close"):
            await _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )

        assert client.producers[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_send_and_close_failure_prioritizes_send(self) -> None:
        client = MockClient(send_error_at=1, close_error=RuntimeError("inject close"))
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        with pytest.raises(RuntimeError, match="injected send failure") as exc_info:
            await _MessageHandler()._apply_actions(
                context_provider,
                [_action(message_key="k", value=b"v1")],
            )

        assert exc_info.value.__notes__ == [
            "Additionally failed to close Pulsar producer: RuntimeError('inject close')"
        ]

    @pytest.mark.asyncio
    async def test_failures_stop_after_group(self) -> None:
        client = MockClient(send_error_at=2)
        context_provider = MagicMock(spec=ContextProvider)
        context_provider.get.return_value = client

        with pytest.raises(RuntimeError, match="injected send failure"):
            await _MessageHandler()._apply_actions(
                context_provider,
                [
                    _action(topic="t1", message_key="k1", value=b"v1"),
                    _action(topic="t1", message_key="k2", value=b"v2"),
                    _action(topic="t2", message_key="k2", value=b"v2"),
                ],
            )

        assert client.topics == ["t1"]


class TestPublicApi:
    def test_exports_exist(self) -> None:
        for name in [
            "DeletionValueFn",
            "PulsarTopicTarget",
            "declare_pulsar_topic_target",
            "mount_pulsar_topic_target",
            "pulsar_topic_target",
        ]:
            assert hasattr(pulsar_connector, name)

    @pytest.mark.asyncio
    async def test_wrapper_memo_identity_is_stable(self) -> None:
        provider = MagicMock()
        provider.memo_key = "memo-key"
        target1 = pulsar_connector.PulsarTopicTarget(
            provider,
            client_key=_APP_CLIENT.key,
            topic="topic-1",
            deletion_value_fn=None,
        )
        target2 = pulsar_connector.PulsarTopicTarget(
            provider,
            client_key=_APP_CLIENT.key,
            topic="topic-1",
            deletion_value_fn=None,
        )
        assert target1.__coco_memo_key__() == target2.__coco_memo_key__()

    def test_isolated_message_keys(self) -> None:
        handler = _MessageHandler()
        first = handler.reconcile(
            _key(client_key="c1", message_key="shared"),
            _spec("abc"),
            [],
            False,
        )
        assert first is not None

        second = handler.reconcile(
            _key(client_key="c2", message_key="shared"),
            _spec("abc"),
            [],
            False,
        )

        assert second is not None
        assert second.action.key.client_key == "c2"

        third = handler.reconcile(
            _key(client_key="c1", topic="other", message_key="shared"),
            _spec("abc"),
            [],
            False,
        )
        assert third is not None
        assert third.action.key.topic == "other"

    @pytest.mark.asyncio
    async def test_runtime_client_replacement_preserves_identity(self) -> None:
        global _app_topic, _app_desired, _app_include_topic, _app_delete_fn

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_client_replacement_{uuid.uuid4().hex}"
        _app_desired = {"order": b"value"}
        _app_include_topic = True
        _app_delete_fn = None
        first_client = MockClient()
        second_client = MockClient()
        context = coco.ContextProvider()
        context.provide(_APP_CLIENT, _as_client(first_client))
        app = coco.App(
            coco.AppConfig(
                name=app_name,
                environment=coco.Environment(
                    coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
                    context_provider=context,
                ),
            ),
            _declare_app_target,
        )
        try:
            await app.update()
            context.provide(_APP_CLIENT, _as_client(second_client))
            await app.update()
            assert first_client.producers[0].messages == [(b"value", "order")]
            assert second_client.producers == []
        finally:
            _app_desired.clear()
            _app_include_topic = True
            _app_delete_fn = None

    @pytest.mark.asyncio
    async def test_deletion_policy_update_is_metadata_only(self) -> None:
        global _app_topic, _app_desired, _app_include_topic, _app_delete_fn

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_policy_update_{uuid.uuid4().hex}"
        _app_desired = {"order": b"value"}
        _app_include_topic = True
        _app_delete_fn = lambda key: f"old:{key}"
        client = MockClient()
        context = coco.ContextProvider()
        context.provide(_APP_CLIENT, _as_client(client))
        app = coco.App(
            coco.AppConfig(
                name=app_name,
                environment=coco.Environment(
                    coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
                    context_provider=context,
                ),
            ),
            _declare_app_target,
        )
        try:
            await app.update()
            producer_count = len(client.producers)
            _app_delete_fn = lambda key: f"new:{key}"
            await app.update()
            assert len(client.producers) == producer_count
            _app_desired.clear()
            await app.update()
            assert client.producers[-1].messages == [(b"new:order", "order")]
        finally:
            _app_desired.clear()
            _app_include_topic = True
            _app_delete_fn = None

    @pytest.mark.asyncio
    async def test_parent_removal_emits_delete(self) -> None:
        global _app_topic, _app_desired, _app_include_topic

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_app_parent_removal_{uuid.uuid4().hex}"
        db_path = common.get_env_db_path(app_name)
        client = MockClient()
        _app_desired = {"order-1": b"value"}
        _app_include_topic = True

        environment = coco.Environment(
            coco.Settings.from_env(db_path=db_path),
            context_provider=coco.ContextProvider(),
        )
        environment.context_provider.provide(_APP_CLIENT, _as_client(client))
        app = coco.App(
            coco.AppConfig(name=app_name, environment=environment), _declare_app_target
        )

        await app.update()
        assert any(
            msg == (b"value", "order-1") for msg in client.producers[-1].messages
        )

        _app_include_topic = False
        await app.update()

        assert client.producers[-1].messages[-1] == (None, "order-1")

    @pytest.mark.asyncio
    async def test_app_drop_emits_delete(self) -> None:
        global _app_topic, _app_desired, _app_include_topic

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_app_drop_{uuid.uuid4().hex}"
        db_path = common.get_env_db_path(app_name)
        client = MockClient()
        _app_desired = {"order": b"value"}
        _app_include_topic = True

        environment = coco.Environment(
            coco.Settings.from_env(db_path=db_path),
            context_provider=coco.ContextProvider(),
        )
        environment.context_provider.provide(_APP_CLIENT, _as_client(client))
        app = coco.App(
            coco.AppConfig(name=app_name, environment=environment), _declare_app_target
        )

        await app.update()
        await app.drop()

        assert any(
            msg[0] is None and msg[1] == "order"
            for msg in client.producers[-1].messages
        )

    @pytest.mark.asyncio
    async def test_app_drop_failure_is_retryable(self) -> None:
        global _app_topic, _app_desired, _app_include_topic, _app_delete_fn

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_drop_retry_{uuid.uuid4().hex}"
        _app_desired = {"order": b"value"}
        _app_include_topic = True
        _app_delete_fn = None
        client = MockClient()
        context = coco.ContextProvider()
        context.provide(_APP_CLIENT, _as_client(client))
        app = coco.App(
            coco.AppConfig(
                name=app_name,
                environment=coco.Environment(
                    coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
                    context_provider=context,
                ),
            ),
            _declare_app_target,
        )
        try:
            await app.update()
            client.send_error_at = 1
            with pytest.raises(RuntimeError, match="injected send failure"):
                await app.drop()
            client.send_error_at = None
            await app.drop()
            assert client.producers[-1].messages == [(None, "order")]
            assert client.close_calls == 0
        finally:
            _app_desired.clear()
            _app_include_topic = True
            _app_delete_fn = None

    @pytest.mark.asyncio
    async def test_partial_batch_failure_then_retry(self) -> None:
        global _app_topic, _app_desired

        _app_topic = f"persistent://public/default/{uuid.uuid4().hex}"
        app_name = f"test_pulsar_partial_retry_{uuid.uuid4().hex}"
        db_path = common.get_env_db_path(app_name)
        _app_desired = {"a": b"v1", "b": b"v2"}

        fail_client = MockClient(send_error_at=2)
        good_client = MockClient()

        context = coco.ContextProvider()
        context.provide(_APP_CLIENT, _as_client(fail_client))
        environment = coco.Environment(
            coco.Settings.from_env(db_path=db_path),
            context_provider=context,
        )
        bad_app = coco.App(
            coco.AppConfig(
                name=app_name,
                environment=environment,
            ),
            _declare_app_target,
        )

        with pytest.raises(RuntimeError, match="injected send failure"):
            await bad_app.update()

        context.provide(_APP_CLIENT, _as_client(good_client))
        await bad_app.update()

        assert sorted(fail_client.producers[0].messages) == sorted(
            [(b"v1", "a"), (b"v2", "b")]
        )
        assert sorted(good_client.producers[0].messages) == sorted(
            [(b"v1", "a"), (b"v2", "b")]
        )


@dataclass(frozen=True)
class _RestartPhase:
    mode: str
    app_name: str
    db_path: pathlib.Path
    service_url: str
    topic: str
    client_key: str


def _run_spawned_restart_phase(phase: _RestartPhase) -> None:
    import asyncio

    client_key = ContextKey[pulsar.Client](phase.client_key)

    @coco.fn
    async def _target_app() -> None:
        target = await coco.use_mount(
            coco.component_subpath("pulsar-topic"),
            pulsar_connector.declare_pulsar_topic_target,
            client_key,
            phase.topic,
            deletion_value_fn=_custom_delete_value,
        )
        if phase.mode != "drop":
            target.declare_target_state(key="order-1", value="value")

    async def _run() -> None:
        client = await asyncio.to_thread(pulsar.Client, phase.service_url)
        environment = coco.Environment(
            coco.Settings.from_env(db_path=phase.db_path),
            context_provider=coco.ContextProvider(),
        )
        environment.context_provider.provide(client_key, client)
        app = coco.App(
            coco.AppConfig(name=phase.app_name, environment=environment),
            _target_app,
        )

        if phase.mode == "drop":
            await app.drop()
        else:
            await app.update()

        await asyncio.to_thread(client.close)

    asyncio.run(_run())


def _run_crash_before_tracking_phase(
    phase: _RestartPhase, checkpoint: Connection
) -> None:
    import asyncio

    client_key = ContextKey[pulsar.Client](phase.client_key)

    class _AcknowledgedProducer:
        def __init__(self, producer: pulsar.Producer) -> None:
            self._producer = producer

        def send(self, content: bytes | None, *, partition_key: str) -> Any:
            message_id = self._producer.send(content, partition_key=partition_key)
            checkpoint.send(message_id.serialize())
            checkpoint.recv()
            return message_id

        def close(self) -> None:
            self._producer.close()

    class _AcknowledgedClient:
        def __init__(self, client: pulsar.Client) -> None:
            self._client = client

        def create_producer(self, topic: str) -> _AcknowledgedProducer:
            return _AcknowledgedProducer(self._client.create_producer(topic))

    @coco.fn
    async def _target_app() -> None:
        target = await coco.use_mount(
            coco.component_subpath("pulsar-topic"),
            pulsar_connector.declare_pulsar_topic_target,
            client_key,
            phase.topic,
            deletion_value_fn=_custom_delete_value,
        )
        target.declare_target_state(key="order-1", value="value")

    async def _run() -> None:
        client = await asyncio.to_thread(
            pulsar.Client,
            phase.service_url,
            operation_timeout_seconds=5,
        )
        environment = coco.Environment(
            coco.Settings.from_env(db_path=phase.db_path),
            context_provider=coco.ContextProvider(),
        )
        environment.context_provider.provide(
            client_key, cast(pulsar.Client, _AcknowledgedClient(client))
        )
        app = coco.App(
            coco.AppConfig(name=phase.app_name, environment=environment),
            _target_app,
        )
        try:
            await app.update()
        finally:
            await asyncio.to_thread(client.close)

    try:
        asyncio.run(_run())
    finally:
        checkpoint.close()


@requires_pulsar_server
@pytest.mark.timeout(180)
def test_live_crash_after_ack_before_tracking_replays() -> None:
    run_id = uuid.uuid4().hex
    phase = _RestartPhase(
        mode="create",
        app_name=f"pulsar-crash-{run_id}",
        db_path=common.get_env_db_path(f"pulsar-crash-{run_id}") / "state",
        service_url=_PULSAR_SERVICE_URL,
        topic=f"persistent://public/default/pulsar-crash-{run_id}",
        client_key=f"pulsar-crash-{run_id}",
    )

    observer_client = pulsar.Client(_PULSAR_SERVICE_URL)
    reader = observer_client.create_reader(phase.topic, pulsar.MessageId.earliest)
    spawn = get_context("spawn")
    parent_checkpoint, child_checkpoint = spawn.Pipe()
    crash_process = spawn.Process(
        target=_run_crash_before_tracking_phase,
        args=(phase, child_checkpoint),
    )
    try:
        crash_process.start()
        child_checkpoint.close()
        if not parent_checkpoint.poll(30):
            pytest.fail("crash child did not reach the acknowledged-send checkpoint")
        acknowledged_id = parent_checkpoint.recv()
        first = reader.read_next(15_000)
        assert first.partition_key() == "order-1"
        assert first.data() == b"value"
        assert not first.has_null_value()
        assert first.message_id().serialize() == acknowledged_id

        crash_process.terminate()
        crash_process.join(timeout=10)
        assert not crash_process.is_alive()
        assert crash_process.exitcode not in (None, 0)

        recovery_process = spawn.Process(
            target=_run_spawned_restart_phase,
            args=(phase,),
        )
        recovery_process.start()
        recovery_process.join(timeout=60)
        if recovery_process.is_alive():
            recovery_process.terminate()
            recovery_process.join(timeout=10)
            pytest.fail("recovery child timed out")
        assert recovery_process.exitcode == 0

        replay = reader.read_next(15_000)
        assert replay.partition_key() == "order-1"
        assert replay.data() == b"value"
        assert not replay.has_null_value()
        replay_id = replay.message_id().serialize()
        assert replay_id != acknowledged_id
        print(
            "CRASH_BOUNDARY_OBSERVED="
            f"accepted:{acknowledged_id.hex()} -> crash -> replay:{replay_id.hex()}"
        )

        noop_process = spawn.Process(
            target=_run_spawned_restart_phase,
            args=(phase,),
        )
        noop_process.start()
        noop_process.join(timeout=60)
        if noop_process.is_alive():
            noop_process.terminate()
            noop_process.join(timeout=10)
            pytest.fail("post-recovery no-op child timed out")
        assert noop_process.exitcode == 0
        with pytest.raises(pulsar.Timeout):
            reader.read_next(2_000)
    finally:
        parent_checkpoint.close()
        if crash_process.is_alive():
            crash_process.terminate()
            crash_process.join(timeout=10)
        reader.close()
        observer_client.close()


@requires_pulsar_server
@pytest.mark.timeout(120)
def test_spawned_process_restart_replays_from_tracking() -> None:
    run_id = uuid.uuid4().hex
    topic = f"persistent://public/default/pulsar-restart-{run_id}"
    app_name = f"pulsar-restart-{run_id}"
    context_key = f"pulsar-restart-{run_id}"
    db_path = common.get_env_db_path(app_name) / "state"

    observer_client = pulsar.Client(_PULSAR_SERVICE_URL)
    reader = observer_client.create_reader(topic, pulsar.MessageId.earliest)
    try:
        phase_create = _RestartPhase(
            mode="create",
            app_name=app_name,
            db_path=db_path,
            service_url=_PULSAR_SERVICE_URL,
            topic=topic,
            client_key=context_key,
        )
        phase_noop = _RestartPhase(
            mode="create",
            app_name=app_name,
            db_path=db_path,
            service_url=_PULSAR_SERVICE_URL,
            topic=topic,
            client_key=context_key,
        )
        phase_drop = _RestartPhase(
            mode="drop",
            app_name=app_name,
            db_path=db_path,
            service_url=_PULSAR_SERVICE_URL,
            topic=topic,
            client_key=context_key,
        )

        loop = asyncio.new_event_loop()
        for phase, expected_payload in (
            (phase_create, b"value"),
            (phase_noop, None),
            (phase_drop, b"deleted:order-1"),
        ):
            proc = get_context("spawn").Process(
                target=_run_spawned_restart_phase, args=(phase,)
            )
            proc.start()
            proc.join(timeout=60)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                pytest.fail(f"spawned Pulsar phase {phase.mode!r} timed out")
            assert proc.exitcode == 0
            if expected_payload is None:
                with pytest.raises(pulsar.Timeout):
                    loop.run_until_complete(_read_message(reader, timeout_millis=2_000))
                continue
            msg = loop.run_until_complete(_read_message(reader, timeout_millis=15_000))
            assert msg.partition_key() == "order-1"
            assert msg.data() == expected_payload
            assert not msg.has_null_value()
        loop.close()
    finally:
        reader.close()
        observer_client.close()


@requires_pulsar_server
@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_live_path_create_update_delete_custom_tombstone() -> None:
    global _live_topic, _app_desired, _app_include_topic, _app_delete_fn

    run_id = uuid.uuid4().hex
    _live_topic = f"persistent://public/default/pulsar-live-{run_id}"
    app_name = f"PulsarLive{run_id}"
    _app_desired = {"order-1": b"v1"}
    _app_include_topic = True
    _app_delete_fn = _custom_delete_value

    target_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    observer_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    reader = await asyncio.to_thread(
        observer_client.create_reader,
        _live_topic,
        pulsar.MessageId.earliest,
    )
    environment = coco.Environment(
        coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
        context_provider=coco.ContextProvider(),
    )
    environment.context_provider.provide(_LIVE_CLIENT, target_client)
    app = coco.App(
        coco.AppConfig(name=app_name, environment=environment), _declare_live_target
    )

    try:
        await app.update()
        created = await _read_message(reader)
        assert created.partition_key() == "order-1"
        assert created.data() == b"v1"
        assert not created.has_null_value()

        await app.update()
        await _assert_no_message(reader)

        _app_desired["order-1"] = "v2"
        await app.update()
        updated = await _read_message(reader)
        assert updated.data() == b"v2"

        _app_desired.clear()
        await app.update()
        deleted = await _read_message(reader)
        assert deleted.partition_key() == "order-1"
        assert deleted.data() == b"deleted:order-1"
        assert not deleted.has_null_value()

        _app_desired["order-1"] = b""
        await app.update()
        recreated = await _read_message(reader)
        assert recreated.data() == b""
        assert not recreated.has_null_value()
    finally:
        _live_topic = ""
        _app_desired.clear()
        _app_include_topic = True
        _app_delete_fn = None
        await asyncio.to_thread(reader.close)
        await asyncio.to_thread(observer_client.close)
        await asyncio.to_thread(target_client.close)


@requires_pulsar_server
@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_live_parent_removal_publishes_delete() -> None:
    global _app_desired, _app_include_topic, _live_topic, _app_delete_fn

    run_id = uuid.uuid4().hex
    _live_topic = f"persistent://public/default/pulsar-live-parent-drop-{run_id}"
    app_name = f"PulsarParentDrop{run_id}"
    _app_desired = {"order-1": b"v1"}
    _app_include_topic = True
    _app_delete_fn = None

    target_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    observer_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    reader = await asyncio.to_thread(
        observer_client.create_reader,
        _live_topic,
        pulsar.MessageId.earliest,
    )
    environment = coco.Environment(
        coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
        context_provider=coco.ContextProvider(),
    )
    environment.context_provider.provide(_LIVE_CLIENT, target_client)
    app = coco.App(
        coco.AppConfig(name=app_name, environment=environment),
        _declare_live_target,
    )

    try:
        await app.update()
        assert (await _read_message(reader)).data() == b"v1"

        _app_include_topic = False
        await app.update()
        deleted = await _read_message(reader)
        assert deleted.has_null_value()

        # Recreate using the same caller-owned client; the connector never deletes the topic.
        _app_include_topic = True
        await app.update()
        recreated = await _read_message(reader)
        assert recreated.data() == b"v1"
    finally:
        _app_include_topic = True
        _app_desired.clear()
        _live_topic = ""
        await asyncio.to_thread(reader.close)
        await asyncio.to_thread(observer_client.close)
        await asyncio.to_thread(target_client.close)


@requires_pulsar_server
@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_live_app_drop_publishes_delete_and_topic_remains_usable() -> None:
    global _app_desired, _live_topic, _app_delete_fn

    run_id = uuid.uuid4().hex
    _live_topic = f"persistent://public/default/pulsar-live-drop-{run_id}"
    app_name = f"PulsarDrop{run_id}"
    _app_desired = {"order-1": b"v1"}
    _app_delete_fn = None

    target_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    observer_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    reader = await asyncio.to_thread(
        observer_client.create_reader, _live_topic, pulsar.MessageId.earliest
    )
    context_provider = coco.ContextProvider()
    context_provider.provide(_LIVE_CLIENT, target_client)
    app = coco.App(
        coco.AppConfig(
            name=app_name,
            environment=coco.Environment(
                coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
                context_provider=context_provider,
            ),
        ),
        _declare_live_target,
    )
    try:
        await app.update()
        assert (await _read_message(reader)).data() == b"v1"
        await app.drop()
        deleted = await _read_message(reader)
        assert deleted.partition_key() == "order-1"
        assert deleted.has_null_value()

        producer = await asyncio.to_thread(target_client.create_producer, _live_topic)
        try:
            await asyncio.to_thread(
                producer.send, b"still-usable", partition_key="probe"
            )
        finally:
            await asyncio.to_thread(producer.close)
        probe = await _read_message(reader)
        assert probe.partition_key() == "probe"
        assert probe.data() == b"still-usable"
    finally:
        _app_desired.clear()
        _live_topic = ""
        _app_delete_fn = None
        await asyncio.to_thread(reader.close)
        await asyncio.to_thread(observer_client.close)
        await asyncio.to_thread(target_client.close)


@requires_pulsar_server
@pytest.mark.asyncio
@pytest.mark.timeout(180)
async def test_live_partitioned_topic_preserves_keys_and_incremental_state() -> None:
    global _app_desired, _live_topic, _app_delete_fn

    run_id = uuid.uuid4().hex
    topic_name = f"pulsar-partitioned-{run_id}"
    _live_topic = f"persistent://public/default/{topic_name}"
    app_name = f"PulsarPartitioned{run_id}"
    _app_desired = {"café": "v1", "東京": b"v2"}
    _app_delete_fn = None

    partitions_url = (
        f"{_PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/"
        f"{topic_name}/partitions"
    )
    create_request = Request(
        partitions_url,
        data=json.dumps(4).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    response = await asyncio.to_thread(urlopen, create_request, timeout=10)
    response.close()

    target_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    observer_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    reader = await asyncio.to_thread(
        observer_client.create_reader,
        _live_topic,
        pulsar.MessageId.earliest,
    )
    environment = coco.Environment(
        coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
        context_provider=coco.ContextProvider(),
    )
    environment.context_provider.provide(_LIVE_CLIENT, target_client)
    app = coco.App(
        coco.AppConfig(name=app_name, environment=environment),
        _declare_live_target,
    )

    try:
        await app.update()
        created = [await _read_message(reader), await _read_message(reader)]
        assert {
            message.partition_key(): (message.data(), message.has_null_value())
            for message in created
        } == {
            "café": (b"v1", False),
            "東京": (b"v2", False),
        }

        _app_desired["café"] = "v3"
        await app.update()
        updated = await _read_message(reader)
        assert updated.partition_key() == "café"
        assert updated.data() == b"v3"
        assert not updated.has_null_value()
        await _assert_no_message(reader)

        del _app_desired["東京"]
        await app.update()
        deleted = await _read_message(reader)
        assert deleted.partition_key() == "東京"
        assert deleted.has_null_value()
        await _assert_no_message(reader)
    finally:
        _app_desired.clear()
        _live_topic = ""
        _app_delete_fn = None
        await asyncio.to_thread(reader.close)
        await asyncio.to_thread(observer_client.close)
        await asyncio.to_thread(target_client.close)
        delete_request = Request(
            f"{partitions_url}?force=true&deleteSchema=true",
            method="DELETE",
        )
        try:
            cleanup_response = await asyncio.to_thread(
                urlopen, delete_request, timeout=10
            )
        except HTTPError as error:
            if error.code != 404:
                warnings.warn(
                    f"failed to delete test partitioned topic: {error}",
                    stacklevel=2,
                )
        except OSError as error:
            warnings.warn(
                f"failed to delete test partitioned topic: {error}",
                stacklevel=2,
            )
        else:
            await asyncio.to_thread(cleanup_response.close)


@requires_pulsar_server
@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_live_compaction_regression() -> None:
    global _app_desired, _live_topic, _app_delete_fn

    run_id = uuid.uuid4().hex
    _live_topic = f"persistent://public/default/pulsar-compaction-{run_id}"
    app_name = f"PulsarCompaction{run_id}"
    _app_desired = {"order-1": b"value"}
    _app_delete_fn = None

    target_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    observer_client = await asyncio.to_thread(pulsar.Client, _PULSAR_SERVICE_URL)
    reader = await asyncio.to_thread(
        observer_client.create_reader,
        _live_topic,
        pulsar.MessageId.earliest,
    )
    environment = coco.Environment(
        coco.Settings.from_env(db_path=common.get_env_db_path(app_name)),
        context_provider=coco.ContextProvider(),
    )
    environment.context_provider.provide(_LIVE_CLIENT, target_client)
    app = coco.App(
        coco.AppConfig(name=app_name, environment=environment),
        _declare_live_target,
    )

    tenant, namespace, topic = _live_topic.removeprefix("persistent://").split("/", 2)
    compact_url = (
        f"{_PULSAR_ADMIN_URL}/admin/v2/persistent/"
        f"{tenant}/{namespace}/{topic}/compaction"
    )

    try:
        await app.update()
        assert (await _read_message(reader)).data() == b"value"

        _app_desired.clear()
        await app.update()
        delete_msg = await _read_message(reader)
        assert delete_msg.has_null_value()

        trigger = Request(compact_url, method="PUT")
        await asyncio.to_thread(urlopen, trigger, timeout=10)

        async def _wait_compaction() -> None:
            req = Request(compact_url, method="GET")
            for _ in range(60):
                try:
                    with await asyncio.to_thread(urlopen, req, timeout=10) as response:  # type: ignore[arg-type]
                        body = response.read().decode("utf-8")
                    status = body.strip().lower()
                    if "running" not in status:
                        return
                except HTTPError as error:
                    if error.code != 409:
                        raise
                await asyncio.sleep(2)
            raise AssertionError("compaction did not finish")

        await _wait_compaction()
        compacted_reader = await asyncio.to_thread(
            observer_client.create_reader,
            _live_topic,
            pulsar.MessageId.earliest,
            is_read_compacted=True,
        )
        try:
            await _assert_no_message(compacted_reader)
        finally:
            await asyncio.to_thread(compacted_reader.close)
    finally:
        _app_desired.clear()
        _live_topic = ""
        await asyncio.to_thread(reader.close)
        await asyncio.to_thread(observer_client.close)
        await asyncio.to_thread(target_client.close)
