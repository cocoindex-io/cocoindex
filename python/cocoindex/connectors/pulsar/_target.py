"""
Apache Pulsar target for CocoIndex.

This module provides a two-level target state system for Pulsar:
1. Topic level: Lightweight container for generation tracking (user-managed topic)
2. Message level: Produces messages to the topic for upserts/deletes

The Pulsar Python client (``pulsar-client``) is synchronous and producers are
created per topic, so a single ``pulsar.Client`` is provided via a ContextKey and
the connector lazily creates/caches one producer per topic, sending off the event
loop via ``asyncio.to_thread``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Collection, Generic, NamedTuple, Sequence

try:
    import pulsar  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "pulsar-client is required to use the Pulsar connector. "
        "Please install cocoindex[pulsar]."
    ) from e

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_bytes, fingerprint_str
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker

# --- Type aliases ---

_MessageFingerprint = bytes

# --- Internal types ---


class _TopicKey(NamedTuple):
    client_key: str
    topic: str


_TOPIC_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _TopicSpec:
    deletion_value_fn: Callable[[bytes | str], bytes | str] | None


class _TopicAction(NamedTuple):
    key: _TopicKey
    spec: _TopicSpec | coco.NonExistenceType


class _MessageAction(NamedTuple):
    key: bytes | str
    value: bytes | str | None  # None = tombstone (no deletion_value_fn)


def _as_partition_key(key: bytes | str) -> str:
    return key.decode("utf-8") if isinstance(key, bytes) else key


def _as_payload(value: bytes | str | None) -> bytes:
    # Pulsar payloads are bytes; a tombstone (value=None) is sent as an empty
    # payload (use ``deletion_value_fn`` for a custom delete marker, and a
    # compacted topic if you need key-based compaction semantics).
    if value is None:
        return b""
    if isinstance(value, str):
        return value.encode("utf-8")
    return value


# --- Message handler (child level) ---


class _MessageHandler:
    """Handler for message-level target states within a topic."""

    __slots__ = ("_client", "_topic", "_deletion_value_fn", "_producer", "_sink")

    _client: pulsar.Client
    _topic: str
    _deletion_value_fn: Callable[[bytes | str], bytes | str] | None
    _producer: pulsar.Producer | None
    _sink: coco.TargetActionSink[_MessageAction]

    def __init__(
        self,
        client: pulsar.Client,
        topic: str,
        deletion_value_fn: Callable[[bytes | str], bytes | str] | None,
    ) -> None:
        self._client = client
        self._topic = topic
        self._deletion_value_fn = deletion_value_fn
        self._producer = None
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    def _ensure_producer(self) -> pulsar.Producer:
        if self._producer is None:
            self._producer = self._client.create_producer(self._topic)
        return self._producer

    def _send(self, producer: pulsar.Producer, action: _MessageAction) -> None:
        producer.send(
            _as_payload(action.value), partition_key=_as_partition_key(action.key)
        )

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_MessageAction],
        /,
    ) -> None:
        if not actions:
            return
        # The pulsar-client producer is synchronous; send each message off the
        # event loop and await all sends to ensure delivery before returning.
        producer = self._ensure_producer()
        await asyncio.gather(
            *(asyncio.to_thread(self._send, producer, action) for action in actions)
        )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: bytes | str | coco.NonExistenceType,
        prev_possible_records: Collection[_MessageFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_MessageAction, _MessageFingerprint] | None:
        assert isinstance(key, (bytes, str))

        if coco.is_non_existence(desired_target_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            deletion_value: bytes | str | None = None
            if self._deletion_value_fn is not None:
                deletion_value = self._deletion_value_fn(key)
            return coco.TargetReconcileOutput(
                action=_MessageAction(key=key, value=deletion_value),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Upsert case
        if isinstance(desired_target_state, bytes):
            target_fp = fingerprint_bytes(desired_target_state)
        else:
            target_fp = fingerprint_str(desired_target_state)

        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_MessageAction(key=key, value=desired_target_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


# --- Topic handler (root level) ---


class _TopicHandler:
    """Handler for topic-level target states. Always returns output for generation tracking."""

    __slots__ = ("_sink",)

    _sink: coco.TargetActionSink[_TopicAction, _MessageHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TopicAction],
        /,
    ) -> list[coco.ChildTargetDef[_MessageHandler] | None]:
        outputs: list[coco.ChildTargetDef[_MessageHandler] | None] = []
        for action in actions:
            if coco.is_non_existence(action.spec):
                outputs.append(None)
            else:
                client = context_provider.get(action.key.client_key, pulsar.Client)
                handler = _MessageHandler(
                    client=client,
                    topic=action.key.topic,
                    deletion_value_fn=action.spec.deletion_value_fn,
                )
                outputs.append(coco.ChildTargetDef(handler=handler))
        return outputs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _TopicSpec | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_TopicAction, None, _MessageHandler]:
        topic_key = _TopicKey(*_TOPIC_KEY_CHECKER.check(key))

        tracking_record: None | coco.NonExistenceType
        if coco.is_non_existence(desired_target_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = None

        return coco.TargetReconcileOutput(
            action=_TopicAction(key=topic_key, spec=desired_target_state),
            sink=self._sink,
            tracking_record=tracking_record,
        )


# --- Root provider registration ---

_topic_provider = coco.register_root_target_states_provider(
    "cocoindex/pulsar/topic", _TopicHandler()
)

# --- Public API ---

DeletionValueFn = Callable[[bytes | str], bytes | str]


class PulsarTopicTarget(
    Generic[coco.MaybePendingS], coco.ResolvesTo["PulsarTopicTarget"]
):
    """
    A target for producing messages to a Pulsar topic.

    The topic is user-managed (CocoIndex does not create/drop topics).
    Messages are produced for upserts and deletes of declared target states.
    """

    _provider: coco.TargetStateProvider[
        bytes | str | coco.NonExistenceType, None, coco.MaybePendingS
    ]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            bytes | str | coco.NonExistenceType, None, coco.MaybePendingS
        ],
    ) -> None:
        self._provider = provider

    def declare_target_state(
        self: "PulsarTopicTarget", *, key: bytes | str, value: bytes | str
    ) -> None:
        """
        Declare a target state backed by a Pulsar message.

        On commit, CocoIndex will produce a message with the given key (as the
        Pulsar ``partition_key``) and value if the state has changed since the
        last run.

        Args:
            key: The message key (used as the stable identity and partition key).
            value: The message value.
        """
        coco.declare_target_state(self._provider.target_state(key, value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def pulsar_topic_target(
    client: ContextKey[pulsar.Client],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> coco.TargetState[_MessageHandler]:
    """
    Create a TargetState for a Pulsar topic target.

    Use with ``coco.mount_target()`` to mount and get a child provider,
    or with ``mount_pulsar_topic_target()`` for a convenience wrapper.

    Args:
        client: ContextKey for the ``pulsar.Client`` connection.
        topic: The Pulsar topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with an empty payload.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    key = _TopicKey(client_key=client.key, topic=topic)
    spec = _TopicSpec(deletion_value_fn=deletion_value_fn)
    return _topic_provider.target_state(key, spec)


@coco.fn
def declare_pulsar_topic_target(
    client: ContextKey[pulsar.Client],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> PulsarTopicTarget[coco.PendingS]:
    """
    Declare a Pulsar topic target and return a PulsarTopicTarget for declaring messages.

    Args:
        client: ContextKey for the ``pulsar.Client`` connection.
        topic: The Pulsar topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with an empty payload.

    Returns:
        A PulsarTopicTarget that can be used to declare target states.
    """
    provider = coco.declare_target_state_with_child(
        pulsar_topic_target(client, topic, deletion_value_fn=deletion_value_fn)
    )
    return PulsarTopicTarget(provider)


async def mount_pulsar_topic_target(
    client: ContextKey[pulsar.Client],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> PulsarTopicTarget[coco.ResolvedS]:
    """
    Mount a Pulsar topic target and return a ready-to-use PulsarTopicTarget.

    Sugar over ``pulsar_topic_target()`` + ``coco.mount_target()`` + wrapping.

    Args:
        client: ContextKey for the ``pulsar.Client`` connection.
        topic: The Pulsar topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with an empty payload.

    Returns:
        A PulsarTopicTarget that can be used to declare target states.
    """
    provider = await coco.mount_target(
        pulsar_topic_target(client, topic, deletion_value_fn=deletion_value_fn)
    )
    return PulsarTopicTarget(provider)


__all__ = [
    "DeletionValueFn",
    "PulsarTopicTarget",
    "declare_pulsar_topic_target",
    "pulsar_topic_target",
    "mount_pulsar_topic_target",
]
