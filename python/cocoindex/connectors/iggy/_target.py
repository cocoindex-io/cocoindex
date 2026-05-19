"""
Iggy target for CocoIndex.

This module provides a two-level target state system for Iggy:
1. Topic level: Lightweight container for generation tracking (user-managed stream/topic)
2. Message level: Sends messages to the topic for upserts/deletes
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Collection, Generic, NamedTuple, Sequence

try:
    from apache_iggy import IggyClient  # type: ignore[import-not-found]
    from apache_iggy import SendMessage as Message  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "apache-iggy is required to use the Iggy connector. "
        "Please install cocoindex[iggy]."
    ) from e

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex._internal.target_state import AsyncTargetActionSinkFn
from cocoindex.connectorkits.fingerprint import fingerprint_bytes, fingerprint_str

# --- Type aliases ---

_MessageFingerprint = bytes

# --- Internal types ---


class _TopicKey(NamedTuple):
    client_key: str
    stream: str
    topic: str
    partition: int


_TOPIC_KEY_CHECKER = TypeChecker(tuple[str, str, str, int])


@dataclass
class _TopicSpec:
    deletion_value_fn: Callable[[bytes | str], bytes | str] | None


class _TopicAction(NamedTuple):
    key: _TopicKey
    spec: _TopicSpec | coco.NonExistenceType


class _MessageAction(NamedTuple):
    key: bytes | str
    value: bytes | str


# --- Message handler (child level) ---


class _MessageHandler(coco.TargetHandler[bytes | str, _MessageFingerprint, None]):
    """Handler for message-level target states within a topic."""

    __slots__ = (
        "_client",
        "_topic",
        "_stream",
        "_deletion_value_fn",
        "_sink",
        "_partition",
    )

    _client: IggyClient
    _topic: str
    _stream: str
    _partition: int
    _deletion_value_fn: Callable[[bytes | str], bytes | str] | None
    _sink: coco.TargetActionSink[_MessageAction, None]

    def __init__(
        self,
        client: IggyClient,
        topic: str,
        stream: str,
        partition: int,
        deletion_value_fn: Callable[[bytes | str], bytes | str] | None,
    ) -> None:
        self._client = client
        self._topic = topic
        self._stream = stream
        self._partition = partition
        self._deletion_value_fn = deletion_value_fn
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_MessageAction],
        /,
    ) -> None:
        if not actions:
            return
        messages = [Message(action.value) for action in actions]
        await self._client.send_messages(
            stream=self._stream,
            topic=self._topic,
            partitioning=self._partition,
            messages=messages,
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
            if self._deletion_value_fn is None:
                raise ValueError(
                    "Iggy does not support Kafka-style tombstones. "
                    "Provide deletion_value_fn or encode deletes in the payload."
                )
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


class _TopicHandler(coco.TargetHandler[_TopicSpec, None, _MessageHandler]):
    """Handler for topic-level target states. Always returns output for generation tracking."""

    __slots__ = ("_sink",)

    _sink: coco.TargetActionSink[_TopicAction, _MessageHandler]

    def __init__(self) -> None:
        sink_fn: AsyncTargetActionSinkFn[_TopicAction, _MessageHandler] = (
            self._apply_actions
        )
        self._sink = coco.TargetActionSink.from_async_fn(sink_fn)

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
                client = context_provider.get(action.key.client_key, IggyClient)
                handler = _MessageHandler(
                    client=client,
                    topic=action.key.topic,
                    stream=action.key.stream,
                    partition=action.key.partition,
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
    "cocoindex/iggy/topic", _TopicHandler()
)

# --- Public API ---

DeletionValueFn = Callable[[bytes | str], bytes | str]


class IggyTopicTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["IggyTopicTarget"]):
    """
    A target for sending messages to an Iggy stream/topic.

    The stream and topic are user-managed (CocoIndex does not create/drop them).
    Messages are sent for upserts and deletes of declared target states.
    """

    _provider: coco.TargetStateProvider[bytes | str, None, coco.MaybePendingS]

    def __init__(
        self,
        provider: coco.TargetStateProvider[bytes | str, None, coco.MaybePendingS],
    ) -> None:
        self._provider = provider

    def declare_target_state(
        self: "IggyTopicTarget", *, key: bytes | str, value: bytes | str
    ) -> None:
        """
        Declare a target state backed by an Iggy message.

        On commit, CocoIndex will send a message with the given value
        if the state has changed since the last run.

        Args:
            key: The stable identity used by CocoIndex.
            value: The message payload.
        """
        coco.declare_target_state(self._provider.target_state(key, value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def iggy_topic_target(
    client: ContextKey[IggyClient],
    stream: str,
    topic: str,
    *,
    partition: int = 0,
    deletion_value_fn: DeletionValueFn | None = None,
) -> coco.TargetState[_MessageHandler]:
    """
    Create a TargetState for an Iggy stream/topic target.

    Use with ``coco.mount_target()`` to mount and get a child provider,
    or with ``mount_iggy_topic_target()`` for a convenience wrapper.

    Args:
        client: ContextKey for the IggyClient connection.
        stream: The Iggy stream name or id.
        topic: The Iggy topic name or id.
        partition: The Iggy partition to send messages to.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            Required if declared target states are deleted.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    key = _TopicKey(
        client_key=client.key, stream=stream, topic=topic, partition=partition
    )
    spec = _TopicSpec(deletion_value_fn=deletion_value_fn)
    return _topic_provider.target_state(key, spec)


@coco.fn
def declare_iggy_topic_target(
    client: ContextKey[IggyClient],
    stream: str,
    topic: str,
    *,
    partition: int = 0,
    deletion_value_fn: DeletionValueFn | None = None,
) -> IggyTopicTarget[coco.PendingS]:
    """
    Declare an Iggy topic target and return an IggyTopicTarget for declaring messages.

    Args:
        client: ContextKey for the IggyClient connection.
        stream: The Iggy stream name or id.
        topic: The Iggy topic name or id.
        partition: The Iggy partition to send messages to.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            Required if declared target states are deleted.

    Returns:
        An IggyTopicTarget that can be used to declare target states.
    """
    provider = coco.declare_target_state_with_child(
        iggy_topic_target(
            client,
            stream,
            topic,
            partition=partition,
            deletion_value_fn=deletion_value_fn,
        )
    )
    return IggyTopicTarget(provider)


async def mount_iggy_topic_target(
    client: ContextKey[IggyClient],
    stream: str,
    topic: str,
    *,
    partition: int = 0,
    deletion_value_fn: DeletionValueFn | None = None,
) -> IggyTopicTarget[coco.ResolvedS]:
    """
    Mount an Iggy topic target and return a ready-to-use IggyTopicTarget.

    Sugar over ``iggy_topic_target()`` + ``coco.mount_target()`` + wrapping.

    Args:
        client: ContextKey for the IggyClient connection.
        stream: The Iggy stream name or id.
        topic: The Iggy topic name or id.
        partition: The Iggy partition to send messages to.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            Required if declared target states are deleted.

    Returns:
        An IggyTopicTarget that can be used to declare target states.
    """
    provider = await coco.mount_target(
        iggy_topic_target(
            client=client,
            stream=stream,
            topic=topic,
            partition=partition,
            deletion_value_fn=deletion_value_fn,
        )
    )
    return IggyTopicTarget(provider)


__all__ = [
    "DeletionValueFn",
    "IggyTopicTarget",
    "declare_iggy_topic_target",
    "iggy_topic_target",
    "mount_iggy_topic_target",
]
