"""Apache Pulsar target for CocoIndex."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Generic, NamedTuple, cast

import msgspec as _msgspec

try:
    import pulsar  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "pulsar-client is required to use the Pulsar connector. "
        "Please install cocoindex[pulsar]."
    ) from e

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex.connectorkits.fingerprint import fingerprint_bytes

MessageFingerprint = bytes


class _TopicKey(NamedTuple):
    client_key: str
    topic: str


class _MessageKey(NamedTuple):
    client_key: str
    topic: str
    message_key: str


_TOPIC_KEY_CHECKER = TypeChecker(tuple[str, str])
_MESSAGE_KEY_CHECKER = TypeChecker(tuple[str, str, str])


def _message_key_from_stable_key(key: coco.StableKey) -> _MessageKey:
    return _MessageKey(*_MESSAGE_KEY_CHECKER.check(key))


@dataclass
class _TopicSpec:
    deletion_value_fn: Callable[[str], bytes | str] | None


@dataclass
class _MessageSpec:
    value: bytes | str
    deletion_value_fn: Callable[[str], bytes | str] | None


class _MessageAction(NamedTuple):
    key: _MessageKey
    value: bytes | None
    publish: bool


class _MessageTrackingRecord(_msgspec.Struct, frozen=True):
    value_fingerprint: MessageFingerprint
    deletion_payload: bytes | None


class _TopicHandle(coco.TargetHandler[_MessageSpec, _MessageTrackingRecord, None]):
    """Handle-only marker child handler for the topic declaration."""

    def __init__(
        self, client_key: str, topic: str, deletion_value_fn: DeletionValueFn | None
    ):
        self._client_key = client_key
        self._topic = topic
        self._deletion_value_fn = deletion_value_fn

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _MessageSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_MessageTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_MessageAction, _MessageTrackingRecord] | None:
        raise RuntimeError(
            "Declaring message states directly through the Pulsar topic child provider "
            "is not supported. Use declare_target_state() on the "
            "PulsarTopicTarget returned by declare_pulsar_topic_target/mount_pulsar_topic_target."
        )


class _TopicAction(NamedTuple):
    key: _TopicKey
    spec: _TopicSpec | coco.NonExistenceType


def _payload_bytes(value: bytes | str) -> bytes:
    if isinstance(value, str):
        return value.encode()
    return value


def _to_wire_bytes(value: bytes | str | None, *, field_name: str) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, (bytes, str)):
        raise TypeError(
            f"Expected bytes or str for {field_name}, got {type(value).__name__}"
        )
    return _payload_bytes(value)


def _safe_apply_callback(
    callback: Callable[[str], bytes | str] | None,
    key: str,
    *,
    field_name: str,
) -> bytes | None:
    if callback is None:
        return None
    payload = callback(key)
    if not isinstance(payload, (bytes, str)):
        raise TypeError(
            f"{field_name} must return bytes or str for key {key!r}, got "
            f"{type(payload).__name__}"
        )
    return _payload_bytes(payload)


def _send_message_groups(
    groups: Sequence[tuple[pulsar.Client, str, Sequence[_MessageAction]]],
) -> None:
    for client, topic, actions in groups:
        producer = client.create_producer(topic)
        try:
            for action in actions:
                producer.send(
                    action.value,
                    partition_key=action.key.message_key,
                )
        except Exception as send_error:
            try:
                producer.close()
            except Exception as close_error:  # noqa: BLE001
                send_error.add_note(
                    f"Additionally failed to close Pulsar producer: {close_error!r}"
                )
            raise
        producer.close()


async def _send_message_groups_off_loop(
    groups: Sequence[tuple[pulsar.Client, str, Sequence[_MessageAction]]],
) -> None:
    """Run synchronous publication without orphaning it on cancellation."""
    worker = asyncio.create_task(asyncio.to_thread(_send_message_groups, groups))
    current_task = asyncio.current_task()
    cancellation: asyncio.CancelledError | None = None

    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as error:
            if current_task is None or current_task.cancelling() == 0:
                raise
            if cancellation is None:
                cancellation = error
        except BaseException:
            if cancellation is None:
                raise
            break

    if cancellation is not None:
        try:
            worker.result()
        except Exception as worker_error:  # noqa: BLE001
            cancellation.add_note(
                f"Pulsar publication also failed during cancellation: {worker_error!r}"
            )
        raise cancellation

    worker.result()


class _MessageHandler(
    coco.TargetHandler[_MessageSpec | coco.NonExistenceType, _MessageTrackingRecord]
):
    """Handler for flat message state in Pulsar."""

    __slots__ = ("_sink",)

    _sink: coco.TargetActionSink[_MessageAction]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_MessageAction],
        /,
    ) -> None:
        if not actions:
            return

        publish_actions = [action for action in actions if action.publish]
        if not publish_actions:
            return

        grouped_actions: dict[tuple[str, str], list[_MessageAction]] = {}
        for action in publish_actions:
            action_key = (action.key.client_key, action.key.topic)
            grouped_actions.setdefault(action_key, []).append(action)

        resolved_groups = [
            (
                cast(pulsar.Client, context_provider.get(client_key)),
                topic,
                grouped,
            )
            for (client_key, topic), grouped in grouped_actions.items()
        ]
        await _send_message_groups_off_loop(resolved_groups)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _MessageSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_MessageTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_MessageAction, _MessageTrackingRecord] | None:
        message_key = _message_key_from_stable_key(key)

        if coco.is_non_existence(desired_target_state):
            if not prev_possible_records:
                if not prev_may_be_missing:
                    return None
                return coco.TargetReconcileOutput(
                    action=_MessageAction(
                        key=message_key,
                        value=None,
                        publish=True,
                    ),
                    sink=self._sink,
                    tracking_record=coco.NON_EXISTENCE,
                )

            deletion_values = {
                record.deletion_payload for record in prev_possible_records
            }
            if len(deletion_values) > 1:
                raise ValueError(
                    "Conflicting prior deletion payloads for Pulsar message "
                    f"{message_key.message_key!r}"
                )

            desired_deletion_payload = next(iter(deletion_values))
            return coco.TargetReconcileOutput(
                action=_MessageAction(
                    key=message_key,
                    value=desired_deletion_payload,
                    publish=True,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        if not isinstance(desired_target_state, _MessageSpec):
            raise TypeError(
                "Pulsar message target values must be declared through PulsarTopicTarget"
            )

        desired_payload = _to_wire_bytes(
            desired_target_state.value,
            field_name="desired message value",
        )
        assert desired_payload is not None
        desired_deletion_payload = _safe_apply_callback(
            desired_target_state.deletion_value_fn,
            message_key.message_key,
            field_name="deletion_value_fn result",
        )

        desired_fingerprint = fingerprint_bytes(desired_payload)

        if not prev_possible_records:
            return coco.TargetReconcileOutput(
                action=_MessageAction(
                    key=message_key,
                    value=desired_payload,
                    publish=True,
                ),
                sink=self._sink,
                tracking_record=_MessageTrackingRecord(
                    value_fingerprint=desired_fingerprint,
                    deletion_payload=desired_deletion_payload,
                ),
            )

        if prev_may_be_missing:
            return coco.TargetReconcileOutput(
                action=_MessageAction(
                    key=message_key,
                    value=desired_payload,
                    publish=True,
                ),
                sink=self._sink,
                tracking_record=_MessageTrackingRecord(
                    value_fingerprint=desired_fingerprint,
                    deletion_payload=desired_deletion_payload,
                ),
            )

        all_value_fingerprints = {
            record.value_fingerprint for record in prev_possible_records
        }
        if (
            len(all_value_fingerprints) == 1
            and next(iter(all_value_fingerprints)) == desired_fingerprint
        ):
            desired_tracking = _MessageTrackingRecord(
                value_fingerprint=desired_fingerprint,
                deletion_payload=desired_deletion_payload,
            )
            if all(
                record.deletion_payload == desired_deletion_payload
                for record in prev_possible_records
            ):
                return None
            return coco.TargetReconcileOutput(
                action=_MessageAction(
                    key=message_key,
                    value=None,
                    publish=False,
                ),
                sink=self._sink,
                tracking_record=desired_tracking,
            )

        return coco.TargetReconcileOutput(
            action=_MessageAction(
                key=message_key,
                value=desired_payload,
                publish=True,
            ),
            sink=self._sink,
            tracking_record=_MessageTrackingRecord(
                value_fingerprint=desired_fingerprint,
                deletion_payload=desired_deletion_payload,
            ),
        )


class _TopicHandler(coco.TargetHandler[_TopicSpec, None, _TopicHandle]):
    """Handle-only topic container: no Pulsar I/O for messages."""

    __slots__ = ("_sink",)

    _sink: coco.TargetActionSink[_TopicAction]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn_with_children(
            self._apply_actions
        )

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TopicAction],
        child_slots: Mapping[int, coco.ChildSlot[_TopicHandle]],
        /,
    ) -> None:
        for i, action in enumerate(actions):
            if coco.is_non_existence(action.spec):
                continue
            child_slots[i].fulfill(
                _TopicHandle(
                    client_key=action.key.client_key,
                    topic=action.key.topic,
                    deletion_value_fn=action.spec.deletion_value_fn,
                )
            )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _TopicSpec | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_TopicAction, None, _TopicHandle]:
        topic_key = _TopicKey(*_TOPIC_KEY_CHECKER.check(key))
        _ = prev_possible_records
        _ = prev_may_be_missing
        tracking: None | coco.NonExistenceType
        if coco.is_non_existence(desired_target_state):
            tracking = coco.NON_EXISTENCE
        else:
            tracking = None
        return coco.TargetReconcileOutput(
            action=_TopicAction(key=topic_key, spec=desired_target_state),
            sink=self._sink,
            tracking_record=tracking,
        )


_topic_provider = coco.register_root_target_states_provider(
    "cocoindex/pulsar/topic", _TopicHandler()
)
_message_provider = coco.register_root_target_states_provider(
    "cocoindex/pulsar/message", _MessageHandler()
)

DeletionValueFn = Callable[[str], bytes | str]


class PulsarTopicTarget(
    coco.ResolvesTo["PulsarTopicTarget"], Generic[coco.MaybePendingS]
):
    """A target for producing messages to a user-managed Pulsar topic."""

    _provider: coco.TargetStateProvider[_MessageSpec, None, coco.MaybePendingS]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_MessageSpec, None, coco.MaybePendingS],
        *,
        client_key: str,
        topic: str,
        deletion_value_fn: DeletionValueFn | None,
    ) -> None:
        self._provider = provider
        self._client_key = client_key
        self._topic = topic
        self._deletion_value_fn = deletion_value_fn

    def declare_target_state(
        self: PulsarTopicTarget, *, key: str, value: bytes | str
    ) -> None:
        message_key = _MessageKey(
            client_key=self._client_key,
            topic=self._topic,
            message_key=key,
        )
        message_spec = _MessageSpec(
            value=value,
            deletion_value_fn=self._deletion_value_fn,
        )
        coco.declare_target_state(
            _message_provider.target_state(message_key, message_spec)
        )

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def pulsar_topic_target(
    client: ContextKey[pulsar.Client],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> coco.TargetState[_TopicHandle]:
    """Create a topic target state for a user-managed Pulsar topic."""
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
    """Declare a Pulsar topic target for use inside a component."""
    provider = coco.declare_target_state_with_child(
        pulsar_topic_target(client, topic, deletion_value_fn=deletion_value_fn)
    )
    return PulsarTopicTarget(
        provider=provider,
        client_key=client.key,
        topic=topic,
        deletion_value_fn=deletion_value_fn,
    )


async def mount_pulsar_topic_target(
    client: ContextKey[pulsar.Client],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> PulsarTopicTarget[coco.ResolvedS]:
    """Mount a Pulsar topic target and return its resolved wrapper."""
    provider = await coco.mount_target(
        pulsar_topic_target(client, topic, deletion_value_fn=deletion_value_fn)
    )
    return PulsarTopicTarget(
        provider=provider,
        client_key=client.key,
        topic=topic,
        deletion_value_fn=deletion_value_fn,
    )


__all__ = [
    "DeletionValueFn",
    "PulsarTopicTarget",
    "declare_pulsar_topic_target",
    "mount_pulsar_topic_target",
    "pulsar_topic_target",
]
