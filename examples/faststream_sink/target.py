"""A flat custom target that publishes revisioned logical upserts and deletes."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, cast

import cocoindex as coco

from .protocol import Event, MessageKey, canonical_json, validate_key, validate_revision


class ConfirmedPublisher(Protocol):
    async def publish(
        self,
        message: bytes,
        topic: str,
        *,
        key: bytes,
        headers: dict[str, str],
        correlation_id: str,
        no_confirm: Literal[False],
    ) -> object: ...


@dataclass(frozen=True)
class KafkaDestination:
    """A publisher whose Kafka cluster identity was verified during startup."""

    publisher: ConfirmedPublisher
    cluster_id: str
    inflight: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(8), init=False, repr=False
    )


@dataclass(frozen=True)
class Record:
    revision: int
    value_json: bytes

    def __post_init__(self) -> None:
        validate_revision(self.revision)


@dataclass(frozen=True)
class TrackingRecord:
    revision: int
    fingerprint: str


async def _publish(
    context_provider: coco.ContextProvider, actions: Sequence[Event], /
) -> None:
    # Resolve and validate every original destination before any batch effect.
    destinations = []
    for event in actions:
        destination = context_provider.get(event.key.destination, KafkaDestination)
        if destination.cluster_id != event.key.cluster_id:
            raise ValueError(
                "The destination now identifies a different Kafka cluster. "
                "Keep the original provider available for cleanup and use a new "
                "ContextKey for the new destination."
            )
        destinations.append(destination)

    for event, destination in zip(actions, destinations, strict=True):
        # A timeout may follow an accepted broker write. Its stable event ID
        # and the consumer's revision fence make a later replay safe.
        async with destination.inflight, asyncio.timeout(15):
            await destination.publisher.publish(
                event.encode(),
                event.key.topic,
                key=canonical_json([event.key.destination, event.key.key]),
                headers={"cocoindex-event-id": event.event_id},
                correlation_id=event.event_id,
                no_confirm=False,
            )


_SINK = coco.TargetActionSink.from_async_fn(_publish)


class MessageHandler(coco.TargetHandler[Record, TrackingRecord, None]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: Record | coco.NonExistenceType,
        prev_possible_records: Collection[TrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[Event, TrackingRecord, None] | None:
        if (
            not isinstance(key, tuple)
            or len(key) != 4
            or not all(isinstance(v, str) for v in key)
        ):
            raise ValueError("Invalid persisted Kafka message key")
        message_key = MessageKey(*cast(tuple[str, str, str, str], key))
        validate_key(message_key)

        previous: dict[int, str] = {}
        for record in prev_possible_records:
            validate_revision(record.revision)
            if re.fullmatch(r"[0-9a-f]{64}", record.fingerprint) is None:
                raise ValueError("Invalid previous content fingerprint")
            old = previous.setdefault(record.revision, record.fingerprint)
            if old != record.fingerprint:
                raise ValueError("Previous records conflict at the same revision")

        if coco.is_non_existence(desired_target_state):
            if not previous:
                if prev_may_be_missing:
                    raise ValueError(
                        "Cannot order a deletion without a previous revision"
                    )
                return None
            return coco.TargetReconcileOutput(
                action=Event(message_key, max(previous), "delete", None),
                sink=_SINK,
                tracking_record=coco.NON_EXISTENCE,
            )

        revision = desired_target_state.revision
        validate_revision(revision)
        fingerprint = hashlib.sha256(desired_target_state.value_json).hexdigest()
        if previous and revision < max(previous):
            raise ValueError("A record revision must not decrease")
        if revision in previous and previous[revision] != fingerprint:
            raise ValueError("Changed content requires a new revision")
        if (
            previous
            and not prev_may_be_missing
            and all(
                old_revision == revision and old_fp == fingerprint
                for old_revision, old_fp in previous.items()
            )
        ):
            return None
        event = Event(message_key, revision, "upsert", desired_target_state.value_json)
        event.encode()  # Validate bounds before the engine commits an action.
        return coco.TargetReconcileOutput(
            action=event,
            sink=_SINK,
            tracking_record=TrackingRecord(revision, fingerprint),
        )


_PROVIDER = coco.register_root_target_states_provider(
    "example/faststream-kafka/revisioned-message-v1", MessageHandler()
)


@dataclass(frozen=True)
class KafkaTarget:
    destination: coco.ContextKey[KafkaDestination]
    cluster_id: str
    topic: str

    def declare(self, key: str, revision: int, value: object) -> None:
        message_key = MessageKey(self.destination.key, self.cluster_id, self.topic, key)
        validate_key(message_key)
        record = Record(revision, canonical_json(value))
        coco.declare_target_state(_PROVIDER.target_state(message_key, record))
