"""The example's versioned message format, independent of its publisher."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal, NamedTuple

Operation = Literal["upsert", "delete"]
MAX_MESSAGE_BYTES = 256 * 1024
_TOPIC = re.compile(r"[A-Za-z0-9._-]{1,249}")


class MessageKey(NamedTuple):
    destination: str
    cluster_id: str
    topic: str
    key: str


def validate_key(key: MessageKey) -> None:
    if any(not isinstance(part, str) or not part for part in key):
        raise ValueError(
            "Destination, cluster, topic, and record key must be nonempty strings"
        )
    if any(len(part.encode("utf-8")) > 1024 for part in key):
        raise ValueError("Message identity fields must be at most 1024 UTF-8 bytes")
    if key.topic in (".", "..") or _TOPIC.fullmatch(key.topic) is None:
        raise ValueError("Invalid Kafka topic name")


def validate_revision(revision: int) -> None:
    if type(revision) is not int or not 0 < revision < 2**63:
        raise ValueError("Revision must be a positive signed 64-bit integer")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object field")
        result[key] = value
    return result


def _identity(key: MessageKey, revision: int, operation: Operation) -> str:
    return hashlib.sha256(canonical_json([1, *key, revision, operation])).hexdigest()


@dataclass(frozen=True)
class Event:
    key: MessageKey
    revision: int
    operation: Operation
    value_json: bytes | None

    def __post_init__(self) -> None:
        validate_key(self.key)
        validate_revision(self.revision)
        if self.operation not in ("upsert", "delete"):
            raise ValueError("Unknown operation")
        if (self.operation == "delete") != (self.value_json is None):
            raise ValueError("Only deletes omit their value")
        if self.value_json is not None:
            value = json.loads(self.value_json, object_pairs_hook=_unique_object)
            if canonical_json(value) != self.value_json:
                raise ValueError("An event value must use canonical JSON")

    @property
    def order(self) -> tuple[int, int]:
        return self.revision, int(self.operation == "delete")

    @property
    def event_id(self) -> str:
        return _identity(self.key, self.revision, self.operation)

    @property
    def fingerprint(self) -> str | None:
        if self.value_json is None:
            return None
        return hashlib.sha256(self.value_json).hexdigest()

    def encode(self) -> bytes:
        message = canonical_json(
            {
                "schema": 1,
                "destination": self.key.destination,
                "cluster_id": self.key.cluster_id,
                "topic": self.key.topic,
                "key": self.key.key,
                "revision": self.revision,
                "operation": self.operation,
                "value": None
                if self.value_json is None
                else json.loads(self.value_json),
                "value_sha256": self.fingerprint,
                "event_id": self.event_id,
            }
        )
        if len(message) > MAX_MESSAGE_BYTES:
            raise ValueError("Encoded event exceeds the 256 KiB example limit")
        return message


def decode_event(message: bytes) -> Event:
    if len(message) > MAX_MESSAGE_BYTES:
        raise ValueError("Encoded event exceeds the 256 KiB example limit")
    fields = json.loads(message, object_pairs_hook=_unique_object)
    expected = {
        "schema",
        "destination",
        "cluster_id",
        "topic",
        "key",
        "revision",
        "operation",
        "value",
        "value_sha256",
        "event_id",
    }
    if not isinstance(fields, dict) or set(fields) != expected:
        raise ValueError("Invalid event envelope")
    if type(fields["schema"]) is not int or fields["schema"] != 1:
        raise ValueError("Unsupported event schema")
    operation = fields["operation"]
    if operation not in ("upsert", "delete"):
        raise ValueError("Unknown operation")
    if operation == "delete" and fields["value"] is not None:
        raise ValueError("A delete must not contain a value")
    event = Event(
        MessageKey(
            *(fields[name] for name in ("destination", "cluster_id", "topic", "key"))
        ),
        fields["revision"],
        operation,
        canonical_json(fields["value"]) if operation == "upsert" else None,
    )
    if (
        fields["event_id"] != event.event_id
        or fields["value_sha256"] != event.fingerprint
    ):
        raise ValueError(
            "Event identity or value fingerprint does not match its content"
        )
    return event
