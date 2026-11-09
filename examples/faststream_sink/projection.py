"""A durable, revision-fenced example consumer projection."""

from __future__ import annotations

import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Literal

from .protocol import Event, MessageKey, canonical_json


class Projection:
    """Store the value and its revision fence in the same SQLite transaction."""

    def __init__(self, path: Path, route: tuple[str, str, str]) -> None:
        self.route = route
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        try:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            with self._db:
                self._db.execute("BEGIN IMMEDIATE")
                self._db.execute(
                    "CREATE TABLE IF NOT EXISTS metadata "
                    "(name TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                self._db.execute(
                    "CREATE TABLE IF NOT EXISTS records ("
                    "record_key TEXT PRIMARY KEY, revision INTEGER NOT NULL, "
                    "phase INTEGER NOT NULL, value_json BLOB, "
                    "fingerprint TEXT, event_id TEXT NOT NULL)"
                )
                metadata = dict(self._db.execute("SELECT name, value FROM metadata"))
                identity = canonical_json([1, *route]).decode()
                if not metadata:
                    if self._db.execute("SELECT 1 FROM records LIMIT 1").fetchone():
                        raise ValueError(
                            "Projection metadata is missing; rebuild the projection"
                        )
                    self.group_id = "cocoindex-faststream-" + uuid.uuid4().hex
                    self._db.executemany(
                        "INSERT INTO metadata VALUES (?, ?)",
                        [("route", identity), ("consumer_group", self.group_id)],
                    )
                else:
                    if (
                        set(metadata) != {"route", "consumer_group"}
                        or metadata["route"] != identity
                    ):
                        raise ValueError(
                            "This projection belongs to a different destination"
                        )
                    self.group_id = metadata["consumer_group"]
                    if not self.group_id:
                        raise ValueError("Projection consumer group is missing")
        except BaseException:
            self._db.close()
            raise

    def apply(self, event: Event) -> Literal["applied", "duplicate", "stale"]:
        if event.key[:3] != self.route:
            raise ValueError("Event belongs to a different destination")
        with self._lock, self._db:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute(
                "SELECT revision, phase, value_json, fingerprint, event_id "
                "FROM records WHERE record_key = ?",
                (event.key.key,),
            ).fetchone()
            if row is not None:
                revision, phase, value_json, fingerprint, event_id = row
                if phase not in (0, 1):
                    raise ValueError("Projection has an invalid operation")
                previous = Event(
                    MessageKey(*self.route, event.key.key),
                    revision,
                    "delete" if phase else "upsert",
                    value_json,
                )
                if previous.fingerprint != fingerprint or previous.event_id != event_id:
                    raise ValueError(
                        "Projection content is inconsistent; rebuild it before replay"
                    )
                if event.order < previous.order:
                    return "stale"
                if event.order == previous.order:
                    if event.fingerprint != previous.fingerprint:
                        raise ValueError("Conflicting content at the same revision")
                    return "duplicate"
            self._db.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(record_key) DO UPDATE SET "
                "revision=excluded.revision, phase=excluded.phase, "
                "value_json=excluded.value_json, fingerprint=excluded.fingerprint, "
                "event_id=excluded.event_id",
                (
                    event.key.key,
                    *event.order,
                    event.value_json,
                    event.fingerprint,
                    event.event_id,
                ),
            )
            return "applied"

    def close(self) -> None:
        with self._lock:
            self._db.close()
