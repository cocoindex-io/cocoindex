"""Reconcile current S3 state from AWS and MinIO notification envelopes."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from contextvars import copy_context
from dataclasses import dataclass, field
from pathlib import PurePath
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError

from cocoindex._internal.live_component import (
    _IMMEDIATE_READY,
    LiveMapSubscriber,
    LiveStream,
    ReadyAwaitable,
)
from cocoindex.connectorkits import SingleWatcherGuard

from ._source import S3File, S3FilePath, S3Walker


def _parse_events(message: bytes) -> list[tuple[str, str]]:
    """Validate the entire batch before returning decoded (bucket, key) hints."""
    try:
        envelope = json.loads(message)
        if not isinstance(envelope, dict):
            raise ValueError
        if envelope.get("Event") == "s3:TestEvent":
            if (
                envelope.get("Service") != "Amazon S3"
                or not isinstance(envelope.get("Bucket"), str)
                or not envelope["Bucket"]
                or "Records" in envelope
            ):
                raise ValueError
            return []
        records = envelope["Records"]
        if not isinstance(records, list) or not records:
            raise ValueError
        events: list[tuple[str, str]] = []
        for record in records:
            if not isinstance(record, dict):
                raise ValueError
            source = record["eventSource"]
            version = record["eventVersion"]
            name = record["eventName"]
            bucket = record["s3"]["bucket"]["name"]
            encoded_key = record["s3"]["object"]["key"]
            if (
                source not in ("aws:s3", "minio:s3")
                or not isinstance(version, str)
                or re.fullmatch(r"2\.[0-9]+", version) is None
                or not isinstance(name, str)
                or not name
                or not isinstance(bucket, str)
                or not bucket
                or not isinstance(encoded_key, str)
                or not encoded_key
                or re.search(r"%(?![0-9a-fA-F]{2})", encoded_key)
            ):
                raise ValueError
            key = unquote_plus(encoded_key, encoding="utf-8", errors="strict")
            name = name.removeprefix("s3:")
            family, separator, action = name.partition(":")
            if not separator or not action:
                raise ValueError
            if family in ("ObjectCreated", "ObjectRemoved"):
                events.append((bucket, key))
        return list(dict.fromkeys(events))
    except (KeyError, TypeError, ValueError, RecursionError) as exc:
        raise ValueError("Invalid S3 notification envelope") from exc


class _LiveS3Items:
    def __init__(self, walker: S3Walker, stream: LiveStream[bytes]) -> None:
        self._walker = walker
        self._stream = stream
        self._watch_guard = SingleWatcherGuard("S3 live view")

    async def __aiter__(self) -> AsyncIterator[tuple[str, S3File]]:
        # Each iteration is an independent, read-only listing. In particular,
        # the native engine's initial scan must not rely on caller Task identity
        # or a ContextVar being propagated across its Python/Rust boundary.
        async for item in self._walker.items():
            yield item

    async def watch(self, subscriber: LiveMapSubscriber[str, S3File]) -> None:
        with self._watch_guard:
            adapter = _S3Subscriber(self._walker, subscriber)
            stream_task = asyncio.create_task(self._stream.watch(adapter))

            async def bootstrap() -> None:
                started = asyncio.create_task(adapter.started.wait())
                try:
                    await asyncio.wait(
                        (started, stream_task), return_when=asyncio.FIRST_COMPLETED
                    )
                    if stream_task.done():
                        await stream_task
                        if not adapter.started.is_set():
                            raise RuntimeError(
                                "S3 event stream ended before subscribing"
                            )
                finally:
                    started.cancel()
                    await asyncio.gather(started, return_exceptions=True)
                await subscriber.update_all()
                # update_all waits for the full scan's commit. Opening the gate
                # here lets the stream finish its catch-up watermark, including
                # notifications delivered while the scan was still running.
                adapter.scan_ready.set()
                await adapter.watermark.wait()
                await adapter.drain()
                adapter.bootstrapped = True
                await subscriber.mark_ready()

            scan_task = asyncio.create_task(bootstrap())
            try:
                done, _ = await asyncio.wait(
                    (stream_task, scan_task), return_when=asyncio.FIRST_EXCEPTION
                )
                for task in done:
                    await task
                await adapter.drain()
            finally:
                scan_task.cancel()
                await adapter.close()
                stream_task.cancel()
                await asyncio.gather(scan_task, stream_task, return_exceptions=True)


@dataclass
class _KeyGate:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class _Completion:
    def __init__(self, task: asyncio.Task[None]) -> None:
        self._task = task

    async def ready(self) -> None:
        # A disconnected waiter must not release this key's gate while the
        # engine is still applying the operation. The watch owns the task.
        await asyncio.shield(self._task)


class _S3Subscriber:
    def __init__(
        self, walker: S3Walker, subscriber: LiveMapSubscriber[str, S3File]
    ) -> None:
        self._walker = walker
        self._subscriber = subscriber
        self.started = asyncio.Event()
        self.scan_ready = asyncio.Event()
        self.watermark = asyncio.Event()
        self.bootstrapped = False
        self._bootstrap_error: BaseException | None = None
        self._closed = False
        self._context = copy_context()
        self._operations: set[asyncio.Task[None]] = set()
        self._keys: dict[str, _KeyGate] = {}

    async def mark_ready(self) -> None:
        self.started.set()
        self.watermark.set()

    async def send(self, message: bytes) -> ReadyAwaitable:
        if self._closed:
            raise RuntimeError("S3 live view is no longer watching")
        self.started.set()
        events = _parse_events(message)
        if not events:
            return _IMMEDIATE_READY
        task = asyncio.create_task(self._apply(events), context=self._context.copy())
        self._operations.add(task)
        task.add_done_callback(self._finished)
        return _Completion(task)

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._operations.discard(task)
        if not task.cancelled():
            error = task.exception()
            if not self.bootstrapped and error is not None:
                self._bootstrap_error = error

    async def drain(self) -> None:
        if self._operations:
            await asyncio.gather(*self._operations)
        if self._bootstrap_error is not None:
            raise self._bootstrap_error

    async def close(self) -> None:
        self._closed = True
        self.scan_ready.set()
        pending = tuple(self._operations)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _apply(self, events: list[tuple[str, str]]) -> None:
        await self.scan_ready.wait()
        if self._closed:
            raise RuntimeError("S3 live view is no longer watching")
        for bucket, key in events:
            walker = self._walker
            if bucket != walker._bucket_name or not key.startswith(walker._prefix):
                continue
            relative = key[len(walker._prefix) :] if walker._prefix else key
            if walker._prefix:
                relative = relative.lstrip("/")
            if not relative or key.endswith("/"):
                continue
            path = PurePath(relative)
            stable_key = path.as_posix()
            gate = self._keys.setdefault(stable_key, _KeyGate())
            gate.users += 1
            try:
                async with gate.lock:
                    entry = S3File(
                        walker._client,
                        S3FilePath(bucket, relative, object_key=key),
                    )
                    included = walker._path_matcher.is_file_included(path)
                    if included:
                        try:
                            size = await entry.size()
                            included = (
                                walker._max_file_size is None
                                or size <= walker._max_file_size
                            )
                        except ClientError as exc:
                            status = exc.response.get("ResponseMetadata", {}).get(
                                "HTTPStatusCode"
                            )
                            if status != 404:
                                raise
                            included = False
                    if included:
                        handle = await self._subscriber.update(stable_key, entry)
                    else:
                        handle = await self._subscriber.delete(stable_key)
                    await handle.ready()
            finally:
                gate.users -= 1
                if not gate.users:
                    del self._keys[stable_key]
