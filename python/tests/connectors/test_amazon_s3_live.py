"""S3 event parsing, completion ownership, and real engine reconciliation."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote_plus

import pytest
from botocore.exceptions import ClientError

import cocoindex as coco
from cocoindex._internal.live_component import (
    LiveMapSubscriber,
    LiveStreamSubscriber,
    ReadyAwaitable,
)
from cocoindex.connectors import amazon_s3, localfs
from cocoindex.connectors.amazon_s3._live import _parse_events
from cocoindex.resources.file import PatternFilePathMatcher

from tests import common


def event(
    *keys: str, bucket: str = "bucket", action: str = "ObjectCreated:Put"
) -> bytes:
    return json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventVersion": "2.1",
                    "eventName": action,
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": quote_plus(key)},
                    },
                }
                for key in keys
            ]
        }
    ).encode()


@pytest.mark.parametrize("key", ["a+b.txt", "a b.txt", "é/猫.txt", "100%.txt", "%2F"])
def test_decode_s3_and_minio_keys_once(key: str) -> None:
    message = event(key)
    assert _parse_events(message) == [("bucket", key)]
    minio = json.loads(message)
    minio.update(EventName="s3:ObjectCreated:Put", Key=f"bucket/{key}")
    record = minio["Records"][0]
    record.update(
        eventSource="minio:s3", eventVersion="2.0", eventName="s3:ObjectCreated:Put"
    )
    assert _parse_events(json.dumps(minio).encode()) == [("bucket", key)]


@pytest.mark.parametrize(
    "message",
    [
        b"x",
        b"[]",
        b"{}",
        b'{"Records": []}',
        b'{"Records": [null]}',
        b'{"Event":"s3:TestEvent","Service":"other","Bucket":"b"}',
    ],
)
def test_invalid_envelopes_fail(message: bytes) -> None:
    with pytest.raises(ValueError, match="Invalid S3"):
        _parse_events(message)


@pytest.mark.parametrize(
    "field,value",
    [
        ("eventSource", "other:s3"),
        ("eventVersion", "3.0"),
        ("eventVersion", None),
        ("eventName", "ObjectCreated"),
    ],
)
def test_invalid_record_rejects_whole_batch(field: str, value: object) -> None:
    message = json.loads(event("good", "bad"))
    message["Records"][1][field] = value
    with pytest.raises(ValueError, match="Invalid S3"):
        _parse_events(json.dumps(message).encode())


@pytest.mark.parametrize("key", ["%", "%G0", "%FF", "x%2"])
def test_invalid_key_encoding_fails(key: str) -> None:
    message = json.loads(event("good"))
    message["Records"][0]["s3"]["object"]["key"] = key
    with pytest.raises(ValueError, match="Invalid S3"):
        _parse_events(json.dumps(message).encode())


def test_test_event_and_unrelated_action_are_not_object_changes() -> None:
    assert (
        _parse_events(b'{"Event":"s3:TestEvent","Service":"Amazon S3","Bucket":"b"}')
        == []
    )
    assert _parse_events(event("a", action="ObjectAccessed:Get")) == []


class _Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    async def __aenter__(self) -> _Body:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def read(self) -> bytes:
        return self.data


class _Client:
    """Deterministic provider with coarse mtimes, fault injection, and real file API."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.etags = True
        self.heads: list[str] = []
        self.error: int | None = None
        self.listing = asyncio.Event()
        self.mtime = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def metadata(self, key: str) -> dict[str, Any]:
        data = self.objects[key]
        result: dict[str, Any] = {
            "ContentLength": len(data),
            "LastModified": self.mtime,
        }
        if self.etags:
            result["ETag"] = f'"{data.hex()}"'
        return result

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.heads.append(Key)
        status = self.error or (404 if Key not in self.objects else None)
        if status:
            raise ClientError(
                {
                    "Error": {"Code": str(status)},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "HeadObject",
            )
        return self.metadata(Key)

    async def get_object(
        self, *, Bucket: str, Key: str, **kwargs: Any
    ) -> dict[str, Any]:
        return {"Body": _Body(self.objects[Key])}

    def get_paginator(self, name: str) -> _Client:
        assert name == "list_objects_v2"
        return self

    async def paginate(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        self.listing.set()
        yield {
            "Contents": [
                {"Key": key, "Size": len(data), **self.metadata(key)}
                for key, data in self.objects.items()
                if key.startswith(kwargs.get("Prefix", ""))
            ]
        }


class _Stream:
    def __init__(self) -> None:
        self.subscriber: LiveStreamSubscriber[bytes] | None = None
        self.active = asyncio.Event()
        self.stop = asyncio.Event()

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        self.subscriber = subscriber
        self.active.set()
        try:
            await subscriber.mark_ready()
            await self.stop.wait()
        finally:
            self.subscriber = None

    async def send(self, message: bytes) -> ReadyAwaitable:
        await self.active.wait()
        assert self.subscriber is not None
        return await self.subscriber.send(message)


class _Ready:
    def __init__(self, apply: Any) -> None:
        self.apply = apply

    async def ready(self) -> None:
        await self.apply()


class _Subscriber:
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.allow_scan = asyncio.Event()
        self.allow_scan.set()
        self.block = asyncio.Event()
        self.block.set()
        self.entered = asyncio.Event()
        self.values: dict[str, bytes] = {}
        self.history: list[tuple[str, bytes | None]] = []
        self.error: Exception | None = None

    async def update_all(self) -> None:
        await self.allow_scan.wait()

    async def mark_ready(self) -> None:
        self.ready.set()

    async def update(self, key: str, value: amazon_s3.S3File) -> _Ready:
        content = await value.read()

        async def apply() -> None:
            if key == "a.txt":
                self.entered.set()
                await self.block.wait()
            if self.error:
                raise self.error
            self.values[key] = content
            self.history.append((key, content))

        return _Ready(apply)

    async def delete(self, key: str) -> _Ready:
        async def apply() -> None:
            if self.error:
                raise self.error
            self.values.pop(key, None)
            self.history.append((key, None))

        return _Ready(apply)


@pytest.mark.asyncio
async def test_barrier_same_key_completion_disconnect_and_cross_key_progress() -> None:
    client, stream, sub = _Client(), _Stream(), _Subscriber()
    client.objects.update({"a.txt": b"first", "b.txt": b"other"})
    sub.allow_scan.clear()
    sub.block.clear()
    view = amazon_s3.list_objects(client, "bucket").live_items(stream)  # type: ignore[arg-type]
    watch = asyncio.create_task(
        view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))
    )
    try:
        await stream.active.wait()
        first_send = asyncio.create_task(stream.send(event("a.txt")))
        await asyncio.sleep(0)
        assert client.heads == []
        sub.allow_scan.set()
        first = await first_send
        await sub.entered.wait()
        client.objects["a.txt"] = b"second"
        second = await stream.send(event("a.txt"))
        other = await stream.send(event("b.txt"))
        await other.ready()
        assert sub.values == {"b.txt": b"other"}
        assert client.heads.count("a.txt") == 1
        waiter = asyncio.create_task(first.ready())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert client.heads.count("a.txt") == 1
        sub.block.set()
        await second.ready()
        assert [value for key, value in sub.history if key == "a.txt"] == [
            b"first",
            b"second",
        ]
        await first.ready()
        assert {key async for key, _ in view} == {"a.txt", "b.txt"}
        with pytest.raises(RuntimeError, match="single active"):
            await view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))
    finally:
        watch.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watch
    # Guard and view can be reused after cancellation.
    stream.stop.set()
    await view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))


@pytest.mark.asyncio
async def test_current_head_filters_failures_and_atomic_validation() -> None:
    client, stream, sub = _Client(), _Stream(), _Subscriber()
    client.objects.update({"prefix/a.txt": b"small", "prefix/b.bin": b"excluded"})
    view = amazon_s3.list_objects(
        client,
        "bucket",
        prefix="prefix/",
        max_file_size=5,  # type: ignore[arg-type]
        path_matcher=PatternFilePathMatcher(included_patterns=["*.txt"]),
    ).live_items(stream)
    watch = asyncio.create_task(
        view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))
    )
    try:
        await sub.ready.wait()
        # An out-of-order delete hint must reconcile the object that exists now.
        await (
            await stream.send(event("prefix/a.txt", action="ObjectRemoved:Delete"))
        ).ready()
        assert sub.values == {"a.txt": b"small"}
        client.objects["prefix/a.txt"] = b"now too large"
        await (await stream.send(event("prefix/a.txt"))).ready()
        assert sub.values == {}
        client.objects["prefix/a.txt"] = b"again"
        await (await stream.send(event("prefix/a.txt"))).ready()
        for status in (403, 429, 500, 503):
            client.error = status
            with pytest.raises(ClientError):
                await (await stream.send(event("prefix/a.txt"))).ready()
            assert sub.values == {"a.txt": b"again"}
        client.error = None
        del client.objects["prefix/a.txt"]
        await (await stream.send(event("prefix/a.txt"))).ready()
        assert sub.values == {}
        previous_heads = list(client.heads)
        await (
            await stream.send(event("elsewhere/a", "prefix/", bucket="other"))
        ).ready()
        assert client.heads == previous_heads
        bad = json.loads(event("prefix/b.bin"))
        bad["Records"].append({})
        previous_history = list(sub.history)
        with pytest.raises(ValueError):
            await stream.send(json.dumps(bad).encode())
        assert sub.history == previous_history
        sub.error = RuntimeError("target failed")
        with pytest.raises(RuntimeError, match="target failed"):
            await (await stream.send(event("prefix/b.bin"))).ready()
    finally:
        watch.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watch


@pytest.mark.asyncio
async def test_shutdown_cancels_owned_work_and_releases_key_gates() -> None:
    client, stream, sub = _Client(), _Stream(), _Subscriber()
    client.objects["a.txt"] = b"blocked"
    sub.block.clear()
    view = amazon_s3.list_objects(client, "bucket").live_items(stream)  # type: ignore[arg-type]
    watch = asyncio.create_task(
        view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))
    )
    await sub.ready.wait()
    handle = await stream.send(event("a.txt"))
    await sub.entered.wait()
    watch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await watch
    with pytest.raises(asyncio.CancelledError):
        await handle.ready()
    assert not sub.history
    sub.block.set()
    stream.stop.set()
    await view.watch(cast(LiveMapSubscriber[str, amazon_s3.S3File], sub))


@pytest.mark.asyncio
@pytest.mark.parametrize("etags", [True, False])
async def test_real_engine_equal_mtime_content_delete_and_restart(
    tmp_path: Path, etags: bool
) -> None:
    client, stream = _Client(), _Stream()
    client.etags = etags
    client.objects["a.txt"] = b"first"
    view = amazon_s3.list_objects(client, "bucket").live_items(stream)  # type: ignore[arg-type]

    errors: list[BaseException] = []

    def propagate(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        errors.append(exc)
        raise exc

    env = common.create_test_env(
        __file__, suffix=f"real-{etags}", exception_handler=propagate
    )
    processed: list[bytes] = []

    @coco.fn(memo=True)
    async def process(entry: amazon_s3.S3File) -> None:
        data = await entry.read()
        processed.append(data)
        localfs.declare_file(tmp_path / entry.file_path.path.name, data)

    @coco.fn
    async def root() -> None:
        await coco.mount_each(process, view)

    app = coco.App(coco.AppConfig(name=f"s3-live-{etags}", environment=env), root)
    task = asyncio.create_task(app.update(live=True).result())
    try:
        await stream.active.wait()
        await (await stream.send(event("a.txt"))).ready()
        assert (tmp_path / "a.txt").read_bytes() == b"first"
        assert processed == [b"first"]
        client.objects["a.txt"] = b"second"
        await (await stream.send(event("a.txt"))).ready()
        assert (tmp_path / "a.txt").read_bytes() == b"second"
        assert processed == [b"first", b"second"]
        await (await stream.send(event("a.txt"))).ready()
        assert processed == [b"first", b"second"]
        del client.objects["a.txt"]
        await (await stream.send(event("a.txt", action="ObjectRemoved:Delete"))).ready()
        assert not (tmp_path / "a.txt").exists()
    finally:
        stream.stop.set()
        await task
        assert not errors, [repr(exc) for exc in errors]
    # Events lost while a volatile HTTP receiver is down are covered by the next scan.
    client.objects["b.txt"] = b"offline write"
    stream.active.clear()
    stream.stop.clear()
    task = asyncio.create_task(app.update(live=True).result())
    try:
        await stream.active.wait()
        await (await stream.send(event("b.txt"))).ready()
        assert (tmp_path / "b.txt").read_bytes() == b"offline write"
        assert processed == [b"first", b"second", b"offline write"]
    finally:
        stream.stop.set()
        await task


@pytest.mark.asyncio
async def test_catch_up_drains_notification_received_during_initial_scan(
    tmp_path: Path,
) -> None:
    client, stream = _Client(), _Stream()
    client.objects["a.txt"] = b"before"
    view = amazon_s3.list_objects(client, "bucket").live_items(stream)  # type: ignore[arg-type]
    entered, release = asyncio.Event(), asyncio.Event()
    errors: list[BaseException] = []

    def propagate(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        errors.append(exc)
        raise exc

    env = common.create_test_env(
        __file__, suffix="catch-up", exception_handler=propagate
    )

    @coco.fn(memo=True)
    async def process(entry: amazon_s3.S3File) -> None:
        content = await entry.read()
        if content == b"before":
            entered.set()
            await release.wait()
        localfs.declare_file(tmp_path / "a.txt", content)

    @coco.fn
    async def root() -> None:
        await coco.mount_each(process, view)

    app = coco.App(coco.AppConfig(name="s3-catch-up", environment=env), root)
    task = asyncio.create_task(app.update().result())
    try:
        await entered.wait()
        client.objects["a.txt"] = b"during scan"
        notification = await stream.send(event("a.txt"))
        waiter = asyncio.create_task(notification.ready())
        await asyncio.sleep(0)
        assert not waiter.done()
        release.set()
        await waiter
        await task
        assert not errors, [repr(exc) for exc in errors]
        assert (tmp_path / "a.txt").read_bytes() == b"during scan"
    finally:
        release.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert not errors, [repr(exc) for exc in errors]
