"""Exercise the example's ASGI boundary without an HTTP client buffering bodies."""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import hashlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

import cocoindex as coco
from tests.connectors.test_amazon_s3_live import event, _Client

_path = Path(__file__).parents[3] / "examples/minio_webhook/main.py"
_spec = importlib.util.spec_from_file_location("minio_webhook_example", _path)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
WebhookStream = _module.WebhookStream


class _Subscriber:
    def __init__(self) -> None:
        self.messages: list[bytes] = []
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self.error = False

    async def mark_ready(self) -> None:
        pass

    async def send(self, message: bytes) -> _Subscriber:
        self.messages.append(message)
        self.entered.set()
        return self

    async def ready(self) -> None:
        await self.gate.wait()
        if self.error:
            raise RuntimeError("target did not commit")


async def request(
    app: Any,
    body: bytes,
    *,
    token: bytes = b"Bearer fixture-token",
    read: list[bool] | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> int:
    responses: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        if read is not None:
            read.append(True)
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        responses.append(message)

    await app(
        {
            "type": "http",
            "path": "/events",
            "method": "POST",
            "headers": [
                (b"authorization", token),
                (b"content-type", b"application/json"),
                *(extra_headers or []),
            ],
        },
        receive,
        send,
    )
    return int(responses[0]["status"])


@pytest.mark.asyncio
async def test_no_ack_or_capacity_release_until_actual_completion_after_disconnect() -> (
    None
):
    stream, sub = WebhookStream("fixture-token", max_pending=1), _Subscriber()
    watcher = asyncio.create_task(stream.watch(sub))
    await stream.active.wait()
    first = asyncio.create_task(request(stream, event("a.txt")))
    try:
        await sub.entered.wait()
        assert not first.done()
        read: list[bool] = []
        assert await request(stream, event("b.txt"), read=read) == 429
        assert read == []
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await request(stream, event("b.txt"), read=read) == 429
        assert read == []
        sub.gate.set()
        await asyncio.gather(*stream._tasks)
        assert await request(stream, event("b.txt")) == 204
        sub.error = True
        assert await request(stream, event("b.txt")) == 503
    finally:
        sub.gate.set()
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher
    assert stream._pending == 0
    assert await request(stream, event("c.txt")) == 503


@pytest.mark.asyncio
async def test_auth_body_record_limits_reject_before_dispatch() -> None:
    stream, sub = (
        WebhookStream("fixture-token", max_body_bytes=1000, max_records=1),
        _Subscriber(),
    )
    sub.gate.set()
    watcher = asyncio.create_task(stream.watch(sub))
    await stream.active.wait()
    try:
        read: list[bool] = []
        assert (
            await request(stream, event("a"), token=b"Bearer wrong", read=read) == 403
        )
        assert read == []
        assert (
            await request(
                stream, b"x", read=read, extra_headers=[(b"content-length", b"1001")]
            )
            == 413
        )
        assert read == []
        assert await request(stream, b"x" * 1001) == 413
        assert await request(stream, event("a", "b")) == 413
        assert await request(stream, b"not JSON") == 400
        assert sub.messages == [] and stream._pending == 0
        assert await request(stream, event("a")) == 204
        assert len(sub.messages) == 1
    finally:
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher


def use_client(monkeypatch: pytest.MonkeyPatch, client: _Client) -> None:
    class Session:
        @contextlib.asynccontextmanager
        async def create_client(self, name: str) -> AsyncIterator[_Client]:
            assert name == "s3"
            yield client

    monkeypatch.setattr(_module.aiobotocore.session, "get_session", Session)


async def start_application(
    app: Any,
) -> tuple[asyncio.Task[None], asyncio.Queue[Any], asyncio.Queue[Any]]:
    receive: asyncio.Queue[Any] = asyncio.Queue()
    send: asyncio.Queue[Any] = asyncio.Queue()
    await receive.put({"type": "lifespan.startup"})
    task = asyncio.create_task(app({"type": "lifespan"}, receive.get, send.put))
    assert (await send.get())["type"] == "lifespan.startup.complete"
    return task, receive, send


async def stop_application(
    task: asyncio.Task[None], receive: asyncio.Queue[Any], send: asyncio.Queue[Any]
) -> None:
    await receive.put({"type": "lifespan.shutdown"})
    assert (await send.get())["type"] == "lifespan.shutdown.complete"
    await task


@pytest.mark.asyncio
async def test_example_lifespan_real_engine_and_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _Client()
    use_client(monkeypatch, client)
    key = "space + unicode/猫.txt"
    client.objects[key] = b"first"
    output, state = tmp_path / "output", tmp_path / "state"
    path = output / (hashlib.sha256(f"bucket\0{key}".encode()).hexdigest() + ".json")
    app = _module.MinioApplication(
        token="fixture-token", bucket="bucket", output=output, state=state
    )
    task, receive, send = await start_application(app)
    try:
        assert await request(app, event(key)) == 204
        assert json.loads(path.read_text()) == {
            "bucket": "bucket",
            "key": key,
            "size": 5,
            "sha256": hashlib.sha256(b"first").hexdigest(),
        }
        client.objects[key] = b"second"
        assert await request(app, event(key)) == 204
        assert (
            json.loads(path.read_text())["sha256"]
            == hashlib.sha256(b"second").hexdigest()
        )
        client.error = 503
        assert await request(app, event(key)) == 503
        assert path.exists()
        client.error = None
        del client.objects[key]
        assert await request(app, event(key, action="ObjectRemoved:Delete")) == 204
        assert not path.exists()
    finally:
        await stop_application(task, receive, send)
    assert app.stream._pending == 0 and not app.stream.accepting
    assert await request(app, event(key)) == 503
    client.objects[key] = b"written offline"
    app = _module.MinioApplication(
        token="fixture-token", bucket="bucket", output=output, state=state
    )
    task, receive, send = await start_application(app)
    try:
        assert await request(app, event(key)) == 204
        assert (
            json.loads(path.read_text())["sha256"]
            == hashlib.sha256(b"written offline").hexdigest()
        )
    finally:
        await stop_application(task, receive, send)


@pytest.mark.asyncio
async def test_example_target_failure_cannot_acknowledge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _Client()
    use_client(monkeypatch, client)

    @coco.fn(memo=True)
    async def failed_target(entry: Any, output: Path) -> None:
        raise ValueError("target write failed")

    monkeypatch.setattr(_module, "index_file", failed_target)
    app = _module.MinioApplication(
        token="fixture-token", bucket="bucket", state=tmp_path / "state"
    )
    task, receive, send = await start_application(app)
    try:
        # Let the empty initial scan commit before introducing a failing event.
        assert await request(app, event("absent")) == 204
        client.objects["a.txt"] = b"payload"
        assert await request(app, event("a.txt")) == 503
    finally:
        await stop_application(task, receive, send)


@pytest.mark.asyncio
async def test_example_shutdown_cancels_inflight_target_before_releasing_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _Client()
    use_client(monkeypatch, client)
    entered, canceled = asyncio.Event(), asyncio.Event()

    @coco.fn(memo=True)
    async def blocked_target(entry: Any, output: Path) -> None:
        entered.set()
        try:
            await asyncio.Future[None]()
        finally:
            canceled.set()

    monkeypatch.setattr(_module, "index_file", blocked_target)
    state = tmp_path / "state"
    app = _module.MinioApplication(token="fixture-token", bucket="bucket", state=state)
    task, receive, send = await start_application(app)
    assert await request(app, event("absent")) == 204
    client.objects["a.txt"] = b"payload"
    delivery = asyncio.create_task(request(app, event("a.txt")))
    await entered.wait()
    assert not delivery.done()
    await stop_application(task, receive, send)
    with pytest.raises(asyncio.CancelledError):
        await delivery
    assert canceled.is_set()
    assert not app.stream.accepting and app.stream._pending == 0
    reopened = coco.Environment(coco.Settings.from_env(db_path=state))
    reopened.close()
