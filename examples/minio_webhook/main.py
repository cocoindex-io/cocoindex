"""Bounded, authenticated MinIO webhook and a live CocoIndex file index.

Each admitted request reserves one slot before reading its body. A slot covers
at most ``max_body_bytes`` and ``max_records`` across body reading, dispatch,
and completion waiting. Disconnecting the HTTP waiter does not release it.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import hashlib
import json
import os
from collections.abc import Awaitable, Callable
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import aiobotocore.session
from dotenv import load_dotenv

import cocoindex as coco
from cocoindex._internal.live_component import LiveStreamSubscriber, ReadyAwaitable
from cocoindex.connectors import amazon_s3, localfs
from cocoindex.connectorkits import SingleWatcherGuard

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class WebhookStream:
    def __init__(
        self,
        token: str,
        *,
        max_pending: int = 8,
        max_body_bytes: int = 1_048_576,
        max_records: int = 64,
    ) -> None:
        if not token or any(char.isspace() for char in token):
            raise ValueError("Configure a nonempty webhook token without whitespace")
        if min(max_pending, max_body_bytes, max_records) <= 0:
            raise ValueError("Webhook limits must be positive")
        self._authorization = f"Bearer {token}".encode()
        self._max_pending = max_pending
        self._max_body_bytes = max_body_bytes
        self._max_records = max_records
        self._pending = 0
        self._guard = SingleWatcherGuard("MinIO webhook stream")
        self._subscriber: LiveStreamSubscriber[bytes] | None = None
        self._generation: object | None = None
        self._tasks: set[asyncio.Task[int]] = set()
        self.active = asyncio.Event()

    @property
    def accepting(self) -> bool:
        return self._subscriber is not None

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        with self._guard:
            self._subscriber = subscriber
            self._generation = object()
            self.active.set()
            try:
                await subscriber.mark_ready()
                await asyncio.Future[None]()
            finally:
                self._subscriber = None
                self._generation = None
                self.active.clear()
                tasks = tuple(self._tasks)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def __call__(self, scope: Message, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise ValueError("WebhookStream handles HTTP requests only")
        if scope["path"] != "/events":
            await self._reply(send, 404)
            return
        if scope["method"] != "POST":
            await self._reply(send, 405)
            return
        headers: dict[bytes, list[bytes]] = {}
        for name, value in scope["headers"]:
            headers.setdefault(name.lower(), []).append(value)
        auth = headers.get(b"authorization", [])
        if len(auth) != 1 or not hmac.compare_digest(auth[0], self._authorization):
            await self._reply(send, 403)
            return
        types = headers.get(b"content-type", [])
        if (
            len(types) != 1
            or types[0].split(b";", 1)[0].strip().lower() != b"application/json"
        ):
            await self._reply(send, 415)
            return
        lengths = headers.get(b"content-length", [])
        if lengths:
            try:
                if len(lengths) != 1 or not lengths[0].isdigit():
                    raise ValueError
                length = int(lengths[0])
            except ValueError:
                await self._reply(send, 400)
                return
            if length > self._max_body_bytes:
                await self._reply(send, 413)
                return
        subscriber, generation = self._subscriber, self._generation
        if subscriber is None:
            await self._reply(send, 503)
            return
        if self._pending >= self._max_pending:
            await self._reply(send, 429)
            return
        self._pending += 1
        transferred = False
        try:
            body = bytearray()
            try:
                async with asyncio.timeout(10):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        if message["type"] != "http.request":
                            raise ValueError
                        chunk = message.get("body", b"")
                        if len(body) + len(chunk) > self._max_body_bytes:
                            await self._reply(send, 413)
                            return
                        body.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                await self._reply(send, 408)
                return
            # Reject a whole oversized batch before any record is dispatched.
            try:
                envelope = json.loads(body)
                if not isinstance(envelope, dict):
                    raise ValueError
                records = envelope.get("Records")
                if records is None and envelope.get("Event") == "s3:TestEvent":
                    record_count = 0
                elif isinstance(records, list) and records:
                    record_count = len(records)
                else:
                    raise ValueError
            except (ValueError, TypeError, RecursionError):
                await self._reply(send, 400)
                return
            if record_count > self._max_records:
                await self._reply(send, 413)
                return
            if self._generation is not generation:
                await self._reply(send, 503)
                return
            task = asyncio.create_task(self._dispatch(subscriber, bytes(body)))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            transferred = True
            status = await asyncio.shield(task)
            await self._reply(send, status)
        finally:
            if not transferred:
                self._pending -= 1

    async def _dispatch(
        self, subscriber: LiveStreamSubscriber[bytes], body: bytes
    ) -> int:
        handle: ReadyAwaitable | None = None
        try:
            try:
                handle = await subscriber.send(body)
            except ValueError:
                return 400
            await handle.ready()
            return 204
        except asyncio.CancelledError:
            # Keep the capacity reservation until an already-submitted operation
            # settles. The S3 view closes its operations before closing this stream.
            if handle is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await handle.ready()
            raise
        except Exception:
            return 503
        finally:
            self._pending -= 1

    @staticmethod
    async def _reply(send: Send, status: int) -> None:
        headers = [(b"content-length", b"0")]
        if status != 204:
            headers.append((b"connection", b"close"))
        await send(
            {"type": "http.response.start", "status": status, "headers": headers}
        )
        await send({"type": "http.response.body", "body": b""})


class _OwnedTasks:
    """Track this example's actual Python work, including native callbacks."""

    def __init__(self) -> None:
        self._closed = False
        self._tasks: set[asyncio.Task[Any]] = set()

    @contextlib.contextmanager
    def track(self) -> Iterator[None]:
        if self._closed:
            raise asyncio.CancelledError
        task = asyncio.current_task()
        assert task is not None
        nested = task in self._tasks
        self._tasks.add(task)
        try:
            yield
        finally:
            if not nested:
                self._tasks.discard(task)

    async def close(self) -> None:
        self._closed = True
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class _OwnedView:
    def __init__(
        self, view: coco.LiveMapView[str, amazon_s3.S3File], owned: _OwnedTasks
    ) -> None:
        self._view, self._owned = view, owned

    async def __aiter__(self) -> AsyncIterator[tuple[str, amazon_s3.S3File]]:
        iterator = aiter(self._view)
        try:
            while True:
                try:
                    with self._owned.track():
                        item = await anext(iterator)
                except StopAsyncIteration:
                    return
                yield item
        finally:
            # The underlying S3 view is an async generator; close its paginator
            # when the native engine releases this iteration early.
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

    async def watch(
        self, subscriber: coco.LiveMapSubscriber[str, amazon_s3.S3File]
    ) -> None:
        with self._owned.track():
            await self._view.watch(subscriber)


async def index_file(entry: amazon_s3.S3File, output: Path) -> None:
    content = await entry.read()
    bucket = entry.file_path.bucket_name
    key = entry.file_path.resolve()
    name = hashlib.sha256(f"{bucket}\0{key}".encode()).hexdigest() + ".json"
    row = {
        "bucket": bucket,
        "key": key,
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    localfs.declare_file(
        output / name,
        json.dumps(row, ensure_ascii=False) + "\n",
        create_parent_dirs=True,
    )


def _propagate(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    # The default handler may log a failed transform and continue. A webhook
    # acknowledgement must instead report that failure to its delivery client.
    raise exc


class MinioApplication:
    def __init__(
        self,
        *,
        token: str,
        bucket: str,
        prefix: str = "",
        output: Path = Path("output"),
        state: Path = Path(".cocoindex"),
        max_file_size: int = 16 * 1024 * 1024,
    ) -> None:
        if not bucket or max_file_size <= 0:
            raise ValueError("A bucket and positive max_file_size are required")
        self.stream = WebhookStream(token)
        self.bucket, self.prefix = bucket, prefix
        self.output, self.state = output, state
        self.max_file_size = max_file_size

    async def __call__(self, scope: Message, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
        else:
            await self.stream(scope, receive, send)

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        message = await receive()
        if message["type"] != "lifespan.startup":
            raise ValueError("Expected ASGI startup")
        started = False
        try:
            async with aiobotocore.session.get_session().create_client("s3") as client:
                environment = coco.Environment(
                    coco.Settings.from_env(db_path=self.state),
                    exception_handler=_propagate,
                )
                owned = _OwnedTasks()
                view = _OwnedView(
                    amazon_s3.list_objects(
                        client,
                        self.bucket,
                        prefix=self.prefix,
                        max_file_size=self.max_file_size,
                    ).live_items(self.stream),
                    owned,
                )

                @coco.fn(memo=True)
                async def process(entry: amazon_s3.S3File, output: Path) -> None:
                    with owned.track():
                        await index_file(entry, output)

                @coco.fn
                async def root() -> None:
                    await coco.mount_each(process, view, self.output)

                pipeline = coco.App(
                    coco.AppConfig(
                        name="MinioWebhook",
                        environment=environment,
                        max_inflight_components=8,
                    ),
                    root,
                )
                result = asyncio.create_task(pipeline.update(live=True).result())
                ready = asyncio.create_task(self.stream.active.wait())
                try:
                    await asyncio.wait(
                        (ready, result), return_when=asyncio.FIRST_COMPLETED
                    )
                    if result.done():
                        await result
                        raise RuntimeError(
                            "Indexing ended before the webhook became active"
                        )
                    await send({"type": "lifespan.startup.complete"})
                    started = True
                    shutdown = asyncio.ensure_future(receive())
                    try:
                        await asyncio.wait(
                            (shutdown, result), return_when=asyncio.FIRST_COMPLETED
                        )
                        if result.done():
                            await result
                            raise RuntimeError("The live indexing pipeline ended")
                        message = await shutdown
                        if message["type"] != "lifespan.shutdown":
                            raise ValueError("Expected ASGI shutdown")
                    finally:
                        shutdown.cancel()
                        await asyncio.gather(shutdown, return_exceptions=True)
                finally:
                    ready.cancel()
                    await asyncio.gather(ready, return_exceptions=True)
                    # Cancel the owned watcher, scan reads, and transforms.
                    # A canceled result waiter alone cannot stop native work.
                    await owned.close()
                    try:
                        with contextlib.suppress(asyncio.CancelledError):
                            await result
                    finally:
                        environment.close()
            await send({"type": "lifespan.shutdown.complete"})
        except Exception:
            await send(
                {
                    "type": "lifespan.shutdown.failed"
                    if started
                    else "lifespan.startup.failed",
                    "message": "MinIO indexing failed; check the server error log",
                }
            )
            raise


def create_app() -> MinioApplication:
    load_dotenv()
    return MinioApplication(
        token=os.environ["WEBHOOK_TOKEN"],
        bucket=os.environ["S3_BUCKET"],
        prefix=os.getenv("S3_PREFIX", ""),
        output=Path(os.getenv("OUTPUT_DIR", "output")),
        state=Path(os.getenv("COCOINDEX_DB", ".cocoindex")),
        max_file_size=int(os.getenv("MAX_FILE_SIZE", str(16 * 1024 * 1024))),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        create_app(),
        host=os.getenv("WEBHOOK_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBHOOK_PORT", "8000")),
        limit_concurrency=32,
        timeout_graceful_shutdown=10,
    )
