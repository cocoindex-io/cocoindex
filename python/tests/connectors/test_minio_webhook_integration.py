"""Real provider → real HTTP → CocoIndex engine → local target integration.

The official container images are no longer publicly pullable. Tests use a
checksum-pinned official release executable, cached between runs. A local run
can instead set COCOINDEX_TEST_MINIO_BINARY to a trusted MinIO executable.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
from collections.abc import AsyncIterator, Callable
from pathlib import Path
import platform
import secrets
import socket
import subprocess
import tempfile
from typing import Any
import urllib.request

import aiohttp
import boto3
from botocore.config import Config
import pytest
import uvicorn

from tests.connectors.test_minio_webhook import _module

_RELEASE = "RELEASE.2025-09-07T16-13-09Z"
_BINARIES = {
    ("Linux", "x86_64"): (
        "linux-amd64",
        "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f",
    ),
    ("Darwin", "arm64"): (
        "darwin-arm64",
        "7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4",
    ),
    ("Darwin", "x86_64"): (
        "darwin-amd64",
        "4759080aeef7385aaceaac1131c30aaeb99605921553967dc6d3ef4e16ac64f9",
    ),
}
pytestmark = [pytest.mark.asyncio, pytest.mark.timeout(180)]


def _checksum(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@pytest.fixture(scope="session")
def minio_binary(pytestconfig: pytest.Config) -> Path:
    if configured := os.environ.get("COCOINDEX_TEST_MINIO_BINARY"):
        return Path(configured).resolve(strict=True)
    artifact = _BINARIES.get((platform.system(), platform.machine()))
    if artifact is None:
        pytest.skip("Set COCOINDEX_TEST_MINIO_BINARY for this platform")
    platform_name, digest = artifact
    name = f"minio.{platform_name}.{_RELEASE}"
    target = pytestconfig.cache.mkdir("minio") / name
    if target.exists() and _checksum(target) == digest:
        return target
    url = f"https://github.com/minio/minio/releases/download/{_RELEASE}/{name}"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
            temporary = Path(output.name)
            with urllib.request.urlopen(url, timeout=30) as response:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
        if _checksum(temporary) != digest:
            raise ValueError("MinIO release checksum mismatch")
        temporary.chmod(0o700)
        temporary.replace(target)
        return target
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


async def eventually(predicate: Callable[[], bool], message: str) -> None:
    async with asyncio.timeout(30):
        while not predicate():
            await asyncio.sleep(0.05)
    assert predicate(), message


def listener(port: int = 0) -> socket.socket:
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    sock.listen(64)
    return sock


@contextlib.asynccontextmanager
async def minio(
    binary: Path,
    directory: Path,
    webhook_port: int,
    token: str,
    access: str,
    secret: str,
) -> AsyncIterator[str]:
    env = {
        "MINIO_ROOT_USER": access,
        "MINIO_ROOT_PASSWORD": secret,
        "MINIO_NOTIFY_WEBHOOK_ENABLE_COCO": "on",
        "MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_COCO": token,
        "MINIO_NOTIFY_WEBHOOK_QUEUE_LIMIT_COCO": "1000",
        "MINIO_BROWSER": "off",
    }
    reservation = listener()
    port = reservation.getsockname()[1]
    reservation.close()
    env.update(
        {
            "MINIO_NOTIFY_WEBHOOK_ENDPOINT_COCO": f"http://127.0.0.1:{webhook_port}/events",
            "MINIO_NOTIFY_WEBHOOK_QUEUE_DIR_COCO": str(directory / "queue"),
        }
    )
    (directory / "data").mkdir(parents=True)
    with (directory / "minio.log").open("wb") as log:
        process = subprocess.Popen(
            [
                str(binary),
                "server",
                "--address",
                f"127.0.0.1:{port}",
                str(directory / "data"),
            ],
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)


async def ready_endpoint(url: str) -> None:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
        async with asyncio.timeout(30):
            while True:
                try:
                    async with session.get(url + "/minio/health/live") as response:
                        if response.status == 200:
                            return
                except (aiohttp.ClientError, TimeoutError):
                    pass
                await asyncio.sleep(0.1)


@contextlib.asynccontextmanager
async def serve(
    sock: socket.socket,
    token: str,
    bucket: str,
    directory: Path,
    deliveries: list[dict[str, Any]],
) -> AsyncIterator[None]:
    app = _module.MinioApplication(
        token=token,
        bucket=bucket,
        prefix="prefix/",
        output=directory / "output",
        state=directory / "state",
    )

    async def observed(scope: Any, receive: Any, send: Any) -> None:
        body = bytearray()

        async def read() -> Any:
            message = await receive()
            body.extend(message.get("body", b""))
            return message

        async def write(message: Any) -> None:
            if scope["type"] == "http" and message["type"] == "http.response.start":
                # Snapshot the target at the actual HTTP acknowledgement boundary.
                rows = [
                    json.loads(path.read_text())
                    for path in (directory / "output").glob("*.json")
                ]
                deliveries.append(
                    {"body": bytes(body), "status": message["status"], "rows": rows}
                )
            await send(message)

        await app(scope, read, write)

    server = uvicorn.Server(
        uvicorn.Config(
            observed,
            lifespan="on",
            log_level="warning",
            limit_concurrency=32,
            timeout_graceful_shutdown=2,
        )
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await eventually(
            lambda: server.started or task.done(), "HTTP server did not start"
        )
        if task.done():
            await task
            raise AssertionError("HTTP server ended during startup")
        yield
    finally:
        server.should_exit = True
        try:
            await task
        finally:
            sock.close()
        assert not app.stream.accepting and app.stream._pending == 0


async def test_minio_notifications_reconcile_and_acknowledge_real_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    minio_binary: Path,
) -> None:
    token, access, secret = (
        secrets.token_urlsafe(24),
        "cocoindex-test",
        secrets.token_urlsafe(24),
    )
    bucket, key = "cocoindex-events", "prefix/space + unicode/猫%2F.txt"
    sock = listener()
    port = sock.getsockname()[1]
    deliveries: list[dict[str, Any]] = []
    async with contextlib.AsyncExitStack() as resources:
        resources.callback(sock.close)
        endpoint = await resources.enter_async_context(
            minio(minio_binary, tmp_path, port, token, access, secret)
        )
        await ready_endpoint(endpoint)
        for name, value in {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ACCESS_KEY_ID": access,
            "AWS_SECRET_ACCESS_KEY": secret,
            "AWS_DEFAULT_REGION": "us-east-1",
        }.items():
            monkeypatch.setenv(name, value)
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=Config(s3={"addressing_style": "path"}),
        )
        resources.callback(client.close)
        await asyncio.to_thread(client.create_bucket, Bucket=bucket)
        await asyncio.to_thread(
            client.put_bucket_notification_configuration,
            Bucket=bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "Id": "cocoindex",
                        "QueueArn": "arn:minio:sqs::COCO:webhook",
                        "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                    }
                ]
            },
        )
        path = (
            tmp_path
            / "output"
            / (hashlib.sha256(f"{bucket}\0{key}".encode()).hexdigest() + ".json")
        )
        webhook_url = f"http://127.0.0.1:{port}/events"
        async with serve(sock, token, bucket, tmp_path, deliveries):
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    webhook_url,
                    json={"Records": []},
                    headers={"Authorization": "Bearer wrong"},
                ) as rejected:
                    assert rejected.status == 403
            for content in (b"first", b"second with same key"):
                previous = len(deliveries)
                await asyncio.to_thread(
                    client.put_object, Bucket=bucket, Key=key, Body=content
                )
                digest = hashlib.sha256(content).hexdigest()
                await eventually(
                    lambda: any(
                        item["status"] == 204
                        and any(
                            row["key"] == key and row["sha256"] == digest
                            for row in item["rows"]
                        )
                        for item in deliveries[previous:]
                    ),
                    "MinIO did not receive an acknowledgement after the expected target commit",
                )
                assert json.loads(path.read_text())["sha256"] == digest
            original = next(
                item["body"] for item in deliveries if item["status"] == 204
            )
            envelope = json.loads(original)
            record = envelope["Records"][0]
            assert record["eventSource"] == "minio:s3"
            assert record["eventVersion"] == "2.0"
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    webhook_url,
                    data=original,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                ) as duplicate:
                    assert duplicate.status == 204
            assert (
                json.loads(path.read_text())["sha256"]
                == hashlib.sha256(b"second with same key").hexdigest()
            )
            previous = len(deliveries)
            await asyncio.to_thread(client.delete_object, Bucket=bucket, Key=key)
            await eventually(
                lambda: any(
                    item["status"] == 204 and not item["rows"]
                    for item in deliveries[previous:]
                ),
                "Delete was not acknowledged after target removal",
            )
            assert not path.exists()
        await asyncio.to_thread(
            client.put_object, Bucket=bucket, Key=key, Body=b"offline update"
        )
        async with serve(listener(port), token, bucket, tmp_path, deliveries):
            await eventually(
                lambda: path.exists(), "Restart scan did not cover the offline change"
            )
            assert (
                json.loads(path.read_text())["sha256"]
                == hashlib.sha256(b"offline update").hexdigest()
            )
