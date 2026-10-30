"""Owned, loopback-only Kafka services for this example's integration tests."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
import socket
import struct
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse
import uuid

import pytest
import pytest_asyncio

KAFKA_IMAGE = "apache/kafka:4.1.2"
_DOCKER_UNAVAILABLE = pytest.StashKey[str | None]()


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    integration = [
        item for item in items if item.path.name == "test_kafka_integration.py"
    ]
    if not integration:
        return

    import docker
    from testcontainers.core.config import testcontainers_config
    from testcontainers.core.docker_client import DockerClient, get_docker_host

    endpoint = get_docker_host()
    parsed = urlparse(endpoint or "unix:///var/run/docker.sock")
    local = parsed.scheme in {"unix", "npipe"} or parsed.hostname in {
        "localhost",
        "127.0.0.1",
        "::1",
    }
    reason = None
    client = None
    if not local:
        reason = "Kafka integration tests require a local Docker daemon"
    else:
        try:
            client = DockerClient()
            client.client.ping()
        except (docker.errors.DockerException, OSError) as exc:
            reason = f"Local Docker is unavailable ({type(exc).__name__})"

    config.stash[_DOCKER_UNAVAILABLE] = reason
    if reason is not None:
        if client is not None:
            client.client.close()
        for item in integration:
            item.add_marker(pytest.mark.skip(reason=reason))
        return

    assert client is not None
    try:
        # Collection runs before pytest-timeout starts a per-test timer. Pull
        # failures are setup failures, never evidence that Docker is unavailable.
        for image in (KAFKA_IMAGE, testcontainers_config.ryuk_image):
            try:
                client.client.images.get(image)
            except docker.errors.ImageNotFound:
                client.client.images.pull(image)
    finally:
        client.client.close()


class ProduceRelay:
    """Forward Kafka bytes, with a barrier before one real Produce request."""

    def __init__(self, upstream_port: int) -> None:
        self.upstream_port = upstream_port
        self.server: asyncio.Server | None = None
        self.handlers: set[asyncio.Task[None]] = set()
        self.produce_seen = asyncio.Event()
        self.release_produce = asyncio.Event()
        self.release_produce.set()
        self._armed = False
        self.errors: list[Exception] = []

    @property
    def port(self) -> int:
        assert self.server is not None and self.server.sockets
        return int(self.server.sockets[0].getsockname()[1])

    def arm(self) -> None:
        assert not self._armed
        self.produce_seen.clear()
        self.release_produce.clear()
        self._armed = True

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._serve, "127.0.0.1", 0)

    async def _requests(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while True:
            header = await reader.readexactly(4)
            size = struct.unpack("!i", header)[0]
            if not 2 <= size <= 8 * 1024 * 1024:
                raise ValueError("Unexpected Kafka request frame size")
            body = await reader.readexactly(size)
            if self._armed and struct.unpack("!h", body[:2])[0] == 0:
                self._armed = False
                self.produce_seen.set()
                await self.release_produce.wait()
            writer.write(header + body)
            await writer.drain()

    @staticmethod
    async def _responses(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.handlers.add(task)
        upstream_writer = None
        forwarding: list[asyncio.Task[None]] = []
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                "127.0.0.1", self.upstream_port
            )
            forwarding = [
                asyncio.create_task(self._requests(reader, upstream_writer)),
                asyncio.create_task(self._responses(upstream_reader, writer)),
            ]
            done, _ = await asyncio.wait(
                forwarding, return_when=asyncio.FIRST_COMPLETED
            )
            for finished in done:
                finished.result()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.errors.append(error)
        finally:
            for forwarder in forwarding:
                forwarder.cancel()
            await asyncio.gather(*forwarding, return_exceptions=True)
            for stream in (writer, upstream_writer):
                if stream is not None:
                    stream.close()
                    with suppress(ConnectionError):
                        await stream.wait_closed()
            self.handlers.discard(task)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        tasks = tuple(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@dataclass
class KafkaService:
    container: Any
    relay: ProduceRelay
    cluster_id: str
    topic: str
    destination: str

    @property
    def servers(self) -> str:
        return f"127.0.0.1:{self.relay.port}"


def _free_loopback_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


@pytest_asyncio.fixture
async def kafka_service(request: pytest.FixtureRequest) -> AsyncIterator[KafkaService]:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.docker_client import DockerClient
    from testcontainers.core.exceptions import ContainerStartException
    from testcontainers.core.labels import SESSION_ID
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    reason = request.config.stash.get(_DOCKER_UNAVAILABLE, None)
    if reason is not None:
        pytest.skip(reason)

    identifier = uuid.uuid4().hex
    name = f"cocoindex-faststream-{identifier}"
    cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
    upstream_port = _free_loopback_port()
    relay = ProduceRelay(upstream_port)
    await relay.start()
    try:
        container = (
            DockerContainer(KAFKA_IMAGE, name=name)
            .with_bind_ports(9092, upstream_port)
            .with_envs(
                CLUSTER_ID=cluster_id,
                KAFKA_NODE_ID="1",
                KAFKA_PROCESS_ROLES="broker,controller",
                KAFKA_LISTENERS=(
                    "INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:9092,"
                    "CONTROLLER://0.0.0.0:9093"
                ),
                KAFKA_ADVERTISED_LISTENERS=(
                    f"INTERNAL://127.0.0.1:19092,EXTERNAL://127.0.0.1:{relay.port}"
                ),
                KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=(
                    "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
                ),
                KAFKA_INTER_BROKER_LISTENER_NAME="INTERNAL",
                KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
                KAFKA_CONTROLLER_QUORUM_VOTERS="1@127.0.0.1:9093",
                KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
                KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS="1",
                KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS="1",
                KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1",
                KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
                KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="0",
                KAFKA_AUTO_CREATE_TOPICS_ENABLE="false",
                KAFKA_LOG_DIRS="/tmp/kafka-data",
                KAFKA_LOG_SEGMENT_BYTES="10485760",
                KAFKA_LOG_INDEX_SIZE_MAX_BYTES="1048576",
                KAFKA_HEAP_OPTS="-Xms128m -Xmx256m",
            )
            .with_kwargs(mem_limit="768m")
            .waiting_for(
                LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(90)
            )
        )
    except BaseException:
        await relay.close()
        raise
    service = KafkaService(
        container, relay, cluster_id, f"coco-faststream-{identifier}", name
    )
    create = DockerClient.create

    def create_loopback(client: Any, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("name") in {name, f"testcontainers-ryuk-{SESSION_ID}"}:
            kwargs["ports"] = {
                port: ("127.0.0.1", host_port)
                for port, host_port in (kwargs.get("ports") or {}).items()
            }
        return create(client, *args, **kwargs)

    try:
        # Testcontainers 4.15's public create API accepts Docker SDK IP/port
        # tuples; with_bind_ports does not. Scope this to our two owned names.
        with patch.object(DockerClient, "create", create_loopback):
            start = asyncio.create_task(asyncio.to_thread(container.start))
            try:
                await asyncio.shield(start)
            except asyncio.CancelledError:
                await start
                raise
        wrapped = container.get_wrapped_container()
        await asyncio.to_thread(wrapped.reload)
        bindings = wrapped.attrs["NetworkSettings"]["Ports"]["9092/tcp"]
        assert bindings == [{"HostIp": "127.0.0.1", "HostPort": str(upstream_port)}]

        admin = AIOKafkaAdminClient(
            bootstrap_servers=service.servers, request_timeout_ms=10_000
        )
        try:
            await admin.start()
            assert (await admin.describe_cluster())["cluster_id"] == cluster_id
            await admin.create_topics(
                [
                    NewTopic(
                        service.topic,
                        num_partitions=1,
                        replication_factor=1,
                        topic_configs={
                            "cleanup.policy": "delete",
                            "retention.ms": "-1",
                            "retention.bytes": "-1",
                        },
                    )
                ]
            )
        finally:
            await admin.close()
        yield service
    finally:
        try:
            await relay.close()
            try:
                wrapped = container.get_wrapped_container()
            except ContainerStartException:
                wrapped = None
            if wrapped is not None:
                await asyncio.to_thread(wrapped.reload)
                if wrapped.attrs.get("State", {}).get("Paused"):
                    await asyncio.to_thread(wrapped.unpause)
        finally:
            await asyncio.to_thread(container.stop)
        assert not relay.errors, relay.errors
