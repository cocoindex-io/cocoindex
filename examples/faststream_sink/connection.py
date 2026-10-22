"""Resources shared by the example producer and its consumer."""

from __future__ import annotations

import os
import asyncio
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from aiokafka.admin import AIOKafkaAdminClient
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from prometheus_client import CollectorRegistry

from .protocol import MessageKey, validate_key
from .target import KafkaDestination


@dataclass(frozen=True)
class Config:
    servers: str
    cluster_id: str
    topic: str
    destination: str

    @classmethod
    def from_env(cls) -> Config:
        config = cls(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            os.environ["KAFKA_CLUSTER_ID"],
            os.environ.get("KAFKA_TOPIC", "cocoindex-faststream"),
            os.environ.get("KAFKA_DESTINATION", "faststream-example-local"),
        )
        validate_key(
            MessageKey(config.destination, config.cluster_id, config.topic, "probe")
        )
        return config

    @property
    def route(self) -> tuple[str, str, str]:
        return self.destination, self.cluster_id, self.topic


async def drain_cleanup(cleanup: Awaitable[None]) -> None:
    """Finish one owned cleanup operation before propagating caller cancellation."""
    pending = asyncio.ensure_future(cleanup)
    interrupted: asyncio.CancelledError | None = None
    while True:
        try:
            await asyncio.shield(pending)
            break
        except asyncio.CancelledError as error:
            if pending.cancelled():
                raise
            interrupted = error
    if interrupted is not None:
        raise interrupted


async def verify_cluster(config: Config) -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=config.servers,
        client_id="cocoindex-faststream-identity",
        request_timeout_ms=10_000,
    )
    try:
        await admin.start()
        cluster = await admin.describe_cluster()
        if cluster.get("cluster_id") != config.cluster_id:
            raise ValueError(
                "Kafka cluster ID does not match KAFKA_CLUSTER_ID. "
                "Do not repoint an existing logical destination to another cluster."
            )
    finally:
        await drain_cleanup(admin.close())


@asynccontextmanager
async def open_destination(
    config: Config, registry: CollectorRegistry
) -> AsyncIterator[KafkaDestination]:
    await verify_cluster(config)
    broker = KafkaBroker(
        config.servers,
        acks="all",
        enable_idempotence=False,
        request_timeout_ms=10_000,
        middlewares=(
            KafkaPrometheusMiddleware(
                registry=registry, app_name="cocoindex-faststream"
            ),
        ),
    )
    try:
        await broker.connect()
        yield KafkaDestination(broker, config.cluster_id)
    finally:
        await drain_cleanup(broker.stop())
