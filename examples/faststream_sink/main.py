"""Publish a versioned JSON snapshot through a custom CocoIndex target."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from pathlib import Path, PurePath

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike
import dotenv
from prometheus_client import CollectorRegistry, start_http_server

from .connection import Config, drain_cleanup, open_destination
from .target import KafkaDestination, KafkaTarget

dotenv.load_dotenv(Path(__file__).with_name(".env"))
CONFIG = Config.from_env()
DESTINATION = coco.ContextKey[KafkaDestination](CONFIG.destination)
SNAPSHOT = Path(
    os.environ.get("SNAPSHOT", str(Path(__file__).with_name("snapshot.json")))
)


class SnapshotMatcher:
    """Select exactly one snapshot filename, including literal glob characters."""

    def is_dir_included(self, path: PurePath) -> bool:
        return path == PurePath(".")

    def is_file_included(self, path: PurePath) -> bool:
        return path == PurePath(SNAPSHOT.name)


@coco.lifespan
async def lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    registry = CollectorRegistry()
    async with open_destination(CONFIG, registry) as destination:
        builder.provide(DESTINATION, destination)
        server, thread = start_http_server(
            int(os.environ.get("METRICS_PORT", "8001")),
            addr="127.0.0.1",
            registry=registry,
        )

        async def close_metrics() -> None:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            await asyncio.to_thread(thread.join)

        try:
            yield
        finally:
            await drain_cleanup(close_metrics())


@coco.fn
async def process_snapshot(file: FileLike, target: KafkaTarget) -> None:
    records = json.loads(await file.read_text())
    if not isinstance(records, list) or any(
        not isinstance(record, dict) or set(record) != {"key", "revision", "value"}
        for record in records
    ):
        raise ValueError("Snapshot must be a list of {key, revision, value} objects")
    keys = [record["key"] for record in records]
    if any(not isinstance(key, str) for key in keys) or len(set(keys)) != len(keys):
        raise ValueError("Snapshot keys must be unique strings")
    for record in records:
        target.declare(record["key"], record["revision"], record["value"])


@coco.fn
async def app_main() -> None:
    target = KafkaTarget(DESTINATION, CONFIG.cluster_id, CONFIG.topic)
    files = localfs.walk_dir(
        SNAPSHOT.parent,
        live=True,
        path_matcher=SnapshotMatcher(),
    )
    await coco.mount_each(process_snapshot, files.items(), target)


app = coco.App(
    coco.AppConfig(name="FastStreamKafkaSnapshot", max_inflight_components=1), app_main
)
