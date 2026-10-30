"""Real Kafka, FastStream, CocoIndex, and consumer lifecycle checks."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import time
from typing import TYPE_CHECKING
from urllib.request import urlopen
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families
import pytest

from examples.faststream_sink.projection import Projection
from examples.faststream_sink.protocol import Event, canonical_json, decode_event

if TYPE_CHECKING:
    from examples.faststream_sink.tests.conftest import KafkaService

pytestmark = pytest.mark.timeout(240)
ROOT = Path(__file__).resolve().parents[3]
CRASH_AFTER_ACK = 73
SNAPSHOT_NAME = "snapshot[version].json"

# This child imports the actual example and uses its real lifespan. The only
# publisher wrapper observes real attempts and acknowledgements (or exits after
# acknowledgement in crash mode); no FastStream/aiokafka internals are replaced.
_WORKER = r"""
import asyncio
from contextlib import asynccontextmanager, suppress
import json
import os
from pathlib import Path
import sys
import traceback

import cocoindex as coco

spec = json.loads(Path(sys.argv[1]).read_text())
mode = spec["mode"]
result = Path(spec["result"])
release = Path(spec["release"])


def report(path, payload):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as output:
        json.dump(payload, output)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


async def released():
    async with asyncio.timeout(180):
        while not release.exists():
            await asyncio.sleep(0.05)


async def consume():
    from examples.faststream_sink.consumer import consume as real_consume

    running = asyncio.create_task(real_consume(Path(spec["projection"])))
    stopping = asyncio.create_task(released())
    report(result, {"phase": "started"})
    try:
        done, _ = await asyncio.wait((running, stopping), return_when=asyncio.FIRST_COMPLETED)
        if running in done:
            await running
            raise AssertionError("The consumer unexpectedly returned")
        await stopping
    finally:
        stopping.cancel()
        running.cancel()
        with suppress(asyncio.CancelledError):
            await running
        with suppress(asyncio.CancelledError):
            await stopping
    report(result, {"phase": "closed"})


async def publish():
    from examples.faststream_sink import main as example
    from examples.faststream_sink.protocol import decode_event
    from examples.faststream_sink.target import KafkaDestination

    real_destination = example.open_destination
    attempted = []
    confirmed = []
    failures = []

    async def record_failure(error, context):
        failures.append("".join(traceback.format_exception(error)))

    @coco.lifespan
    async def observe_failures(builder):
        builder.set_exception_handler(record_failure)
        async with asynccontextmanager(example.lifespan)(builder):
            yield

    class ObservedPublisher:
        def __init__(self, publisher):
            self.publisher = publisher

        async def publish(self, message, topic, **kwargs):
            event = decode_event(message)
            attempted.append(event.event_id)
            metadata = await self.publisher.publish(message, topic, **kwargs)
            assert metadata.topic == topic and metadata.offset >= 0
            confirmed.append(event.event_id)
            if mode == "crash":
                report(result, {
                    "phase": "acknowledged",
                    "event_id": event.event_id,
                    "offset": metadata.offset,
                    "partition": metadata.partition,
                })
                os._exit(73)
            return metadata

    @asynccontextmanager
    async def observed_destination(config, registry):
        async with real_destination(config, registry) as destination:
            yield KafkaDestination(ObservedPublisher(destination.publisher), destination.cluster_id)

    example.open_destination = observed_destination

    async with coco.runtime():
        handle = example.app.update()
        await handle
        stats = handle.stats()
        assert stats is not None and stats.total.num_in_progress == 0
        if mode == "outage":
            # mount_each children are independent: root completion does not
            # imply their success. Check the failed child through public stats.
            assert stats.total.num_errors == 1, stats
            snapshot_stats = next(
                group for name, group in stats.by_component.items()
                if name.endswith("process_snapshot")
            )
            assert snapshot_stats.num_errors == 1 and snapshot_stats.num_processed == 0, stats
            assert len(attempted) == len(failures) == 1 and confirmed == []
        else:
            assert stats.total.num_errors == 0, stats
            assert failures == []
        outcome = {
            "stats": {name: group._asdict() for name, group in stats.by_component.items()},
            "attempted_event_ids": attempted,
            "confirmed_event_ids": confirmed,
            "failure": failures[0] if failures else None,
        }
        report(result, {"phase": "updated", **outcome})
        if mode == "hold":
            await released()
    # This report is only written after normal lifespan cleanup has completed.
    report(result, {"phase": "closed", **outcome})


asyncio.run(consume() if mode == "consumer" else publish())
"""


@dataclass
class Child:
    process: asyncio.subprocess.Process
    log: Path
    result: Path
    release: Path

    def diagnostics(self) -> str:
        return self.log.read_text(errors="replace")[-12000:]

    async def wait(self, *, timeout: float = 45, expected: int = 0) -> None:
        try:
            code = await asyncio.wait_for(self.process.wait(), timeout)
        except TimeoutError:
            pytest.fail(
                f"Owned child exceeded {timeout}s watchdog:\n{self.diagnostics()}"
            )
        assert code == expected, self.diagnostics()

    async def report(self, phase: str, *, timeout: float = 45) -> dict[str, object]:
        async with asyncio.timeout(timeout):
            while True:
                if self.result.exists():
                    result = json.loads(self.result.read_text())
                    if result.get("phase") == phase:
                        return result
                assert self.process.returncode is None, self.diagnostics()
                await asyncio.sleep(0.05)


@asynccontextmanager
async def child(
    tmp_path: Path,
    service: KafkaService,
    mode: str,
    *,
    metrics_port: int = 0,
    projection: Path | None = None,
    configured_topic: str | None = None,
) -> AsyncIterator[Child]:
    run = tmp_path / f"child-{uuid.uuid4().hex}"
    run.mkdir()
    result, release = run / "result.json", run / "release"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("COCOINDEX_", "KAFKA_"))
    }
    environment.update(
        KAFKA_BOOTSTRAP_SERVERS=service.servers,
        KAFKA_CLUSTER_ID=service.cluster_id,
        KAFKA_TOPIC=configured_topic or service.topic,
        KAFKA_DESTINATION=service.destination,
        COCOINDEX_DB=str(tmp_path / "engine"),
        COCOINDEX_LMDB_MAP_SIZE=str(64 * 1024 * 1024),
        SNAPSHOT=str(tmp_path / SNAPSHOT_NAME),
        METRICS_PORT=str(metrics_port),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=os.pathsep.join(
            [str(ROOT), str(ROOT / "python"), environment.get("PYTHONPATH", "")]
        ),
    )
    if mode.startswith("cli-"):
        command = [
            sys.executable,
            "-m",
            "cocoindex.cli",
            mode.removeprefix("cli-"),
            "examples.faststream_sink.main",
            "--quiet",
        ]
        if mode == "cli-drop":
            command.append("--force")
    else:
        worker = run / "worker.py"
        worker.write_text(_WORKER)
        spec = run / "spec.json"
        spec.write_text(
            json.dumps(
                {
                    "mode": mode,
                    "result": str(result),
                    "release": str(release),
                    "projection": str(projection) if projection is not None else None,
                }
            )
        )
        command = [sys.executable, str(worker), str(spec)]
    log = run / "output.log"
    with log.open("wb") as output:
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=tmp_path,
            env=environment,
            stdout=output,
            stderr=asyncio.subprocess.STDOUT,
        )
        owned = Child(process, log, result, release)
        try:
            yield owned
        finally:
            if process.returncode is None:
                release.touch()
                try:
                    await asyncio.wait_for(process.wait(), 15)
                except TimeoutError:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 5)
                    except TimeoutError:
                        process.kill()
                        await process.wait()


def snapshot(tmp_path: Path, revision: int | None, value: object = "A") -> None:
    rows = (
        []
        if revision is None
        else [{"key": "record", "revision": revision, "value": value}]
    )
    (tmp_path / SNAPSHOT_NAME).write_text(json.dumps(rows))


@asynccontextmanager
async def reader(service: KafkaService) -> AsyncIterator[AIOKafkaConsumer]:
    observer = AIOKafkaConsumer(
        bootstrap_servers=service.servers,
        group_id=None,
        enable_auto_commit=False,
        isolation_level="read_committed",
        request_timeout_ms=10_000,
    )
    try:
        await observer.start()
        observer.assign([TopicPartition(service.topic, 0)])
        observer.seek(TopicPartition(service.topic, 0), 0)
        yield observer
    finally:
        await observer.stop()


async def events(observer: AIOKafkaConsumer, topic: str) -> list[Event]:
    partition = TopicPartition(topic, 0)
    last = (await observer.end_offsets([partition]))[partition]
    observer.seek(partition, 0)
    result = []
    async with asyncio.timeout(15):
        while await observer.position(partition) < last:
            batches = await observer.getmany(timeout_ms=1000)
            for messages in batches.values():
                for message in messages:
                    assert message.value is not None
                    event = decode_event(message.value)
                    assert message.key == canonical_json(
                        [event.key.destination, event.key.key]
                    )
                    assert (
                        dict(message.headers)["cocoindex-event-id"]
                        == event.event_id.encode()
                    )
                    result.append(event)
    return result


async def projected(
    path: Path,
    revision: int,
    phase: int,
    value: object = None,
    *,
    consumer: Child,
) -> None:
    expected = (revision, phase, None if phase else canonical_json(value))

    def read() -> tuple[object, ...] | None:
        if not path.exists():
            return None
        with sqlite3.connect(path, timeout=1) as database:
            try:
                return database.execute(
                    "SELECT revision, phase, value_json FROM records WHERE record_key = 'record'"
                ).fetchone()
            except sqlite3.OperationalError:
                return None

    async with asyncio.timeout(30):
        while await asyncio.to_thread(read) != expected:
            assert consumer.process.returncode is None, consumer.diagnostics()
            await asyncio.sleep(0.05)


def _metrics(port: int) -> str:
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as response:
        assert response.status == 200
        return response.read().decode()


def _free_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _assert_port_closed(port: int) -> None:
    with socket.socket() as connection:
        connection.settimeout(1)
        assert connection.connect_ex(("127.0.0.1", port)) != 0


async def test_real_confirmations_metrics_projection_and_fresh_cli_drop(
    kafka_service: KafkaService, tmp_path: Path
) -> None:
    service = kafka_service
    projection = tmp_path / "projection.sqlite3"
    # The old glob matcher would select this sibling instead of the literal
    # filename containing brackets. Only the exact configured file is input.
    (tmp_path / "snapshotv.json").write_text("this sibling must stay ignored")
    snapshot(tmp_path, 1)
    async with child(tmp_path, service, "consumer", projection=projection) as consumer:
        async with reader(service) as observer:
            metrics_port = _free_port()
            async with child(
                tmp_path, service, "hold", metrics_port=metrics_port
            ) as first:
                await first.report("updated")
                first_events = await events(observer, service.topic)
                assert [
                    (event.revision, event.operation) for event in first_events
                ] == [(1, "upsert")]
                metrics = await asyncio.to_thread(_metrics, metrics_port)
                successes = [
                    sample.value
                    for family in text_string_to_metric_families(metrics)
                    for sample in family.samples
                    if sample.name == "faststream_published_messages_total"
                    and sample.labels.get("destination") == service.topic
                    and sample.labels.get("status") == "success"
                    and sample.labels.get("broker") == "kafka"
                ]
                assert successes == [1.0]
                first.release.touch()
                await first.wait()
            _assert_port_closed(metrics_port)
            await projected(projection, 1, 0, "A", consumer=consumer)

            async with child(tmp_path, service, "cli-update") as unchanged:
                await unchanged.wait()
            assert await events(observer, service.topic) == first_events

            for revision, value, phase in [
                (2, "B", 0),
                (3, "A", 0),
                (3, None, 1),
                (4, "A", 0),
            ]:
                snapshot(tmp_path, None if phase else revision, value)
                async with child(tmp_path, service, "cli-update") as update:
                    await update.wait()
                await projected(projection, revision, phase, value, consumer=consumer)

            # Drop is a fresh CLI process with a different configured topic.
            # Persisted actions still route to the original topic/provider. An
            # accidental declaration pass would parse this invalid snapshot.
            (tmp_path / SNAPSHOT_NAME).write_text("not a valid snapshot")
            async with child(
                tmp_path,
                service,
                "cli-drop",
                configured_topic=service.topic + "-replacement",
            ) as dropped:
                await dropped.wait()
            await projected(projection, 4, 1, consumer=consumer)
            actual = await events(observer, service.topic)
            assert [
                (event.revision, event.operation, event.value_json) for event in actual
            ] == [
                (1, "upsert", b'"A"'),
                (2, "upsert", b'"B"'),
                (3, "upsert", b'"A"'),
                (3, "delete", None),
                (4, "upsert", b'"A"'),
                (4, "delete", None),
            ]
            assert all(
                event.key[:3]
                == (service.destination, service.cluster_id, service.topic)
                for event in actual
            )
        consumer.release.touch()
        await consumer.wait()
        assert json.loads(consumer.result.read_text())["phase"] == "closed"


async def test_real_ack_process_crash_replays_the_same_event(
    kafka_service: KafkaService, tmp_path: Path
) -> None:
    service = kafka_service
    snapshot(tmp_path, 1)
    async with child(tmp_path, service, "cli-update") as initial:
        await initial.wait()
    snapshot(tmp_path, 2, "B")
    async with reader(service) as observer:
        async with child(tmp_path, service, "crash") as crashed:
            await crashed.wait(expected=CRASH_AFTER_ACK)
            acknowledgement = json.loads(crashed.result.read_text())
            assert acknowledgement["phase"] == "acknowledged"
        accepted = await events(observer, service.topic)
        assert [event.revision for event in accepted] == [1, 2]
        assert acknowledgement["event_id"] == accepted[-1].event_id
        assert acknowledgement["offset"] == 1
        assert acknowledgement["partition"] == 0

        async with child(tmp_path, service, "cli-update") as resumed:
            await resumed.wait()
        replayed = await events(observer, service.topic)
        assert len(replayed) == 3
        assert replayed[-1] == replayed[-2] == accepted[-1]
        async with child(tmp_path, service, "cli-update") as unchanged:
            await unchanged.wait()
        assert await events(observer, service.topic) == replayed

    path = tmp_path / "projection.sqlite3"
    projection = Projection(
        path, (service.destination, service.cluster_id, service.topic)
    )
    try:
        assert projection.apply(replayed[0]) == "applied"
        assert projection.apply(replayed[1]) == "applied"
    finally:
        projection.close()
    reopened = Projection(
        path, (service.destination, service.cluster_id, service.topic)
    )
    try:
        assert reopened.apply(replayed[2]) == "duplicate"
    finally:
        reopened.close()


async def test_outage_during_confirmation_expires_and_exits_without_forced_cleanup(
    kafka_service: KafkaService, tmp_path: Path
) -> None:
    service = kafka_service
    snapshot(tmp_path, 1)
    source = tmp_path / SNAPSHOT_NAME
    original_bytes, original_mtime = source.read_bytes(), source.stat().st_mtime_ns
    service.relay.arm()
    metrics_port = _free_port()
    async with child(
        tmp_path, service, "outage", metrics_port=metrics_port
    ) as publisher:
        await asyncio.wait_for(service.relay.produce_seen.wait(), 45)
        container = service.container.get_wrapped_container()
        await asyncio.to_thread(container.pause)
        started = time.monotonic()
        try:
            service.relay.release_produce.set()
            # Keep the broker paused throughout the watchdog: recovery cannot
            # accidentally make an indefinitely draining producer look bounded.
            await publisher.wait(timeout=60)
            result = json.loads(publisher.result.read_text())
            assert result["phase"] == "closed"
            failure = result["failure"].lower().replace(" ", "")
            assert "timeout" in failure or "timedout" in failure, result["failure"]
            assert result["confirmed_event_ids"] == []
            assert len(result["attempted_event_ids"]) == 1
            failed = [
                group
                for name, group in result["stats"].items()
                if name.endswith("process_snapshot")
            ]
            assert len(failed) == 1 and failed[0]["num_errors"] == 1
            assert (
                sum(
                    failed[0][field]
                    for field in (
                        "num_unchanged",
                        "num_adds",
                        "num_deletes",
                        "num_reprocesses",
                    )
                )
                == 0
            )
            assert time.monotonic() - started < 60
            _assert_port_closed(metrics_port)
            async with asyncio.timeout(5):
                while service.relay.handlers:
                    await asyncio.sleep(0.05)
        finally:
            await asyncio.to_thread(container.unpause)
    print(
        f"Kafka outage: normal publisher exit and cleanup in {time.monotonic() - started:.2f}s"
    )

    # An uncertain precommit record must not suppress the unchanged-source retry.
    # Kafka may accept the old timed-out send after resuming, so require a new
    # confirmed publisher call and allow duplicate broker records with the same ID.
    async with child(tmp_path, service, "recover") as recovered:
        await recovered.wait()
        recovery = json.loads(recovered.result.read_text())
        assert recovery["phase"] == "closed"
        assert recovery["attempted_event_ids"] == result["attempted_event_ids"]
        assert recovery["confirmed_event_ids"] == result["attempted_event_ids"]
        assert recovery["failure"] is None
        assert all(group["num_errors"] == 0 for group in recovery["stats"].values())

    async with reader(service) as observer:
        accepted = await events(observer, service.topic)
        assert accepted
        assert all(event == accepted[0] for event in accepted)
        assert accepted[0].event_id == result["attempted_event_ids"][0]
        assert accepted[0].revision == 1 and accepted[0].operation == "upsert"
        assert accepted[0].value_json == canonical_json("A")
        async with child(tmp_path, service, "unchanged") as unchanged:
            await unchanged.wait()
            no_op = json.loads(unchanged.result.read_text())
            assert no_op["phase"] == "closed"
            assert no_op["attempted_event_ids"] == no_op["confirmed_event_ids"] == []
        final_events = await events(observer, service.topic)
        assert final_events[: len(accepted)] == accepted
        assert all(event == accepted[0] for event in final_events)

    assert source.read_bytes() == original_bytes
    assert source.stat().st_mtime_ns == original_mtime
