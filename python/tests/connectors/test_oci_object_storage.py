"""Tests for the OCI Object Storage source connector.

Mocks the ``oci`` SDK via ``sys.modules`` injection — same pattern as
``test_kafka_source.py`` / ``test_kafka_target.py``.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import PurePath
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Mock OCI SDK
# ---------------------------------------------------------------------------


class _MockServiceError(Exception):
    """Mock oci.exceptions.ServiceError."""

    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(f"{status}: {message}")
        self.status = status


class _MockObjectSummary:
    """Mock entry in oci ListObjects response."""

    def __init__(
        self,
        name: str,
        size: int,
        time_modified: datetime,
        md5: str | None = None,
        etag: str | None = None,
    ) -> None:
        self.name = name
        self.size = size
        self.time_modified = time_modified
        self.md5 = md5
        self.etag = etag


class _MockListObjectsData:
    def __init__(
        self, objects: list[_MockObjectSummary], next_start_with: str | None = None
    ) -> None:
        self.objects = objects
        self.next_start_with = next_start_with


class _MockListObjectsResponse:
    def __init__(self, data: _MockListObjectsData) -> None:
        self.data = data


class _MockHeadObjectResponse:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers


class _MockGetObjectResponse:
    def __init__(self, body: bytes) -> None:
        self.data = MagicMock()
        self.data.raw = MagicMock()
        self.data.raw.read = MagicMock(return_value=body)


class MockObjectStorageClient:
    """In-memory OCI Object Storage mock.

    Stores objects keyed by full object name. Tracks call counts for
    ``head_object`` so tests can assert metadata caching.
    """

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, datetime, str]] = {}
        # Per-object 404 simulation (set via ``mark_missing``).
        self._missing: set[str] = set()
        self.head_object_calls: list[str] = []
        self.list_objects_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self._page_size = 1000

    # --- Test setup helpers ---

    def put(
        self,
        object_name: str,
        body: bytes,
        modified: datetime | None = None,
        etag: str = "etag-x",
    ) -> None:
        self._missing.discard(object_name)
        self._objects[object_name] = (
            body,
            modified or datetime(2026, 1, 1, tzinfo=timezone.utc),
            etag,
        )

    def remove(self, object_name: str) -> None:
        self._objects.pop(object_name, None)
        self._missing.add(object_name)

    def set_page_size(self, n: int) -> None:
        self._page_size = n

    # --- Mock SDK surface ---

    def list_objects(self, **kwargs: Any) -> _MockListObjectsResponse:
        self.list_objects_calls.append(kwargs)
        prefix = kwargs.get("prefix") or ""
        start = kwargs.get("start")

        names = sorted(n for n in self._objects if n.startswith(prefix))
        if start is not None:
            names = [n for n in names if n >= start]
        page = names[: self._page_size]
        next_start = names[self._page_size] if len(names) > self._page_size else None

        objects: list[_MockObjectSummary] = []
        for name in page:
            body, modified, etag = self._objects[name]
            objects.append(
                _MockObjectSummary(
                    name=name,
                    size=len(body),
                    time_modified=modified,
                    md5=etag,
                    etag=etag,
                )
            )
        return _MockListObjectsResponse(_MockListObjectsData(objects, next_start))

    def head_object(self, **kwargs: Any) -> _MockHeadObjectResponse:
        name = kwargs["object_name"]
        self.head_object_calls.append(name)
        if name in self._missing or name not in self._objects:
            raise _MockServiceError(404, f"NotFound: {name}")
        body, modified, etag = self._objects[name]
        return _MockHeadObjectResponse(
            headers={
                "content-length": str(len(body)),
                "last-modified": format_datetime(modified),
                "etag": etag,
            }
        )

    def get_object(self, **kwargs: Any) -> _MockGetObjectResponse:
        self.get_object_calls.append(kwargs)
        name = kwargs["object_name"]
        if name in self._missing or name not in self._objects:
            raise _MockServiceError(404, f"NotFound: {name}")
        body, _, _ = self._objects[name]
        rng = kwargs.get("range")
        if rng:
            assert rng.startswith("bytes=0-")
            end = int(rng[len("bytes=0-") :])
            body = body[: end + 1]
        return _MockGetObjectResponse(body)


# ---------------------------------------------------------------------------
# Inject mock oci modules before importing the connector
# ---------------------------------------------------------------------------


_mock_exceptions = MagicMock()
_mock_exceptions.ServiceError = _MockServiceError

_mock_object_storage = MagicMock()
_mock_object_storage.ObjectStorageClient = MockObjectStorageClient

_mock_oci = MagicMock()
_mock_oci.exceptions = _mock_exceptions
_mock_oci.object_storage = _mock_object_storage

sys.modules.setdefault("oci", _mock_oci)
sys.modules.setdefault("oci.exceptions", _mock_exceptions)
sys.modules.setdefault("oci.object_storage", _mock_object_storage)


from cocoindex._internal.live_component import (  # noqa: E402
    _IMMEDIATE_READY,
    LiveMapView,
    LiveStreamSubscriber,
    ReadyAwaitable,
)
from cocoindex.connectors.oci_object_storage import (  # noqa: E402
    OCIFile,
    OCIFilePath,
    OCIWalker,
    get_object,
    list_objects,
    read,
)
from cocoindex.resources.file import PatternFilePathMatcher  # noqa: E402


def _live_items(walker: OCIWalker) -> LiveMapView[str, OCIFile]:
    items = walker.items()
    assert isinstance(items, LiveMapView)
    return items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def oci_client() -> MockObjectStorageClient:
    return MockObjectStorageClient()


def _stage_basic_bucket(client: MockObjectStorageClient) -> None:
    client.put("file1.txt", b"hello")
    client.put("file2.md", b"# Title")
    client.put("data/nested.json", b'{"k": "v"}')
    client.put("data/deep/file.txt", b"deep content")


# ===========================================================================
# Scan path
# ===========================================================================


@pytest.mark.asyncio
async def test_oci_list_objects_basic(oci_client: MockObjectStorageClient) -> None:
    _stage_basic_bucket(oci_client)
    walker = list_objects(oci_client, "ns", "bucket")
    files: list[OCIFile] = []
    async for f in walker:
        files.append(f)

    relative_keys = sorted(f.file_path.path.as_posix() for f in files)
    assert relative_keys == [
        "data/deep/file.txt",
        "data/nested.json",
        "file1.txt",
        "file2.md",
    ]
    for f in files:
        assert isinstance(f.file_path, OCIFilePath)
        assert f.file_path.namespace == "ns"
        assert f.file_path.bucket_name == "bucket"
        # Pre-populated metadata — no head_object call needed.
        assert (await f.size()) > 0
    assert oci_client.head_object_calls == []


@pytest.mark.asyncio
async def test_oci_list_objects_prefix_and_matcher(
    oci_client: MockObjectStorageClient,
) -> None:
    _stage_basic_bucket(oci_client)
    walker = list_objects(
        oci_client,
        "ns",
        "bucket",
        prefix="data/",
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
    )
    files = [f async for f in walker]
    assert len(files) == 1
    f = files[0]
    assert f.file_path.path.as_posix() == "deep/file.txt"
    assert f.file_path.object_name == "data/deep/file.txt"


@pytest.mark.asyncio
async def test_oci_list_objects_max_file_size(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("small.txt", b"x" * 5)
    oci_client.put("large.bin", b"x" * 10_000)
    walker = list_objects(oci_client, "ns", "bucket", max_file_size=100)
    relative = [f.file_path.path.as_posix() async for f in walker]
    assert relative == ["small.txt"]


@pytest.mark.asyncio
async def test_oci_list_objects_pagination(
    oci_client: MockObjectStorageClient,
) -> None:
    for i in range(5):
        oci_client.put(f"obj-{i:02d}.txt", str(i).encode())
    oci_client.set_page_size(2)
    walker = list_objects(oci_client, "ns", "bucket")
    relative = [f.file_path.path.as_posix() async for f in walker]
    assert sorted(relative) == [f"obj-{i:02d}.txt" for i in range(5)]
    # Multi-page paginate.
    assert len(oci_client.list_objects_calls) >= 2


@pytest.mark.asyncio
async def test_oci_get_object_and_read(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("file.txt", b"abcdef")
    f = await get_object(oci_client, "ns", "bucket", "file.txt")
    assert (await f.size()) == 6
    assert (await f.read()) == b"abcdef"

    # Convenience read with size.
    partial = await read(oci_client, "ns", "bucket", "file.txt", size=3)
    assert partial == b"abc"


# ===========================================================================
# OCIFile.exists()
# ===========================================================================


@pytest.mark.asyncio
async def test_oci_exists_true_caches_metadata(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("file.txt", b"hi")
    fp = OCIFilePath("ns", "bucket", "file.txt", object_name="file.txt")
    f = OCIFile(oci_client, fp)
    assert await f.exists() is True
    # size() now uses cached metadata — no second head_object.
    assert (await f.size()) == 2
    assert oci_client.head_object_calls == ["file.txt"]


@pytest.mark.asyncio
async def test_oci_exists_false_on_404(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.remove("missing.txt")
    fp = OCIFilePath("ns", "bucket", "missing.txt", object_name="missing.txt")
    f = OCIFile(oci_client, fp)
    assert await f.exists() is False
    # Second exists() returns cached False without a re-probe.
    assert await f.exists() is False
    assert oci_client.head_object_calls == ["missing.txt"]
    # Bare size() (without prior exists() check) re-probes and re-raises 404.
    f2 = OCIFile(oci_client, fp)
    with pytest.raises(_MockServiceError):
        await f2.size()


# ===========================================================================
# Live view — drain + scan + trigger choreography
# ===========================================================================


class _MockMapSubscriber:
    """Records LiveMapSubscriber calls; auto-resolved handles."""

    def __init__(self) -> None:
        self.update_all_called = False
        self.mark_ready_called = False
        self.updates: list[tuple[str, OCIFile]] = []
        self.deletes: list[str] = []

    async def update_all(self) -> None:
        self.update_all_called = True

    async def mark_ready(self) -> None:
        self.mark_ready_called = True

    async def update(self, key: str, value: OCIFile) -> ReadyAwaitable:
        self.updates.append((key, value))
        return _IMMEDIATE_READY

    async def delete(self, key: str) -> ReadyAwaitable:
        self.deletes.append(key)
        return _IMMEDIATE_READY


_FAR_PAST = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
_FUTURE = datetime(2099, 1, 1, tzinfo=timezone.utc).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event(
    namespace: str,
    bucket: str,
    object_name: str,
    event_type: str = "com.oraclecloud.objectstorage.createobject",
    event_time: str | None = None,
) -> bytes:
    """Build an OCI event payload. ``event_time`` defaults to ``now()``
    so events are post-cutoff by default.
    """
    body: dict[str, Any] = {
        "eventType": event_type,
        "eventTime": event_time if event_time is not None else _now_iso(),
        "data": {
            "resourceName": object_name,
            "additionalDetails": {
                "namespace": namespace,
                "bucketName": bucket,
            },
        },
    }
    return json.dumps(body).encode("utf-8")


class _ManualLiveStream:
    """A LiveStream[bytes] whose event delivery is driven by tests.

    The OCI adapter ignores the stream's ``mark_ready`` (round-6 design), so
    this mock no longer exposes a helper for it. Tests drive ordering by
    awaiting ``stream.send_event`` and observing adapter side effects.
    """

    def __init__(self) -> None:
        self.subscriber: LiveStreamSubscriber[bytes] | None = None
        self.send_results: list[Any] = []
        self.watch_started = asyncio.Event()
        self._end = asyncio.Event()

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        self.subscriber = subscriber
        self.watch_started.set()
        await self._end.wait()

    async def send_event(self, payload: bytes) -> None:
        assert self.subscriber is not None
        result = await self.subscriber.send(payload)
        self.send_results.append(result)

    def end(self) -> None:
        self._end.set()


async def _drive_to_ready(stream: _ManualLiveStream, sub: "_MockMapSubscriber") -> None:
    """Wait for stream watch to start AND for the adapter to be ready
    (scan + map_sub.mark_ready both done).
    """
    await stream.watch_started.wait()
    while not sub.mark_ready_called:
        await asyncio.sleep(0)


# --- Test cases ---


@pytest.mark.asyncio
async def test_oci_live_view_event_before_cutoff_dropped(
    oci_client: MockObjectStorageClient,
) -> None:
    """Events whose eventTime predates the cutoff are discarded (no HEAD)."""
    oci_client.put("a.txt", b"a")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    head_count_before = len(oci_client.head_object_calls)
    await stream.send_event(_event("ns", "bucket", "a.txt", event_time=_FAR_PAST))
    assert stream.send_results[-1] is _IMMEDIATE_READY
    assert oci_client.head_object_calls[head_count_before:] == []
    assert sub.updates == [] and sub.deletes == []

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_event_after_cutoff_blocks_until_ready(
    oci_client: MockObjectStorageClient,
) -> None:
    """A post-cutoff event arriving during the scan blocks on _ready_complete,
    then dispatches once the scan + parent mark_ready have completed.
    """
    oci_client.put("present.txt", b"data")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    scan_gate = asyncio.Event()

    class _SlowSub(_MockMapSubscriber):
        async def update_all(self) -> None:
            await scan_gate.wait()
            await super().update_all()

    sub = _SlowSub()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await stream.watch_started.wait()

    # Send a post-cutoff event before the scan completes — send() blocks
    # inside _ready_complete.wait().
    send_task = asyncio.create_task(
        stream.subscriber.send(_event("ns", "bucket", "present.txt"))  # type: ignore[union-attr]
    )
    for _ in range(5):
        await asyncio.sleep(0)
    assert not send_task.done()  # parked on the barrier
    assert sub.updates == []  # not yet dispatched

    # Release the scan; this lets watch() reach mark_ready_complete().
    scan_gate.set()
    await send_task
    assert sub.update_all_called
    assert sub.mark_ready_called
    assert sub.updates and sub.updates[-1][0] == "present.txt"

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_unparseable_event_time_falls_through(
    oci_client: MockObjectStorageClient,
) -> None:
    """Missing / unparseable eventTime → not dropped; processed in trigger mode."""
    oci_client.put("file.txt", b"x")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    # Missing eventTime field.
    payload_missing = json.dumps(
        {
            "eventType": "com.oraclecloud.objectstorage.createobject",
            "data": {
                "resourceName": "file.txt",
                "additionalDetails": {"namespace": "ns", "bucketName": "bucket"},
            },
        }
    ).encode("utf-8")
    await stream.send_event(payload_missing)
    assert sub.updates and sub.updates[-1][0] == "file.txt"

    # Unparseable eventTime string.
    sub.updates.clear()
    await stream.send_event(
        _event("ns", "bucket", "file.txt", event_time="not-a-timestamp")
    )
    assert sub.updates and sub.updates[-1][0] == "file.txt"

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_future_event_time_processed(
    oci_client: MockObjectStorageClient,
) -> None:
    """Future-dated eventTime (clock skew) is treated as post-cutoff."""
    oci_client.put("file.txt", b"x")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(_event("ns", "bucket", "file.txt", event_time=_FUTURE))
    assert sub.updates and sub.updates[-1][0] == "file.txt"

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_ready_complete_set_after_mark_ready(
    oci_client: MockObjectStorageClient,
) -> None:
    """`_ready_complete` is set strictly after both `update_all` and `map_sub.mark_ready` complete."""
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    observations: list[tuple[str, bool, bool]] = []

    class _ObservingSub(_MockMapSubscriber):
        async def update_all(self) -> None:
            observations.append(("update_all_enter", False, False))
            await super().update_all()
            observations.append(
                ("update_all_exit", self.update_all_called, self.mark_ready_called)
            )

        async def mark_ready(self) -> None:
            observations.append(
                ("mark_ready_enter", self.update_all_called, self.mark_ready_called)
            )
            await super().mark_ready()
            observations.append(
                ("mark_ready_exit", self.update_all_called, self.mark_ready_called)
            )

    sub = _ObservingSub()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    # Sequence: update_all_enter → update_all_exit → mark_ready_enter → mark_ready_exit
    assert [name for name, _, _ in observations] == [
        "update_all_enter",
        "update_all_exit",
        "mark_ready_enter",
        "mark_ready_exit",
    ]
    # When mark_ready_exit was observed, both flags were True.
    last = observations[-1]
    assert last == ("mark_ready_exit", True, True)

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_trigger_create_existing_object(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("present.txt", b"data")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(_event("ns", "bucket", "present.txt"))
    assert sub.updates and sub.updates[-1][0] == "present.txt"
    assert sub.deletes == []

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_trigger_event_for_deleted_object(
    oci_client: MockObjectStorageClient,
) -> None:
    """Re-read trumps event type: a 'createobject' for a missing object → delete."""
    oci_client.remove("gone.txt")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(
        _event(
            "ns",
            "bucket",
            "gone.txt",
            event_type="com.oraclecloud.objectstorage.createobject",
        )
    )
    assert sub.deletes == ["gone.txt"]
    assert sub.updates == []

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_cross_bucket_event_filtered(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("file.txt", b"x")
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    head_count_before = len(oci_client.head_object_calls)
    await stream.send_event(_event("ns", "OTHER-BUCKET", "file.txt"))
    assert stream.send_results[-1] is _IMMEDIATE_READY
    assert oci_client.head_object_calls[head_count_before:] == []
    assert sub.updates == [] and sub.deletes == []

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_max_file_size_filters_via_size(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("big.bin", b"x" * 10_000)
    stream = _ManualLiveStream()
    walker = list_objects(
        oci_client, "ns", "bucket", max_file_size=100, live_stream=stream
    )

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(_event("ns", "bucket", "big.bin"))
    assert stream.send_results[-1] is _IMMEDIATE_READY
    assert sub.updates == []
    # exists() + size() share one head_object call (cached).
    assert oci_client.head_object_calls.count("big.bin") == 1

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_transient_service_error_skips(
    oci_client: MockObjectStorageClient,
) -> None:
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    original_head = oci_client.head_object

    def _flaky_head(**kwargs: Any) -> _MockHeadObjectResponse:
        oci_client.head_object_calls.append(kwargs["object_name"])
        raise _MockServiceError(500, "InternalError")

    oci_client.head_object = _flaky_head  # type: ignore[method-assign]
    await stream.send_event(_event("ns", "bucket", "any.txt"))
    assert stream.send_results[-1] is _IMMEDIATE_READY
    assert sub.updates == [] and sub.deletes == []

    oci_client.head_object = original_head  # type: ignore[method-assign]
    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_cancel_while_send_blocked_on_ready(
    oci_client: MockObjectStorageClient,
) -> None:
    """Cancelling the outer watch with a send() blocked on _ready_complete is safe.

    The send() call happens inside the stream's watch task (modelling real
    Kafka stream behavior where send() is awaited inline in the poll loop).
    Cancelling the outer watch cancels the stream task, which propagates
    CancelledError into the in-flight send().
    """
    scan_gate = asyncio.Event()

    class _SlowSub(_MockMapSubscriber):
        async def update_all(self) -> None:
            await scan_gate.wait()
            await super().update_all()

    class _SendingStream:
        """Simulates a stream that delivers one post-cutoff event inline."""

        def __init__(self) -> None:
            self.subscriber: LiveStreamSubscriber[bytes] | None = None
            self.send_started = asyncio.Event()

        async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
            self.subscriber = subscriber
            self.send_started.set()
            # send() will block on _ready_complete inside the adapter.
            await subscriber.send(_event("ns", "bucket", "x.txt"))

    stream = _SendingStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)
    sub = _SlowSub()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await stream.send_started.wait()
    # Yield to let send() reach the _ready_complete.wait().
    for _ in range(5):
        await asyncio.sleep(0)

    # Cancel the outer watch; release scan to let unwind progress.
    watch_task.cancel()
    scan_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await watch_task
    # No assertion-level fanout; the test passes if unwinding completes
    # without hanging or raising.


@pytest.mark.asyncio
async def test_oci_live_view_scan_failure_propagates(
    oci_client: MockObjectStorageClient,
) -> None:
    stream = _ManualLiveStream()

    class _FailingSub(_MockMapSubscriber):
        async def update_all(self) -> None:
            raise RuntimeError("scan boom")

    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)
    sub = _FailingSub()
    items = _live_items(walker)
    with pytest.raises(RuntimeError, match="scan boom"):
        await items.watch(sub)  # type: ignore[arg-type]


# ===========================================================================
# Event parsing (additional coverage)
# ===========================================================================


@pytest.mark.asyncio
async def test_oci_live_view_malformed_event_skipped(
    oci_client: MockObjectStorageClient,
) -> None:
    stream = _ManualLiveStream()
    walker = list_objects(oci_client, "ns", "bucket", live_stream=stream)

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(b"{not-json")
    await stream.send_event(json.dumps({"eventType": "x"}).encode("utf-8"))
    await stream.send_event(_event("ns", "bucket", "x.txt", event_type="other"))

    assert sub.updates == [] and sub.deletes == []
    for r in stream.send_results[-3:]:
        assert r is _IMMEDIATE_READY

    stream.end()
    await watch_task


@pytest.mark.asyncio
async def test_oci_live_view_path_matcher_filters(
    oci_client: MockObjectStorageClient,
) -> None:
    oci_client.put("a.json", b"{}")
    stream = _ManualLiveStream()
    walker = list_objects(
        oci_client,
        "ns",
        "bucket",
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
        live_stream=stream,
    )

    sub = _MockMapSubscriber()
    items = _live_items(walker)
    watch_task = asyncio.create_task(items.watch(sub))  # type: ignore[arg-type]

    await _drive_to_ready(stream, sub)

    await stream.send_event(_event("ns", "bucket", "a.json"))
    assert stream.send_results[-1] is _IMMEDIATE_READY
    assert sub.updates == [] and sub.deletes == []

    stream.end()
    await watch_task


# ---------------------------------------------------------------------------
# OCIFilePath sanity
# ---------------------------------------------------------------------------


def test_oci_file_path_memo_key() -> None:
    fp = OCIFilePath("ns", "b", "rel/x.txt", object_name="prefix/rel/x.txt")
    assert fp.__coco_memo_key__() == ("ns", "b", PurePath("rel/x.txt"))
    assert fp.namespace == "ns"
    assert fp.bucket_name == "b"
    assert fp.object_name == "prefix/rel/x.txt"
    assert fp.resolve() == "prefix/rel/x.txt"


# ===========================================================================
# End-to-end integration with the LiveComponent machinery
# ===========================================================================


import cocoindex as coco  # noqa: E402

from tests import common  # noqa: E402
from tests.common.target_states import (  # noqa: E402
    DictDataWithPrev,
    GlobalDictTarget,
)


_oci_e2e_env = common.create_test_env(__file__)


def _declare_oci_file(oci_file: OCIFile) -> None:
    """Per-item processor for the E2E mount_each.

    Stashes object_name length into the global dict target keyed by relative path.
    """
    coco.declare_target_state(
        GlobalDictTarget.target_state(
            oci_file.file_path.path.as_posix(),
            len(oci_file.file_path.object_name),
        )
    )


def test_oci_walker_scan_through_real_mount_each() -> None:
    """E2E: walker.items() drives mount_each through the real LiveComponent path."""
    GlobalDictTarget.store.clear()
    client = MockObjectStorageClient()
    client.put("file1.txt", b"hello")
    client.put("data/nested.json", b"{}")
    client.put("data/deep/file.txt", b"deep")

    walker = list_objects(client, "ns", "bucket")

    async def _main() -> None:
        # walker.items() returns a plain async iterable when no live_stream is set.
        await coco.mount_each(_declare_oci_file, walker.items())  # type: ignore[call-overload]

    app = coco.App(
        coco.AppConfig(name="test_oci_e2e_scan", environment=_oci_e2e_env),
        _main,
    )
    app.update_blocking()

    # All three relative keys appear in the target.
    assert sorted(GlobalDictTarget.store.data.keys()) == [
        "data/deep/file.txt",
        "data/nested.json",
        "file1.txt",
    ]
    # Each value records the length of the full OCI object_name.
    assert GlobalDictTarget.store.data["file1.txt"] == DictDataWithPrev(
        data=len("file1.txt"), prev=[], prev_may_be_missing=True
    )
