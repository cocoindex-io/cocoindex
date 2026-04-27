"""Oracle Cloud Infrastructure (OCI) Object Storage source utilities.

Listing, reading, and live-watching objects from an OCI Object Storage bucket.

Live mode is opted into by passing a ``LiveStream[bytes]`` (typically from
:func:`cocoindex.connectors.kafka.topic_as_stream(...).payloads()`) when
constructing the walker. With a live stream, ``OCIWalker.items()`` returns a
``LiveMapView`` that performs an initial scan + watches OCI Object Storage
events delivered via OCI Streaming.
"""

from __future__ import annotations

__all__ = [
    "OCIFile",
    "OCIFilePath",
    "OCIWalker",
    "get_object",
    "list_objects",
    "read",
]

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any, Iterator, cast

try:
    from oci.exceptions import ServiceError  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "oci is required to use the Oracle Cloud Infrastructure Object "
        "Storage source connector. Please install cocoindex[oci]."
    ) from e

from cocoindex._internal.live_component import (
    _IMMEDIATE_READY,
    LiveMapSubscriber,
    LiveStream,
    ReadyAwaitable,
)
from cocoindex.resources import file

_logger = logging.getLogger(__name__)

_OBJECT_EVENT_PREFIX = "com.oraclecloud.objectstorage."

# Wall-clock tolerance for the event-time cutoff. Events with eventTime older
# than this window before _LiveOCIItems.watch() starts are dropped — the scan
# is authoritative for their state. Hardcoded; promote to a kwarg only if a
# concrete need emerges (per CLAUDE.md "Minimize API surface").
_SKEW_TOLERANCE = timedelta(seconds=5)


def _parse_event_time(s: Any) -> datetime | None:
    """Parse an OCI event ``eventTime`` (ISO-8601). Returns ``None`` on
    missing / unparseable, which the cutoff treats as "fall through (do not drop)".
    """
    if not isinstance(s, str):
        return None
    # OCI uses trailing 'Z'; fromisoformat (3.11+) accepts it but normalize to be safe.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


class OCIFilePath(file.FilePath[str]):
    """File path for OCI Object Storage objects.

    Holds the namespace, bucket name, and full object name. ``resolve()``
    returns the full object name (the value passed to ``head_object`` /
    ``get_object``), while ``path`` is the prefix-stripped relative form
    used for memoization. For a walker with prefix ``"logs/"`` and object
    ``"logs/a.txt"``, ``object_name == "logs/a.txt"`` and
    ``path == PurePath("a.txt")``.
    """

    __slots__ = ("_namespace", "_bucket_name", "_object_name")

    _namespace: str
    _bucket_name: str
    _object_name: str

    def __init__(
        self,
        namespace: str,
        bucket_name: str,
        path: str | PurePath,
        *,
        object_name: str,
    ) -> None:
        super().__init__(None, PurePath(path))
        self._namespace = namespace
        self._bucket_name = bucket_name
        self._object_name = object_name

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def object_name(self) -> str:
        return self._object_name

    def resolve(self) -> str:
        """Return the full OCI object name."""
        return self._object_name

    def _with_path(self, path: PurePath) -> OCIFilePath:
        return type(self)(
            self._namespace,
            self._bucket_name,
            path,
            object_name=self._object_name,
        )

    def __coco_memo_key__(self) -> object:
        return (self._namespace, self._bucket_name, self._path)


class OCIFile(file.FileLike[str]):
    """An OCI Object Storage object exposed as a :class:`FileLike`.

    Calls into the (synchronous) ``oci`` SDK are wrapped with
    :func:`asyncio.to_thread`. Metadata (size, modified time, ETag) is fetched
    lazily via ``head_object`` and cached per-instance.

    ``exists()`` is the recommended way to probe presence in the live-event
    path: a True verdict caches both ``_exists`` and ``FileMetadata``; a False
    verdict caches only ``_exists``.
    """

    _file_path: OCIFilePath
    _client: Any
    _exists: bool | None

    def __init__(
        self,
        client: Any,
        file_path: OCIFilePath,
        *,
        _metadata: file.FileMetadata | None = None,
    ) -> None:
        super().__init__(file_path, _metadata=_metadata)
        self._client = client
        self._exists = None if _metadata is None else True

    @property
    def file_path(self) -> OCIFilePath:  # narrowed return type from FileLike
        return self._file_path

    async def _fetch_metadata(self) -> file.FileMetadata:
        head = await asyncio.to_thread(
            self._client.head_object,
            namespace_name=self._file_path.namespace,
            bucket_name=self._file_path.bucket_name,
            object_name=self._file_path.object_name,
        )
        headers = head.headers
        return _metadata_from_head(headers)

    async def _read_impl(self, size: int = -1) -> bytes:
        kwargs: dict[str, Any] = {
            "namespace_name": self._file_path.namespace,
            "bucket_name": self._file_path.bucket_name,
            "object_name": self._file_path.object_name,
        }
        if size >= 0:
            kwargs["range"] = f"bytes=0-{size - 1}"

        def _fetch() -> bytes:
            response = self._client.get_object(**kwargs)
            return cast(bytes, response.data.raw.read())

        return await asyncio.to_thread(_fetch)

    async def exists(self) -> bool:
        """Whether this object currently exists in OCI.

        Performs a ``head_object`` on first call; the True/False verdict is
        cached in ``_exists`` for the lifetime of this :class:`OCIFile`
        instance (not refreshed — construct a new :class:`OCIFile` for a
        fresh probe). A True result also caches the :class:`FileMetadata`
        on :class:`FileLike` (so ``size()``/``read()`` do not re-probe);
        a False result does not cache metadata — a subsequent bare
        ``size()``/``read()`` without ``exists()`` re-probes and re-raises
        the 404. Expected use: call ``exists()`` first, branch on the result.
        """
        if self._exists is None:
            try:
                await self.size()
            except ServiceError as e:
                if e.status == 404:
                    self._exists = False
                    return False
                raise
            self._exists = True
        return self._exists


def _metadata_from_head(headers: Any) -> file.FileMetadata:
    """Map OCI head_object response headers to :class:`FileMetadata`."""
    from email.utils import parsedate_to_datetime

    size = int(headers["content-length"])
    last_modified_str = headers["last-modified"]
    modified_time = parsedate_to_datetime(last_modified_str)
    etag = headers.get("etag")
    fingerprint = etag.encode("utf-8") if isinstance(etag, str) else None
    return file.FileMetadata(
        size=size,
        modified_time=modified_time,
        content_fingerprint=fingerprint,
    )


def _metadata_from_summary(summary: Any) -> file.FileMetadata:
    """Map an OCI ListObjects entry to :class:`FileMetadata`."""
    fingerprint: bytes | None = None
    md5 = getattr(summary, "md5", None)
    if isinstance(md5, str) and md5:
        fingerprint = md5.encode("utf-8")
    elif isinstance(md5, (bytes, bytearray)) and md5:
        fingerprint = bytes(md5)
    return file.FileMetadata(
        size=int(summary.size),
        modified_time=summary.time_modified,
        content_fingerprint=fingerprint,
    )


class OCIWalker:
    """An async walker over an OCI Object Storage bucket.

    Async iteration yields :class:`OCIFile` objects. When ``live_stream`` is
    provided, ``items()`` returns a ``LiveMapView`` that performs an initial
    scan and continues watching for changes via the supplied stream.
    """

    _client: Any
    _namespace: str
    _bucket_name: str
    _prefix: str
    _path_matcher: file.FilePathMatcher
    _max_file_size: int | None
    _live_stream: LiveStream[bytes] | None

    def __init__(
        self,
        client: Any,
        namespace: str,
        bucket_name: str,
        *,
        prefix: str = "",
        path_matcher: file.FilePathMatcher | None = None,
        max_file_size: int | None = None,
        live_stream: LiveStream[bytes] | None = None,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._bucket_name = bucket_name
        self._prefix = prefix
        self._path_matcher = path_matcher or file.MatchAllFilePathMatcher()
        self._max_file_size = max_file_size
        self._live_stream = live_stream

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def _walk_sync(self) -> Iterator[OCIFile]:
        """Synchronously paginate ListObjects and yield matching OCIFile objects."""
        start: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "namespace_name": self._namespace,
                "bucket_name": self._bucket_name,
                "fields": "name,size,md5,timeModified,etag",
            }
            if self._prefix:
                kwargs["prefix"] = self._prefix
            if start is not None:
                kwargs["start"] = start

            response = self._client.list_objects(**kwargs)
            data = response.data
            for summary in data.objects:
                full_name: str = summary.name
                if full_name.endswith("/"):
                    continue
                if self._prefix and full_name.startswith(self._prefix):
                    relative_key = full_name[len(self._prefix) :]
                else:
                    relative_key = full_name
                relative_key = relative_key.lstrip("/")
                if not relative_key:
                    continue

                relative_path = PurePath(relative_key)
                if not self._path_matcher.is_file_included(relative_path):
                    continue

                size = int(summary.size)
                if self._max_file_size is not None and size > self._max_file_size:
                    continue

                file_path = OCIFilePath(
                    self._namespace,
                    self._bucket_name,
                    relative_key,
                    object_name=full_name,
                )
                metadata = _metadata_from_summary(summary)
                yield OCIFile(self._client, file_path, _metadata=metadata)

            next_start = getattr(data, "next_start_with", None)
            if not next_start:
                return
            start = next_start

    async def __aiter__(self) -> AsyncIterator[OCIFile]:
        from cocoindex.connectorkits.async_adapters import sync_to_async_iter

        async for f in sync_to_async_iter(lambda: self._walk_sync()):
            yield f

    def items(self) -> AsyncIterable[tuple[str, OCIFile]]:
        """Return ``(relative_key, OCIFile)`` pairs for use with ``mount_each``.

        With ``live_stream`` set, returns a ``LiveMapView`` that supports
        live watching. Otherwise returns a plain async iterator.
        """
        if self._live_stream is not None:
            return _LiveOCIItems(self, self._live_stream)
        return self._items_iter()

    async def _items_iter(self) -> AsyncIterator[tuple[str, OCIFile]]:
        async for f in self:
            yield (f.file_path.path.as_posix(), f)


class _LiveOCIItems:
    """``LiveMapView[str, OCIFile]`` for an :class:`OCIWalker` with live stream.

    Choreography:

    1. Snapshot ``cutoff = now() - _SKEW_TOLERANCE`` before any side effects.
    2. Spawn the stream task (it begins delivering events to the adapter).
    3. Run the full scan via ``map_sub.update_all()``.
    4. Signal parent readiness via ``map_sub.mark_ready()`` (which blocks until
       the controller's ``update_full`` has fully committed).
    5. Set ``adapter.mark_ready_complete()`` — unblocks any post-cutoff
       ``send()`` calls parked on ``_ready_complete``.
    6. Await the stream task; it runs until cancellation.
    """

    __slots__ = ("_walker", "_live_stream")

    def __init__(self, walker: OCIWalker, live_stream: LiveStream[bytes]) -> None:
        self._walker = walker
        self._live_stream = live_stream

    def __aiter__(self) -> AsyncIterator[tuple[str, OCIFile]]:
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[tuple[str, OCIFile]]:
        async for pair in self._walker._items_iter():
            yield pair

    async def watch(self, subscriber: LiveMapSubscriber[str, OCIFile]) -> None:
        cutoff = datetime.now(timezone.utc) - _SKEW_TOLERANCE
        adapter = _OCIStreamSubscriber(self._walker, subscriber, cutoff)
        stream_task = asyncio.create_task(self._live_stream.watch(adapter))
        try:
            await subscriber.update_all()
            await subscriber.mark_ready()
            adapter.mark_ready_complete()
            await stream_task
        finally:
            stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stream_task


class _OCIStreamSubscriber:
    """``LiveStreamSubscriber[bytes]`` driving live OCI map updates.

    Each event flows through ``send()`` with these gates (in order):

    1. Parse JSON and extract ``eventType`` / ``eventTime`` / ``resourceName`` /
       ``additionalDetails.{namespace, bucketName}``.
    2. Envelope filter — ``eventType`` must start with
       ``com.oraclecloud.objectstorage.`` (event types outside this namespace
       are silently dropped). Note: this is an envelope filter, not a dispatch
       signal — for accepted events we re-read OCI authoritatively via
       ``OCIFile.exists()``, regardless of create/update/delete event type.
    3. Namespace + bucket filter (cross-bucket events on a shared topic).
    4. **Event-time cutoff.** ``event_time < self._cutoff`` → drop (the scan
       covers this state). Missing / unparseable / future-dated events fall
       through to be processed.
    5. Prefix + path-matcher filter.
    6. **Block on ``self._ready_complete``** — ensures dispatch happens
       strictly after both the scan's ``update_full`` has committed and
       ``map_sub.mark_ready()`` has returned. The blocking inline (combined
       with the inline HEAD + dispatch below) is the back-pressure mechanism:
       at most one event in flight at a time, bounding adapter memory to O(1).
    7. Construct ``OCIFile`` and re-read via ``exists()`` → dispatch
       ``map_sub.delete`` (404) or (optional ``max_file_size`` gate +)
       ``map_sub.update``.

    Transient ``ServiceError`` (non-404) on the HEAD path is logged and
    converted to ``_IMMEDIATE_READY`` so the stream continues. Other
    exceptions propagate, terminating the stream task.
    """

    __slots__ = ("_walker", "_map_sub", "_cutoff", "_ready_complete")

    def __init__(
        self,
        walker: OCIWalker,
        map_sub: LiveMapSubscriber[str, OCIFile],
        cutoff: datetime,
    ) -> None:
        self._walker = walker
        self._map_sub = map_sub
        self._cutoff = cutoff
        self._ready_complete = asyncio.Event()

    def mark_ready_complete(self) -> None:
        """Called by ``_LiveOCIItems.watch()`` after the scan and parent
        ``mark_ready()`` complete. Unblocks ``send()`` calls awaiting the barrier.
        """
        self._ready_complete.set()

    async def mark_ready(self) -> None:
        """No-op. The OCI adapter does not consume the stream's readiness
        signal; readiness is driven by ``_LiveOCIItems.watch()`` after the
        scan. This method exists only to satisfy ``LiveStreamSubscriber``.
        """

    async def send(self, message: bytes) -> ReadyAwaitable:
        try:
            event = json.loads(message)
            event_type = event["eventType"]
            event_time_raw = event.get("eventTime")
            data = event["data"]
            resource_name = data["resourceName"]
            additional = data["additionalDetails"]
            namespace = additional["namespace"]
            bucket_name = additional["bucketName"]
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            _logger.warning("Skipping malformed OCI event: %s", e)
            return _IMMEDIATE_READY

        # Envelope filter
        if not isinstance(event_type, str) or not event_type.startswith(
            _OBJECT_EVENT_PREFIX
        ):
            return _IMMEDIATE_READY

        # Namespace + bucket filter
        if (
            namespace != self._walker.namespace
            or bucket_name != self._walker.bucket_name
        ):
            return _IMMEDIATE_READY

        # Event-time cutoff. Missing / unparseable / future-dated → fall through.
        event_time = _parse_event_time(event_time_raw)
        if event_time is not None and event_time < self._cutoff:
            return _IMMEDIATE_READY

        # Prefix + path-matcher filter
        prefix = self._walker._prefix
        if prefix and not resource_name.startswith(prefix):
            return _IMMEDIATE_READY
        relative_key = (
            resource_name[len(prefix) :] if prefix else resource_name
        ).lstrip("/")
        if not relative_key:
            return _IMMEDIATE_READY
        if not self._walker._path_matcher.is_file_included(PurePath(relative_key)):
            return _IMMEDIATE_READY

        # Back-pressure: block until scan + parent mark_ready have completed.
        await self._ready_complete.wait()

        oci_file = OCIFile(
            self._walker._client,
            OCIFilePath(
                self._walker.namespace,
                self._walker.bucket_name,
                relative_key,
                object_name=resource_name,
            ),
        )
        try:
            present = await oci_file.exists()
        except ServiceError as e:
            _logger.warning(
                "OCI HEAD failed for %s/%s/%s: status=%s; skipping event",
                self._walker.namespace,
                self._walker.bucket_name,
                resource_name,
                getattr(e, "status", "?"),
            )
            return _IMMEDIATE_READY

        if not present:
            return await self._map_sub.delete(relative_key)

        max_size = self._walker._max_file_size
        if max_size is not None:
            try:
                size = await oci_file.size()
            except ServiceError as e:
                _logger.warning(
                    "OCI size probe failed for %s: status=%s",
                    resource_name,
                    getattr(e, "status", "?"),
                )
                return _IMMEDIATE_READY
            if size > max_size:
                return _IMMEDIATE_READY

        return await self._map_sub.update(relative_key, oci_file)


# ---------------------------------------------------------------------------
# Top-level convenience API
# ---------------------------------------------------------------------------


async def get_object(
    client: Any,
    namespace: str,
    bucket_name: str,
    object_name: str,
) -> OCIFile:
    """Fetch an :class:`OCIFile` for a single object, populating its metadata."""
    file_path = OCIFilePath(
        namespace, bucket_name, object_name, object_name=object_name
    )
    oci_file = OCIFile(client, file_path)
    # Force metadata fetch.
    await oci_file.size()
    return oci_file


async def read(
    client: Any,
    namespace: str,
    bucket_name: str,
    object_name: str,
    size: int = -1,
) -> bytes:
    """Read object content directly without first fetching metadata.

    Args:
        client: An ``oci.object_storage.ObjectStorageClient``.
        namespace: OCI namespace.
        bucket_name: Bucket name.
        object_name: Full object name.
        size: Number of bytes to read. If -1 (default), read the entire object.
    """
    kwargs: dict[str, Any] = {
        "namespace_name": namespace,
        "bucket_name": bucket_name,
        "object_name": object_name,
    }
    if size >= 0:
        kwargs["range"] = f"bytes=0-{size - 1}"

    def _fetch() -> bytes:
        response = client.get_object(**kwargs)
        return cast(bytes, response.data.raw.read())

    return await asyncio.to_thread(_fetch)


def list_objects(
    client: Any,
    namespace: str,
    bucket_name: str,
    *,
    prefix: str = "",
    path_matcher: file.FilePathMatcher | None = None,
    max_file_size: int | None = None,
    live_stream: LiveStream[bytes] | None = None,
) -> OCIWalker:
    """List objects in an OCI bucket and yield file entries.

    Returns an :class:`OCIWalker` that supports async iteration. With
    ``live_stream`` provided (typically a Kafka topic stream over OCI Streaming
    consumed via ``topic_as_stream(...).payloads()``), ``walker.items()``
    returns a ``LiveMapView`` for live watching via ``mount_each``.

    Args:
        client: An ``oci.object_storage.ObjectStorageClient``.
        namespace: OCI namespace.
        bucket_name: Bucket name.
        prefix: Only list objects whose name starts with this prefix.
        path_matcher: Optional file path matcher (matched against the relative
            path, after prefix stripping).
        max_file_size: Skip objects larger than this size in bytes.
        live_stream: Optional ``LiveStream[bytes]`` of OCI Object Storage events.
    """
    return OCIWalker(
        client,
        namespace,
        bucket_name,
        prefix=prefix,
        path_matcher=path_matcher,
        max_file_size=max_file_size,
        live_stream=live_stream,
    )
