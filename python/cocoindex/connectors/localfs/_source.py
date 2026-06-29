"""Local filesystem source utilities with async file reading."""

from __future__ import annotations

import asyncio
import datetime
import os
from pathlib import Path
from collections.abc import AsyncIterable as _AsyncIterable
from typing import AsyncIterator, Iterator

import pathlib

from cocoindex.resources import file as _file
from cocoindex.resources.file import (
    FileMetadata,
    FilePathMatcher,
    MatchAllFilePathMatcher,
)

from cocoindex._internal.context_keys import ContextKey
from cocoindex._internal.live_component import LiveMapSubscriber
from cocoindex.connectorkits import SingleWatcherGuard

from ._common import FilePath, to_file_path


def _stat_to_metadata(stat: os.stat_result) -> FileMetadata:
    """Convert an os.stat_result to FileMetadata."""
    seconds, us = divmod(stat.st_mtime_ns // 1_000, 1_000_000)
    mtime = datetime.datetime.fromtimestamp(seconds).replace(microsecond=us)
    return FileMetadata(size=stat.st_size, modified_time=mtime)


class File(_file.FileLike[pathlib.Path]):
    """Represents a file entry from the directory walk.

    Implements ``FileLike`` with async read methods.
    File I/O is performed in a thread pool to avoid blocking the event loop.
    """

    def __init__(
        self,
        file_path: FilePath,
        *,
        _stat: os.stat_result | None = None,
    ) -> None:
        metadata = _stat_to_metadata(_stat) if _stat is not None else None
        super().__init__(file_path, _metadata=metadata)

    async def _fetch_metadata(self) -> FileMetadata:
        """Fetch metadata via os.stat() in a thread pool."""
        stat = await asyncio.to_thread(os.stat, self._file_path.resolve())
        return _stat_to_metadata(stat)

    def _read_sync(self, size: int = -1) -> bytes:
        """Synchronously read file content (internal helper)."""
        path = self._file_path.resolve()
        if size < 0:
            return path.read_bytes()
        with path.open("rb") as f:
            return f.read(size)

    async def _read_impl(self, size: int = -1) -> bytes:
        """Read file content in a thread pool."""
        return await asyncio.to_thread(self._read_sync, size)


_DEFAULT_RESCAN_INTERVAL: datetime.timedelta = datetime.timedelta(hours=1)


class DirWalker:
    """An async directory walker.

    Use as an async iterator to get ``File`` objects::

        async for file in walk_dir(path):
            content = await file.read()

    When ``live=True``, ``items()`` returns a ``LiveMapView`` that supports
    live file watching via ``mount_each()``.
    """

    _root_path: FilePath
    _recursive: bool
    _path_matcher: FilePathMatcher
    _live: bool
    _rescan_interval: datetime.timedelta | None

    def __init__(
        self,
        path: FilePath | Path | ContextKey[Path],
        *,
        live: bool = False,
        recursive: bool = False,
        path_matcher: FilePathMatcher | None = None,
        rescan_interval: datetime.timedelta | None = _DEFAULT_RESCAN_INTERVAL,
    ) -> None:
        self._root_path = to_file_path(path)
        self._live = live
        self._recursive = recursive
        self._path_matcher = path_matcher or MatchAllFilePathMatcher()
        self._rescan_interval = rescan_interval

    def _walk_sync(self) -> Iterator[File]:
        """Synchronously walk the directory and yield File objects (internal helper)."""
        root_resolved = self._root_path.resolve()

        if not root_resolved.is_dir():
            raise ValueError(f"Path is not a directory: {root_resolved}")

        dirs_to_process: list[Path] = [root_resolved]

        while dirs_to_process:
            current_dir = dirs_to_process.pop()

            try:
                entries = list(current_dir.iterdir())
            except PermissionError:
                continue

            subdirs: list[Path] = []

            for entry in entries:
                try:
                    relative_path = entry.relative_to(root_resolved)
                except ValueError:
                    # Should not happen, but skip if it does
                    continue

                if entry.is_dir():
                    if self._recursive and self._path_matcher.is_dir_included(
                        relative_path
                    ):
                        subdirs.append(entry)
                elif entry.is_file():
                    if not self._path_matcher.is_file_included(relative_path):
                        continue

                    # Get file stats
                    try:
                        stat = entry.stat()
                    except OSError:
                        continue

                    # Create FilePath for this file by joining root with relative path
                    file_path = self._root_path / relative_path

                    yield File(file_path, _stat=stat)

            # Add subdirectories in reverse order to maintain consistent traversal
            dirs_to_process.extend(reversed(subdirs))

    def items(self) -> _AsyncIterable[tuple[str, File]]:
        """Return keyed ``(relative_path, File)`` pairs for use with ``mount_each()``.

        When ``live=True``, returns a ``LiveMapView`` that supports live watching.
        Otherwise returns a plain ``AsyncIterable``.
        """
        if self._live:
            return _LiveDirItems(self)
        return self._items_iter()

    async def _items_iter(self) -> AsyncIterator[tuple[str, File]]:
        """Async iterate over (key, file) pairs (catch-up path)."""
        root_path = self._root_path.path
        async for file in self:
            yield (file.file_path.path.relative_to(root_path).as_posix(), file)

    async def __aiter__(self) -> AsyncIterator[File]:
        """Asynchronously iterate over files, yielding File objects."""
        from cocoindex.connectorkits.async_adapters import sync_to_async_iter

        async for file in sync_to_async_iter(lambda: self._walk_sync()):
            yield file


def _stop_observer(observer: object, *, join_timeout: float = 5.0) -> None:
    """Stop and join a watchdog observer.

    All blocking work lives here so callers can offload the entire
    shutdown to a thread via ``asyncio.to_thread(_stop_observer, obs)``.

    TypeError is suppressed because during interpreter shutdown watchdog's
    platform observer may already have its C-library globals torn down
    (e.g. macOS FSEvents calling a function that's been GC'd to None).
    """
    try:
        observer.stop()  # type: ignore[union-attr]
    except TypeError:
        pass
    try:
        observer.join(timeout=join_timeout)  # type: ignore[union-attr]
    except TypeError:
        pass


class _LiveDirItems:
    """``LiveMapView`` returned by ``DirWalker.items()`` when ``live=True``."""

    def __init__(self, walker: DirWalker) -> None:
        self._walker = walker
        self._resolved_root = walker._root_path.resolve()
        self._watch_guard = SingleWatcherGuard("localfs live directory")

    def __aiter__(self) -> AsyncIterator[tuple[str, File]]:
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[tuple[str, File]]:
        async for pair in self._walker._items_iter():
            yield pair

    async def watch(self, subscriber: LiveMapSubscriber[str, File]) -> None:
        """Deliver an initial scan then live filesystem changes to the subscriber."""
        with self._watch_guard:
            await self._watch(subscriber)

    async def _watch(self, subscriber: LiveMapSubscriber[str, File]) -> None:
        from watchdog.events import (
            EVENT_TYPE_CREATED,
            EVENT_TYPE_DELETED,
            EVENT_TYPE_MODIFIED,
            EVENT_TYPE_MOVED,
            FileSystemEvent,
            FileSystemEventHandler,
        )
        from watchdog.observers import Observer

        root_resolved = self._resolved_root
        loop = asyncio.get_running_loop()
        td = self._walker._rescan_interval
        rescan_seconds: float | None = td.total_seconds() if td is not None else None
        # Watchdog dispatches events from a background thread; we forward
        # them into this asyncio.Queue via call_soon_threadsafe.
        events_queue: asyncio.Queue[FileSystemEvent] = asyncio.Queue()

        class _Handler(FileSystemEventHandler):
            def on_any_event(self, event: FileSystemEvent) -> None:
                loop.call_soon_threadsafe(events_queue.put_nowait, event)

        handler = _Handler()

        def _start_observer() -> Observer:
            obs = Observer()
            obs.schedule(handler, str(root_resolved), recursive=self._walker._recursive)
            # Observer.start() is synchronous and arms the OS-level watch
            # (e.g. inotify / FSEvents) before returning, so a scan that
            # follows cannot miss events that occur after this point.
            obs.start()
            return obs

        observer = _start_observer()
        try:
            await subscriber.update_all()
            await subscriber.mark_ready()

            last_rescan = loop.time()

            while True:
                # Compute timeout until next periodic rescan.
                if rescan_seconds is not None:
                    remaining = rescan_seconds - (loop.time() - last_rescan)
                    if remaining <= 0:
                        # Periodic rescan cycle: tear down the old
                        # OS-level watcher, drain stale events, create a
                        # fresh watcher (new FSEvents/inotify stream),
                        # then do a full rescan to catch anything the old
                        # watcher may have silently dropped.
                        await asyncio.to_thread(_stop_observer, observer)

                        while not events_queue.empty():
                            try:
                                events_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                break

                        observer = _start_observer()
                        await subscriber.update_all()
                        last_rescan = loop.time()
                        continue
                    timeout: float | None = remaining
                else:
                    timeout = None

                try:
                    event = await asyncio.wait_for(events_queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    # Rescan interval elapsed; loop back to trigger rescan.
                    continue

                # On a move event with a dest_path, treat as
                # delete(src) + create(dest). All other events carry the
                # affected path in src_path. src_path/dest_path are typed
                # as bytes | str — normalize via fsdecode.
                src_path = os.fsdecode(event.src_path)
                if event.event_type == EVENT_TYPE_MOVED and event.dest_path:
                    paths: list[tuple[str, str]] = [
                        (EVENT_TYPE_DELETED, src_path),
                        (EVENT_TYPE_CREATED, os.fsdecode(event.dest_path)),
                    ]
                else:
                    paths = [(event.event_type, src_path)]

                for kind, raw_path in paths:
                    changed_path = Path(raw_path)
                    try:
                        relative = changed_path.relative_to(root_resolved)
                    except ValueError:
                        continue

                    key = relative.as_posix()

                    if kind == EVENT_TYPE_DELETED:
                        if event.is_directory:
                            # Directory deletes/moves: individual file
                            # events are emitted on most platforms, but
                            # not all. Trigger a rescan as a safety net.
                            await subscriber.update_all()
                            continue
                        if self._walker._path_matcher.is_file_included(relative):
                            handle = await subscriber.delete(key)
                            await handle.ready()
                        continue

                    if kind not in (EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED):
                        continue
                    if event.is_directory:
                        continue
                    if not self._walker._path_matcher.is_file_included(relative):
                        continue

                    file_path = self._walker._root_path / relative
                    try:
                        stat = changed_path.stat()
                    except OSError:
                        continue
                    file = File(file_path, _stat=stat)
                    handle = await subscriber.update(key, file)
                    await handle.ready()
        finally:
            # Offload the blocking stop+join to a thread. During
            # interpreter shutdown the default executor may already be
            # closed, so fall back to a synchronous call (TypeError /
            # RuntimeError are suppressed inside _stop_observer).
            try:
                await asyncio.to_thread(_stop_observer, observer)
            except RuntimeError:
                _stop_observer(observer)


def walk_dir(
    path: FilePath | Path | ContextKey[Path],
    *,
    live: bool = False,
    recursive: bool = False,
    path_matcher: FilePathMatcher | None = None,
    rescan_interval: datetime.timedelta | None = _DEFAULT_RESCAN_INTERVAL,
) -> DirWalker:
    """
    Walk through a directory and yield file entries.

    Returns a ``DirWalker`` that supports async iteration, yielding ``File``
    objects with async read methods.

    Args:
        path: The root directory path to walk through. Can be a FilePath (with stable
            base directory key) or a pathlib.Path (uses CWD as base directory).
        live: If True, ``items()`` returns a ``LiveMapView`` that supports
            live file watching via ``mount_each()``.
        recursive: If True, recursively walk subdirectories. If False, only list files
            in the immediate directory.
        path_matcher: Optional file path matcher to filter files and directories.
            If not provided, all files and directories are included.
        rescan_interval: When ``live=True``, interval between periodic full
            rescans that recreate the OS-level file watcher. Defends against
            platform-specific watcher failures (e.g. macOS FSEvents silently
            stopping). Defaults to 1 hour. Set to ``None`` to disable.
            Ignored when ``live=False``.

    Returns:
        A DirWalker that can be used with ``async for`` loops.
    """
    return DirWalker(
        path,
        live=live,
        recursive=recursive,
        path_matcher=path_matcher,
        rescan_interval=rescan_interval,
    )


__all__ = ["walk_dir", "File", "DirWalker"]
