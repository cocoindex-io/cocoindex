"""Local filesystem source utilities with sync and async support."""

from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator, Iterator
from datetime import datetime
from pathlib import Path

from cocoindex.resources.file import (
    FileLike,
    FilePathMatcher,
    MatchAllFilePathMatcher,
)


class File(FileLike):
    """Represents a file entry from the directory walk."""

    _relative_path: Path
    _base_path: Path
    _stat: os.stat_result

    def __init__(
        self,
        relative_path: Path,
        base_path: Path,
        stat: os.stat_result,
    ) -> None:
        self._relative_path = relative_path
        self._base_path = base_path
        self._stat = stat

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self._stat.st_size

    @property
    def modified_time(self) -> datetime:
        """Return the file modification time as a datetime."""
        seconds, us = divmod(self._stat.st_mtime_ns // 1_000, 1_000_000)
        return datetime.fromtimestamp(seconds).replace(microsecond=us)

    def read(self, size: int = -1) -> bytes:
        """Read and return the file content as bytes.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.

        Returns:
            The file content as bytes.
        """
        path = self._base_path / self._relative_path
        if size < 0:
            return path.read_bytes()
        with path.open("rb") as f:
            return f.read(size)

    @property
    def relative_path(self) -> Path:
        """Return the relative path of the file."""
        return self._relative_path

    @property
    def path(self) -> Path:
        """Return the path of the file."""
        return self._base_path / self._relative_path

    def __coco_memo_key__(self) -> object:
        return (self._base_path, self._relative_path, self.modified_time)


class AsyncFile:
    """Async wrapper around a File that provides async read methods.

    Implements the AsyncFileLike protocol.
    """

    _file: File

    def __init__(self, file: File) -> None:
        self._file = file

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self._file.size

    @property
    def modified_time(self) -> datetime:
        """Return the file modification time as a datetime."""
        return self._file.modified_time

    @property
    def relative_path(self) -> Path:
        """Return the relative path of the file."""
        return self._file.relative_path

    async def read(self, size: int = -1) -> bytes:
        """Asynchronously read and return the file content as bytes.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.

        Returns:
            The file content as bytes.
        """
        return await asyncio.to_thread(self._file.read, size)

    async def read_text(
        self, encoding: str | None = None, errors: str = "replace"
    ) -> str:
        """Asynchronously read and return the file content as text.

        Args:
            encoding: The encoding to use. If None, the encoding is detected automatically
                using BOM detection, falling back to UTF-8.
            errors: The error handling scheme. Common values: 'strict', 'ignore', 'replace'.

        Returns:
            The file content as text.
        """
        return await asyncio.to_thread(self._file.read_text, encoding, errors)


class DirWalker:
    """A directory walker that supports both sync and async iteration.

    Use as a sync iterator to get `File` objects:
        for file in walk_dir(path):
            content = file.read()

    Use as an async iterator to get `AsyncFile` objects:
        async for file in walk_dir(path):
            content = await file.read()
    """

    _path: Path
    _recursive: bool
    _path_matcher: FilePathMatcher

    def __init__(
        self,
        path: str | Path,
        *,
        recursive: bool = False,
        path_matcher: FilePathMatcher | None = None,
    ) -> None:
        self._path = Path(path)
        self._recursive = recursive
        self._path_matcher = path_matcher or MatchAllFilePathMatcher()

    def __iter__(self) -> Iterator[File]:
        """Synchronously iterate over files, yielding File objects."""
        root_path = self._path.resolve()

        if not root_path.is_dir():
            raise ValueError(f"Path is not a directory: {root_path}")

        dirs_to_process: list[Path] = [root_path]

        while dirs_to_process:
            current_dir = dirs_to_process.pop()

            try:
                entries = list(current_dir.iterdir())
            except PermissionError:
                continue

            subdirs: list[Path] = []

            for entry in entries:
                try:
                    relative_path = entry.relative_to(root_path)
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

                    yield File(
                        relative_path=relative_path,
                        base_path=root_path,
                        stat=stat,
                    )

            # Add subdirectories in reverse order to maintain consistent traversal
            dirs_to_process.extend(reversed(subdirs))

    async def __aiter__(self) -> AsyncIterator[AsyncFile]:
        """Asynchronously iterate over files, yielding AsyncFile objects."""
        from cocoindex.connectorkits.async_adpaters import sync_to_async_iter

        async for file in sync_to_async_iter(lambda: iter(self)):
            yield AsyncFile(file)


def walk_dir(
    path: str | Path,
    *,
    recursive: bool = False,
    path_matcher: FilePathMatcher | None = None,
) -> DirWalker:
    """
    Walk through a directory and yield file entries.

    Returns a DirWalker that supports both sync and async iteration:
    - Sync iteration yields `File` objects
    - Async iteration yields `AsyncFile` objects with async read methods

    Args:
        path: The root directory path to walk through.
        recursive: If True, recursively walk subdirectories. If False, only list files
            in the immediate directory.
        path_matcher: Optional file path matcher to filter files and directories.
            If not provided, all files and directories are included.

    Returns:
        A DirWalker that can be used with both `for` and `async for` loops.

    Examples:
        Sync iteration:
            for file in walk_dir("/path/to/dir"):
                content = file.read()

        Async iteration:
            async for file in walk_dir("/path/to/dir"):
                content = await file.read()
    """
    return DirWalker(path, recursive=recursive, path_matcher=path_matcher)


__all__ = ["walk_dir", "File", "AsyncFile", "DirWalker"]
