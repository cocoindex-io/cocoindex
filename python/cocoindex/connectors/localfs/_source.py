"""Local filesystem source utilities with sync and async support."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Iterator

import pathlib

from cocoindex.resources.file import (
    FileLike,
    FilePathMatcher,
    MatchAllFilePathMatcher,
)

from ._common import FilePath, to_file_path


class File(FileLike[pathlib.Path]):
    """Represents a file entry from the directory walk."""

    _file_path: FilePath
    _stat: os.stat_result

    def __init__(
        self,
        file_path: FilePath,
        stat: os.stat_result,
    ) -> None:
        self._file_path = file_path
        self._stat = stat

    @property
    def file_path(self) -> FilePath:
        """Return the FilePath of this file."""
        return self._file_path

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
        path = self._file_path.resolve()
        if size < 0:
            return path.read_bytes()
        with path.open("rb") as f:
            return f.read(size)


class AsyncFile:
    """Async wrapper around a File that provides async read methods.

    Implements the AsyncFileLike protocol.
    """

    _file: File

    def __init__(self, file: File) -> None:
        self._file = file

    @property
    def file_path(self) -> FilePath:
        """Return the FilePath of this file."""
        return self._file.file_path

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self._file.size

    @property
    def modified_time(self) -> datetime:
        """Return the file modification time as a datetime."""
        return self._file.modified_time

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

    _root_path: FilePath
    _recursive: bool
    _path_matcher: FilePathMatcher

    def __init__(
        self,
        path: FilePath | Path,
        *,
        recursive: bool = False,
        path_matcher: FilePathMatcher | None = None,
    ) -> None:
        self._root_path = to_file_path(path)
        self._recursive = recursive
        self._path_matcher = path_matcher or MatchAllFilePathMatcher()

    def __iter__(self) -> Iterator[File]:
        """Synchronously iterate over files, yielding File objects."""
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

                    yield File(
                        file_path=file_path,
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
    path: FilePath | Path,
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
        path: The root directory path to walk through. Can be a FilePath (with stable
            base directory key) or a pathlib.Path (uses CWD as base directory).
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

        With stable base directory:
            source_dir = register_base_dir("source", Path("./data"))
            for file in walk_dir(source_dir):
                # file.file_path has stable memo key based on "source" key
                content = file.read()
    """
    return DirWalker(path, recursive=recursive, path_matcher=path_matcher)


__all__ = ["walk_dir", "File", "AsyncFile", "DirWalker"]
