"""Local filesystem source synchronous utilities."""

import os
from typing import Iterator
from datetime import datetime
from pathlib import Path

from cocoindex.resources.files import (
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


def walk_dir(
    path: str | Path,
    *,
    recursive: bool = False,
    path_matcher: FilePathMatcher | None = None,
) -> Iterator[File]:
    """
    Walk through a directory and yield file entries.

    Args:
        path: The root directory path to walk through.
        recursive: If True, recursively walk subdirectories. If False, only list files
            in the immediate directory.
        path_matcher: Optional file path matcher to filter files and directories.
            If not provided, all files and directories are included.

    Yields:
        File objects containing relative path and file stats.
    """
    root_path = Path(path).resolve()

    if not root_path.is_dir():
        raise ValueError(f"Path is not a directory: {root_path}")

    if path_matcher is None:
        path_matcher = MatchAllFilePathMatcher()

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
                if recursive and path_matcher.is_dir_included(relative_path):
                    subdirs.append(entry)
            elif entry.is_file():
                if not path_matcher.is_file_included(relative_path):
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


__all__ = ["walk_dir", "File"]
