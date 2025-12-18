"""File-related protocols and utilities."""

import codecs
from datetime import datetime
from pathlib import PurePath
from typing import Protocol, runtime_checkable


@runtime_checkable
class FileLike(Protocol):
    """Protocol for file-like objects with path, size, modified time, and read capability."""

    @property
    def relative_path(self) -> PurePath:
        """Return the relative path of the file."""
        ...

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        ...

    @property
    def modified_time(self) -> datetime:
        """Return the file modification time."""
        ...

    def read(self, size: int = -1) -> bytes:
        """Read and return the file content as bytes.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.

        Returns:
            The file content as bytes.
        """
        ...

    def read_text(self, encoding: str | None = None, errors: str = "replace") -> str:
        """Read and return the file content as text.

        Args:
            encoding: The encoding to use. If None, the encoding is detected automatically
                using BOM detection, falling back to UTF-8.
            errors: The error handling scheme. Common values: 'strict', 'ignore', 'replace'.

        Returns:
            The file content as text.
        """
        return _decode_bytes(self.read(), encoding, errors)


class FilePathMatcher(Protocol):
    """Protocol for file path matchers that filter directories and files."""

    def is_dir_included(self, path: PurePath) -> bool:
        """Check if a directory should be included (traversed)."""
        ...

    def is_file_included(self, path: PurePath) -> bool:
        """Check if a file should be included."""
        ...


class MatchAllFilePathMatcher(FilePathMatcher):
    """A file path matcher that includes all files and directories."""

    def is_dir_included(self, _path: PurePath) -> bool:
        """Always returns True - all directories are included."""
        return True

    def is_file_included(self, _path: PurePath) -> bool:
        """Always returns True - all files are included."""
        return True


class PatternFilePathMatcher(FilePathMatcher):
    """Pattern matcher that handles include and exclude glob patterns for files."""

    def __init__(
        self,
        included_patterns: list[str] | None = None,
        excluded_patterns: list[str] | None = None,
    ) -> None:
        """
        Create a new PatternFilePathMatcher from optional include and exclude pattern lists.

        Args:
            included_patterns: Patterns matching full path of files to be included.
            excluded_patterns: Patterns matching full path of files and directories
                to be excluded. If a directory is excluded, all files and
                subdirectories within it are also excluded.
        """
        self._included_patterns = included_patterns
        self._excluded_patterns = excluded_patterns

    def _matches_any(self, path: PurePath, patterns: list[str]) -> bool:
        """Check if the path matches any of the given glob patterns."""
        return any(path.match(pattern) for pattern in patterns)

    def _is_excluded(self, path: PurePath) -> bool:
        """Check if a file or directory is excluded by the exclude patterns."""
        if self._excluded_patterns is None:
            return False
        return self._matches_any(path, self._excluded_patterns)

    def is_dir_included(self, path: PurePath) -> bool:
        """Check if a directory should be included based on the exclude patterns."""
        return not self._is_excluded(path)

    def is_file_included(self, path: PurePath) -> bool:
        """
        Check if a file should be included based on both include and exclude patterns.

        Should be called for each file.
        """
        if self._is_excluded(path):
            return False
        if self._included_patterns is None:
            return True
        return self._matches_any(path, self._included_patterns)


_BOM_ENCODINGS = [
    (codecs.BOM_UTF32_LE, "utf-32-le"),
    (codecs.BOM_UTF32_BE, "utf-32-be"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
    (codecs.BOM_UTF8, "utf-8-sig"),
]


def _decode_bytes(data: bytes, encoding: str | None, errors: str) -> str:
    """Decode bytes to text using the given encoding.

    Args:
        data: The bytes to decode.
        encoding: The encoding to use. If None, the encoding is detected automatically
            using BOM detection, falling back to UTF-8.
        errors: The error handling scheme.
            Common values: 'strict', 'ignore', 'replace'.

    Returns:
        The decoded text.
    """
    if encoding is not None:
        return data.decode(encoding, errors)

    # Try to detect encoding using BOM (check longer BOMs first)

    for bom, enc in _BOM_ENCODINGS:
        if data.startswith(bom):
            return data.decode(enc, errors)

    # Fallback to UTF-8
    return data.decode("utf-8", errors)
