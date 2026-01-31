"""File-related protocols and utilities."""

from __future__ import annotations

__all__ = [
    "AsyncFileLike",
    "BaseDir",
    "FileLike",
    "FilePath",
    "FilePathMatcher",
    "MatchAllFilePathMatcher",
    "PatternFilePathMatcher",
]

import codecs as _codecs
from abc import abstractmethod as _abstractmethod
from datetime import datetime as _datetime
from pathlib import PurePath as _PurePath
from typing import (
    Generic as _Generic,
    Protocol as _Protocol,
    Self as _Self,
    TypeVar as _TypeVar,
)

from cocoindex import StableKey as _StableKey
from cocoindex.connectorkits import connection as _connection

# Type variable for the resolved path type (e.g., pathlib.Path for local filesystem)
ResolvedPathT = _TypeVar("ResolvedPathT")

# Type alias for base directory - a KeyedConnection holding the resolved base path
BaseDir = _connection.KeyedConnection[ResolvedPathT]


class FileLike(_Protocol[ResolvedPathT]):
    """Protocol for file-like objects with path, size, modified time, and read capability.

    Type Parameters:
        ResolvedPathT: The type of the resolved path (e.g., `pathlib.Path` for local filesystem).
    """

    @property
    def stable_key(self) -> _StableKey:
        """Return the stable key for this file."""
        return str(self.file_path.path)

    @property
    def file_path(self) -> "FilePath[ResolvedPathT]":
        """Return the FilePath of this file."""
        ...

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        ...

    @property
    def modified_time(self) -> _datetime:
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

    def __coco_memo_key__(self) -> object:
        return (self.file_path.__coco_memo_key__(), self.modified_time)


class AsyncFileLike(_Protocol[ResolvedPathT]):
    """Protocol for async file-like objects with path, size, modified time, and async read.

    Type Parameters:
        ResolvedPathT: The type of the resolved path (e.g., `pathlib.Path` for local filesystem).
    """

    @property
    def file_path(self) -> "FilePath[ResolvedPathT]":
        """Return the FilePath of this file."""
        ...

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        ...

    @property
    def modified_time(self) -> _datetime:
        """Return the file modification time."""
        ...

    async def read(self, size: int = -1) -> bytes:
        """Asynchronously read and return the file content as bytes.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.

        Returns:
            The file content as bytes.
        """
        raise NotImplementedError

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
        return _decode_bytes(await self.read(), encoding, errors)


class FilePathMatcher(_Protocol):
    """Protocol for file path matchers that filter directories and files."""

    def is_dir_included(self, path: _PurePath) -> bool:
        """Check if a directory should be included (traversed)."""

    def is_file_included(self, path: _PurePath) -> bool:
        """Check if a file should be included."""


class MatchAllFilePathMatcher(FilePathMatcher):
    """A file path matcher that includes all files and directories."""

    def is_dir_included(self, path: _PurePath) -> bool:  # noqa: ARG002
        """Always returns True - all directories are included."""
        del path  # unused
        return True

    def is_file_included(self, path: _PurePath) -> bool:  # noqa: ARG002
        """Always returns True - all files are included."""
        del path  # unused
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

    def _matches_any(self, path: _PurePath, patterns: list[str]) -> bool:
        """Check if the path matches any of the given glob patterns."""
        return any(path.match(pattern) for pattern in patterns)

    def _is_excluded(self, path: _PurePath) -> bool:
        """Check if a file or directory is excluded by the exclude patterns."""
        if self._excluded_patterns is None:
            return False
        return self._matches_any(path, self._excluded_patterns)

    def is_dir_included(self, path: _PurePath) -> bool:
        """Check if a directory should be included based on the exclude patterns."""
        return not self._is_excluded(path)

    def is_file_included(self, path: _PurePath) -> bool:
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
    (_codecs.BOM_UTF32_LE, "utf-32-le"),
    (_codecs.BOM_UTF32_BE, "utf-32-be"),
    (_codecs.BOM_UTF16_LE, "utf-16-le"),
    (_codecs.BOM_UTF16_BE, "utf-16-be"),
    (_codecs.BOM_UTF8, "utf-8-sig"),
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


class FilePath(_Generic[ResolvedPathT]):
    """
    Base class for file paths with stable base directory support for memoization.

    FilePath combines a base directory (which provides a stable key) with a relative path.
    This allows file operations to remain stable even when the base directory is moved.

    Subclasses should implement:
    - `resolve()` method: returns the resolved path of type `ResolvedPathT`
    - `_with_path()` method: creates a new instance with a different relative path

    FilePath supports most operations that `pathlib.PurePath` supports:
    - `/` operator for joining paths
    - `parent`, `name`, `stem`, `suffix`, `parts` properties
    - `with_name()`, `with_stem()`, `with_suffix()` methods
    - `is_absolute()`, `is_relative_to()`, `match()` methods

    Type Parameters:
        ResolvedPathT: The type of the resolved path (e.g., `pathlib.Path` for local filesystem).
    """

    __slots__ = ("_base_dir", "_path")

    _base_dir: _connection.KeyedConnection[ResolvedPathT]
    _path: _PurePath

    @property
    def base_dir(self) -> _connection.KeyedConnection[ResolvedPathT]:
        """The base directory for this path."""
        return self._base_dir

    @_abstractmethod
    def resolve(self) -> ResolvedPathT:
        """Resolve this FilePath to the full path."""

    @_abstractmethod
    def _with_path(self, path: _PurePath) -> _Self:
        """Create a new FilePath with the given relative path, keeping the same base directory."""

    @property
    def path(self) -> _PurePath:
        """The path relative to the base directory."""
        return self._path

    # PurePath-like operations

    def __truediv__(self, other: str | _PurePath) -> _Self:
        """Join this path with another path segment."""
        return self._with_path(self._path / other)

    def __rtruediv__(self, other: str | _PurePath) -> _Self:
        """Join another path segment with this path (rarely used)."""
        return self._with_path(other / self._path)

    @property
    def parent(self) -> _Self:
        """The logical parent of this path."""
        return self._with_path(self._path.parent)

    @property
    def parents(self) -> tuple[_Self, ...]:
        """An immutable sequence of the path's logical parents."""
        return tuple(self._with_path(p) for p in self._path.parents)

    @property
    def name(self) -> str:
        """The final component of this path."""
        return self._path.name

    @property
    def stem(self) -> str:
        """The final component without its suffix."""
        return self._path.stem

    @property
    def suffix(self) -> str:
        """The file extension of the final component."""
        return self._path.suffix

    @property
    def suffixes(self) -> list[str]:
        """A list of the path's file extensions."""
        return self._path.suffixes

    @property
    def parts(self) -> tuple[str, ...]:
        """An object providing sequence-like access to the path's components."""
        return self._path.parts

    def with_name(self, name: str) -> _Self:
        """Return a new path with the file name changed."""
        return self._with_path(self._path.with_name(name))

    def with_stem(self, stem: str) -> _Self:
        """Return a new path with the stem changed."""
        return self._with_path(self._path.with_stem(stem))

    def with_suffix(self, suffix: str) -> _Self:
        """Return a new path with the suffix changed."""
        return self._with_path(self._path.with_suffix(suffix))

    def with_segments(self, *pathsegments: str) -> _Self:
        """Return a new path with the segments replaced."""
        return self._with_path(_PurePath(*pathsegments))

    def is_absolute(self) -> bool:
        """Return True if the path is absolute."""
        return self._path.is_absolute()

    def is_relative_to(self, other: str | _PurePath) -> bool:
        """Return True if the path is relative to another path."""
        return self._path.is_relative_to(other)

    def relative_to(self, other: str | _PurePath) -> _PurePath:
        """Return the relative path to another path."""
        return self._path.relative_to(other)

    def match(self, pattern: str) -> bool:
        """Match this path against the provided glob-style pattern."""
        return self._path.match(pattern)

    def as_posix(self) -> str:
        """Return the string representation with forward slashes."""
        return self._path.as_posix()

    def joinpath(self, *pathsegments: str | _PurePath) -> _Self:
        """Combine this path with one or more path segments."""
        return self._with_path(self._path.joinpath(*pathsegments))

    # String representations

    def __str__(self) -> str:
        return str(self._path)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}({self._path!r}, base_dir_key={self.base_dir.key!r})"
        )

    def __fspath__(self) -> str:
        """Return the file system path as a string for os.fspath() compatibility."""
        return str(self.resolve())

    # Comparison and hashing

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FilePath):
            return NotImplemented
        return self.base_dir.key == other.base_dir.key and self._path == other._path

    def __hash__(self) -> int:
        return hash((self.base_dir.key, self._path))

    def __lt__(self, other: _Self) -> bool:
        if not isinstance(other, FilePath):
            return NotImplemented
        if self.base_dir.key != other.base_dir.key:
            return self.base_dir.key < other.base_dir.key
        return self._path < other._path

    def __le__(self, other: _Self) -> bool:
        return self == other or self < other

    def __gt__(self, other: _Self) -> bool:
        if not isinstance(other, FilePath):
            return NotImplemented
        return other < self

    def __ge__(self, other: _Self) -> bool:
        return self == other or self > other

    # Memoization support

    def __coco_memo_key__(self) -> object:
        return (self.base_dir.__coco_memo_key__(), self._path)
