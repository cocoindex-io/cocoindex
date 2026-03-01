"""File-related protocols and utilities."""

from __future__ import annotations

__all__ = [
    "BaseDir",
    "FileMetadata",
    "FileLike",
    "FilePath",
    "FilePathMatcher",
    "MatchAllFilePathMatcher",
    "PatternFilePathMatcher",
]

import codecs as _codecs
from abc import ABC as _ABC, abstractmethod as _abstractmethod
from datetime import datetime as _datetime
from pathlib import PurePath as _PurePath
from typing import (
    Generic as _Generic,
    NamedTuple as _NamedTuple,
    Protocol as _Protocol,
    Self as _Self,
    TypeVar as _TypeVar,
)

from cocoindex._internal import core as _core
from cocoindex._internal.typing import (
    MemoStateOutcome as _MemoStateOutcome,
    is_non_existence as _is_non_existence,
)
from cocoindex import StableKey as _StableKey
from cocoindex.connectorkits import connection as _connection
from cocoindex.connectorkits.fingerprint import fingerprint_bytes as _fingerprint_bytes

# Type variable for the resolved path type (e.g., pathlib.Path for local filesystem)
ResolvedPathT = _TypeVar("ResolvedPathT")

# Type alias for base directory - a KeyedConnection holding the resolved base path
BaseDir = _connection.KeyedConnection[ResolvedPathT]


class FileMetadata(_NamedTuple):
    """Batch metadata for a file.

    Attributes:
        size: File size in bytes.
        modified_time: File modification time.
        content_fingerprint: Optional backend-provided content fingerprint (e.g., S3 ETag).
            When present, used directly by :meth:`FileLike.content_fingerprint` instead of
            hashing the full content.
    """

    size: int
    modified_time: _datetime
    content_fingerprint: object | None = None


class FileLike(_ABC, _Generic[ResolvedPathT]):
    """Base class for file-like objects with lazy metadata, content caching, and memoization.

    Subclasses must implement:

    - :meth:`_fetch_metadata` — async batch fetch of size, mtime, and optional fingerprint.
    - :meth:`_read_impl` — async read of file content from the backend.

    Optionally override :meth:`_compute_content_fingerprint` for backends with native
    fingerprinting (the default checks metadata, then falls back to hashing content).

    Type Parameters:
        ResolvedPathT: The type of the resolved path (e.g., `pathlib.Path` for local filesystem).
    """

    _file_path: "FilePath[ResolvedPathT]"
    _metadata: FileMetadata | None
    _cached_content: bytes | None
    _cached_content_fingerprint: object | None

    def __init__(
        self,
        file_path: "FilePath[ResolvedPathT]",
        *,
        _metadata: FileMetadata | None = None,
    ) -> None:
        self._file_path = file_path
        self._metadata = _metadata
        self._cached_content = None
        self._cached_content_fingerprint = None

    @property
    def file_path(self) -> "FilePath[ResolvedPathT]":
        """Return the FilePath of this file."""
        return self._file_path

    # --- Metadata (lazy, cached) ---

    async def _ensure_metadata(self) -> FileMetadata:
        """Return cached metadata, fetching lazily if needed."""
        if self._metadata is None:
            self._metadata = await self._fetch_metadata()
        return self._metadata

    @_abstractmethod
    async def _fetch_metadata(self) -> FileMetadata:
        """Fetch file metadata from the backend.

        Called lazily on first metadata access.  Implementations should batch-fetch
        all available metadata (size, mtime, content fingerprint if cheap) in one call.
        """

    async def size(self) -> int:
        """Return the file size in bytes."""
        return (await self._ensure_metadata()).size

    # --- Content I/O (cached) ---

    @_abstractmethod
    async def _read_impl(self, size: int = -1) -> bytes:
        """Read file content from the backend.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.
        """

    async def read(self, size: int = -1) -> bytes:
        """Read and return the file content as bytes.

        Full reads (``size < 0``) are cached: the first call reads from the backend,
        subsequent calls return the cached content.  Partial reads return from cache
        if available, otherwise delegate to the backend without caching.

        Args:
            size: Number of bytes to read. If -1 (default), read the entire file.

        Returns:
            The file content as bytes.
        """
        if size < 0:
            if self._cached_content is None:
                self._cached_content = await self._read_impl()
            return self._cached_content
        if self._cached_content is not None:
            return self._cached_content[:size]
        return await self._read_impl(size)

    async def read_text(
        self, encoding: str | None = None, errors: str = "replace"
    ) -> str:
        """Read and return the file content as text.

        Args:
            encoding: The encoding to use. If None, the encoding is detected automatically
                using BOM detection, falling back to UTF-8.
            errors: The error handling scheme. Common values: 'strict', 'ignore', 'replace'.

        Returns:
            The file content as text.
        """
        return _decode_bytes(await self.read(), encoding, errors)

    # --- Content fingerprinting (cached) ---

    async def _compute_content_fingerprint(self) -> object:
        """Compute a content fingerprint for this file.

        The default checks metadata for a pre-existing fingerprint (e.g., S3 ETag),
        then falls back to hashing the full content via :func:`fingerprint_bytes`.
        Override in subclasses for custom fingerprinting.
        """
        metadata = await self._ensure_metadata()
        if metadata.content_fingerprint is not None:
            return metadata.content_fingerprint
        return _fingerprint_bytes(await self.read())

    async def content_fingerprint(self) -> object:
        """Return a cached content fingerprint for this file."""
        if self._cached_content_fingerprint is None:
            self._cached_content_fingerprint = await self._compute_content_fingerprint()
        return self._cached_content_fingerprint

    # --- Memoization ---

    def __coco_memo_key__(self) -> object:
        return self._file_path.__coco_memo_key__()

    async def __coco_memo_state__(self, prev_state: object) -> _MemoStateOutcome:
        metadata = await self._ensure_metadata()
        current_mtime = metadata.modified_time

        if _is_non_existence(prev_state):
            fp = await self.content_fingerprint()
            return _MemoStateOutcome(state=(current_mtime, fp), memo_valid=True)

        assert isinstance(prev_state, tuple)
        prev_mtime, prev_fp = prev_state
        if current_mtime == prev_mtime:
            return _MemoStateOutcome(state=prev_state, memo_valid=True)

        fp = await self.content_fingerprint()
        return _MemoStateOutcome(
            state=(current_mtime, fp),
            memo_valid=(fp == prev_fp),
        )


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
        return True

    def is_file_included(self, path: _PurePath) -> bool:  # noqa: ARG002
        """Always returns True - all files are included."""
        return True


class PatternFilePathMatcher(FilePathMatcher):
    """Pattern matcher that handles include and exclude glob patterns for files.

    Uses `globset <https://docs.rs/globset>` semantics for pattern matching.
    Patterns are matched against the full relative path (with forward slashes).
    Common patterns:

    - `**/*.py` — matches Python files at any depth
    - `*.py` — matches Python files only in the root directory
    - `**/.*` — matches dot-prefixed entries (hidden files/dirs) at any depth
    - `{*.md,*.txt}` — matches multiple extensions using alternation
    """

    def __init__(
        self,
        included_patterns: list[str] | None = None,
        excluded_patterns: list[str] | None = None,
    ) -> None:
        """
        Create a new PatternFilePathMatcher from optional include and exclude pattern lists.

        Args:
            included_patterns: Glob patterns (globset syntax) matching full path of files
                to be included. Use ``**/*.ext`` to match at any depth.
            excluded_patterns: Glob patterns (globset syntax) matching full path of files
                and directories to be excluded. If a directory is excluded, all files and
                subdirectories within it are also excluded.

        Raises:
            ValueError: If any pattern is invalid.
        """
        self._matcher = _core.PatternMatcher(included_patterns, excluded_patterns)

    def is_dir_included(self, path: _PurePath) -> bool:
        """Check if a directory should be included based on the exclude patterns."""
        return self._matcher.is_dir_included(path.as_posix())

    def is_file_included(self, path: _PurePath) -> bool:
        """Check if a file should be included based on both include and exclude patterns."""
        return self._matcher.is_file_included(path.as_posix())


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

    def __init__(
        self,
        base_dir: _connection.KeyedConnection[ResolvedPathT],
        path: _PurePath,
    ) -> None:
        self._base_dir = base_dir
        self._path = path

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
