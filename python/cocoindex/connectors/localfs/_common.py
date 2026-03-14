"""Common types for the localfs connector."""

from __future__ import annotations

import pathlib
from typing import Self

from cocoindex._internal.context_keys import ContextKey
from cocoindex.resources import file


class FilePath(file.FilePath[pathlib.Path]):
    """
    A local file path with an optional base directory for memoization.

    FilePath combines an optional base directory (which provides a stable key) with a
    relative path. This allows file operations to remain stable even when the entire
    directory tree is moved.

    This class inherits all path operations from the base `FilePath` class and specializes
    it for local filesystem paths (`pathlib.Path`).

    Example:
        ```python
        # Using default CWD (no base directory)
        path = FilePath("docs/readme.md")

        # Using a context key for a named base directory
        SOURCE_DIR = coco.ContextKey[pathlib.Path]("source_dir", tracked=False)
        path = FilePath("docs/readme.md", _base_dir=SOURCE_DIR)
        ```
    """

    __slots__ = ()

    def __init__(
        self,
        path: str | pathlib.PurePath = ".",
        *,
        _base_dir: ContextKey[pathlib.Path] | None = None,
    ) -> None:
        """
        Create a FilePath.

        Args:
            path: The path (relative to the base directory, or absolute).
            _base_dir: Optional context key for the base directory. If None, resolves
                       relative to the current working directory.
        """
        super().__init__(
            _base_dir,
            pathlib.PurePath(path),
        )

    def resolve(self) -> pathlib.Path:
        """Resolve this FilePath to an absolute filesystem path."""
        if self._base_dir is not None:
            import cocoindex as coco

            base = coco.use_context(self._base_dir)
            return (base / self._path).resolve()
        return pathlib.Path(self._path).resolve()

    def _with_path(self, path: pathlib.PurePath) -> Self:
        """Create a new FilePath with the given relative path, keeping the same base directory."""
        return type(self)(path, _base_dir=self._base_dir)  # type: ignore[return-value]


def to_file_path(path: FilePath | pathlib.Path) -> FilePath:
    """Convert a Path or FilePath to a FilePath."""
    if isinstance(path, FilePath):
        return path
    return FilePath(path)


__all__ = [
    "FilePath",
]
