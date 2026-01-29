"""Common types for the localfs connector."""

from __future__ import annotations

import pathlib
from typing import Self

from cocoindex.connectorkits import connection
from cocoindex.resources import file

# Registry for base directory paths
path_registry: connection.ConnectionRegistry[pathlib.Path] = (
    connection.ConnectionRegistry("cocoindex/localfs")
)

# The default base directory pointing to the current working directory (not registered)
CWD_BASE_DIR = path_registry.register("", pathlib.Path("."))


class FilePath(file.FilePath[pathlib.Path]):
    """
    A local file path with a stable base directory for memoization.

    FilePath combines a base directory (which provides a stable key) with a relative path.
    This allows file operations to remain stable even when the entire directory tree is moved.

    This class inherits all path operations from the base `FilePath` class and specializes
    it for local filesystem paths (`pathlib.Path`).

    Example:
        ```python
        # Using default CWD base directory
        path = FilePath("docs/readme.md")

        # Using a registered base directory
        base = register_base_dir("my_project", Path("/path/to/project"))
        path = base / "docs" / "readme.md"
        ```
    """

    __slots__ = ()

    def __init__(
        self,
        path: str | pathlib.PurePath = ".",
        *,
        _base_dir: connection.KeyedConnection[pathlib.Path] | None = None,
    ) -> None:
        """
        Create a FilePath.

        Args:
            path: The path (relative to the base directory, or absolute).
            _base_dir: Internal parameter. The base directory. If None, uses CWD_BASE_DIR.
        """
        self._base_dir = _base_dir if _base_dir is not None else CWD_BASE_DIR
        self._path = pathlib.PurePath(path)

    def resolve(self) -> pathlib.Path:
        """Resolve this FilePath to an absolute filesystem path."""
        return (self._base_dir.value / self._path).resolve()

    def _with_path(self, path: pathlib.PurePath) -> Self:
        """Create a new FilePath with the given relative path, keeping the same base directory."""
        return type(self)(path, _base_dir=self._base_dir)  # type: ignore[return-value]


def register_base_dir(key: str, path: pathlib.Path) -> FilePath:
    """
    Register a base directory with a stable key.

    The key should be stable across runs - it identifies the logical base directory.
    The path can change (e.g., when the project is moved) as long as the same key is used.

    Args:
        key: A stable identifier for this base directory (e.g., "source", "output").
             Must be unique - raises ValueError if a base directory with this key
             is already registered.
        path: The filesystem path of the base directory.

    Returns:
        A FilePath representing the base directory itself (with path ".").

    Raises:
        ValueError: If a base directory with the given key is already registered.

    Example:
        ```python
        # Register a base directory
        source_dir = register_base_dir("source", Path("./data"))

        # Use it to create file paths
        file_path = source_dir / "subdir" / "file.txt"
        ```
    """
    base_dir = path_registry.register(key, path)
    return FilePath(".", _base_dir=base_dir)


def unregister_base_dir(key: str) -> None:
    """
    Unregister a base directory.

    Args:
        key: The base directory key to unregister.
    """
    path_registry.unregister(key)


def to_file_path(path: FilePath | pathlib.Path) -> FilePath:
    """Convert a Path or FilePath to a FilePath."""
    if isinstance(path, FilePath):
        return path
    return FilePath(path)


__all__ = [
    "FilePath",
    "register_base_dir",
    "unregister_base_dir",
]
