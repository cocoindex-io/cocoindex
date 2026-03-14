"""Tests for FilePath memoization keys and localfs removed symbols."""

from __future__ import annotations

from pathlib import PurePath

import pytest

from cocoindex.resources.file import FilePath
from cocoindex.connectors.localfs import FilePath as LocalfsFilePath


class _ConcreteFilePath(FilePath[str]):
    """Minimal concrete FilePath subclass for testing."""

    def resolve(self) -> str:
        return str(self._path)

    def _with_path(self, path: PurePath) -> "_ConcreteFilePath":
        return _ConcreteFilePath(None, path)


def test_filepath_no_base_dir_memo_key() -> None:
    """FilePath with no base_dir has a memo key equal to the path."""
    fp = _ConcreteFilePath(None, PurePath("some/file.txt"))
    assert fp.__coco_memo_key__() == PurePath("some/file.txt")


def test_filepath_with_base_dir_memo_key() -> None:
    """FilePath with a ContextKey base_dir has a tuple memo key."""
    from cocoindex._internal.context_keys import ContextKey

    key = ContextKey[str]("test_filepath_source_dir_unique_13", tracked=False)
    fp = _ConcreteFilePath(key, PurePath("file.txt"))
    assert fp.__coco_memo_key__() == (
        "test_filepath_source_dir_unique_13",
        PurePath("file.txt"),
    )


def test_localfs_filepath_no_base_dir_memo_key() -> None:
    """localfs.FilePath with no base_dir has a memo key equal to the path."""
    fp = LocalfsFilePath("some/file.txt")
    assert fp.__coco_memo_key__() == PurePath("some/file.txt")


def test_localfs_filepath_with_base_dir_memo_key() -> None:
    """localfs.FilePath with a ContextKey base_dir has a tuple memo key."""
    from cocoindex._internal.context_keys import ContextKey
    import pathlib

    key = ContextKey[pathlib.Path]("test_localfs_source_dir_unique_17", tracked=False)
    fp = LocalfsFilePath("file.txt", _base_dir=key)
    assert fp.__coco_memo_key__() == (
        "test_localfs_source_dir_unique_17",
        PurePath("file.txt"),
    )


def test_localfs_has_no_register_base_dir() -> None:
    """localfs no longer exports register_base_dir."""
    import cocoindex.connectors.localfs as localfs

    assert not hasattr(localfs, "register_base_dir"), (
        "register_base_dir should not be exported from localfs"
    )


def test_localfs_has_no_unregister_base_dir() -> None:
    """localfs no longer exports unregister_base_dir."""
    import cocoindex.connectors.localfs as localfs

    assert not hasattr(localfs, "unregister_base_dir"), (
        "unregister_base_dir should not be exported from localfs"
    )


def test_localfs_has_no_cwd_base_dir() -> None:
    """localfs no longer exports CWD_BASE_DIR."""
    import cocoindex.connectors.localfs as localfs

    assert not hasattr(localfs, "CWD_BASE_DIR"), (
        "CWD_BASE_DIR should not be exported from localfs"
    )
