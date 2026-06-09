"""Unit tests for localfs target helper logic.

Tests cover:
- _execute_entry_action: file/directory creation, deletion, content writing
- _get_base_dir_key: base directory key extraction
- _EntrySpec / _DirSpec: specification construction
"""

from __future__ import annotations

import pathlib
from pathlib import Path

import pytest

from cocoindex._internal.context_keys import ContextKey
from cocoindex.connectors.localfs._common import FilePath
from cocoindex.connectors.localfs._target import (
    _DirSpec,
    _EntryAction,
    _EntrySpec,
    _execute_entry_action,
    _get_base_dir_key,
)


# =============================================================================
# _get_base_dir_key
# =============================================================================


class TestGetBaseDirKey:
    def test_returns_none_for_cwd_path(self) -> None:
        fp = FilePath("some/path.txt")
        assert _get_base_dir_key(fp) is None

    def test_returns_key_for_context_key_base(self) -> None:
        key: ContextKey[pathlib.Path] = ContextKey("test_base_dir_key_unique_42")
        fp = FilePath("some/path.txt", base_dir=key)
        assert _get_base_dir_key(fp) == key.key

    def test_key_string_matches_context_key_name(self) -> None:
        key: ContextKey[pathlib.Path] = ContextKey("my_source_dir")
        fp = FilePath("file.txt", base_dir=key)
        result = _get_base_dir_key(fp)
        assert result == "my_source_dir"


# =============================================================================
# _EntrySpec and _DirSpec construction
# =============================================================================


class TestEntrySpecConstruction:
    def test_file_spec_stores_content(self) -> None:
        content = b"hello"
        spec = _EntrySpec(entry_spec=content, create_parent_dirs=False)
        assert spec.entry_spec == content
        assert spec.create_parent_dirs is False

    def test_dir_spec_is_sentinel(self) -> None:
        spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=False)
        assert isinstance(spec.entry_spec, _DirSpec)

    def test_create_parent_dirs_flag(self) -> None:
        spec = _EntrySpec(entry_spec=b"data", create_parent_dirs=True)
        assert spec.create_parent_dirs is True


# =============================================================================
# _execute_entry_action
# =============================================================================


class TestExecuteEntryAction:
    # --- File creation ---

    def test_creates_file_with_content(self, tmp_path: Path) -> None:
        target = tmp_path / "output.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"hello world",
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert target.read_bytes() == b"hello world"

    def test_creates_file_with_empty_content(self, tmp_path: Path) -> None:
        target = tmp_path / "empty.bin"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"",
            create_parents=False,
        )
        _execute_entry_action(target, action)
        assert target.exists()
        assert target.read_bytes() == b""

    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        target = tmp_path / "data.txt"
        target.write_bytes(b"old content")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"new content",
            create_parents=False,
        )
        _execute_entry_action(target, action)
        assert target.read_bytes() == b"new content"

    def test_file_with_create_parents(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "c.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"nested",
            create_parents=True,
        )
        _execute_entry_action(target, action)
        assert target.read_bytes() == b"nested"

    def test_file_without_create_parents_raises(self, tmp_path: Path) -> None:
        target = tmp_path / "missing_dir" / "file.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"x",
            create_parents=False,
        )
        with pytest.raises(FileNotFoundError):
            _execute_entry_action(target, action)

    # --- Directory creation ---

    def test_creates_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "newdir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result == target
        assert target.is_dir()

    def test_creates_directory_with_parents(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "newdir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=True,
        )
        result = _execute_entry_action(target, action)
        assert result == target
        assert target.is_dir()

    def test_create_dir_is_idempotent(self, tmp_path: Path) -> None:
        target = tmp_path / "existing"
        target.mkdir()
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=False,
        )
        # Should not raise even if directory already exists
        result = _execute_entry_action(target, action)
        assert result == target

    # --- Deletion ---

    def test_deletes_existing_file(self, tmp_path: Path) -> None:
        target = tmp_path / "to_delete.txt"
        target.write_bytes(b"bye")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=None,
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert not target.exists()

    def test_delete_missing_file_is_noop(self, tmp_path: Path) -> None:
        target = tmp_path / "nonexistent.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=None,
            create_parents=False,
        )
        # Should not raise
        result = _execute_entry_action(target, action)
        assert result is None

    def test_deletes_existing_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "dir_to_delete"
        target.mkdir()
        (target / "child.txt").write_bytes(b"x")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=None,
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert not target.exists()

    def test_delete_missing_directory_is_noop(self, tmp_path: Path) -> None:
        target = tmp_path / "ghost_dir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=None,
            create_parents=False,
        )
        # Should not raise
        result = _execute_entry_action(target, action)
        assert result is None
