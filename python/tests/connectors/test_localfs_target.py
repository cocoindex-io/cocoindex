"""Unit tests for localfs target helper logic.

Tests cover:
- _execute_entry_action: file/directory creation, deletion, content writing
- _get_base_dir_key: base directory key extraction
- _EntrySpec / _DirSpec: specification construction
- Preview of directory targets with files in them
"""

from __future__ import annotations

import pathlib
from pathlib import Path

from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey
from cocoindex.connectors import localfs
from cocoindex.connectors.localfs._common import FilePath
from cocoindex.connectors.localfs._target import (
    _DirSpec,
    _EntryAction,
    _EntrySpec,
    _execute_entry_action,
    _get_base_dir_key,
)

from tests import common

coco_env = common.create_test_env(__file__)


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
        _execute_entry_action(target, action)
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
        _execute_entry_action(target, action)
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
        _execute_entry_action(target, action)
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
        _execute_entry_action(target, action)

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
        _execute_entry_action(target, action)
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
        _execute_entry_action(target, action)

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
        _execute_entry_action(target, action)
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
        _execute_entry_action(target, action)


# =============================================================================
# Preview
# =============================================================================

_preview_files: dict[str, str] = {}


def _declare_sub_dir(
    target: localfs.DirTarget[coco.ResolvedS],
) -> localfs.DirTarget[coco.PendingS]:
    return target.declare_dir_target("sub")


async def _declare_preview_files(outdir: Path) -> None:
    target = await localfs.mount_dir_target(outdir)
    sub = await coco.use_mount(coco.component_subpath("sub"), _declare_sub_dir, target)
    for name, content in _preview_files.items():
        sub.declare_file(name, content)


def _planned_entries(
    actions: list[Any] | None, outdir: Path
) -> dict[tuple[str, str], bytes | None]:
    """Planned entry actions as ``(entry type, path under outdir) -> content``
    (``None``: delete)."""
    assert actions is not None
    root = outdir.resolve()
    plan: dict[tuple[str, str], bytes | None] = {}
    for action in actions:
        assert isinstance(action, _EntryAction) and action.base_dir_key is None
        path = Path(action.path).relative_to(root).as_posix()
        plan[(action.entry_type, path)] = action.content
    assert len(plan) == len(actions)
    return plan


def test_preview_dir_target(tmp_path: Path) -> None:
    """Preview plans the directories and their files without writing anything."""
    outdir = tmp_path / "out"
    _preview_files.clear()
    app = coco.App(
        coco.AppConfig(name="test_preview_dir_target", environment=coco_env),
        _declare_preview_files,
        outdir=outdir,
    )

    # Both directories' actions and one per file; nothing is written.
    _preview_files.update({"a.txt": "a", "b.txt": "b"})
    actions = app.update_blocking(preview=True)
    assert _planned_entries(actions, outdir) == {
        ("dir", "."): b"",
        ("dir", "sub"): b"",
        ("file", "sub/a.txt"): b"a",
        ("file", "sub/b.txt"): b"b",
    }
    assert not outdir.exists()

    app.update_blocking()
    assert (outdir / "sub" / "a.txt").read_text() == "a"
    assert (outdir / "sub" / "b.txt").read_text() == "b"

    # Against the applied state: both directories' actions, `a.txt` changed
    # and `c.txt` added; `b.txt` is unchanged. Nothing is written.
    _preview_files.update({"a.txt": "a2", "c.txt": "c"})
    actions = app.update_blocking(preview=True)
    assert _planned_entries(actions, outdir) == {
        ("dir", "."): b"",
        ("dir", "sub"): b"",
        ("file", "sub/a.txt"): b"a2",
        ("file", "sub/c.txt"): b"c",
    }
    assert (outdir / "sub" / "a.txt").read_text() == "a"
    assert not (outdir / "sub" / "c.txt").exists()

    app.update_blocking()
    assert (outdir / "sub" / "a.txt").read_text() == "a2"
    assert (outdir / "sub" / "c.txt").read_text() == "c"
