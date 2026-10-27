"""Unit tests for localfs target helper logic.

Tests cover:
- _execute_entry_action: file/directory creation, deletion, content writing
- _get_base_dir_key: base directory key extraction
- _EntrySpec / _DirSpec: specification construction
- Preview of directory targets with files in them
- Entry name normalization and rejection
- Switching between two keys that name one entry, within one update
- Warnings for overlapping root directory targets
"""

from __future__ import annotations

import os
import pathlib
from pathlib import Path

from typing import Any, Awaitable, Callable, cast

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
    _normalize_entry_name,
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

    # Against the applied state: `a.txt` changed and `c.txt` added; `b.txt`
    # is unchanged; both container actions remain listed. Nothing is written.
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


# =============================================================================
# Entry names
# =============================================================================


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a.txt", "a.txt"),
        ("sub/a.txt", "sub/a.txt"),
        (Path("sub") / "a.txt", "sub/a.txt"),
        ("./a.txt", "a.txt"),
        ("sub/./a.txt", "sub/a.txt"),
        ("sub//a.txt", "sub/a.txt"),
        ("sub/", "sub"),
        ("sub/../a.txt", "a.txt"),
        ("sub/x/../../a.txt", "a.txt"),
    ],
)
def test_entry_name_normalization(name: str | Path, expected: str) -> None:
    assert _normalize_entry_name(name) == expected


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("/a.txt", "must be relative"),
        ("../a.txt", "leaves its directory target"),
        ("sub/../../a.txt", "leaves its directory target"),
        ("", "names its directory target itself"),
        (".", "names its directory target itself"),
        ("sub/..", "names its directory target itself"),
    ],
)
def test_entry_name_rejection(name: str, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        _normalize_entry_name(name)


async def _declare_escaping_file(outdir: Path) -> None:
    target = await localfs.mount_dir_target(outdir)
    target.declare_file("../escaped.txt", "x")


def test_declare_file_rejects_name_leaving_directory(tmp_path: Path) -> None:
    app = coco.App(
        coco.AppConfig(name="test_declare_file_rejects_escape", environment=coco_env),
        _declare_escaping_file,
        outdir=tmp_path / "out",
    )
    with pytest.raises(Exception, match="leaves its directory target"):
        app.update_blocking()
    assert not (tmp_path / "escaped.txt").exists()


# =============================================================================
# Two keys naming one entry
# =============================================================================

# How the app below declares its entries; each test sets it before each update.
_declare_entries: list[
    Callable[[localfs.DirTarget[coco.ResolvedS]], Awaitable[None]]
] = []


async def _declare_entries_app(outdir: Path) -> None:
    target = await localfs.mount_dir_target(outdir)
    await _declare_entries[-1](target)


def _update(
    app: coco.App[[Path], None],
    declare: Callable[[localfs.DirTarget[coco.ResolvedS]], Awaitable[None]],
) -> None:
    _declare_entries[:] = [declare]
    app.update_blocking()


def _declare_named_sub_dir(
    target: localfs.DirTarget[coco.ResolvedS], name: str
) -> localfs.DirTarget[coco.PendingS]:
    return target.declare_dir_target(name)


def _declare_verbatim_sub_dir(
    target: localfs.DirTarget[coco.ResolvedS], name: str
) -> localfs.DirTarget[coco.PendingS]:
    """Declares the subdirectory under `name` as is, as older versions did."""
    spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=False)
    provider = coco.declare_target_state_with_child(
        target._provider.target_state(name, spec)
    )
    return localfs.DirTarget(provider)


def _declare_verbatim_file(
    target: localfs.DirTarget[coco.ResolvedS], name: str, content: str
) -> None:
    """Declares the file under `name` as is, as older versions did."""
    spec = _EntrySpec(entry_spec=content.encode(), create_parent_dirs=False)
    coco.declare_target_state(
        cast(coco.TargetState[None], target._provider.target_state(name, spec))
    )


def _new_app(name: str, outdir: Path) -> coco.App[[Path], None]:
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env),
        _declare_entries_app,
        outdir=outdir,
    )


def test_switch_from_flat_path_to_nested_dir_target_keeps_file(
    tmp_path: Path,
) -> None:
    outdir = tmp_path / "out"
    app = _new_app("test_switch_flat_to_nested", outdir)

    async def flat(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("sub/a.txt", "a", create_parent_dirs=True)

    async def nested(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        sub = await coco.use_mount(
            coco.component_subpath("sub"), _declare_named_sub_dir, target, "sub"
        )
        sub.declare_file("a.txt", "a")

    _update(app, flat)
    _update(app, nested)
    assert (outdir / "sub" / "a.txt").read_text() == "a"
    _update(app, nested)
    assert (outdir / "sub" / "a.txt").read_text() == "a"


def test_delete_of_another_file_in_the_same_update_runs(tmp_path: Path) -> None:
    """The guard spares only the entry just written: a removed file is deleted in
    the same call that rewrites its sibling."""
    outdir = tmp_path / "out"
    app = _new_app("test_delete_of_another_file", outdir)

    async def both(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("a.txt", "a")
        target.declare_file("b.txt", "b")

    async def only_a(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("a.txt", "a2")

    _update(app, both)
    _update(app, only_a)
    assert sorted(os.listdir(outdir)) == ["a.txt"]
    assert (outdir / "a.txt").read_text() == "a2"


def test_case_only_rename_keeps_file(tmp_path: Path) -> None:
    """On a case-insensitive file system both names reach one file, and the old
    name's delete must not remove the new name's write."""
    outdir = tmp_path / "out"
    app = _new_app("test_case_only_rename", outdir)

    async def upper(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("A.txt", "old")

    async def lower(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("a.txt", "new")

    _update(app, upper)
    _update(app, lower)
    names = os.listdir(outdir)
    assert len(names) == 1 and names[0].lower() == "a.txt"
    assert (outdir / "a.txt").read_text() == "new"


def test_upgrade_keeps_file_declared_with_dot_segment(tmp_path: Path) -> None:
    """A file declared as `./a.txt` by an older version, which kept the name as
    its key, survives the switch to the normalized key `a.txt`."""
    outdir = tmp_path / "out"
    app = _new_app("test_upgrade_dot_segment_file", outdir)

    async def before_upgrade(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        _declare_verbatim_file(target, "./a.txt", "a")

    async def after_upgrade(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        target.declare_file("./a.txt", "a")

    _update(app, before_upgrade)
    assert (outdir / "a.txt").read_text() == "a"

    # The upgrade switches keys: one write for `a.txt`, one delete for `./a.txt`.
    _declare_entries[:] = [after_upgrade]
    actions = app.update_blocking(preview=True)
    assert actions is not None
    assert [
        (Path(a.path).name, a.content) for a in actions if a.entry_type == "file"
    ] == [("a.txt", b"a"), ("a.txt", None)]

    _update(app, after_upgrade)
    assert (outdir / "a.txt").read_text() == "a"
    _update(app, after_upgrade)
    assert (outdir / "a.txt").read_text() == "a"


def test_upgrade_keeps_dir_target_declared_with_dot_segment(tmp_path: Path) -> None:
    """A directory target declared as `./sub` by an older version survives the
    switch to the normalized key `sub`, with its files."""
    outdir = tmp_path / "out"
    app = _new_app("test_upgrade_dot_segment_dir", outdir)

    async def before_upgrade(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        sub = await coco.use_mount(
            coco.component_subpath("sub"), _declare_verbatim_sub_dir, target, "./sub"
        )
        sub.declare_file("a.txt", "a")

    async def after_upgrade(target: localfs.DirTarget[coco.ResolvedS]) -> None:
        sub = await coco.use_mount(
            coco.component_subpath("sub"), _declare_named_sub_dir, target, "./sub"
        )
        sub.declare_file("a.txt", "a")

    _update(app, before_upgrade)
    assert (outdir / "sub" / "a.txt").read_text() == "a"
    _update(app, after_upgrade)
    assert (outdir / "sub" / "a.txt").read_text() == "a"
    _update(app, after_upgrade)
    assert (outdir / "sub" / "a.txt").read_text() == "a"


# =============================================================================
# Overlapping root directory targets
# =============================================================================

_OVERLAP_BASE_A = ContextKey[pathlib.Path]("localfs_target_test_overlap_base_a")
_OVERLAP_BASE_B = ContextKey[pathlib.Path]("localfs_target_test_overlap_base_b")

# The root directory targets the app below mounts; each test sets it.
_root_dirs: list[FilePath | Path | ContextKey[Path]] = []


async def _mount_root_dirs() -> None:
    for root in _root_dirs:
        await localfs.mount_dir_target(root)


def _overlap_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "WARNING" and "overlap" in record.getMessage()
    ]


def _run_root_dirs(
    app: coco.App[[], None],
    roots: list[FilePath | Path | ContextKey[Path]],
    caplog: pytest.LogCaptureFixture,
) -> list[str]:
    _root_dirs[:] = roots
    caplog.clear()
    with caplog.at_level("WARNING"):
        app.update_blocking()
    return _overlap_warnings(caplog)


def _new_root_dirs_app(name: str) -> coco.App[[], None]:
    return coco.App(coco.AppConfig(name=name, environment=coco_env), _mount_root_dirs)


def test_warns_on_nested_root_dir_targets(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    coco_env.context_provider.provide(_OVERLAP_BASE_A, tmp_path)
    app = _new_root_dirs_app("test_warns_on_nested_root_dir_targets")
    roots: list[FilePath | Path | ContextKey[Path]] = [
        FilePath("out", base_dir=_OVERLAP_BASE_A),
        tmp_path / "out" / "sub",
    ]

    warnings = _run_root_dirs(app, roots, caplog)
    assert len(warnings) == 1
    assert "localfs_target_test_overlap_base_a" in warnings[0]
    # Warned once per process, not on every update.
    assert _run_root_dirs(app, roots, caplog) == []


def test_warns_on_equal_root_dir_targets(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    try:
        (tmp_path / "link").symlink_to(real, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlinks unavailable: {exc}")
    app = _new_root_dirs_app("test_warns_on_equal_root_dir_targets")

    assert len(_run_root_dirs(app, [real, tmp_path / "link"], caplog)) == 1


def test_no_warning_for_disjoint_root_dir_targets(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    coco_env.context_provider.provide(_OVERLAP_BASE_A, tmp_path)
    coco_env.context_provider.provide(_OVERLAP_BASE_B, tmp_path)
    app = _new_root_dirs_app("test_no_warning_for_disjoint_root_dir_targets")

    roots: list[FilePath | Path | ContextKey[Path]] = [
        FilePath("foo", base_dir=_OVERLAP_BASE_A),
        FilePath("bar", base_dir=_OVERLAP_BASE_B),
        FilePath("foobar", base_dir=_OVERLAP_BASE_B),
    ]
    assert _run_root_dirs(app, roots, caplog) == []

    # Moving a root to another key over two updates (remove, then add) is fine.
    assert _run_root_dirs(app, roots[1:], caplog) == []
    assert _run_root_dirs(app, [*roots[1:], tmp_path / "foo"], caplog) == []
