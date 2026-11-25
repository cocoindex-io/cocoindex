"""Local filesystem target utilities."""

from __future__ import annotations

import logging
import os
import pathlib
import shutil
import stat
import threading
from dataclasses import dataclass
from typing import Collection, Generic, Literal, NamedTuple, Sequence, cast

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_bytes
from cocoindex._internal.datatype import TypeChecker

import msgspec

from cocoindex._internal.context_keys import ContextKey, ContextProvider

from ._common import FilePath, to_file_path

_logger = logging.getLogger(__name__)

# =============================================================================
# Shared types and helpers
# =============================================================================

_EntryName = str  # File or directory name (path segment)
_ENTRY_NAME_CHECKER = TypeChecker(str)
_FileContent = bytes
_FileFingerprint = bytes


class _RootKey(NamedTuple):
    """Key for root-level entries: (base_dir_key, path_string)."""

    base_dir_key: str | None  # None for CWD
    path: str


_ROOT_KEY_CHECKER = TypeChecker(tuple[str | None, str])


class _EntryAction(NamedTuple):
    """Action to perform on a file or directory entry."""

    base_dir_key: (
        str | None
    )  # Context key for base dir; None means path is already absolute
    path: str  # Absolute path string if base_dir_key is None; relative path otherwise
    entry_type: Literal["file", "dir"]
    content: _FileContent | None  # For files; None means delete
    create_parents: bool  # Whether to create parent directories
    root_key: _RootKey | None = None  # Key of a root-level entry; None under a dir
    # A previous run declared the other kind of entry here (a directory where a
    # file is now declared, or the other way round): replace it.
    replaces_other_kind: bool = False
    # A root directory target that existed before CocoIndex declared it is no
    # longer declared: nothing to do on disk (`content` is None).
    forget: bool = False


def _normalize_entry_name(name: str | pathlib.PurePath) -> _EntryName:
    """The key of an entry declared on a `DirTarget`: its relative path with `.`
    segments dropped, `..` segments folded and `/` separators.

    Raises `ValueError` for a name that is absolute, leaves the directory, or names
    the directory itself.
    """
    path = pathlib.PurePath(name)
    if path.anchor:
        raise ValueError(
            f"Entry name {str(name)!r} must be relative to its directory target"
        )
    parts: list[str] = []
    for part in path.parts:
        if part != "..":
            parts.append(part)
        elif parts:
            parts.pop()
        else:
            raise ValueError(f"Entry name {str(name)!r} leaves its directory target")
    if not parts:
        raise ValueError(f"Entry name {str(name)!r} names its directory target itself")
    return "/".join(parts)


@dataclass(frozen=True, slots=True)
class _DirSpec:
    """Marker for a directory entry (no content)."""

    # For a root directory target: a directory was already at its path when it
    # was declared. Always False under a directory target.
    existed_before: bool = False


@dataclass(frozen=True, slots=True)
class _EntrySpec:
    """Specification for an entry: content/type plus options."""

    entry_spec: _FileContent | _DirSpec
    create_parent_dirs: bool


_ATOMIC_WRITE_TMP_SUFFIX = ".cocoindex-tmp"
# Flags of the exclusive create of a temporary file: `O_EXCL` refuses any
# entry already at the name, a symlink included; `O_NOFOLLOW` and `O_BINARY`
# exist only on some platforms.
_TMP_OPEN_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_BINARY", 0)
)


def _atomic_write_tmp_path(path: pathlib.Path) -> pathlib.Path:
    """The temporary file `_write_file_atomically` writes `path` through."""
    return path.with_name(f".{path.name}{_ATOMIC_WRITE_TMP_SUFFIX}")


def _write_file_atomically(path: pathlib.Path, content: bytes) -> None:
    """Write `content` to `path` through a temporary file in the same directory,
    renamed over `path`: a reader sees the old content or the new, never a
    partial file, and a write that fails (a full disk, a crash) leaves the old
    file. A symlink at `path` is written through, to the file it names, as an
    in-place write would; the new file takes the permissions of the file it
    replaces.

    The temporary file has one name per target file, `.<name>.cocoindex-tmp`: a
    process killed between writing it and renaming it leaves it behind, and the
    next write of the file removes it before writing its own. It is removed and
    created exclusively, never opened over, so a symlink someone placed there is
    never written through.
    """
    target = path
    if path.is_symlink():
        try:
            target = path.resolve(strict=True)
        except OSError:
            pass  # A dangling symlink: replace the link itself.
    tmp = _atomic_write_tmp_path(target)
    try:
        tmp.unlink(missing_ok=True)
        fd = os.open(tmp, _TMP_OPEN_FLAGS, 0o666)
        with os.fdopen(fd, "wb") as f:
            f.write(content)
        try:
            mode = os.stat(target).st_mode
        except FileNotFoundError:
            pass
        else:
            os.chmod(tmp, stat.S_IMODE(mode))
        os.replace(tmp, target)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _execute_entry_action(path: pathlib.Path, action: _EntryAction) -> None:
    """Execute a single entry action."""
    if action.forget:
        return
    if action.content is None:
        # Delete
        if action.entry_type == "file":
            path.unlink(missing_ok=True)
        else:
            if os.path.isdir(path):
                shutil.rmtree(path)
        return

    if action.entry_type == "file":
        # Write file, replacing a directory a previous run declared here (a
        # symlink there is removed, not followed).
        if action.replaces_other_kind:
            if path.is_symlink():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
        if action.create_parents:
            path.parent.mkdir(parents=True, exist_ok=True)
        _write_file_atomically(path, action.content)
        return

    # Create directory, replacing a file a previous run declared here (a
    # symlink there is removed, not followed).
    if action.replaces_other_kind and (path.is_symlink() or path.is_file()):
        path.unlink()
    if action.create_parents:
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.mkdir(exist_ok=True)


def _entry_path(
    context_provider: ContextProvider, action: _EntryAction
) -> pathlib.Path:
    """The absolute path an entry action applies to."""
    if action.base_dir_key is not None:
        base = context_provider.get(action.base_dir_key, pathlib.Path)
        return (base / action.path).resolve()
    return pathlib.Path(action.path)  # already absolute


def _dir_entry_handler(
    context_provider: ContextProvider, action: _EntryAction, /
) -> _EntryHandler:
    """The handler for the entries under a created directory (its path)."""
    return _EntryHandler(_entry_path(context_provider, action))


def _entry_identity(
    path: pathlib.Path, *, follow_symlinks: bool
) -> tuple[int, int] | None:
    """The (device, inode) pair of the file or directory at `path`; None where the
    file system reports no inode numbers (0)."""
    st = os.stat(path, follow_symlinks=follow_symlinks)
    return (st.st_dev, st.st_ino) if st.st_ino else None


def _apply_entry_actions(
    context_provider: ContextProvider,
    actions: Sequence[_EntryAction],
    /,
) -> None:
    """Apply entry actions.

    Two keys can name one entry: `sub/a.txt` declared on a directory target and
    `a.txt` on its `sub` directory target, names that differ only in case on a
    case-insensitive file system, or `./sub` kept as is by older versions and its
    normalized name `sub`. When an update moves an entry from one key to the
    other, the write for the new key and the delete for the old key arrive in one
    call. The delete is skipped there: it would remove the entry just written.
    """
    # Identities of the entries written so far; only a call with deletes needs them.
    track_written = any(action.content is None for action in actions)
    written: set[tuple[int, int]] = set()
    for action in actions:
        path = _entry_path(context_provider, action)
        if action.forget:
            continue
        if action.content is None:
            try:
                identity = _entry_identity(path, follow_symlinks=False)
            except (FileNotFoundError, NotADirectoryError):
                continue  # Nothing to delete
            if identity in written:
                continue
        _execute_entry_action(path, action)
        if track_written and action.content is not None:
            identity = _entry_identity(path, follow_symlinks=True)
            if identity is not None:
                written.add(identity)
    _track_root_dirs(context_provider, actions)


# Resolved paths of the root directory targets applied in this process, by key.
_root_dirs: dict[_RootKey, pathlib.Path] = {}
_root_dirs_lock = threading.Lock()


def _track_root_dirs(
    context_provider: ContextProvider, actions: Sequence[_EntryAction]
) -> None:
    """Record the root directory targets that `actions` write or delete, and warn
    when a newly written one is, contains, or sits inside another one."""
    removed: list[_RootKey] = []
    written: list[tuple[_RootKey, pathlib.Path]] = []
    for action in actions:
        if action.root_key is None or action.entry_type != "dir":
            continue
        if action.content is None:
            removed.append(action.root_key)
        else:
            written.append((action.root_key, _entry_path(context_provider, action)))
    if not removed and not written:
        return
    overlaps: list[tuple[str, str]] = []
    with _root_dirs_lock:
        for key in removed:
            _root_dirs.pop(key, None)
        for key, path in written:
            if _root_dirs.get(key) == path:
                continue
            _root_dirs[key] = path
            overlaps.extend(
                (_describe_root_dir(key, path), _describe_root_dir(other_key, other))
                for other_key, other in _root_dirs.items()
                if other_key != key
                and (path.is_relative_to(other) or other.is_relative_to(path))
            )
    for described, other_described in overlaps:
        _logger.warning(
            "localfs directory targets %s and %s overlap. A file reached through "
            "both is managed twice, and removing one target removes files of the "
            'other. See "One file, one declaration" in the localfs connector docs.',
            described,
            other_described,
        )


def _describe_root_dir(key: _RootKey, path: pathlib.Path) -> str:
    if key.base_dir_key is not None:
        return f"{key.path!r} under ContextKey {key.base_dir_key!r} ({path})"
    if pathlib.PurePath(key.path) == path:
        return str(path)
    return f"{key.path!r} ({path})"


# Shared by files and directories: the engine calls `_dir_entry_handler` only
# for directories, the only entries declared with a child.
_entry_action_sink = coco.TargetActionSink[_EntryAction].from_fn(
    _apply_entry_actions, child_handler=_dir_entry_handler
)


def _reconcile_entry(
    path_str: str,
    root_key: _RootKey | None,
    desired_state: _EntrySpec | coco.NonExistenceType,
    prev_possible_records: Collection[_EntryTrackingRecord],
    prev_may_be_missing: bool,
) -> (
    coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, _EntryHandler] | None
):
    """Common reconcile logic for both root and non-root entries."""
    base_dir_key = root_key.base_dir_key if root_key is not None else None
    prev_dirs = [prev for prev in prev_possible_records if prev.fingerprint is None]
    if coco.is_non_existence(desired_state):
        # Determine entry type from previous state (None fingerprint = dir)
        entry_type: Literal["file", "dir"] = "dir" if prev_dirs else "file"
        # A root directory target is removed with everything under it only if
        # CocoIndex created it. One that existed before is left as it is: the
        # engine does not visit the entries under a removed directory, so the
        # ones CocoIndex wrote there stay too.
        forget = (
            root_key is not None
            and entry_type == "dir"
            and not all(prev.created_dir for prev in prev_dirs)
        )
        return coco.TargetReconcileOutput(
            action=_EntryAction(
                base_dir_key,
                path_str,
                entry_type,
                None,
                False,
                root_key,
                forget=forget,
            ),
            sink=_entry_action_sink,
            tracking_record=coco.NON_EXISTENCE,
        )

    entry_spec = desired_state.entry_spec
    create_parents = desired_state.create_parent_dirs
    # What a previous run declared here. Only an entry CocoIndex declared is
    # replaced by one of the other kind; something else there fails the write.
    was_dir = any(prev.fingerprint is None for prev in prev_possible_records)
    was_file = any(prev.fingerprint is not None for prev in prev_possible_records)

    if isinstance(entry_spec, _DirSpec):
        # Directory entry (fingerprint=None means directory). A root directory
        # target records whether CocoIndex created it: one it created stays one
        # it created (it exists on later runs because of the first), one that
        # existed before (or one recorded before this was tracked) stays one it
        # didn't, and a new one is CocoIndex's unless it existed when declared.
        if root_key is None:
            created_dir = False
        elif prev_dirs:
            created_dir = all(prev.created_dir for prev in prev_dirs)
        else:
            created_dir = not entry_spec.existed_before
        return coco.TargetReconcileOutput(
            action=_EntryAction(
                base_dir_key,
                path_str,
                "dir",
                b"",
                create_parents,
                root_key,
                replaces_other_kind=was_file,
            ),
            sink=_entry_action_sink,
            tracking_record=_EntryTrackingRecord(
                fingerprint=None, created_dir=created_dir
            ),
        )

    # File entry
    target_fp = fingerprint_bytes(entry_spec)

    # Check if update needed
    if not prev_may_be_missing and all(
        prev.fingerprint == target_fp for prev in prev_possible_records
    ):
        return None

    return coco.TargetReconcileOutput(
        action=_EntryAction(
            base_dir_key,
            path_str,
            "file",
            entry_spec,
            create_parents,
            root_key,
            replaces_other_kind=was_dir,
        ),
        sink=_entry_action_sink,
        tracking_record=_EntryTrackingRecord(fingerprint=target_fp),
    )


# =============================================================================
# Entry handler (for non-root entries within a directory)
# =============================================================================


class _EntryTrackingRecord(msgspec.Struct, frozen=True, omit_defaults=True):
    """Tracking record for an entry. If fingerprint is None, it's a directory."""

    fingerprint: _FileFingerprint | None
    # For a root directory target: CocoIndex created the directory, so removing
    # the target removes it with everything under it. False for every other
    # entry, and for a root directory recorded by a version that did not track
    # this: such a directory is kept when its target is removed, as it may have
    # existed before CocoIndex declared it.
    created_dir: bool = False


class _EntryHandler(
    coco.TargetHandler[_EntrySpec, _EntryTrackingRecord, "_EntryHandler"]
):
    """Handler for file and directory entries within a parent directory."""

    __slots__ = ("_base_path",)

    _base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path) -> None:
        self._base_path = base_path

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _EntrySpec | coco.NonExistenceType,
        prev_possible_records: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, _EntryHandler]
        | None
    ):
        key = _ENTRY_NAME_CHECKER.check(key)
        path = self._base_path / key
        return _reconcile_entry(
            str(path),
            None,
            desired_state,
            prev_possible_records,
            prev_may_be_missing,
        )


# =============================================================================
# Root-level helpers
# =============================================================================


def _get_base_dir_key(file_path: FilePath) -> str | None:
    """Get the base directory key, returning None for CWD."""
    base_dir = file_path.base_dir
    return base_dir.key if base_dir is not None else None


# =============================================================================
# Root handler (for root-level files and directories)
# =============================================================================


class _RootHandler(coco.TargetHandler[_EntrySpec, _EntryTrackingRecord, _EntryHandler]):
    """Handler for root-level entries (files and directories)."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _EntrySpec | coco.NonExistenceType,
        prev_possible_records: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, _EntryHandler]
        | None
    ):
        root_key = _RootKey(*_ROOT_KEY_CHECKER.check(key))
        if root_key.base_dir_key is None:
            path_str = str((pathlib.Path.cwd() / root_key.path).resolve())
        else:
            path_str = root_key.path
        return _reconcile_entry(
            path_str,
            root_key,
            desired_state,
            prev_possible_records,
            prev_may_be_missing,
        )


# =============================================================================
# Register root provider
# =============================================================================

_root_provider = coco.register_root_target_states_provider(
    "cocoindex/localfs", _RootHandler()
)


# =============================================================================
# Public API
# =============================================================================


class DirTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["DirTarget"]):
    """
    A target for writing files and subdirectories to a local directory.

    The directory is managed as a target state, with automatic cleanup of
    files and directories that are no longer declared. A changed file is
    written to a temporary file next to it and renamed over it, so a reader
    never sees part of a file.

    A root directory target that is no longer declared is removed with
    everything under it if CocoIndex created it. One that already existed when
    it was first declared is left in place, with everything in it.
    """

    _provider: coco.TargetStateProvider[_EntrySpec, _EntryHandler, coco.MaybePendingS]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            _EntrySpec, _EntryHandler, coco.MaybePendingS
        ],
    ) -> None:
        self._provider = provider

    def declare_file(
        self: "DirTarget",
        filename: str | pathlib.PurePath,
        content: bytes | str,
        *,
        create_parent_dirs: bool = False,
    ) -> None:
        """
        Declare a file to be written to this directory.

        Args:
            filename: The name of the file (can include subdirectory path). `.` and
                `..` segments are normalized; the name must stay inside this
                directory.
            content: The content of the file (bytes or str).
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.

        Raises:
            ValueError: If the name is absolute, leaves this directory, or names it.
        """
        if isinstance(content, str):
            content = content.encode()
        name = _normalize_entry_name(filename)
        spec = _EntrySpec(entry_spec=content, create_parent_dirs=create_parent_dirs)
        # Files don't have children, but the provider type allows for them (for directories).
        # Cast is safe since file entries never produce child handlers at runtime.
        target_state = cast(
            coco.TargetState[None], self._provider.target_state(name, spec)
        )
        coco.declare_target_state(target_state)

    def declare_dir_target(
        self: "DirTarget",
        path: str | pathlib.PurePath,
        *,
        create_parent_dirs: bool = False,
    ) -> "DirTarget[coco.PendingS]":
        """
        Declare a subdirectory target within this directory.

        Args:
            path: The path of the subdirectory (relative to this directory). `.` and
                `..` segments are normalized; the path must stay inside this
                directory.
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.

        Returns:
            A DirTarget for the subdirectory.

        Raises:
            ValueError: If the path is absolute, leaves this directory, or names it.
        """
        name = _normalize_entry_name(path)
        spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=create_parent_dirs)
        provider = coco.declare_target_state_with_child(
            self._provider.target_state(name, spec)
        )
        return DirTarget(provider)

    def __coco_memo_key__(self) -> object:
        return self._provider.memo_key


@coco.fn
def declare_dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[coco.PendingS]:
    """
    Declare a directory target for writing files.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A DirTarget that can be used to declare files and subdirectories.

    Example:
        ```python
        target = coco.use_mount(
            coco.component_subpath("setup"),
            localfs.declare_dir_target,
            Path("./output"),
        )

        target.declare_file("hello.txt", content="Hello, world!")
        ```
    """
    provider = coco.declare_target_state_with_child(
        dir_target(path, create_parent_dirs=create_parent_dirs)
    )
    return DirTarget(provider)


def dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> coco.TargetState[_EntryHandler]:
    """
    Create a TargetState for a local directory target.

    Use with ``coco.mount_target()`` to mount and get a child provider,
    or with ``mount_dir_target()`` for a convenience wrapper.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    file_path = to_file_path(path)
    key = _RootKey(
        base_dir_key=_get_base_dir_key(file_path),
        path=file_path.path.as_posix(),
    )
    spec = _EntrySpec(
        entry_spec=_DirSpec(existed_before=file_path.resolve().is_dir()),
        create_parent_dirs=create_parent_dirs,
    )
    return _root_provider.target_state(key, spec)


async def mount_dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[coco.ResolvedS]:
    """
    Mount a directory target and return a ready-to-use DirTarget.

    Sugar over ``dir_target()`` + ``coco.mount_target()`` + wrapping.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A DirTarget that can be used to declare files and subdirectories.
    """
    provider = await coco.mount_target(
        dir_target(path, create_parent_dirs=create_parent_dirs)
    )
    return DirTarget(provider)


@coco.fn
def declare_file(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    content: bytes | str,
    *,
    create_parent_dirs: bool = False,
) -> None:
    """
    Declare a single file target.

    This is a convenience function for declaring a single file without
    first creating a directory target.

    Args:
        path: The filesystem path for the file. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        content: The content of the file (bytes or str).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to False.

    Example:
        ```python
        coco.mount(
            coco.component_subpath("output"),
            localfs.declare_file,
            Path("./output/hello.txt"),
            content="Hello, world!",
            create_parent_dirs=True,
        )
        ```
    """
    if isinstance(content, str):
        content = content.encode()

    file_path = to_file_path(path)
    key = _RootKey(
        base_dir_key=_get_base_dir_key(file_path),
        path=file_path.path.as_posix(),
    )
    spec = _EntrySpec(
        entry_spec=content,
        create_parent_dirs=create_parent_dirs,
    )
    # Files don't have children, but the provider type allows for them (for directories).
    # Cast is safe since file entries never produce child handlers at runtime.
    target_state = cast(coco.TargetState[None], _root_provider.target_state(key, spec))
    coco.declare_target_state(target_state)


__all__ = [
    "DirTarget",
    "declare_dir_target",
    "declare_file",
    "dir_target",
    "mount_dir_target",
]
