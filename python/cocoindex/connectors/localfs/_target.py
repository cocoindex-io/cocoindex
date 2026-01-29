"""Local filesystem target utilities."""

from __future__ import annotations

import os
import pathlib
import shutil
from dataclasses import dataclass
from typing import Collection, Generic, Literal, NamedTuple, Sequence, cast

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_bytes

from ._common import FilePath, CWD_BASE_DIR, path_registry, to_file_path

# =============================================================================
# Shared types and helpers
# =============================================================================

_EntryName = str  # File or directory name (path segment)
_FileContent = bytes
_FileFingerprint = bytes


class _EntryAction(NamedTuple):
    """Action to perform on a file or directory entry."""

    path: pathlib.Path  # Absolute path to the entry
    entry_type: Literal["file", "dir"]
    content: _FileContent | None  # For files; None means delete
    create_parents: bool  # Whether to create parent directories


@dataclass(frozen=True, slots=True)
class _DirSpec:
    """Marker for a directory entry (no content)."""

    pass


@dataclass(frozen=True, slots=True)
class _EntrySpec:
    """Specification for an entry: content/type plus options."""

    entry_spec: _FileContent | _DirSpec
    create_parent_dirs: bool


def _execute_entry_action(action: _EntryAction) -> pathlib.Path | None:
    """
    Execute a single entry action.

    Returns the path for directories (to create child handler), None otherwise.
    """
    path = action.path

    if action.content is None:
        # Delete
        if action.entry_type == "file":
            path.unlink(missing_ok=True)
        else:
            if os.path.isdir(path):
                shutil.rmtree(path)
        return None

    if action.entry_type == "file":
        # Write file
        if action.create_parents:
            path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(action.content)
        return None

    # Create directory
    if action.create_parents:
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.mkdir(exist_ok=True)
    return path


def _apply_actions_with_child(
    actions: Sequence[_EntryAction],
) -> list[coco.ChildTargetDef["_EntryHandler"] | None]:
    """Apply actions and return child handlers for directories."""
    outputs: list[coco.ChildTargetDef[_EntryHandler] | None] = []
    for action in actions:
        result_path = _execute_entry_action(action)
        if result_path is not None:
            outputs.append(coco.ChildTargetDef(handler=_EntryHandler(result_path)))
        else:
            outputs.append(None)
    return outputs


# Shared action sink
_action_sink_with_child = coco.TargetActionSink[
    "_EntryAction", "_EntryHandler"
].from_fn(_apply_actions_with_child)


def _reconcile_entry(
    path: pathlib.Path,
    desired_state: _EntrySpec | coco.NonExistenceType,
    prev_possible_states: Collection[_EntryTrackingRecord],
    prev_may_be_missing: bool,
) -> (
    coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, "_EntryHandler"]
    | None
):
    """Common reconcile logic for both root and non-root entries."""
    if coco.is_non_existence(desired_state):
        # Determine entry type from previous state (None fingerprint = dir)
        entry_type: Literal["file", "dir"] = "file"
        for prev in prev_possible_states:
            if prev.fingerprint is None:
                entry_type = "dir"
                break

        return coco.TargetReconcileOutput(
            action=_EntryAction(
                path=path,
                entry_type=entry_type,
                content=None,
                create_parents=False,
            ),
            sink=_action_sink_with_child,
            tracking_record=coco.NON_EXISTENCE,
        )

    entry_spec = desired_state.entry_spec
    create_parents = desired_state.create_parent_dirs

    if isinstance(entry_spec, _DirSpec):
        # Directory entry (fingerprint=None means directory)
        return coco.TargetReconcileOutput(
            action=_EntryAction(
                path=path,
                entry_type="dir",
                content=b"",  # Non-None to indicate creation
                create_parents=create_parents,
            ),
            sink=_action_sink_with_child,
            tracking_record=_EntryTrackingRecord(fingerprint=None),
        )

    # File entry
    target_fp = fingerprint_bytes(entry_spec)

    # Check if update needed
    if not prev_may_be_missing and all(
        prev.fingerprint == target_fp for prev in prev_possible_states
    ):
        return None

    return coco.TargetReconcileOutput(
        action=_EntryAction(
            path=path,
            entry_type="file",
            content=entry_spec,
            create_parents=create_parents,
        ),
        sink=_action_sink_with_child,
        tracking_record=_EntryTrackingRecord(fingerprint=target_fp),
    )


# =============================================================================
# Entry handler (for non-root entries within a directory)
# =============================================================================


@dataclass(frozen=True, slots=True)
class _EntryTrackingRecord:
    """Tracking record for an entry. If fingerprint is None, it's a directory."""

    fingerprint: _FileFingerprint | None


class _EntryHandler(
    coco.TargetHandler[_EntryName, _EntrySpec, _EntryTrackingRecord, "_EntryHandler"]
):
    """Handler for file and directory entries within a parent directory."""

    __slots__ = ("_base_path",)

    _base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path) -> None:
        self._base_path = base_path

    def reconcile(
        self,
        key: _EntryName,
        desired_state: _EntrySpec | coco.NonExistenceType,
        prev_possible_states: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, "_EntryHandler"]
        | None
    ):
        path = self._base_path / key
        return _reconcile_entry(
            path, desired_state, prev_possible_states, prev_may_be_missing
        )


# =============================================================================
# Root-level types (shared key)
# =============================================================================


class _RootKey(NamedTuple):
    """Key for root-level entries: (base_dir_key, path_string)."""

    base_dir_key: str | None  # None for CWD
    path: str


def _get_base_dir_key(file_path: FilePath) -> str | None:
    """Get the base directory key, returning None for CWD (empty string)."""
    key = file_path.base_dir.key
    return key if key else None


# =============================================================================
# Root handler (for root-level files and directories)
# =============================================================================


def _resolve_root_path(key: _RootKey) -> pathlib.Path:
    """Resolve a root key to an absolute path using the current base directory."""
    if key.base_dir_key is None:
        # CWD
        base_path = CWD_BASE_DIR.value
    else:
        base_path = path_registry.get(key.base_dir_key)
    return (base_path / key.path).resolve()


class _RootHandler(
    coco.TargetHandler[_RootKey, _EntrySpec, _EntryTrackingRecord, _EntryHandler]
):
    """Handler for root-level entries (files and directories)."""

    def reconcile(
        self,
        key: _RootKey,
        desired_state: _EntrySpec | coco.NonExistenceType,
        prev_possible_states: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, _EntryHandler]
        | None
    ):
        path = _resolve_root_path(key)
        return _reconcile_entry(
            path, desired_state, prev_possible_states, prev_may_be_missing
        )


# =============================================================================
# Register root provider
# =============================================================================

_root_provider = coco.register_root_target_states_provider(
    "cocoindex.io/localfs", _RootHandler()
)


# =============================================================================
# Public API
# =============================================================================


class DirTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["DirTarget"]):
    """
    A target for writing files and subdirectories to a local directory.

    The directory is managed as a target state, with automatic cleanup of
    files and directories that are no longer declared.
    """

    _provider: coco.TargetStateProvider[
        _EntryName, _EntrySpec, _EntryHandler, coco.MaybePendingS
    ]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            _EntryName, _EntrySpec, _EntryHandler, coco.MaybePendingS
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
            filename: The name of the file (can include subdirectory path).
            content: The content of the file (bytes or str).
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.
        """
        if isinstance(content, str):
            content = content.encode()
        name = str(filename) if isinstance(filename, pathlib.PurePath) else filename
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
            path: The path of the subdirectory (relative to this directory).
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.

        Returns:
            A DirTarget for the subdirectory.
        """
        name = str(path) if isinstance(path, pathlib.PurePath) else path
        spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=create_parent_dirs)
        provider = coco.declare_target_state_with_child(
            self._provider.target_state(name, spec)
        )
        return DirTarget(provider)

    def __coco_memo_key__(self) -> object:
        return self._provider.memo_key


@coco.function
def declare_dir_target(
    path: FilePath | pathlib.Path,
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[coco.PendingS]:
    """
    Declare a directory target for writing files.

    Args:
        path: The filesystem path for the directory. Can be a FilePath (with stable
            base directory key) or a pathlib.Path (uses CWD as base directory).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A DirTarget that can be used to declare files and subdirectories.

    Example:
        ```python
        target = coco.mount_run(
            coco.component_subpath("setup"),
            localfs.declare_dir_target,
            Path("./output"),
        ).result()

        target.declare_file("hello.txt", content="Hello, world!")
        ```
    """
    file_path = to_file_path(path)
    key = _RootKey(
        base_dir_key=_get_base_dir_key(file_path),
        path=str(file_path.path),
    )
    spec = _EntrySpec(
        entry_spec=_DirSpec(),
        create_parent_dirs=create_parent_dirs,
    )
    provider = coco.declare_target_state_with_child(
        _root_provider.target_state(key, spec)
    )
    return DirTarget(provider)


@coco.function
def declare_file(
    path: FilePath | pathlib.Path,
    content: bytes | str,
    *,
    create_parent_dirs: bool = False,
) -> None:
    """
    Declare a single file target.

    This is a convenience function for declaring a single file without
    first creating a directory target.

    Args:
        path: The filesystem path for the file. Can be a FilePath (with stable
            base directory key) or a pathlib.Path (uses CWD as base directory).
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
        path=str(file_path.path),
    )
    spec = _EntrySpec(
        entry_spec=content,
        create_parent_dirs=create_parent_dirs,
    )
    # Files don't have children, but the provider type allows for them (for directories).
    # Cast is safe since file entries never produce child handlers at runtime.
    target_state = cast(coco.TargetState[None], _root_provider.target_state(key, spec))
    coco.declare_target_state(target_state)


__all__ = ["DirTarget", "declare_dir_target", "declare_file"]
