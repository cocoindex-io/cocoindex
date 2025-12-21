from __future__ import annotations

import pathlib
import shutil
import os
from dataclasses import dataclass
from hashlib import blake2b
from typing import Collection, Generic, Literal, NamedTuple, Sequence, TYPE_CHECKING

import cocoindex as coco

if TYPE_CHECKING:
    from cocoindex import Scope


_FileName = str
_FileContent = bytes


class _FileAction(NamedTuple):
    path: pathlib.PurePath
    content: _FileContent | None


_FileFingerprint = bytes


class _FileHandler(coco.EffectHandler[_FileName, _FileContent, _FileFingerprint]):
    _base_path: pathlib.Path
    _sink: coco.EffectSink[_FileAction]

    def __init__(self, base_path: pathlib.Path) -> None:
        self._base_path = base_path
        self._sink = coco.EffectSink.from_fn(self._apply_actions)

    def _apply_actions(self, actions: Sequence[_FileAction]) -> None:
        for action in actions:
            path = self._base_path / action.path
            if action.content is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(action.content)

    def reconcile(
        self,
        key: _FileName,
        desired_effect: _FileContent | coco.NonExistenceType,
        prev_possible_states: Collection[_FileFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_FileAction, _FileFingerprint] | None:
        if coco.is_non_existence(desired_effect):
            return coco.EffectReconcileOutput(
                action=_FileAction(path=pathlib.PurePath(key), content=None),
                sink=self._sink,
                state=coco.NON_EXISTENCE,
            )

        # TODO: Replace with fingerprinting offered by CocoIndex core engine (e.g. the same mechanism used to detect cached function argument changes).
        target_fp = blake2b(desired_effect).digest()
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            return None
        return coco.EffectReconcileOutput(
            action=_FileAction(path=pathlib.PurePath(key), content=desired_effect),
            sink=self._sink,
            state=target_fp,
        )


class _DirKey(NamedTuple):
    # Exactly one of the two will be set
    stable_key: coco.StableKey | None = None
    path: pathlib.Path | None = None


@dataclass
class _DirSpec:
    path: pathlib.Path
    managed_by: Literal["system", "user"] = "system"


class _DirAction(NamedTuple):
    curr_path: pathlib.Path | None
    curr_path_action: Literal["insert", "upsert"] | None
    to_delete: set[pathlib.Path]


class _DirHandler(coco.EffectHandler[_DirKey, _DirSpec, _DirSpec, _FileHandler]):
    _sink: coco.EffectSink[_DirAction, _FileHandler]

    def __init__(self) -> None:
        self._sink = coco.EffectSink.from_fn(self._apply_actions)

    def _apply_actions(
        self, actions: Sequence[_DirAction]
    ) -> Sequence[coco.ChildEffectDef[_FileHandler] | None]:
        outputs: list[coco.ChildEffectDef[_FileHandler] | None] = []
        for action in actions:
            if action.curr_path is not None:
                curr_path = pathlib.Path(action.curr_path)
                if action.curr_path_action == "insert":
                    curr_path.mkdir(parents=True, exist_ok=False)
                elif action.curr_path_action == "upsert":
                    curr_path.mkdir(parents=True, exist_ok=True)
                outputs.append(coco.ChildEffectDef(handler=_FileHandler(curr_path)))
            else:
                outputs.append(None)

            for path in action.to_delete:
                if os.path.isdir(path):
                    shutil.rmtree(path)

        return outputs

    def reconcile(
        self,
        key: _DirKey,
        desired_effect: _DirSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_DirSpec],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_DirAction, _DirSpec, _FileHandler]:
        curr_path: pathlib.Path | None = None
        curr_path_action: Literal["insert", "upsert"] | None = None
        if not coco.is_non_existence(desired_effect):
            if desired_effect.managed_by == "system":
                may_exists = any(
                    prev.path == desired_effect.path for prev in prev_possible_states
                )
                must_exists = not prev_may_be_missing and all(
                    prev.path == desired_effect.path and prev.managed_by == "system"
                    for prev in prev_possible_states
                )
                if not must_exists:
                    curr_path_action = "upsert" if may_exists else "insert"

        curr_path = (
            desired_effect.path if not coco.is_non_existence(desired_effect) else None
        )

        to_delete: set[pathlib.Path] = set()
        for prev in prev_possible_states:
            if prev.managed_by == "system" and prev.path != curr_path:
                to_delete.add(prev.path)

        return coco.EffectReconcileOutput(
            action=_DirAction(
                curr_path=curr_path,
                curr_path_action=curr_path_action,
                to_delete=to_delete,
            ),
            sink=self._sink,
            state=desired_effect,
        )


_dir_provider = coco.register_root_effect_provider(
    "cocoindex.io/localfs/dir", _DirHandler()
)


class DirTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["DirTarget"]):
    """
    A target for writing files to a local directory.

    The directory is managed as an effect, with the scope used to scope the effect.
    """

    _provider: coco.EffectProvider[_FileName, _FileContent, None, coco.MaybePendingS]

    def __init__(
        self,
        _provider: coco.EffectProvider[
            _FileName, _FileContent, None, coco.MaybePendingS
        ],
    ) -> None:
        self._provider = _provider

    def declare_file(
        self: DirTarget, scope: Scope, *, filename: str, content: bytes | str
    ) -> None:
        """
        Declare a file to be written to this directory.

        Args:
            scope: The scope for effect declaration.
            filename: The name of the file.
            content: The content of the file (bytes or str).
        """
        if isinstance(content, str):
            content = content.encode()
        coco.declare_effect(scope, self._provider.effect(filename, content))


@coco.function
def dir_target(
    scope: Scope,
    path: pathlib.Path,
    *,
    stable_key: coco.StableKey | None = None,
    managed_by: Literal["system", "user"] = "system",
) -> DirTarget[coco.PendingS]:
    """
    Create a DirTarget.

    Args:
        scope: The scope for effect declaration.
        stable_key: Optional stable key for identifying the directory.
        path: The filesystem path for the directory.
        managed_by: Whether the directory is managed by "system" or "user".
    """
    key = (
        _DirKey(stable_key=stable_key) if stable_key is not None else _DirKey(path=path)
    )
    spec = _DirSpec(path=path, managed_by=managed_by)
    provider = coco.declare_effect_with_child(scope, _dir_provider.effect(key, spec))
    return DirTarget(provider)


__all__ = ["DirTarget", "dir_target"]
