"""Local TurboQuant vector target for CocoIndex.

This connector stores only quantized vectors in a ``.tvim`` file. Pair it with a
regular metadata target, such as SQLite, when callers need to retrieve payloads
or precompute search allowlists from relational filters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Collection, Generic, NamedTuple, Sequence
from uuid import uuid4

import msgspec
import numpy as np
import numpy.typing as npt
from typing_extensions import TypeVar

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_bytes
from cocoindex._internal import core

_VectorT = npt.NDArray[np.float32] | Sequence[float]
RowT = TypeVar("RowT", default=dict[str, Any])


class VectorRow(NamedTuple):
    """A vector row addressed by a stable unsigned 64-bit ID."""

    id: int
    vector: _VectorT


class _IndexSpec(msgspec.Struct, frozen=True):
    bit_width: int = 4


class _IndexTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    bit_width: int


class _IndexAction(NamedTuple):
    path: str
    spec: _IndexSpec | None
    reset: bool = False


class _VectorTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    fingerprint: bytes


class _VectorAction(NamedTuple):
    id: int
    vector: npt.NDArray[np.float32] | None


@dataclass
class _CachedIndex:
    stat_key: tuple[int, int]
    index: core.TurboQuantIdMapIndex
    prepared: bool


_INDEX_CACHE: dict[str, _CachedIndex] = {}


def _normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def _index_stat_key(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_mtime_ns, stat.st_size)


def _cache_index(
    path: Path, index: core.TurboQuantIdMapIndex, *, prepared: bool
) -> None:
    _INDEX_CACHE[_normalize_path(path)] = _CachedIndex(
        stat_key=_index_stat_key(path),
        index=index,
        prepared=prepared,
    )


def _validate_id(id_: int) -> None:
    if id_ < 0 or id_ > np.iinfo(np.uint64).max:
        raise ValueError(f"TurboQuant vector id must fit uint64, got {id_}")


def _vector_array(vector: _VectorT) -> npt.NDArray[np.float32]:
    arr = np.asarray(vector, dtype=np.float32)
    if arr.ndim != 1:
        raise ValueError(f"TurboQuant vector must be 1-D, got shape {arr.shape}")
    return np.ascontiguousarray(arr)


def _vector_fingerprint(vector: npt.NDArray[np.float32]) -> bytes:
    return fingerprint_bytes(vector.tobytes())


class _VectorHandler(coco.TargetHandler[VectorRow, _VectorTrackingRecord]):
    _path: Path
    _bit_width: int
    _index: core.TurboQuantIdMapIndex | None
    _sink: coco.TargetActionSink[_VectorAction, None]

    def __init__(self, path: str, bit_width: int) -> None:
        self._path = Path(path)
        self._bit_width = bit_width
        self._index = None
        self._sink = coco.TargetActionSink[_VectorAction, None].from_fn(
            self._apply_actions
        )

    def _load_index(self) -> core.TurboQuantIdMapIndex:
        if self._index is not None:
            return self._index

        if self._path.exists():
            index = core.TurboQuantIdMapIndex.load(str(self._path))
            if index.bit_width != self._bit_width:
                self._path.unlink(missing_ok=True)
                index = core.TurboQuantIdMapIndex(bit_width=self._bit_width)
        else:
            index = core.TurboQuantIdMapIndex(bit_width=self._bit_width)
        self._index = index
        return index

    def _write_index(self, index: core.TurboQuantIdMapIndex) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_name(f".{self._path.name}.{uuid4().hex}.tmp")
        try:
            index.write(str(tmp_path))
            tmp_path.replace(self._path)
            _cache_index(self._path, index, prepared=False)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _apply_actions(
        self,
        context_provider: coco.ContextProvider,
        actions: Sequence[_VectorAction],
        /,
    ) -> None:
        if not actions:
            return

        index = self._load_index()
        changed = False
        upsert_vectors: list[npt.NDArray[np.float32]] = []
        upsert_ids: list[int] = []

        for action in actions:
            _validate_id(action.id)
            if action.vector is None:
                changed = index.remove(action.id) or changed
                continue

            if index.contains(action.id):
                index.remove(action.id)
            upsert_vectors.append(action.vector)
            upsert_ids.append(action.id)
            changed = True

        if upsert_vectors:
            vectors = np.vstack(upsert_vectors).astype(np.float32, copy=False)
            ids = np.asarray(upsert_ids, dtype=np.uint64)
            index.add_with_ids(np.ascontiguousarray(vectors), ids)

        if changed:
            self._write_index(index)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: VectorRow | coco.NonExistenceType,
        prev_possible_records: Collection[_VectorTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_VectorAction, _VectorTrackingRecord] | None:
        id_ = int(key)
        _validate_id(id_)

        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_VectorAction(id_, None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        vector = _vector_array(desired_state.vector)
        fingerprint = _vector_fingerprint(vector)
        tracking_record = _VectorTrackingRecord(fingerprint)
        if not prev_may_be_missing and all(
            prev == tracking_record for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_VectorAction(id_, vector),
            sink=self._sink,
            tracking_record=tracking_record,
        )


class _IndexHandler(
    coco.TargetHandler[_IndexSpec, _IndexTrackingRecord, _VectorHandler]
):
    _sink: coco.TargetActionSink[_IndexAction, _VectorHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink[_IndexAction, _VectorHandler].from_fn(
            self._apply_actions
        )

    def _apply_actions(
        self,
        context_provider: coco.ContextProvider,
        actions: Sequence[_IndexAction],
        /,
    ) -> list[coco.ChildTargetDef[_VectorHandler] | None]:
        outputs: list[coco.ChildTargetDef[_VectorHandler] | None] = []
        for action in actions:
            path = Path(action.path)
            if action.spec is None:
                path.unlink(missing_ok=True)
                outputs.append(None)
                continue
            if action.reset:
                path.unlink(missing_ok=True)
            outputs.append(
                coco.ChildTargetDef(
                    _VectorHandler(action.path, bit_width=action.spec.bit_width)
                )
            )
        return outputs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _IndexSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_IndexTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_IndexAction, _IndexTrackingRecord, _VectorHandler]
        | None
    ):
        path = str(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_IndexAction(path, None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
                child_invalidation="destructive",
            )

        tracking_record = _IndexTrackingRecord(bit_width=desired_state.bit_width)
        reset = prev_may_be_missing or any(
            prev != tracking_record for prev in prev_possible_records
        )
        return coco.TargetReconcileOutput(
            action=_IndexAction(path, desired_state, reset=reset),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation="destructive" if reset else None,
        )


_index_provider = coco.register_root_target_states_provider(
    "cocoindex/turboquant/index", _IndexHandler()
)


class IndexTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["IndexTarget"]):
    """Target for writing vectors to a local TurboQuant index file."""

    _provider: coco.TargetStateProvider[VectorRow, None, coco.MaybePendingS]

    def __init__(
        self, provider: coco.TargetStateProvider[VectorRow, None, coco.MaybePendingS]
    ):
        self._provider = provider

    def declare_vector(self, *, id: int, vector: _VectorT) -> None:
        _validate_id(id)
        coco.declare_target_state(
            self._provider.target_state(id, VectorRow(id=id, vector=vector))
        )

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def index_target(
    path: str | Path,
    *,
    bit_width: int = 4,
) -> coco.TargetState[_VectorHandler]:
    """Create a target state for a local TurboQuant index file."""
    if bit_width not in {2, 3, 4}:
        raise ValueError(f"TurboQuant bit_width must be 2, 3, or 4, got {bit_width}")
    resolved_path = _normalize_path(path)
    return _index_provider.target_state(resolved_path, _IndexSpec(bit_width=bit_width))


def declare_index_target(
    path: str | Path,
    *,
    bit_width: int = 4,
) -> IndexTarget[coco.PendingS]:
    """Declare a local TurboQuant index target and return a vector writer."""
    provider = coco.declare_target_state_with_child(
        index_target(path, bit_width=bit_width)
    )
    return IndexTarget(provider)


async def mount_index_target(
    path: str | Path,
    *,
    bit_width: int = 4,
) -> IndexTarget[coco.ResolvedS]:
    """Mount a local TurboQuant index target and return a vector writer."""
    provider = await coco.mount_target(index_target(path, bit_width=bit_width))
    return IndexTarget(provider)


def load_index(path: str | Path) -> core.TurboQuantIdMapIndex:
    """Load a local TurboQuant index for querying."""
    normalized_path = _normalize_path(path)
    resolved_path = Path(normalized_path)
    stat_key = _index_stat_key(resolved_path)
    cached = _INDEX_CACHE.get(normalized_path)
    if cached is None or cached.stat_key != stat_key:
        cached = _CachedIndex(
            stat_key=stat_key,
            index=core.TurboQuantIdMapIndex.load(normalized_path),
            prepared=False,
        )
        _INDEX_CACHE[normalized_path] = cached
    if not cached.prepared:
        cached.index.prepare()
        cached.prepared = True
    return cached.index


@dataclass(frozen=True)
class SearchResult:
    score: float
    id: int


def search(
    path: str | Path,
    query: _VectorT,
    k: int,
    *,
    allowlist: Sequence[int] | npt.NDArray[np.uint64] | None = None,
) -> list[SearchResult]:
    """Search a local TurboQuant index and return flattened results for one query."""
    index = load_index(path)
    query_array = np.ascontiguousarray(_vector_array(query)[None, :])
    allowlist_array = (
        None
        if allowlist is None
        else np.ascontiguousarray(np.asarray(allowlist, dtype=np.uint64))
    )
    scores, ids = index.search(query_array, k, allowlist=allowlist_array)
    return [
        SearchResult(score=float(score), id=int(id_))
        for score, id_ in zip(scores.reshape(-1), ids.reshape(-1), strict=True)
    ]


__all__ = [
    "IndexTarget",
    "SearchResult",
    "VectorRow",
    "declare_index_target",
    "index_target",
    "load_index",
    "mount_index_target",
    "search",
]
