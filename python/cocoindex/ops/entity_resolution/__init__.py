"""Entity resolution via embedding similarity + pluggable pair resolution.

See ``specs/entity_resolution/requirement.md`` for the full contract.
"""

from __future__ import annotations

import asyncio as _asyncio
import dataclasses as _dataclasses
import enum as _enum
import typing as _typing
from collections.abc import (
    Callable as _Callable,
    Iterable as _Iterable,
    Iterator as _Iterator,
)

import faiss as _faiss
import numpy as _np
from numpy.typing import NDArray as _NDArray

from cocoindex.resources.embedder import Embedder as _Embedder

__all__ = [
    "CanonicalSide",
    "ExistingCanonicalPolicy",
    "PairDecision",
    "PairResolver",
    "ResolutionEvent",
    "ResolvedEntities",
    "resolve_entities",
]


class CanonicalSide(_enum.StrEnum):
    """Which side of a positive pair-match should become canonical."""

    NEW = "new"
    """The entity being processed."""

    MATCHED = "matched"
    """The already-in-map candidate."""


@_dataclasses.dataclass(frozen=True, slots=True)
class PairDecision:
    """Outcome of comparing a new entity against a list of candidates."""

    matched: str | None = None
    """Name of the matching candidate, or None if no match."""

    canonical: CanonicalSide = CanonicalSide.MATCHED
    """Which side should be canonical. Ignored when `matched` is None.
    Advisory: may be overridden by `existing_policy` in ``resolve_entities``."""


class ExistingCanonicalPolicy(_enum.StrEnum):
    """How ``is_existing_canonical`` interacts with the pair-resolver's verdict."""

    PINNED = "pinned"
    """Existings are pinned as independent canonicals without consulting the
    resolver. Two existings never merge; non-existings that match an existing
    are always chained under the existing."""

    PREFERRED = "preferred"
    """Resolver is always consulted; existing status breaks ties."""


@_dataclasses.dataclass(frozen=True, slots=True)
class ResolutionEvent:
    """A single entity's resolution outcome. Emitted in real time as
    ``resolve_entities`` proceeds."""

    entity: str
    """The entity just resolved."""

    canonical: str
    """Its canonical after this event (may equal ``entity``)."""

    candidates: list[str]
    """Candidates passed to the resolver (empty if no resolver call)."""

    decision: PairDecision | None = None
    """The resolver's raw verdict. None iff the resolver wasn't called.
    Compare against `canonical`/`repointed` to detect policy overrides."""

    repointed: str | None = None
    """Prior-canonical name demoted to chain under `entity` (None if no
    repoint happened)."""

    seeded: bool = False
    """True if the entity was added as canonical directly, without any
    candidate search or resolver call (pinned existing-canonical seeding)."""


@_typing.runtime_checkable
class PairResolver(_typing.Protocol):
    """Callable that decides if ``entity`` matches any of ``candidates``.

    ``candidates`` is a non-empty, de-duplicated list of canonical names
    (chain-walked at call time) with cosine similarity to ``entity`` above
    the threshold. Must return a :class:`PairDecision` whose ``matched`` is
    either None or one of the supplied ``candidates`` (else
    ``resolve_entities`` raises :exc:`ValueError`).
    """

    async def __call__(
        self,
        entity: str,
        candidates: list[str],
    ) -> PairDecision: ...


@_dataclasses.dataclass(frozen=True, slots=True)
class ResolvedEntities:
    """Result of entity resolution.

    Wraps the underlying ``name -> canonical | None`` dedup map and provides
    safe chain-walking. Treat as read-only; mutations are not part of the
    contract.
    """

    _dedup: dict[str, str | None]

    def canonical_of(self, name: str) -> str:
        """Return the canonical name for ``name``.

        Returns ``name`` itself if it is already canonical. Raises
        :exc:`KeyError` if unknown. Terminates without cycle detection
        because the dedup map is acyclic by construction (see
        requirement.md § "Acyclic by construction").
        """
        if name not in self._dedup:
            raise KeyError(name)
        current = name
        while True:
            target = self._dedup[current]
            if target is None:
                return current
            current = target

    def canonicals(self) -> set[str]:
        """Set of all canonical names (entries whose value is None)."""
        return {name for name, target in self._dedup.items() if target is None}

    def groups(self) -> dict[str, set[str]]:
        """Map each canonical name to the set of all names that resolve to
        it (including itself)."""
        out: dict[str, set[str]] = {c: {c} for c in self.canonicals()}
        for name in self._dedup:
            out[self.canonical_of(name)].add(name)
        return out

    def __iter__(self) -> _Iterator[str]:
        return iter(self._dedup)

    def __contains__(self, name: str) -> bool:
        return name in self._dedup

    def __len__(self) -> int:
        return len(self._dedup)

    def to_dict(self) -> dict[str, str | None]:
        """Return a copy of the underlying dedup map."""
        return dict(self._dedup)


class _EntityInfo:
    """Per-entity precomputed state. Populated once; used throughout resolution."""

    __slots__ = ("name", "normalized_vec", "is_existing")

    def __init__(
        self,
        name: str,
        normalized_vec: _NDArray[_np.float32],
        is_existing: bool,
    ) -> None:
        self.name = name
        self.normalized_vec = normalized_vec
        self.is_existing = is_existing


@_dataclasses.dataclass(frozen=True, slots=True)
class _DecisionApplication:
    canonical: str
    repointed: str | None = None


class _CandidateIndex:
    """FAISS index plus canonical-chain aware candidate lookup."""

    __slots__ = ("_dedup", "_index", "_indexed_names", "_max_distance", "_top_n")

    def __init__(
        self,
        *,
        dim: int,
        dedup: dict[str, str | None],
        max_distance: float,
        top_n: int,
    ) -> None:
        self._dedup = dedup
        self._index = _faiss.IndexFlatIP(dim)
        self._indexed_names: list[str] = []
        self._max_distance = max_distance
        self._top_n = top_n

    def add(self, info: _EntityInfo) -> None:
        self._index.add(info.normalized_vec)
        self._indexed_names.append(info.name)

    def search(self, info: _EntityInfo) -> list[str]:
        if self._index.ntotal == 0 or self._top_n <= 0:
            return []
        threshold = 1.0 - self._max_distance
        k = min(self._top_n, self._index.ntotal)

        while True:
            scores, idxs = self._index.search(info.normalized_vec, k)
            candidates = self._distinct_canonicals(scores[0], idxs[0], threshold, info)
            if len(candidates) >= self._top_n:
                return candidates
            if k >= self._index.ntotal or scores[0][-1] < threshold:
                return candidates
            k = min(self._index.ntotal, max(k + 1, k * 2))

    def _distinct_canonicals(
        self,
        scores: _NDArray[_np.float32],
        idxs: _NDArray[_np.int64],
        threshold: float,
        info: _EntityInfo,
    ) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for score, idx in zip(scores, idxs):
            if idx < 0 or score < threshold:
                continue
            canonical = _chain_walk(self._dedup, self._indexed_names[idx])
            if canonical == info.name or canonical in seen:
                continue
            seen.add(canonical)
            out.append(canonical)
        return out


def _chain_walk(dedup: dict[str, str | None], name: str) -> str:
    current = name
    while True:
        target = dedup.get(current)
        if target is None:
            return current
        current = target


def _validate_pair_decision(
    *,
    entity: str,
    candidates: list[str],
    decision: PairDecision,
) -> None:
    if decision.matched is not None and (
        decision.matched not in candidates or decision.matched == entity
    ):
        raise ValueError(
            f"resolve_pair returned matched={decision.matched!r}, "
            f"which is not in candidates={candidates!r}. This is a "
            f"contract violation (see requirement.md)."
        )


def _apply_pair_decision(
    *,
    info: _EntityInfo,
    decision: PairDecision,
    entity_map: dict[str, _EntityInfo],
    dedup: dict[str, str | None],
    existing_policy: ExistingCanonicalPolicy,
) -> _DecisionApplication:
    if decision.matched is None:
        dedup[info.name] = None
        return _DecisionApplication(canonical=info.name)

    matched = decision.matched
    if _new_wins(
        entity_info=info,
        matched_info=entity_map[matched],
        decision=decision,
        existing_policy=existing_policy,
    ):
        dedup[info.name] = None
        dedup[matched] = info.name
        return _DecisionApplication(canonical=info.name, repointed=matched)

    dedup[info.name] = matched
    return _DecisionApplication(canonical=matched)


def _event_for_seeded_existing(info: _EntityInfo) -> ResolutionEvent:
    return ResolutionEvent(
        entity=info.name,
        canonical=info.name,
        candidates=[],
        seeded=True,
    )


def _event_for_new_canonical(info: _EntityInfo) -> ResolutionEvent:
    return ResolutionEvent(
        entity=info.name,
        canonical=info.name,
        candidates=[],
    )


def _event_for_pair_decision(
    *,
    info: _EntityInfo,
    candidates: list[str],
    decision: PairDecision,
    application: _DecisionApplication,
) -> ResolutionEvent:
    return ResolutionEvent(
        entity=info.name,
        canonical=application.canonical,
        candidates=candidates,
        decision=decision,
        repointed=application.repointed,
    )


async def resolve_entities(
    entities: _Iterable[str],
    *,
    embedder: _Embedder,
    resolve_pair: PairResolver,
    is_existing_canonical: _Callable[[str], bool] | None = None,
    existing_policy: ExistingCanonicalPolicy = ExistingCanonicalPolicy.PINNED,
    on_resolution: _Callable[[ResolutionEvent], None] | None = None,
    max_distance: float = 0.3,
    top_n: int = 5,
) -> ResolvedEntities:
    """Resolve a set of raw entity names into a canonical dedup map.

    See ``specs/entity_resolution/requirement.md`` for the full contract,
    including existing-canonical policies (PINNED / PREFERRED),
    canonical-selection rules, and event-emission semantics.
    """
    entity_list = sorted(set(entities))
    if not entity_list:
        return ResolvedEntities({})

    raw_embeddings = await _asyncio.gather(
        *(embedder.embed(name) for name in entity_list)
    )

    entity_map: dict[str, _EntityInfo] = {}
    for name, raw_vec in zip(entity_list, raw_embeddings):
        vec = raw_vec.reshape(1, -1).copy()
        _faiss.normalize_L2(vec)
        entity_map[name] = _EntityInfo(
            name=name,
            normalized_vec=vec,
            is_existing=(
                is_existing_canonical(name)
                if is_existing_canonical is not None
                else False
            ),
        )

    dedup: dict[str, str | None] = {}
    candidate_index = _CandidateIndex(
        dim=int(raw_embeddings[0].shape[-1]),
        dedup=dedup,
        max_distance=max_distance,
        top_n=top_n,
    )

    def _emit(event: ResolutionEvent) -> None:
        if on_resolution is not None:
            on_resolution(event)

    if existing_policy == ExistingCanonicalPolicy.PINNED:
        pass_1 = [i for i in entity_map.values() if i.is_existing]
        pass_2 = [i for i in entity_map.values() if not i.is_existing]
    else:
        pass_1 = []
        pass_2 = list(entity_map.values())

    for info in pass_1:
        dedup[info.name] = None
        candidate_index.add(info)
        _emit(_event_for_seeded_existing(info))

    for info in pass_2:
        candidates = candidate_index.search(info)

        if not candidates:
            dedup[info.name] = None
            candidate_index.add(info)
            _emit(_event_for_new_canonical(info))
            continue

        decision = await resolve_pair(info.name, candidates)
        _validate_pair_decision(
            entity=info.name,
            candidates=candidates,
            decision=decision,
        )
        application = _apply_pair_decision(
            info=info,
            decision=decision,
            entity_map=entity_map,
            dedup=dedup,
            existing_policy=existing_policy,
        )
        candidate_index.add(info)
        _emit(
            _event_for_pair_decision(
                info=info,
                candidates=candidates,
                decision=decision,
                application=application,
            )
        )

    return ResolvedEntities(dedup)


def _new_wins(
    *,
    entity_info: _EntityInfo,
    matched_info: _EntityInfo,
    decision: PairDecision,
    existing_policy: ExistingCanonicalPolicy,
) -> bool:
    """Apply canonical selection based on existing-canonical policy."""
    if existing_policy == ExistingCanonicalPolicy.PINNED:
        if matched_info.is_existing:
            return False
        return decision.canonical == CanonicalSide.NEW

    if existing_policy == ExistingCanonicalPolicy.PREFERRED:
        if entity_info.is_existing and not matched_info.is_existing:
            return True
        if matched_info.is_existing and not entity_info.is_existing:
            return False

    return decision.canonical == CanonicalSide.NEW
