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

    dim = int(raw_embeddings[0].shape[-1])
    index = _faiss.IndexFlatIP(dim)
    indexed_names: list[str] = []
    dedup: dict[str, str | None] = {}

    def _emit(event: ResolutionEvent) -> None:
        if on_resolution is not None:
            on_resolution(event)

    def _add_to_index(info: _EntityInfo) -> None:
        index.add(info.normalized_vec)
        indexed_names.append(info.name)

    def _chain_walk(name: str) -> str:
        current = name
        while True:
            target = dedup.get(current)
            if target is None:
                return current
            current = target

    def _search_candidates(info: _EntityInfo) -> list[str]:
        if index.ntotal == 0:
            return []
        k = min(top_n, index.ntotal)
        scores, idxs = index.search(info.normalized_vec, k)
        threshold = 1.0 - max_distance
        seen: set[str] = set()
        out: list[str] = []
        for score, idx in zip(scores[0], idxs[0]):
            if idx < 0 or score < threshold:
                continue
            canonical = _chain_walk(indexed_names[idx])
            if canonical == info.name or canonical in seen:
                continue
            seen.add(canonical)
            out.append(canonical)
        return out

    if existing_policy == ExistingCanonicalPolicy.PINNED:
        pass_1 = [i for i in entity_map.values() if i.is_existing]
        pass_2 = [i for i in entity_map.values() if not i.is_existing]
    else:
        pass_1 = []
        pass_2 = list(entity_map.values())

    for info in pass_1:
        dedup[info.name] = None
        _add_to_index(info)
        _emit(
            ResolutionEvent(
                entity=info.name,
                canonical=info.name,
                candidates=[],
                seeded=True,
            )
        )

    for info in pass_2:
        candidates = _search_candidates(info)

        if not candidates:
            dedup[info.name] = None
            _add_to_index(info)
            _emit(
                ResolutionEvent(
                    entity=info.name,
                    canonical=info.name,
                    candidates=[],
                )
            )
            continue

        decision = await resolve_pair(info.name, candidates)

        if decision.matched is not None and (
            decision.matched not in candidates or decision.matched == info.name
        ):
            raise ValueError(
                f"resolve_pair returned matched={decision.matched!r}, "
                f"which is not in candidates={candidates!r}. This is a "
                f"contract violation (see requirement.md)."
            )

        if decision.matched is None:
            dedup[info.name] = None
            canonical = info.name
            repointed: str | None = None
        else:
            matched = decision.matched
            if _new_wins(
                entity_info=info,
                matched_info=entity_map[matched],
                decision=decision,
                existing_policy=existing_policy,
            ):
                dedup[info.name] = None
                dedup[matched] = info.name
                canonical = info.name
                repointed = matched
            else:
                dedup[info.name] = matched
                canonical = matched
                repointed = None

        _add_to_index(info)
        _emit(
            ResolutionEvent(
                entity=info.name,
                canonical=canonical,
                candidates=candidates,
                decision=decision,
                repointed=repointed,
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
