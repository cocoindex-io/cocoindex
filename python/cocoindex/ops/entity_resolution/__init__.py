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
    """A single entity's resolution outcome. ``resolve_entities`` collects
    one event per resolved entity and invokes ``on_resolution`` for each
    one after all components finish, in the same order the prior
    single-pass implementation used (PREFERRED: sorted by entity name;
    PINNED: pass-1 existings first then pass-2 non-existings, each in
    sorted order)."""

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

    ``__call__`` may be invoked concurrently. ``resolve_entities`` partitions
    entities into independent components and resolves them in parallel, so a
    resolver instance can see overlapping ``__call__`` invocations. Built-in
    resolvers are concurrency-safe; custom implementations with mutable
    internal state should guard it accordingly.
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
            if len(out) >= self._top_n:
                # ``top_n`` bounds the candidate list size — the public
                # docs frame it as a maximum. Stop as soon as we have
                # enough distinct canonicals so the backfill loop in
                # ``search`` returns exactly ``top_n`` and never more.
                break
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


@_dataclasses.dataclass(frozen=True, slots=True)
class _ComponentEvents:
    """Per-component events split by resolution phase.

    The split is the source of truth for cross-component ordering. The
    PINNED contract is 'all pass-1 (seeded existings) first, then all
    pass-2 (non-existings)', and storing the phases separately means the
    merge step does not have to infer phase from a derived field on
    ResolutionEvent (which historically conflated `seeded=True` with
    'belongs in pass-1').
    """

    pass_1: list[ResolutionEvent]
    pass_2: list[ResolutionEvent]


async def _resolve_component(
    infos: list[_EntityInfo],
    *,
    entity_map: dict[str, _EntityInfo],
    dedup: dict[str, str | None],
    candidate_index: _CandidateIndex,
    existing_policy: ExistingCanonicalPolicy,
    resolve_pair: PairResolver,
    events: _ComponentEvents,
) -> None:
    """Greedy two-pass resolution over one connected component (or one
    entire input, when no graph partitioning has happened yet).

    Mutates ``dedup``, ``candidate_index``, and ``events`` in place.
    ``entity_map`` is read-only for canonical-selection lookups during
    decision application.
    """
    if existing_policy == ExistingCanonicalPolicy.PINNED:
        pass_1 = [i for i in infos if i.is_existing]
        pass_2 = [i for i in infos if not i.is_existing]
    else:
        pass_1 = []
        pass_2 = infos

    for info in pass_1:
        dedup[info.name] = None
        candidate_index.add(info)
        events.pass_1.append(_event_for_seeded_existing(info))

    for info in pass_2:
        candidates = candidate_index.search(info)

        if not candidates:
            dedup[info.name] = None
            candidate_index.add(info)
            events.pass_2.append(_event_for_new_canonical(info))
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
        events.pass_2.append(
            _event_for_pair_decision(
                info=info,
                candidates=candidates,
                decision=decision,
                application=application,
            )
        )


def _partition_components(
    entity_list: list[str],
    normalized_vecs: list[_NDArray[_np.float32]],
    *,
    max_distance: float,
) -> list[list[int]]:
    """Connected components of the conservative candidate-edge graph.

    Edges: every (i, j) pair with cosine similarity ≥ ``1 - max_distance``.
    This is a superset of any edge the runtime greedy ``_CandidateIndex``
    could ever surface (which only ever filters down via chain-walk). Extra
    edges reduce parallelism but cannot change correctness; missing edges
    can change behavior, so we must not under-approximate.

    Returns a list of components, each as a sorted list of indexes into
    ``entity_list``. The component list itself is sorted by lex-min entity
    name so downstream dispatch is deterministic.
    """
    n = len(entity_list)
    if n == 0:
        return []
    if n == 1:
        return [[0]]

    dim = int(normalized_vecs[0].shape[-1])
    matrix = _np.stack([v.reshape(-1) for v in normalized_vecs]).astype(_np.float32)
    index = _faiss.IndexFlatIP(dim)
    index.add(matrix)
    # FAISS IndexFlatIP.range_search is strict (returns pairs with score >
    # radius). The runtime _CandidateIndex.search filter is inclusive
    # (score >= threshold). Stepping the radius one float32 below the
    # threshold ensures the boundary-equal pairs that the runtime would
    # surface are also captured by the partition, preserving the
    # superset invariant at the exact equality boundary.
    threshold = _np.float32(1.0 - max_distance)
    radius = float(_np.nextafter(threshold, _np.float32(-_np.inf)))
    lims, _scores, idxs = index.range_search(matrix, radius)

    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i in range(n):
        for slot in range(int(lims[i]), int(lims[i + 1])):
            j = int(idxs[slot])
            if j <= i:
                continue
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[ri] = rj

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    return sorted(
        groups.values(),
        key=lambda members: entity_list[members[0]],
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

    Resolution proceeds in three phases. First, all entities are embedded
    concurrently. Second, a transient FAISS index partitions entities into
    connected components of the candidate-similarity graph — entities in
    different components can never compare to each other under any chain
    walk. Third, each component is resolved by an independent greedy runner
    (same algorithm as the single-component case) and runners execute
    concurrently. Within a component, processing order is deterministic
    (sorted by entity name); across components, the ``on_resolution``
    callback fires in the same per-policy order as the single-component
    case (PREFERRED: globally sorted; PINNED: all existings first sorted,
    then all non-existings sorted).

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
    normalized_vecs: list[_NDArray[_np.float32]] = []
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
        normalized_vecs.append(vec)

    dim = int(raw_embeddings[0].shape[-1])
    components = _partition_components(
        entity_list, normalized_vecs, max_distance=max_distance
    )

    dedup: dict[str, str | None] = {}
    # Pre-allocated per-component event records. Storing the lists in the
    # parent scope (instead of returning them from each task) keeps
    # already-emitted events reachable even if a sibling task gets
    # cancelled mid-flight — important for preserving partial-failure
    # observability through on_resolution.
    per_component_events: list[_ComponentEvents] = [
        _ComponentEvents(pass_1=[], pass_2=[]) for _ in components
    ]

    async def _run_one(events: _ComponentEvents, member_idxs: list[int]) -> None:
        component_index = _CandidateIndex(
            dim=dim,
            dedup=dedup,
            max_distance=max_distance,
            top_n=top_n,
        )
        infos = [entity_map[entity_list[i]] for i in member_idxs]
        await _resolve_component(
            infos,
            entity_map=entity_map,
            dedup=dedup,
            candidate_index=component_index,
            existing_policy=existing_policy,
            resolve_pair=resolve_pair,
            events=events,
        )

    # Cancel sibling component tasks on the first exception. Bare
    # asyncio.gather lets siblings keep running after the exception has
    # already propagated to the caller, leaking resolver calls (and any
    # LLM cost they incur) for results that will never be observed. A
    # TaskGroup would also achieve this but wraps the exception in
    # BaseExceptionGroup, hiding the original type from callers; spelling
    # the cancellation out preserves the original exception.
    tasks: list[_asyncio.Task[None]] = [
        _asyncio.create_task(_run_one(events, members))
        for events, members in zip(per_component_events, components)
    ]
    try:
        await _asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await _asyncio.gather(*tasks, return_exceptions=True)
        # Even on partial failure, surface the events that completed
        # before the exception. Otherwise on_resolution loses every
        # already-resolved entity, breaking observability the prior
        # single-pass implementation provided via real-time emission.
        if on_resolution is not None:
            _deliver_events(per_component_events, on_resolution)
        raise

    if on_resolution is not None:
        _deliver_events(per_component_events, on_resolution)

    # Rebuild the dedup map in sorted entity order. Concurrent component
    # runners populated the shared dict in scheduler-interleaved order;
    # exposing that as ResolvedEntities iteration order would be a
    # behavioral regression vs the prior single-pass implementation that
    # iterated `sorted(set(entities))`.
    ordered_dedup: dict[str, str | None] = {name: dedup[name] for name in entity_list}
    return ResolvedEntities(ordered_dedup)


def _deliver_events(
    per_component_events: list[_ComponentEvents],
    on_resolution: _Callable[[ResolutionEvent], None],
) -> None:
    # PINNED requires 'all pass-1 first, then all pass-2'. PREFERRED's
    # pass_1 lists are always empty (the policy only seeds in PINNED), so
    # the same two-phase emit collapses to a single sorted stream there.
    # Using the explicit phase lists — rather than inferring from
    # ``ResolutionEvent.seeded`` — keeps the ordering invariant tied to
    # the source of truth in _resolve_component.
    pass_1 = sorted(
        (e for events in per_component_events for e in events.pass_1),
        key=lambda e: e.entity,
    )
    pass_2 = sorted(
        (e for events in per_component_events for e in events.pass_2),
        key=lambda e: e.entity,
    )
    for event in pass_1:
        on_resolution(event)
    for event in pass_2:
        on_resolution(event)


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
