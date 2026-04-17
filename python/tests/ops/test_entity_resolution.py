"""Tests for cocoindex.ops.entity_resolution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import numpy as np
import pytest
from numpy.typing import NDArray

pytest.importorskip("faiss", reason="faiss-cpu not installed")

from cocoindex.ops.entity_resolution import (  # noqa: E402
    CanonicalSide,
    ExistingCanonicalPolicy,
    PairDecision,
    ResolutionEvent,
    ResolvedEntities,
    resolve_entities,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class MockEmbedder:
    """Deterministic embedder producing controlled similarity structure.

    Names in the same `similarity_groups` entry map to vectors with cosine
    similarity ≈ 1.0; names in different groups map to vectors with cosine
    similarity ≈ 0. Names not listed produce distinct isolated vectors.
    """

    def __init__(self, similarity_groups: list[set[str]], dim: int = 32) -> None:
        if len(similarity_groups) > dim // 2:
            raise ValueError("too many groups for chosen dim")
        self._vectors: dict[str, NDArray[np.float32]] = {}
        for group_idx, group in enumerate(similarity_groups):
            for member_idx, name in enumerate(sorted(group)):
                vec = np.zeros(dim, dtype=np.float32)
                vec[group_idx] = 1.0
                # Tiny perturbation keeps intra-group cosine ~0.9975
                vec[dim // 2 + member_idx % (dim // 2)] = 0.05
                vec /= np.linalg.norm(vec)
                self._vectors[name] = vec.astype(np.float32)

    def add_isolated(self, name: str, axis: int, dim: int = 32) -> None:
        """Add a name with a far-away vector (for no-match scenarios)."""
        vec = np.zeros(dim, dtype=np.float32)
        vec[axis] = 1.0
        self._vectors[name] = vec

    async def embed(self, text: str) -> NDArray[np.float32]:
        if text not in self._vectors:
            raise KeyError(f"MockEmbedder has no vector for {text!r}")
        return self._vectors[text]


@dataclass
class ScriptedResolver:
    """PairResolver with pre-scripted decisions per (entity, candidates) call.

    Candidates are matched by frozenset membership, so order doesn't matter.
    An unscripted call raises AssertionError to fail the test loudly.
    """

    decisions: dict[tuple[str, frozenset[str]], PairDecision]
    calls: list[tuple[str, list[str]]] = field(default_factory=list)

    async def __call__(self, entity: str, candidates: list[str]) -> PairDecision:
        self.calls.append((entity, list(candidates)))
        key = (entity, frozenset(candidates))
        if key not in self.decisions:
            raise AssertionError(
                f"ScriptedResolver: no decision for {entity=!r}, "
                f"candidates={candidates!r}"
            )
        return self.decisions[key]


def capture_events() -> tuple[list[ResolutionEvent], Callable[[ResolutionEvent], None]]:
    """Return (events_list, on_resolution_callback)."""
    events: list[ResolutionEvent] = []

    def _on(event: ResolutionEvent) -> None:
        events.append(event)

    return events, _on


# ---------------------------------------------------------------------------
# Core algorithm tests (Mode 1, no predicate)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_input() -> None:
    result = await resolve_entities(
        entities=set(),
        embedder=MockEmbedder([]),
        resolve_pair=ScriptedResolver({}),
    )
    assert len(result) == 0
    assert result.canonicals() == set()
    assert result.groups() == {}
    assert result.to_dict() == {}


@pytest.mark.asyncio
async def test_single_entity() -> None:
    resolver = ScriptedResolver({})
    result = await resolve_entities(
        entities={"A"},
        embedder=MockEmbedder([{"A"}]),
        resolve_pair=resolver,
    )
    assert result.canonical_of("A") == "A"
    assert result.canonicals() == {"A"}
    assert result.groups() == {"A": {"A"}}
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_no_matches_all_canonical() -> None:
    # Three distinct groups → no matches expected
    embedder = MockEmbedder([{"A"}, {"B"}, {"C"}])
    resolver = ScriptedResolver({})  # should never be called
    result = await resolve_entities(
        entities={"A", "B", "C"},
        embedder=embedder,
        resolve_pair=resolver,
    )
    assert result.canonicals() == {"A", "B", "C"}
    assert resolver.calls == []  # no candidates above threshold


@pytest.mark.asyncio
async def test_matched_wins_default() -> None:
    # A and B are near-duplicates; visiting order is sorted → A first.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver({("B", frozenset({"A"})): PairDecision(matched="A")})
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
    )
    assert result.canonical_of("A") == "A"
    assert result.canonical_of("B") == "A"
    assert result.canonicals() == {"A"}
    assert result.groups() == {"A": {"A", "B"}}


@pytest.mark.asyncio
async def test_new_wins_repoints() -> None:
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(
                matched="A", canonical=CanonicalSide.NEW
            )
        }
    )
    events, on_res = capture_events()
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        on_resolution=on_res,
    )
    assert result.canonical_of("A") == "B"
    assert result.canonical_of("B") == "B"
    assert result.canonicals() == {"B"}
    assert result.groups() == {"B": {"A", "B"}}
    # Event for B should have repointed=A
    b_event = next(e for e in events if e.entity == "B")
    assert b_event.repointed == "A"
    assert b_event.canonical == "B"


@pytest.mark.asyncio
async def test_multi_hop_chain() -> None:
    # A, B, C all similar. Sorted order: A → B → C.
    # B matches A (MATCHED wins). C searches index, finds A and B as neighbors;
    # both chain-walk to A; dedup → candidates = ["A"]. Resolver says A.
    embedder = MockEmbedder([{"A", "B", "C"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(matched="A"),
            ("C", frozenset({"A"})): PairDecision(matched="A"),
        }
    )
    result = await resolve_entities(
        entities={"A", "B", "C"},
        embedder=embedder,
        resolve_pair=resolver,
    )
    assert result.canonical_of("A") == "A"
    assert result.canonical_of("B") == "A"
    assert result.canonical_of("C") == "A"
    assert result.groups() == {"A": {"A", "B", "C"}}


# ---------------------------------------------------------------------------
# Mode 2 PREFERRED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mode2_preference_exactly_one_existing() -> None:
    # A existing, B non-existing, near-duplicates.
    # Resolver says NEW wins, but policy forces existing A to stay canonical.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(
                matched="A", canonical=CanonicalSide.NEW
            )
        }
    )
    events, on_res = capture_events()
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n == "A",
        existing_policy=ExistingCanonicalPolicy.PREFERRED,
        on_resolution=on_res,
    )
    assert result.canonical_of("B") == "A"
    assert result.canonicals() == {"A"}
    b_event = next(e for e in events if e.entity == "B")
    # Decision field reflects the resolver's raw verdict even though policy overrode
    assert b_event.decision is not None
    assert b_event.decision.canonical == CanonicalSide.NEW
    assert b_event.repointed is None  # policy stopped the repoint
    assert b_event.canonical == "A"


@pytest.mark.asyncio
async def test_mode2_preference_both_existing_merges() -> None:
    # Both A and B existing; resolver picks NEW → wiki-style: B becomes canonical.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(
                matched="A", canonical=CanonicalSide.NEW
            )
        }
    )
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n in {"A", "B"},
        existing_policy=ExistingCanonicalPolicy.PREFERRED,
    )
    # Documents "PREFERRED is a tiebreaker, not a lock"
    assert result.canonical_of("A") == "B"
    assert result.canonical_of("B") == "B"
    assert result.canonicals() == {"B"}


# ---------------------------------------------------------------------------
# Mode 3 PINNED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mode3_binding_pass1_seeds_no_resolver() -> None:
    embedder = MockEmbedder([{"A"}, {"B"}, {"C"}])
    resolver = ScriptedResolver({})  # never called in Pass 1 (and Pass 2 empty)
    events, on_res = capture_events()
    result = await resolve_entities(
        entities={"A", "B", "C"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: True,  # all existing
        existing_policy=ExistingCanonicalPolicy.PINNED,
        on_resolution=on_res,
    )
    assert result.canonicals() == {"A", "B", "C"}
    assert resolver.calls == []
    # All events should have seeded=True, candidates=[], decision=None
    assert all(e.seeded for e in events)
    assert all(e.candidates == [] for e in events)
    assert all(e.decision is None for e in events)
    assert all(e.repointed is None for e in events)


@pytest.mark.asyncio
async def test_mode3_binding_existings_never_merge() -> None:
    # Both existings with high similarity — would merge under Mode 2,
    # stay independent under PINNED.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver({})  # should never be called
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n in {"A", "B"},
        existing_policy=ExistingCanonicalPolicy.PINNED,
    )
    assert result.canonicals() == {"A", "B"}
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_mode3_binding_pass2_existing_wins() -> None:
    # A existing (seeded in Pass 1); B non-existing (Pass 2). Near-duplicates.
    # Resolver says NEW wins, but PINNED ignores that when matched is existing.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(
                matched="A", canonical=CanonicalSide.NEW
            )
        }
    )
    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n == "A",
        existing_policy=ExistingCanonicalPolicy.PINNED,
    )
    assert result.canonical_of("B") == "A"
    assert result.canonicals() == {"A"}


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_value_error_matched_not_in_candidates() -> None:
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {("B", frozenset({"A"})): PairDecision(matched="ghost")}
    )
    with pytest.raises(ValueError, match="not in candidates"):
        await resolve_entities(
            entities={"A", "B"},
            embedder=embedder,
            resolve_pair=resolver,
        )


@pytest.mark.asyncio
async def test_value_error_matched_equals_entity() -> None:
    # Script the resolver to return matched=entity — should be rejected even
    # though entity is never in candidates (defensive check).
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver({("B", frozenset({"A"})): PairDecision(matched="B")})
    with pytest.raises(ValueError, match="not in candidates"):
        await resolve_entities(
            entities={"A", "B"},
            embedder=embedder,
            resolve_pair=resolver,
        )


@pytest.mark.asyncio
async def test_existing_policy_ignored_without_predicate() -> None:
    # existing_policy=PINNED is the default but harmless without a predicate —
    # all entities are treated as non-existing, so the policy has no effect.
    resolver = ScriptedResolver({})
    result = await resolve_entities(
        entities={"A"},
        embedder=MockEmbedder([{"A"}]),
        resolve_pair=resolver,
        existing_policy=ExistingCanonicalPolicy.PINNED,
    )
    assert result.canonical_of("A") == "A"


# ---------------------------------------------------------------------------
# Event tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_resolution_decision_field() -> None:
    # Mode 2, exactly-one-existing: decision.canonical=NEW, but policy overrides.
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(
                matched="A", canonical=CanonicalSide.NEW
            )
        }
    )
    events, on_res = capture_events()
    await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n == "A",
        existing_policy=ExistingCanonicalPolicy.PREFERRED,
        on_resolution=on_res,
    )
    b_event = next(e for e in events if e.entity == "B")
    # decision reflects resolver's raw verdict; canonical reflects policy-adjusted outcome
    assert b_event.decision == PairDecision(matched="A", canonical=CanonicalSide.NEW)
    assert b_event.canonical == "A"  # policy kept A canonical
    # Caller can detect the override via comparison
    assert b_event.decision.canonical == CanonicalSide.NEW
    assert b_event.repointed is None


@pytest.mark.asyncio
async def test_on_resolution_exception_aborts() -> None:
    embedder = MockEmbedder([{"A"}, {"B"}, {"C"}])
    resolver = ScriptedResolver({})

    call_count = {"n": 0}

    def _on(event: ResolutionEvent) -> None:
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await resolve_entities(
            entities={"A", "B", "C"},
            embedder=embedder,
            resolve_pair=resolver,
            on_resolution=_on,
        )
    # Second callback fired → aborted during/after second entity processing.
    assert call_count["n"] == 2
