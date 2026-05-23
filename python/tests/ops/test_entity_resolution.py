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


class VectorEmbedder:
    """Embedder backed by explicit vectors for ranking-sensitive tests."""

    def __init__(self, vectors: dict[str, tuple[float, ...]]) -> None:
        self._vectors = {
            name: np.array(vector, dtype=np.float32) for name, vector in vectors.items()
        }

    async def embed(self, text: str) -> NDArray[np.float32]:
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
async def test_duplicates_collapsed_and_processed_in_sorted_order() -> None:
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver({("B", frozenset({"A"})): PairDecision(matched="A")})
    events, on_res = capture_events()

    result = await resolve_entities(
        entities=["B", "A", "B", "A"],
        embedder=embedder,
        resolve_pair=resolver,
        on_resolution=on_res,
    )

    assert len(result) == 2
    assert result.to_dict() == {"A": None, "B": "A"}
    assert [event.entity for event in events] == ["A", "B"]
    assert resolver.calls == [("B", ["A"])]


@pytest.mark.asyncio
async def test_resolved_entities_unknown_name_raises_key_error() -> None:
    result = await resolve_entities(
        entities={"A"},
        embedder=MockEmbedder([{"A"}]),
        resolve_pair=ScriptedResolver({}),
    )

    with pytest.raises(KeyError, match="unknown"):
        result.canonical_of("unknown")


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
async def test_max_distance_threshold_excludes_candidate() -> None:
    embedder = VectorEmbedder({"A": (1.0, 0.0), "B": (0.8, 0.6)})
    resolver = ScriptedResolver({})

    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        max_distance=0.1,
    )

    assert result.canonicals() == {"A", "B"}
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_top_n_zero_disables_candidate_search() -> None:
    embedder = MockEmbedder([{"A", "B"}])
    resolver = ScriptedResolver({})

    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        top_n=0,
    )

    assert result.canonicals() == {"A", "B"}
    assert resolver.calls == []


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


@pytest.mark.asyncio
async def test_candidate_search_continues_until_distinct_canonicals() -> None:
    embedder = VectorEmbedder(
        {
            "A": (1.0, 0.0),
            "A1": (1.0, 0.0),
            "A2": (1.0, 0.0),
            "X": (0.8, 0.6),
            "Z": (1.0, 0.0),
        }
    )
    resolver = ScriptedResolver(
        {
            ("A1", frozenset({"A"})): PairDecision(matched="A"),
            ("A2", frozenset({"A"})): PairDecision(matched="A"),
            ("X", frozenset({"A"})): PairDecision(),
            ("Z", frozenset({"A", "X"})): PairDecision(matched="X"),
        }
    )

    result = await resolve_entities(
        entities={"A", "A1", "A2", "X", "Z"},
        embedder=embedder,
        resolve_pair=resolver,
        top_n=2,
    )

    assert result.canonical_of("Z") == "X"
    assert ("Z", ["A", "X"]) in resolver.calls


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


@pytest.mark.asyncio
async def test_mode3_binding_new_can_repoint_non_existing_canonical() -> None:
    # PINNED only locks existing canonicals. If the matched canonical is not
    # existing, the resolver can still promote the new entity.
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
        is_existing_canonical=lambda n: False,
        existing_policy=ExistingCanonicalPolicy.PINNED,
        on_resolution=on_res,
    )

    assert result.canonical_of("A") == "B"
    assert result.canonical_of("B") == "B"
    assert result.canonicals() == {"B"}
    b_event = next(e for e in events if e.entity == "B")
    assert b_event.repointed == "A"


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
async def test_event_order_preferred_mode_is_sorted_by_entity() -> None:
    """PREFERRED policy: events emit in sorted(set(entities)) order.

    Locks in the callback order so a future component-graph runner can
    parallelize across components without changing user-visible event order.
    """
    embedder = MockEmbedder([{"A", "B"}, {"C", "D"}])
    resolver = ScriptedResolver(
        {
            ("B", frozenset({"A"})): PairDecision(matched="A"),
            ("D", frozenset({"C"})): PairDecision(matched="C"),
        }
    )
    events, on_res = capture_events()
    await resolve_entities(
        entities={"D", "C", "B", "A"},
        embedder=embedder,
        resolve_pair=resolver,
        existing_policy=ExistingCanonicalPolicy.PREFERRED,
        on_resolution=on_res,
    )
    assert [e.entity for e in events] == ["A", "B", "C", "D"]


@pytest.mark.asyncio
async def test_event_order_pinned_mode_pass1_then_pass2_each_sorted() -> None:
    """PINNED policy: all pass-1 (existings) events emit first in sorted
    order, then all pass-2 (non-existings) events in sorted order. Commit 3
    parallelism must preserve this exact two-phase order."""
    embedder = MockEmbedder([{"A", "B"}, {"C", "D"}])
    resolver = ScriptedResolver(
        {
            ("A", frozenset({"B"})): PairDecision(matched="B"),
            ("C", frozenset({"D"})): PairDecision(matched="D"),
        }
    )
    events, on_res = capture_events()
    await resolve_entities(
        entities={"A", "B", "C", "D"},
        embedder=embedder,
        resolve_pair=resolver,
        is_existing_canonical=lambda n: n in {"B", "D"},
        existing_policy=ExistingCanonicalPolicy.PINNED,
        on_resolution=on_res,
    )
    # pass_1 existings in sorted order: B, D; then pass_2 non-existings: A, C.
    assert [e.entity for e in events] == ["B", "D", "A", "C"]
    assert [e.seeded for e in events] == [True, True, False, False]


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
