"""Tests for cocoindex.ops.entity_resolution."""

from __future__ import annotations

import asyncio
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
async def test_partition_includes_edges_at_exact_threshold() -> None:
    # Two L2-normalized vectors with cosine similarity exactly 1 - max_distance.
    # FAISS IndexFlatIP.range_search uses strict `> radius`, while the runtime
    # _CandidateIndex.search filter is inclusive `>= threshold`. Without the
    # nextafter step-down, the partition would drop this boundary edge and
    # place A and B in disjoint components, while the sequential implementation
    # would have surfaced B as a candidate of A.
    embedder = VectorEmbedder({"A": (1.0, 0.0), "B": (0.5, float(np.sqrt(0.75)))})
    resolver = ScriptedResolver({("B", frozenset({"A"})): PairDecision(matched="A")})

    result = await resolve_entities(
        entities={"A", "B"},
        embedder=embedder,
        resolve_pair=resolver,
        max_distance=0.5,
    )

    assert result.to_dict() == {"A": None, "B": "A"}
    assert resolver.calls == [("B", ["A"])]


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
async def test_candidate_search_respects_top_n_upper_bound() -> None:
    # Six near-identical entities and top_n=2: the backfill loop must not
    # return more than two candidates to the resolver, matching the public
    # docs that frame top_n as the maximum candidates surfaced per entity.
    embedder = VectorEmbedder(
        {
            "A": (1.0, 0.0),
            "B": (1.0, 0.0),
            "C": (1.0, 0.0),
            "D": (1.0, 0.0),
            "E": (1.0, 0.0),
            "Z": (1.0, 0.0),
        }
    )
    calls: list[tuple[str, list[str]]] = []

    async def resolver(entity: str, candidates: list[str]) -> PairDecision:
        calls.append((entity, candidates))
        return PairDecision()

    await resolve_entities(
        entities={"A", "B", "C", "D", "E", "Z"},
        embedder=embedder,
        resolve_pair=resolver,
        top_n=2,
    )

    for _, candidates in calls:
        assert len(candidates) <= 2, f"top_n=2 violated: resolver got {candidates!r}"


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
async def test_resolved_entities_iteration_order_is_sorted() -> None:
    """The dedup map is mutated by concurrent component runners in
    scheduler-interleaved order. Iteration order, however, must stay
    sorted-by-entity so callers using ``for name in result`` or
    ``result.to_dict()`` see deterministic results across runs."""
    pairs = [("A1", "A2"), ("B1", "B2"), ("C1", "C2"), ("D1", "D2"), ("E1", "E2")]
    embedder = MockEmbedder([{a, b} for a, b in pairs])

    async def resolver(entity: str, candidates: list[str]) -> PairDecision:
        # Asymmetric delays force the components to finish out of order;
        # without re-ordering, the dedup dict would reflect interleaved
        # writes from the asyncio scheduler.
        await asyncio.sleep(0.01 if entity > "C2" else 0.0)
        return PairDecision(matched=candidates[0])

    result = await resolve_entities(
        entities={n for pair in pairs for n in pair},
        embedder=embedder,
        resolve_pair=resolver,
    )

    keys = list(result)
    assert keys == sorted(keys)


@pytest.mark.asyncio
async def test_resolver_partitions_oversized_component_into_multiple_canonicals() -> (
    None
):
    """A single FAISS component contains two ground-truth clusters; the
    resolver rejects cross-cluster candidates and the algorithm produces
    the right partition.

    Mirrors what real embedders do when canonicals share vocabulary
    (e.g. "Acme Analytics" and "Northwind Analytics" both ride the
    "Analytics" axis). FAISS groups them; the LLM tells them apart. This
    test exercises the no-match path of the greedy loop inside a single
    oversized component — a regime the orthogonal MockEmbedder defaults
    don't naturally produce.
    """
    embedder = MockEmbedder([{"a", "b", "c", "d"}])
    decisions = {
        ("b", frozenset({"a"})): PairDecision(matched="a"),
        ("c", frozenset({"a"})): PairDecision(),
        ("d", frozenset({"a", "c"})): PairDecision(matched="c"),
    }
    result = await resolve_entities(
        entities={"a", "b", "c", "d"},
        embedder=embedder,
        resolve_pair=ScriptedResolver(decisions),
    )
    assert result.to_dict() == {"a": None, "b": "a", "c": None, "d": "c"}
    assert result.canonicals() == {"a", "c"}


@pytest.mark.asyncio
async def test_pinned_existings_in_one_component_attach_correctly() -> None:
    """Two PINNED existings sit in the same FAISS component as their
    non-existing aliases; the resolver chooses which existing each alias
    attaches to. Verifies PINNED's two-phase order (existings seeded
    first) combined with resolver-driven partitioning inside one
    component."""
    embedder = MockEmbedder([{"M1", "M2", "X1", "X2"}])
    decisions = {
        ("X1", frozenset({"M1", "M2"})): PairDecision(matched="M1"),
        ("X2", frozenset({"M1", "M2"})): PairDecision(matched="M2"),
    }
    result = await resolve_entities(
        entities={"M1", "M2", "X1", "X2"},
        embedder=embedder,
        resolve_pair=ScriptedResolver(decisions),
        is_existing_canonical=lambda n: n in {"M1", "M2"},
        existing_policy=ExistingCanonicalPolicy.PINNED,
    )
    assert result.to_dict() == {"M1": None, "M2": None, "X1": "M1", "X2": "M2"}


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


@dataclass(frozen=True)
class _ParityScenario:
    """One input to the parity test: drives both the parallel-dispatch and
    forced-single-component paths and asserts identical dedup maps."""

    name: str
    entities: set[str]
    embedder: MockEmbedder
    resolve_pair: ScriptedResolver
    is_existing_canonical: Callable[[str], bool] | None = None
    existing_policy: ExistingCanonicalPolicy = ExistingCanonicalPolicy.PINNED
    top_n: int = 5


def _build_parity_scenarios() -> list[_ParityScenario]:
    scenarios: list[_ParityScenario] = []

    pairs = [("A1", "A2"), ("B1", "B2"), ("C1", "C2"), ("D1", "D2")]
    scenarios.append(
        _ParityScenario(
            name="many_small_components",
            entities={n for p in pairs for n in p},
            embedder=MockEmbedder([{a, b} for a, b in pairs]),
            resolve_pair=ScriptedResolver(
                {(b, frozenset({a})): PairDecision(matched=a) for a, b in pairs}
            ),
        )
    )

    scenarios.append(
        _ParityScenario(
            name="preferred_mixed_existing",
            entities={"A", "B", "C", "D", "E"},
            embedder=MockEmbedder([{"A", "B", "C"}, {"D", "E"}]),
            resolve_pair=ScriptedResolver(
                {
                    ("B", frozenset({"A"})): PairDecision(matched="A"),
                    ("C", frozenset({"A"})): PairDecision(matched="A"),
                    ("E", frozenset({"D"})): PairDecision(
                        matched="D", canonical=CanonicalSide.NEW
                    ),
                }
            ),
            is_existing_canonical=lambda n: n == "A",
            existing_policy=ExistingCanonicalPolicy.PREFERRED,
        )
    )

    scenarios.append(
        _ParityScenario(
            name="pinned_two_existings_two_new",
            entities={"A", "B", "X", "Y"},
            embedder=MockEmbedder([{"A", "X"}, {"B", "Y"}]),
            resolve_pair=ScriptedResolver(
                {
                    ("X", frozenset({"A"})): PairDecision(matched="A"),
                    ("Y", frozenset({"B"})): PairDecision(matched="B"),
                }
            ),
            is_existing_canonical=lambda n: n in {"A", "B"},
            existing_policy=ExistingCanonicalPolicy.PINNED,
        )
    )

    isolates_embedder = MockEmbedder([{"A", "B"}, {"C", "D"}])
    isolates_embedder.add_isolated("solo1", axis=10)
    isolates_embedder.add_isolated("solo2", axis=11)
    scenarios.append(
        _ParityScenario(
            name="isolates_and_clusters_mixed",
            entities={"A", "B", "C", "D", "solo1", "solo2"},
            embedder=isolates_embedder,
            resolve_pair=ScriptedResolver(
                {
                    ("B", frozenset({"A"})): PairDecision(matched="A"),
                    ("D", frozenset({"C"})): PairDecision(matched="C"),
                }
            ),
        )
    )

    return scenarios


_PARITY_SCENARIOS = _build_parity_scenarios()


async def _run_resolve(scenario: _ParityScenario) -> dict[str, str | None]:
    result = await resolve_entities(
        entities=scenario.entities,
        embedder=scenario.embedder,
        resolve_pair=scenario.resolve_pair,
        is_existing_canonical=scenario.is_existing_canonical,
        existing_policy=scenario.existing_policy,
        top_n=scenario.top_n,
    )
    return result.to_dict()


@pytest.mark.parametrize("scenario", _PARITY_SCENARIOS, ids=lambda s: s.name)
@pytest.mark.asyncio
async def test_parallel_dispatch_matches_forced_single_component(
    scenario: _ParityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Feed the same input through both the real component-graph dispatch
    and a forced one-big-component dispatch; assert byte-identical dedup
    maps. The one-component dispatch is the pure greedy path with no
    partitioning, so equality here proves the partitioning + parallel
    runner does not perturb canonical selection.
    """
    parallel_dedup = await _run_resolve(scenario)

    import cocoindex.ops.entity_resolution as er

    def _one_component(
        entity_list: list[str],
        normalized_vecs: list[NDArray[np.float32]],
        *,
        max_distance: float,
    ) -> list[list[int]]:
        return [list(range(len(entity_list)))] if entity_list else []

    monkeypatch.setattr(er, "_partition_components", _one_component)
    single_component_dedup = await _run_resolve(scenario)

    assert parallel_dedup == single_component_dedup, (
        f"scenario {scenario.name!r}: parallel and single-component dedup maps differ"
    )


@pytest.mark.asyncio
async def test_independent_components_resolve_concurrently() -> None:
    """Disjoint similarity components must overlap their resolver calls.

    Uses 5 vertex-disjoint 2-entity clusters. Each second-entity match is one
    resolver call. A resolver that yields the event loop between increment
    and decrement of an "active" counter records the peak observed
    concurrency; a sequential implementation would observe peak=1.
    """
    pairs = [("A1", "A2"), ("B1", "B2"), ("C1", "C2"), ("D1", "D2"), ("E1", "E2")]
    embedder = MockEmbedder([{a, b} for a, b in pairs])
    decisions = {(b, frozenset({a})): PairDecision(matched=a) for a, b in pairs}

    active = 0
    peak = 0

    async def resolver(entity: str, candidates: list[str]) -> PairDecision:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        # Yield once so other component tasks reach this point before we
        # return. asyncio.sleep(0) re-schedules the current task at the back
        # of the ready queue without any wall-clock dependency.
        await asyncio.sleep(0)
        active -= 1
        return decisions[(entity, frozenset(candidates))]

    result = await resolve_entities(
        entities={n for pair in pairs for n in pair},
        embedder=embedder,
        resolve_pair=resolver,
    )
    assert peak >= 2, f"expected concurrent resolver calls, observed peak={peak}"
    # And the partitioning didn't break correctness — every pair merged.
    canonicals = result.canonicals()
    assert len(canonicals) == 5
    for a, b in pairs:
        assert result.canonical_of(a) == result.canonical_of(b)


@pytest.mark.asyncio
async def test_resolver_error_cancels_sibling_components() -> None:
    """If one component's resolve_pair raises, sibling component coroutines
    must be cancelled — not allowed to keep firing resolver calls in the
    background after resolve_entities has already raised.
    """
    pairs = [("A1", "A2"), ("B1", "B2"), ("C1", "C2"), ("D1", "D2"), ("E1", "E2")]
    embedder = MockEmbedder([{a, b} for a, b in pairs])

    started = 0
    completed_normally = 0

    async def resolver(entity: str, candidates: list[str]) -> PairDecision:
        nonlocal started, completed_normally
        started += 1
        if entity == "A2":
            await asyncio.sleep(0)
            raise RuntimeError("boom")
        await asyncio.sleep(1.0)
        completed_normally += 1
        return PairDecision(matched=candidates[0])

    with pytest.raises(RuntimeError, match="boom"):
        await resolve_entities(
            entities={n for pair in pairs for n in pair},
            embedder=embedder,
            resolve_pair=resolver,
        )

    # Give any leaked siblings a chance to run to completion.
    await asyncio.sleep(0.2)
    assert started >= 2, "test setup: at least the failing call should have started"
    assert completed_normally == 0, (
        f"sibling resolver calls were not cancelled: started={started}, "
        f"completed_normally={completed_normally}"
    )


@pytest.mark.asyncio
async def test_on_resolution_receives_partial_events_on_resolver_error() -> None:
    """When one component's resolve_pair raises, the events emitted by
    components that finished before the failure must still be delivered to
    on_resolution. Otherwise observability into 'which entities completed
    successfully' is lost — a regression vs the pre-parallelization
    real-time emission semantics.
    """
    pairs = [("A1", "A2"), ("B1", "B2"), ("C1", "C2")]
    embedder = MockEmbedder([{a, b} for a, b in pairs])

    # Stagger resolver latency so two components finish before the failing
    # one raises.
    delays = {"A2": 0.0, "B2": 0.0, "C2": 0.05}
    matches = {"A2": "A1", "B2": "B1"}

    async def resolver(entity: str, candidates: list[str]) -> PairDecision:
        await asyncio.sleep(delays[entity])
        if entity == "C2":
            raise RuntimeError("boom")
        return PairDecision(matched=matches[entity])

    events, on_res = capture_events()
    with pytest.raises(RuntimeError, match="boom"):
        await resolve_entities(
            entities={n for pair in pairs for n in pair},
            embedder=embedder,
            resolve_pair=resolver,
            on_resolution=on_res,
        )

    delivered = {e.entity for e in events}
    assert {"A1", "A2", "B1", "B2"}.issubset(delivered), (
        f"completed events were not delivered: {delivered}"
    )


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
