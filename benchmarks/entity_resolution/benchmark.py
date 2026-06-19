from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
import json
import math
import os
from pathlib import Path
import random
import sys
import time
from typing import Protocol

from dotenv import load_dotenv
import faiss
import numpy as np
from numpy.typing import NDArray

from cocoindex.ops.entity_resolution import (
    CanonicalSide,
    PairDecision,
    ResolutionEvent,
    resolve_entities,
)


BENCH_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_DIR = BENCH_ROOT / ".work" / "cocoindex"
ENV_PATH = BENCH_ROOT / ".env"


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    embedder: str
    resolver: str
    groups: int
    aliases_per_group: int
    isolated: int
    cluster_sizes: tuple[int, ...] | None = None
    seed: int = 7
    max_distance: float = 0.3
    top_n: int = 5
    embedding_model: str = "text-embedding-3-small"
    synthetic_dim: int = 384
    synthetic_embed_delay_ms: float = 0.0
    rule_resolver_delay_ms: float = 0.0
    llm_model: str = "openai/gpt-5.4-nano"
    entity_type: str = "organization"


BENCHMARK_PROFILES: dict[str, BenchmarkProfile] = {
    "synthetic-fast": BenchmarkProfile(
        embedder="synthetic",
        resolver="ground-truth",
        groups=100,
        aliases_per_group=4,
        isolated=100,
    ),
    "synthetic-latency": BenchmarkProfile(
        embedder="synthetic",
        resolver="ground-truth",
        groups=100,
        aliases_per_group=4,
        isolated=100,
        rule_resolver_delay_ms=20.0,
    ),
    "synthetic-many-components": BenchmarkProfile(
        embedder="synthetic",
        resolver="ground-truth",
        groups=0,
        aliases_per_group=0,
        isolated=0,
        cluster_sizes=tuple([2] * 50 + [3] * 30 + [1] * 100),
        rule_resolver_delay_ms=20.0,
    ),
    "synthetic-one-giant": BenchmarkProfile(
        embedder="synthetic",
        resolver="ground-truth",
        groups=0,
        aliases_per_group=0,
        isolated=0,
        cluster_sizes=(100,),
        rule_resolver_delay_ms=20.0,
    ),
    "openai-small": BenchmarkProfile(
        embedder="litellm",
        resolver="llm",
        groups=5,
        aliases_per_group=3,
        isolated=5,
    ),
    "openai-many-components": BenchmarkProfile(
        embedder="litellm",
        resolver="llm",
        groups=0,
        aliases_per_group=0,
        isolated=0,
        cluster_sizes=tuple([3] * 10 + [1] * 5),
        max_distance=0.15,
    ),
}

_PROFILE_OPTIONS = {
    "embedder": "--embedder",
    "resolver": "--resolver",
    "groups": "--groups",
    "aliases_per_group": "--aliases-per-group",
    "isolated": "--isolated",
    "cluster_sizes": "--cluster-sizes",
    "seed": "--seed",
    "max_distance": "--max-distance",
    "top_n": "--top-n",
    "embedding_model": "--embedding-model",
    "synthetic_dim": "--synthetic-dim",
    "synthetic_embed_delay_ms": "--synthetic-embed-delay-ms",
    "rule_resolver_delay_ms": "--rule-resolver-delay-ms",
    "llm_model": "--llm-model",
    "entity_type": "--entity-type",
}


class Embedder(Protocol):
    async def embed(self, text: str) -> NDArray[np.float32]: ...


class PairResolver(Protocol):
    async def __call__(self, entity: str, candidates: list[str]) -> PairDecision: ...


@dataclass(frozen=True, slots=True)
class SyntheticDataset:
    entities: list[str]
    expected_canonical: dict[str, str]
    grouped_entities: dict[str, list[str]]


@dataclass(slots=True)
class TimedCallMetrics:
    calls: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0
    max_concurrency: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def record(self, latency_ms: float) -> None:
        self.calls += 1
        self.total_ms += latency_ms
        self.max_ms = max(self.max_ms, latency_ms)
        self.latencies_ms.append(latency_ms)

    def summary(self) -> dict[str, float | int]:
        return {
            "calls": self.calls,
            "total_ms": round(self.total_ms, 3),
            "avg_ms": round(self.total_ms / self.calls, 3) if self.calls else 0.0,
            "p50_ms": round(_percentile(self.latencies_ms, 50), 3),
            "p95_ms": round(_percentile(self.latencies_ms, 95), 3),
            "max_ms": round(self.max_ms, 3),
            "max_concurrency": self.max_concurrency,
        }


@dataclass(slots=True)
class ResolverMetrics:
    timing: TimedCallMetrics = field(default_factory=TimedCallMetrics)
    total_candidates: int = 0
    max_candidates: int = 0
    matched: int = 0
    no_match: int = 0
    candidate_counts: list[int] = field(default_factory=list)

    def record(self, candidates: list[str], decision: PairDecision) -> None:
        count = len(candidates)
        self.total_candidates += count
        self.max_candidates = max(self.max_candidates, count)
        self.candidate_counts.append(count)
        if decision.matched is None:
            self.no_match += 1
        else:
            self.matched += 1

    def summary(self) -> dict[str, float | int]:
        timing = self.timing.summary()
        return {
            **timing,
            "matched": self.matched,
            "no_match": self.no_match,
            "total_candidates": self.total_candidates,
            "avg_candidates": (
                round(self.total_candidates / self.timing.calls, 3)
                if self.timing.calls
                else 0.0
            ),
            "p50_candidates": round(_percentile(self.candidate_counts, 50), 3),
            "p95_candidates": round(_percentile(self.candidate_counts, 95), 3),
            "max_candidates": self.max_candidates,
        }


@dataclass(slots=True)
class EventMetrics:
    events: int = 0
    zero_candidate_events: int = 0
    resolver_events: int = 0
    seeded_events: int = 0
    repointed_events: int = 0

    def on_resolution(self, event: ResolutionEvent) -> None:
        self.events += 1
        if not event.candidates:
            self.zero_candidate_events += 1
        if event.decision is not None:
            self.resolver_events += 1
        if event.seeded:
            self.seeded_events += 1
        if event.repointed is not None:
            self.repointed_events += 1


class TimedEmbedder:
    def __init__(self, embedder: Embedder) -> None:
        self._embedder = embedder
        self.metrics = TimedCallMetrics()
        self._active = 0
        self.captured: dict[str, NDArray[np.float32]] = {}

    async def embed(self, text: str) -> NDArray[np.float32]:
        self._active += 1
        self.metrics.max_concurrency = max(self.metrics.max_concurrency, self._active)
        start = time.perf_counter()
        try:
            vec = await self._embedder.embed(text)
            self.captured[text] = vec
            return vec
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            self.metrics.record(latency_ms)
            self._active -= 1


class TimedResolver:
    def __init__(self, resolver: PairResolver) -> None:
        self._resolver = resolver
        self.metrics = ResolverMetrics()
        self._active = 0

    async def __call__(self, entity: str, candidates: list[str]) -> PairDecision:
        self._active += 1
        self.metrics.timing.max_concurrency = max(
            self.metrics.timing.max_concurrency, self._active
        )
        start = time.perf_counter()
        try:
            decision = await self._resolver(entity, candidates)
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            self.metrics.timing.record(latency_ms)
            self._active -= 1
        self.metrics.record(candidates, decision)
        return decision


class SyntheticEmbedder:
    def __init__(
        self,
        dataset: SyntheticDataset,
        *,
        dim: int,
        seed: int,
        delay_ms: float,
    ) -> None:
        self._vectors = _build_vectors(dataset, dim=dim, seed=seed)
        self._delay_s = delay_ms / 1000.0

    async def embed(self, text: str) -> NDArray[np.float32]:
        if self._delay_s:
            await asyncio.sleep(self._delay_s)
        else:
            await asyncio.sleep(0)
        return self._vectors[text]


class GroundTruthResolver:
    def __init__(self, dataset: SyntheticDataset, *, delay_ms: float) -> None:
        self._expected_canonical = dataset.expected_canonical
        self._delay_s = delay_ms / 1000.0

    async def __call__(self, entity: str, candidates: list[str]) -> PairDecision:
        if self._delay_s:
            await asyncio.sleep(self._delay_s)

        entity_canonical = self._expected_canonical[entity]
        for candidate in candidates:
            if self._expected_canonical[candidate] == entity_canonical:
                canonical_side = (
                    CanonicalSide.NEW
                    if entity == entity_canonical and candidate != entity
                    else CanonicalSide.MATCHED
                )
                return PairDecision(matched=candidate, canonical=canonical_side)
        return PairDecision()


def _percentile(values: list[float] | list[int], percentile: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(sorted_values[lower])
    weight = rank - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def _slug(index: int) -> str:
    prefixes = [
        "Acme",
        "Northwind",
        "Bluepeak",
        "Vertex",
        "Redwood",
        "Summit",
        "Brightline",
        "Silverline",
        "Evergreen",
        "Nimbus",
    ]
    nouns = [
        "Analytics",
        "Robotics",
        "Systems",
        "Networks",
        "Research",
        "Labs",
        "Capital",
        "Health",
        "Energy",
        "Logistics",
    ]
    return f"{prefixes[index % len(prefixes)]} {nouns[(index // len(prefixes)) % len(nouns)]} {index:04d}"


def _aliases(canonical: str, aliases_per_group: int) -> list[str]:
    variants = [
        canonical,
        f"{canonical}, Inc.",
        f"{canonical} Incorporated",
        f"{canonical} LLC",
        f"The {canonical} Group",
        f"{canonical} Co.",
        f"{canonical} Corporation",
        f"{canonical} Ltd.",
    ]
    if aliases_per_group <= len(variants):
        return variants[:aliases_per_group]
    extra = [
        f"{canonical} Alias {i:02d}" for i in range(aliases_per_group - len(variants))
    ]
    return variants + extra


def generate_dataset(
    *,
    groups: int,
    aliases_per_group: int,
    isolated: int,
    seed: int,
    cluster_sizes: tuple[int, ...] | None = None,
) -> SyntheticDataset:
    expected_canonical: dict[str, str] = {}
    grouped_entities: dict[str, list[str]] = {}

    if cluster_sizes is not None:
        for cluster_idx, size in enumerate(cluster_sizes):
            if size < 1:
                raise ValueError(
                    f"cluster_sizes entries must be >= 1; got {size} at index {cluster_idx}"
                )
            canonical = _slug(cluster_idx)
            aliases = _aliases(canonical, size)
            grouped_entities[canonical] = aliases
            for alias in aliases:
                expected_canonical[alias] = canonical
    else:
        for group_idx in range(groups):
            canonical = _slug(group_idx)
            aliases = _aliases(canonical, aliases_per_group)
            grouped_entities[canonical] = aliases
            for alias in aliases:
                expected_canonical[alias] = canonical

        for isolated_idx in range(isolated):
            canonical = f"Isolated Entity {isolated_idx:04d}"
            grouped_entities[canonical] = [canonical]
            expected_canonical[canonical] = canonical

    entities = list(expected_canonical)
    random.Random(seed).shuffle(entities)
    return SyntheticDataset(
        entities=entities,
        expected_canonical=expected_canonical,
        grouped_entities=grouped_entities,
    )


def _build_vectors(
    dataset: SyntheticDataset,
    *,
    dim: int,
    seed: int,
) -> dict[str, NDArray[np.float32]]:
    rng = np.random.default_rng(seed)
    vectors: dict[str, NDArray[np.float32]] = {}
    for canonical, entities in dataset.grouped_entities.items():
        base = rng.normal(size=dim).astype(np.float32)
        base /= np.linalg.norm(base)
        for alias_idx, entity in enumerate(entities):
            noise = rng.normal(scale=0.015, size=dim).astype(np.float32)
            noise += (alias_idx + 1) * 0.0001
            vec = base + noise
            vec /= np.linalg.norm(vec)
            vectors[entity] = vec.astype(np.float32)
    return vectors


def _count_wrong_groups(
    groups: dict[str, set[str]], expected_canonical: dict[str, str]
) -> int:
    wrong = 0
    for members in groups.values():
        expected = {expected_canonical[member] for member in members}
        if len(expected) != 1:
            wrong += 1
    return wrong


def _count_candidate_components(
    captured: dict[str, NDArray[np.float32]],
    *,
    max_distance: float,
) -> int:
    """Connected components of the candidate-edge graph (post-hoc, benchmark-only).

    Build a transient FAISS index over the embeddings actually used by the run,
    range-search at the similarity threshold, and union-find the edges. This
    captures the parallelism opportunity available to a component-graph resolver:
    each component is a set of entities that *could* share a candidate.
    """
    if not captured:
        return 0
    names = list(captured)
    dim = int(captured[names[0]].shape[-1])
    matrix = np.stack(
        [np.asarray(captured[n], dtype=np.float32).reshape(-1) for n in names]
    ).astype(np.float32)
    faiss.normalize_L2(matrix)
    index = faiss.IndexFlatIP(dim)
    index.add(matrix)
    threshold = np.float32(1.0 - max_distance)
    radius = float(np.nextafter(threshold, np.float32(-np.inf)))
    lims, _scores, idxs = index.range_search(matrix, radius)

    parent = list(range(len(names)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(len(names)):
        for slot in range(int(lims[i]), int(lims[i + 1])):
            j = int(idxs[slot])
            if j > i:
                union(i, j)

    return len({find(i) for i in range(len(names))})


def _load_litellm_embedder(model: str) -> Embedder:
    from cocoindex.ops.litellm import LiteLLMEmbedder

    return LiteLLMEmbedder(model)


def _load_sentence_transformer_embedder(model: str) -> Embedder:
    from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

    return SentenceTransformerEmbedder(model)


def _load_llm_resolver(
    *,
    model: str,
    entity_type: str | None,
    extra_guidance: str | None,
) -> PairResolver:
    from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver

    return LlmPairResolver(
        model=model,
        entity_type=entity_type,
        extra_guidance=extra_guidance,
    )


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return default if value is None else int(value)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    return default if value is None else float(value)


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return default if value is None else Path(value)


def _parse_cluster_sizes(value: str) -> tuple[int, ...]:
    parts = [part.strip() for part in value.split(",")]
    return tuple(int(part) for part in parts if part)


def _cli_has_option(option: str) -> bool:
    return any(arg == option or arg.startswith(f"{option}=") for arg in sys.argv[1:])


def _apply_profile(args: argparse.Namespace) -> None:
    if args.profile is None:
        return
    profile = BENCHMARK_PROFILES[args.profile]
    for field_name, option in _PROFILE_OPTIONS.items():
        if not _cli_has_option(option):
            setattr(args, field_name, getattr(profile, field_name))


def parse_args() -> argparse.Namespace:
    load_dotenv(ENV_PATH)
    parser = argparse.ArgumentParser(
        description="Run a synthetic benchmark for CocoIndex entity resolution."
    )
    parser.add_argument(
        "--profile",
        choices=sorted(BENCHMARK_PROFILES),
        default=os.environ.get("ER_BENCH_PROFILE"),
        help=(
            "Use a repeatable benchmark profile. Profile defaults override .env "
            "values, and explicit CLI flags override profile defaults."
        ),
    )
    parser.add_argument("--groups", type=int, default=_env_int("ER_BENCH_GROUPS", 100))
    parser.add_argument(
        "--aliases-per-group",
        type=int,
        default=_env_int("ER_BENCH_ALIASES_PER_GROUP", 4),
    )
    parser.add_argument(
        "--isolated", type=int, default=_env_int("ER_BENCH_ISOLATED", 100)
    )
    cluster_sizes_default_env = os.environ.get("ER_BENCH_CLUSTER_SIZES")
    parser.add_argument(
        "--cluster-sizes",
        type=_parse_cluster_sizes,
        default=(
            _parse_cluster_sizes(cluster_sizes_default_env)
            if cluster_sizes_default_env
            else None
        ),
        help=(
            "Comma-separated list of per-cluster sizes (e.g. '2,2,3,1,1'). "
            "When set, overrides --groups/--aliases-per-group/--isolated."
        ),
    )
    parser.add_argument("--seed", type=int, default=_env_int("ER_BENCH_SEED", 7))
    parser.add_argument(
        "--max-distance",
        type=float,
        default=_env_float("ER_BENCH_MAX_DISTANCE", 0.3),
    )
    parser.add_argument("--top-n", type=int, default=_env_int("ER_BENCH_TOP_N", 5))
    parser.add_argument(
        "--state",
        type=Path,
        default=_env_path("ER_BENCH_STATE", DEFAULT_STATE_DIR),
    )
    parser.add_argument("--output-json", type=Path)
    parser.add_argument(
        "--embedder",
        choices=["synthetic", "litellm", "sentence-transformers"],
        default=_env_str("ER_BENCH_EMBEDDER", "synthetic"),
    )
    parser.add_argument(
        "--embedding-model",
        default=_env_str("ER_BENCH_EMBEDDING_MODEL", "text-embedding-3-small"),
    )
    parser.add_argument(
        "--synthetic-dim", type=int, default=_env_int("ER_BENCH_SYNTHETIC_DIM", 384)
    )
    parser.add_argument(
        "--synthetic-embed-delay-ms",
        type=float,
        default=_env_float("ER_BENCH_SYNTHETIC_EMBED_DELAY_MS", 0.0),
    )
    parser.add_argument(
        "--resolver",
        choices=["ground-truth", "llm"],
        default=_env_str("ER_BENCH_RESOLVER", "ground-truth"),
    )
    parser.add_argument(
        "--rule-resolver-delay-ms",
        type=float,
        default=_env_float("ER_BENCH_RULE_RESOLVER_DELAY_MS", 0.0),
    )
    parser.add_argument(
        "--llm-model", default=_env_str("ER_BENCH_LLM_MODEL", "openai/gpt-4o-mini")
    )
    parser.add_argument(
        "--entity-type", default=_env_str("ER_BENCH_ENTITY_TYPE", "organization")
    )
    parser.add_argument(
        "--extra-guidance", default=os.environ.get("ER_BENCH_EXTRA_GUIDANCE")
    )
    args = parser.parse_args()
    _apply_profile(args)
    return args


def _build_embedder(args: argparse.Namespace, dataset: SyntheticDataset) -> Embedder:
    if args.embedder == "synthetic":
        return SyntheticEmbedder(
            dataset,
            dim=args.synthetic_dim,
            seed=args.seed,
            delay_ms=args.synthetic_embed_delay_ms,
        )
    if args.embedder == "litellm":
        return _load_litellm_embedder(args.embedding_model)
    return _load_sentence_transformer_embedder(args.embedding_model)


def _build_resolver(
    args: argparse.Namespace, dataset: SyntheticDataset
) -> PairResolver:
    if args.resolver == "ground-truth":
        return GroundTruthResolver(dataset, delay_ms=args.rule_resolver_delay_ms)
    return _load_llm_resolver(
        model=args.llm_model,
        entity_type=args.entity_type,
        extra_guidance=args.extra_guidance,
    )


async def run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    args.state.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("COCOINDEX_DB", str(args.state))

    dataset = generate_dataset(
        groups=args.groups,
        aliases_per_group=args.aliases_per_group,
        isolated=args.isolated,
        seed=args.seed,
        cluster_sizes=args.cluster_sizes,
    )
    timed_embedder = TimedEmbedder(_build_embedder(args, dataset))
    timed_resolver = TimedResolver(_build_resolver(args, dataset))
    events = EventMetrics()

    start = time.perf_counter()
    result = await resolve_entities(
        dataset.entities,
        embedder=timed_embedder,
        resolve_pair=timed_resolver,
        on_resolution=events.on_resolution,
        max_distance=args.max_distance,
        top_n=args.top_n,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    groups = result.groups()
    candidate_components = _count_candidate_components(
        timed_embedder.captured, max_distance=args.max_distance
    )
    metrics: dict[str, object] = {
        "elapsed_ms": round(elapsed_ms, 3),
        "config": {
            "profile": args.profile,
            "groups": args.groups,
            "aliases_per_group": args.aliases_per_group,
            "isolated": args.isolated,
            "cluster_sizes": (
                list(args.cluster_sizes) if args.cluster_sizes is not None else None
            ),
            "entities": len(dataset.entities),
            "embedder": args.embedder,
            "embedding_model": args.embedding_model,
            "resolver": args.resolver,
            "llm_model": args.llm_model if args.resolver == "llm" else None,
            "max_distance": args.max_distance,
            "top_n": args.top_n,
        },
        "embedding": timed_embedder.metrics.summary(),
        "resolver": timed_resolver.metrics.summary(),
        "events": asdict(events),
        "result": {
            "resolved_entities": len(result),
            "canonicals": len(result.canonicals()),
            "groups": len(groups),
            "wrong_mixed_groups": _count_wrong_groups(
                groups, dataset.expected_canonical
            ),
            "candidate_components": candidate_components,
        },
    }
    return metrics


def _print_summary(metrics: dict[str, object]) -> None:
    config = metrics["config"]
    embedding = metrics["embedding"]
    resolver = metrics["resolver"]
    events = metrics["events"]
    result = metrics["result"]
    if not isinstance(config, dict):
        raise TypeError("config metrics must be a dict")
    if not isinstance(embedding, dict):
        raise TypeError("embedding metrics must be a dict")
    if not isinstance(resolver, dict):
        raise TypeError("resolver metrics must be a dict")
    if not isinstance(events, dict):
        raise TypeError("event metrics must be a dict")
    if not isinstance(result, dict):
        raise TypeError("result metrics must be a dict")

    rows = [
        ("entities", config["entities"]),
        ("elapsed_ms", metrics["elapsed_ms"]),
        ("embed_calls", embedding["calls"]),
        ("embed_total_ms", embedding["total_ms"]),
        ("embed_max_concurrency", embedding["max_concurrency"]),
        ("resolver_calls", resolver["calls"]),
        ("resolver_total_ms", resolver["total_ms"]),
        ("resolver_avg_ms", resolver["avg_ms"]),
        ("resolver_max_concurrency", resolver["max_concurrency"]),
        ("resolver_avg_candidates", resolver["avg_candidates"]),
        ("events_without_candidates", events["zero_candidate_events"]),
        ("canonicals", result["canonicals"]),
        ("wrong_mixed_groups", result["wrong_mixed_groups"]),
        ("candidate_components", result["candidate_components"]),
    ]
    width = max(len(name) for name, _value in rows)
    for name, value in rows:
        print(f"{name:{width}}  {value}")


def main() -> None:
    args = parse_args()
    metrics = asyncio.run(run_benchmark(args))
    _print_summary(metrics)

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(f"\nWrote metrics to {args.output_json}")
    else:
        print("\nFull metrics:")
        print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
