# Entity resolution benchmark

Synthetic benchmark for `cocoindex.ops.entity_resolution.resolve_entities`.

The goal is to measure the current operation before changing the algorithm. It
generates deterministic clusters of entity-name aliases, runs the real
`resolve_entities` implementation, and records where time goes:

- total entities, groups, and aliases
- embedding call count, latency, and max logical concurrency
- resolver call count, latency, candidate counts, and max concurrency
- resolution events, zero-candidate events, matches, and repoints
- end-to-end elapsed time

## Fast local baseline

This uses a deterministic in-memory embedder and resolver. It is useful for
measuring FAISS/search/loop overhead and for simulating expensive resolver
latency without spending LLM tokens.

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-fast \
  --output-json benchmarks/entity_resolution/.work/synthetic-fast.json
```

Simulate resolver latency to make resolver-call latency the dominant cost:

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-latency \
  --output-json benchmarks/entity_resolution/.work/synthetic-latency.json
```

## Component parallelism

`resolve_entities` partitions entities into connected components of the
candidate-similarity graph and resolves each component concurrently. Two
profiles measure the two extremes:

```sh
# Many independent clusters → high resolver-call concurrency.
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-many-components \
  --output-json benchmarks/entity_resolution/.work/synthetic-many-components.json

# A single giant cluster → no parallelism opportunity (regression check).
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-one-giant \
  --output-json benchmarks/entity_resolution/.work/synthetic-one-giant.json
```

Before (sequential) vs. after (component-parallel), 20 ms simulated resolver
latency:

| Profile | Components | Before | After | Resolver concurrency |
|---|---|---|---|---|
| `synthetic-latency` | 200 | 6397 ms | 69 ms | 1 → 100 |
| `synthetic-many-components` | 180 | 2448 ms | 47 ms | 1 → 80 |
| `synthetic-one-giant` | 1 | 2110 ms | 2111 ms | 1 → 1 |

`wrong_mixed_groups` stays at 0 and the canonical map matches the prior
sequential implementation in every profile.

The benchmark measures logical `embed(...)` calls. CocoIndex embedders such as
`LiteLLMEmbedder` and `SentenceTransformerEmbedder` may batch those concurrent
logical calls into fewer provider/model calls internally.

## OpenAI-backed run

The benchmark automatically loads `benchmarks/entity_resolution/.env`.
`.env.example` contains a small OpenAI-backed configuration using
LiteLLM-backed embeddings and the built-in `LlmPairResolver`.

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py
```

Two OpenAI-backed profiles ship out of the box:

```sh
# Small sanity check (5 templated groups + 5 isolates, default threshold).
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile openai-small \
  --output-json benchmarks/entity_resolution/.work/openai-small.json

# Many-component workload designed to exercise resolver-call parallelism.
# Uses cluster_sizes=10×3+5×1 and max_distance=0.15 to keep ground-truth
# clusters in separate similarity components even with shared vocabulary.
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile openai-many-components \
  --output-json benchmarks/entity_resolution/.work/openai-many-components.json
```

Measured before (sequential) vs. after (component-parallel) on
`openai/gpt-5.4-nano` + `text-embedding-3-small`:

| Profile | max_distance | Components | Before | After | Speedup |
|---|---|---|---|---|---|
| `openai-small` | 0.3 (default) | 2 | 10513 ms | 8691 ms | 1.21× |
| `openai-many-components` | 0.15 | 15 | 15063 ms | 3075 ms | **4.90×** |

Two takeaways. First, the synthetic numbers above are an upper bound that
assumes embeddings produce perfectly disjoint components; real embedders
may collapse multiple ground-truth clusters into one similarity component
when canonicals share vocabulary, in which case the resolver still
correctly partitions them but parallelism is bounded by the component
count. Second, `max_distance` controls how aggressively the component
graph admits edges — looser thresholds add safety candidates for the
resolver but reduce parallelism; tighter thresholds increase parallelism
but assume your embeddings cleanly separate distinct entities.

Keep OpenAI-backed runs small at first. The benchmark prints the number of LLM
resolver calls, which is the main cost driver.

## JSON output

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-fast \
  --output-json benchmarks/entity_resolution/.work/metrics.json
```

The benchmark sets `COCOINDEX_DB` to `.work/cocoindex` by default so memoized
CocoIndex functions, such as the LiteLLM embedder and LLM resolver, have a local
state path.
