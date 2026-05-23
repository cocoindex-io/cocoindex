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

Simulate resolver latency to make the current sequential pair processing
visible:

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile synthetic-latency \
  --output-json benchmarks/entity_resolution/.work/synthetic-latency.json
```

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

You can also override any `.env` setting with CLI flags:

```sh
uv run --project benchmarks/entity_resolution python benchmarks/entity_resolution/benchmark.py \
  --profile openai-small \
  --embedding-model text-embedding-3-small \
  --llm-model openai/gpt-4o-mini \
  --output-json benchmarks/entity_resolution/.work/openai-small.json
```

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
