---
title: Entity Resolution
description: Deduplicate entity names using embedding similarity (FAISS) and a pluggable pair-resolver, with optional existing-canonical policies.
---

# Entity Resolution

The `cocoindex.ops.entity_resolution` package resolves a set of raw entity names into a deduplicated canonical map. It finds near-duplicate names via embedding similarity (FAISS), then asks a pair-resolver (typically an LLM) to confirm matches and pick the better canonical name.

```python
from cocoindex.ops.entity_resolution import resolve_entities
```

:::note Dependencies
This module requires additional dependencies. Install with:

```bash
# With built-in LLM resolver (recommended)
pip install cocoindex[entity_resolution_llm]

# Core only (for custom resolver implementations)
pip install cocoindex[entity_resolution]
```

:::

## Basic usage

```python
import cocoindex as coco
from cocoindex.ops.entity_resolution import resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)

@coco.fn(memo=True)
async def resolve_my_entities(raw_entities: set[str]) -> dict[str, str | None]:
    result = await resolve_entities(
        entities=raw_entities,
        embedder=coco.use_context(EMBEDDER),
        resolve_pair=LlmPairResolver(model=coco.use_context(LLM_MODEL)),
    )
    return result.to_dict()
```

## `ResolvedEntities`

`resolve_entities` returns a `ResolvedEntities` object — a read-only wrapper around the dedup map with safe chain-walking:

```python
result = await resolve_entities(...)

result.canonical_of("Microsoft Corp.")  # -> "Microsoft"
result.canonical_of("Microsoft")        # -> "Microsoft" (self-canonical)
result.canonicals()                     # -> {"Microsoft", "OpenAI", ...}
result.groups()                         # -> {"Microsoft": {"Microsoft", "Microsoft Corp."}, ...}
result.to_dict()                        # -> {"Microsoft": None, "Microsoft Corp.": "Microsoft", ...}
```

## Existing-canonical handling

If some entity names are already established as canonical (e.g., they have on-disk files or database records), you can pass `is_existing_canonical` to influence how matches involving those names are resolved.

### Without `is_existing_canonical` (default)

The resolver decides which side becomes canonical on every match. No special treatment for any name.

```python
result = await resolve_entities(
    entities=raw_entities,
    embedder=embedder,
    resolve_pair=resolver,
)
```

### With `is_existing_canonical`

Pass a sync predicate that returns `True` for names you consider already-established canonicals. The `existing_policy` parameter controls how strongly that status is enforced:

#### `PINNED` (default)

Existing canonicals are immutable. They are seeded directly as canonicals without consulting the resolver. Two existings never merge. Non-existing entities are resolved in a second pass; if they match an existing canonical, the existing always wins.

```python
result = await resolve_entities(
    entities=raw_entities,
    embedder=embedder,
    resolve_pair=resolver,
    is_existing_canonical=lambda name: name in existing_files,
    # existing_policy defaults to PINNED
)
```

#### `PREFERRED`

A softer policy: existing-canonical status breaks ties, but the resolver is always consulted. When exactly one side of a match is existing-canonical, that side wins regardless of the resolver's verdict. When both or neither are existing, the resolver decides.

```python
from cocoindex.ops.entity_resolution import ExistingCanonicalPolicy

result = await resolve_entities(
    entities=raw_entities,
    embedder=embedder,
    resolve_pair=resolver,
    is_existing_canonical=lambda name: name in existing_files,
    existing_policy=ExistingCanonicalPolicy.PREFERRED,
)
```

## Events

Pass `on_resolution` to receive a `ResolutionEvent` for each entity as resolution proceeds — useful for streaming progress logs:

```python
from cocoindex.ops.entity_resolution import ResolutionEvent

def log_resolution(event: ResolutionEvent) -> None:
    if event.decision and event.decision.matched:
        print(f"  {event.entity!r} -> {event.canonical!r}")

result = await resolve_entities(
    entities=raw_entities,
    embedder=embedder,
    resolve_pair=resolver,
    on_resolution=log_resolution,
)
```

Each event includes:
- `entity` / `canonical` — the entity and its resolved canonical
- `candidates` — what was passed to the resolver (empty if no resolver call)
- `decision` — the resolver's raw verdict (compare with `canonical` to detect policy overrides)
- `repointed` — if a prior canonical was demoted, its name
- `seeded` — `True` for pinned existing-canonical entities seeded without resolver

## `resolve_entities` parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `entities` | `Iterable[str]` | *(required)* | Raw entity names. Duplicates collapsed; iterated in sorted order. |
| `embedder` | [`Embedder`](../common_resources/data_types.md#embedder) | *(required)* | Async single-text embedder. `LiteLLMEmbedder` and `SentenceTransformerEmbedder` both work. |
| `resolve_pair` | `PairResolver` | *(required)* | Pair-resolution callback. See [`LlmPairResolver`](#llmpairresolver) for the built-in option. |
| `is_existing_canonical` | `Callable[[str], bool] \| None` | `None` | Sync predicate for existing-canonical detection. |
| `existing_policy` | `ExistingCanonicalPolicy` | `PINNED` | How to treat existing canonicals. Ignored when `is_existing_canonical` is `None`. |
| `on_resolution` | `Callable[[ResolutionEvent], None] \| None` | `None` | Sync callback fired per entity in real time. |
| `max_distance` | `float` | `0.3` | Cosine distance threshold (similarity >= 0.7). |
| `top_n` | `int` | `5` | Max candidates surfaced per entity. |

---

## `LlmPairResolver`

`cocoindex.ops.entity_resolution.llm_resolver` provides `LlmPairResolver`, a built-in resolver that uses an LLM (via [litellm](https://docs.litellm.ai/)) to decide pair matches. It sends each `(entity, candidates)` pair to the model and returns a structured decision — with automatic validation and retry when the model hallucinates a name not in the candidate list.

### Usage

```python
from cocoindex.ops.entity_resolution import resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver

resolver = LlmPairResolver(model="openai/gpt-4o-mini")

result = await resolve_entities(
    entities={"Barack Obama", "Barack H. Obama", "OpenAI"},
    embedder=embedder,
    resolve_pair=resolver,
)
```

### Model strings

Uses the same litellm model-string format as [`LiteLLMEmbedder`](./litellm.md):

| Provider | Example |
|----------|---------|
| OpenAI | `openai/gpt-4o-mini` |
| Anthropic | `anthropic/claude-haiku-4-5` |
| Google (Gemini) | `gemini/gemini-2.0-flash` |
| Groq | `groq/llama-3.3-70b-versatile` |

See the [LiteLLM docs](https://docs.litellm.ai/docs/providers) for the full list of 100+ supported providers.

### Entity type hints

Pass `entity_type` to tailor the prompt for specific entity categories:

```python
person_resolver = LlmPairResolver(model="openai/gpt-4o-mini", entity_type="person")
tech_resolver = LlmPairResolver(model="openai/gpt-4o-mini", entity_type="technology")
```

This adds context to the system prompt, helping the model make better judgments (e.g., being more conservative with personal names).

### Extra guidance

Append domain-specific rules to the default prompt via `extra_guidance`:

```python
resolver = LlmPairResolver(
    model="openai/gpt-4o-mini",
    entity_type="organization",
    extra_guidance=(
        "A parent organization and its subsidiary/division are DISTINCT things. "
        "'Amazon' is not the same as 'AWS'. 'Google' is not the same as 'YouTube'."
    ),
)
```

`extra_guidance` is for **domain rules only** — do not include output-format instructions.

### Validation and retry

After each LLM call, the response is validated:

1. **Structural**: the JSON response must parse into the expected schema
2. **Semantic**: `matched` (if non-null) must be in the supplied `candidates`. If not, the LLM is re-prompted with explicit feedback and the conversation continues

The default retry budget is 2. If exhausted, no match is returned.

```python
resolver = LlmPairResolver(model="openai/gpt-4o-mini", retries=3)  # more retries
resolver = LlmPairResolver(model="openai/gpt-4o-mini", retries=1)  # fewer retries
```

### Memoization

Each unique `(entity, candidates)` pair's decision is persisted across runs via `@coco.fn(memo=True)`. Changing the model or prompt invalidates the cache. No additional memoization wrapper is needed.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model` | `str` | *(required)* | Litellm model string (e.g. `"openai/gpt-4o-mini"`). |
| `entity_type` | `str \| None` | `None` | Entity type hint woven into the prompt. |
| `extra_guidance` | `str \| None` | `None` | Domain rules appended to the default prompt. |
| `retries` | `int` | `2` | Max retries on invalid `matched` output. |

---

## Custom resolvers

For cases where the built-in `LlmPairResolver` doesn't fit (rule-based matching, different LLM framework, etc.), implement the `PairResolver` protocol:

```python
from cocoindex.ops.entity_resolution import PairResolver, PairDecision, CanonicalSide

class MyResolver:
    async def __call__(self, entity: str, candidates: list[str]) -> PairDecision:
        # `candidates` is a non-empty, de-duplicated list of canonical names
        # with cosine similarity to `entity` above the threshold.

        # No match:
        return PairDecision()

        # Match — existing candidate stays canonical (default):
        return PairDecision(matched="Microsoft")

        # Match — new entity is the better canonical name:
        return PairDecision(matched="Microsoft Corp.", canonical=CanonicalSide.NEW)
```

Any async callable with the signature `(entity: str, candidates: list[str]) -> PairDecision` works — no subclassing required. `PairDecision.canonical` is advisory: the `existing_policy` may override it (see [Existing-canonical handling](#existing-canonical-handling)).
