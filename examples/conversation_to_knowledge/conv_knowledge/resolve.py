"""Entity resolution via embedding similarity (faiss) + LLM confirmation."""

from __future__ import annotations

import asyncio

import cocoindex as coco
import faiss
import instructor
import litellm
import numpy as np

litellm.drop_params = True
import pydantic
from numpy.typing import NDArray

from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

from .models import RESOLUTION_LLM_MODEL, resolve_canonical

EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

MAX_DISTANCE = 0.3  # cosine distance threshold (similarity > 0.7)
TOP_N = 5  # max candidates to consider

RESOLUTION_PROMPT = """\
You are resolving entity names. Given a new entity name and a numbered list of \
existing canonical entities, determine if the new entity refers to the same thing \
as any existing one.

Reply with ONLY the number of the matching entity, or "none" if it does not match any.

If there is a match, also indicate which name should be canonical (the clearest, \
most complete, Wikipedia-style name).

Format your response as:
- "none" if no match
- "<number>" if the new entity is a duplicate (existing one is canonical)
- "<number> replace" if the new entity is a better canonical name
"""


class EntityResolution(pydantic.BaseModel):
    """LLM output for entity resolution."""

    match_index: int | None = None  # 1-based index of matching candidate, or None
    replace: bool = False  # True if new entity should be canonical instead


@coco.fn(memo=True)
async def compute_entity_embedding(name: str) -> NDArray[np.float32]:
    """Compute embedding for an entity name."""
    embedder = coco.use_context(EMBEDDER)
    return await embedder.embed(name)


@coco.fn(memo=True)
async def resolve_entity_pair(entity: str, candidates: list[str]) -> EntityResolution:
    """LLM decides if entity matches any candidate."""
    candidate_list = "\n".join(f"{i + 1}. {c}" for i, c in enumerate(candidates))
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(RESOLUTION_LLM_MODEL),
        response_model=EntityResolution,
        messages=[
            {"role": "system", "content": RESOLUTION_PROMPT},
            {
                "role": "user",
                "content": (
                    f'New entity: "{entity}"\n\nExisting entities:\n{candidate_list}'
                ),
            },
        ],
    )
    return EntityResolution.model_validate(result.model_dump())


@coco.fn(memo=True)
async def resolve_entities(
    all_raw_entities: set[str],
) -> dict[str, str | None]:
    """
    Build a deduplication dict using embedding similarity + LLM confirmation.

    Returns dict mapping entity name -> canonical name (None if self is canonical).
    """
    dim = await coco.use_context(EMBEDDER).dimension()
    index = faiss.IndexFlatIP(dim)
    index_names: list[str] = []
    dedup: dict[str, str | None] = {}

    # Pre-compute all embeddings concurrently
    entity_list = sorted(all_raw_entities)
    embeddings_list = await asyncio.gather(
        *(compute_entity_embedding(name) for name in entity_list)
    )
    embeddings = dict(zip(entity_list, embeddings_list))

    for entity in entity_list:
        vec = embeddings[entity].reshape(1, -1).copy()
        faiss.normalize_L2(vec)

        candidates: list[str] = []
        if index.ntotal > 0:
            sims, idxs = index.search(vec, k=min(TOP_N, index.ntotal))
            for sim, idx in zip(sims[0], idxs[0]):
                if sim >= 1.0 - MAX_DISTANCE and idx >= 0:
                    cand = index_names[idx]
                    canonical = resolve_canonical(cand, dedup)
                    if canonical != entity:
                        candidates.append(canonical)

        # Deduplicate candidate list while preserving order
        seen: set[str] = set()
        unique_candidates: list[str] = []
        for c in candidates:
            if c not in seen:
                seen.add(c)
                unique_candidates.append(c)

        if unique_candidates:
            resolution = await resolve_entity_pair(entity, unique_candidates)
            if (
                resolution.match_index is not None
                and 1 <= resolution.match_index <= len(unique_candidates)
            ):
                matched = unique_candidates[resolution.match_index - 1]
                if resolution.replace:
                    # New entity becomes canonical; old one points to new
                    dedup[entity] = None
                    dedup[matched] = entity
                else:
                    # Existing one stays canonical
                    dedup[entity] = matched
            else:
                dedup[entity] = None  # new canonical
        else:
            dedup[entity] = None  # new canonical

        index.add(vec)
        index_names.append(entity)

    return dedup
