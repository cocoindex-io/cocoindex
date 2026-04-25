"""Built-in LLM-based :class:`PairResolver` for entity resolution.

Uses ``instructor`` + ``litellm`` for structured LLM output with a simple
validation-and-retry loop.
"""

from __future__ import annotations

import pydantic as _pydantic

import cocoindex as _coco
import instructor as _instructor
import litellm as _litellm
from cocoindex.ops.entity_resolution import (
    CanonicalSide as _CanonicalSide,
    PairDecision as _PairDecision,
)

__all__ = ["LlmPairResolver"]


class _LlmResponse(_pydantic.BaseModel):
    """Pydantic model for instructor's ``response_model``.
    Converted to :class:`PairDecision` after validation."""

    matched: str | None = None
    canonical: _CanonicalSide = _CanonicalSide.MATCHED


_DEFAULT_PROMPT_TEMPLATE = """\
You are resolving entity names{scope}. Given a new entity name and a list of \
existing canonical entity names, decide whether the new entity refers to the \
same thing as any existing one.

Rules for the JSON response:
- If no existing candidate refers to the same thing, set `matched` to null.
- If the new entity matches an existing candidate, set `matched` to the exact \
candidate name (case-sensitive; it MUST be one of the candidates listed below).
- When `matched` is set, also set `canonical`:
  - `"new"`  — the new entity's name is a better canonical (clearer, more \
concise, Wikipedia-style) than the matched one.
  - `"matched"` (default) — the existing matched name stays canonical.

If you are unsure whether two names refer to the same thing, err on the side \
of `matched` being null.
"""


class LlmPairResolver:
    """Built-in :class:`PairResolver` using ``instructor`` + ``litellm``.

    Configuration is bound at construction; each call only takes
    ``(entity, candidates)`` matching the :class:`PairResolver` protocol.

    Per-pair results are memoized via CocoIndex's ``@coco.fn(memo=True)``
    decorator, persisted across runs.
    """

    def __init__(
        self,
        *,
        model: str,
        entity_type: str | None = None,
        extra_guidance: str | None = None,
        retries: int = 5,
    ) -> None:
        """
        Args:
            model: A litellm model string (e.g. ``"openai/gpt-4o-mini"``,
                ``"anthropic/claude-haiku-4-5"``). Same format as
                :class:`~cocoindex.ops.litellm.LiteLLMEmbedder`.
            entity_type: Optional entity-type hint woven into the prompt
                (e.g. ``"person"``, ``"technology"``, ``"organization"``).
            extra_guidance: Optional domain rules appended to the default
                prompt. Do **not** include output-format instructions.
            retries: Max retries when the LLM returns an invalid ``matched``
                value. Default 2.
        """
        self._model = model
        self._retries = retries
        self._system_prompt = _build_prompt(entity_type, extra_guidance)
        self._memo_key = _coco.memo_fingerprint((model, entity_type, extra_guidance))

    @_coco.fn(memo=True, logic_tracking="self")
    async def __call__(
        self,
        entity: str,
        candidates: list[str],
    ) -> _PairDecision:
        """Resolve a single ``(entity, candidates)`` pair."""
        user_message = (
            f"New entity: {entity!r}\n\n"
            "Existing canonical candidates:\n"
            + "\n".join(f"  - {c!r}" for c in candidates)
        )

        client = _instructor.from_litellm(
            _litellm.acompletion, mode=_instructor.Mode.JSON
        )

        messages: list[dict[str, str]] = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": user_message},
        ]

        for attempt in range(1 + self._retries):
            result = await client.chat.completions.create(
                model=self._model,
                response_model=_LlmResponse,
                messages=messages,
            )

            if result.matched is None or result.matched in candidates:
                return _PairDecision(matched=result.matched, canonical=result.canonical)

            feedback = (
                f"matched={result.matched!r} isn't in the candidate list "
                f"{candidates!r}; pick from that list or set matched to null."
            )
            messages.append({"role": "assistant", "content": result.model_dump_json()})
            messages.append({"role": "user", "content": feedback})

        return _PairDecision()

    def __coco_memo_key__(self) -> object:
        return self._memo_key


def _build_prompt(entity_type: str | None, extra_guidance: str | None) -> str:
    scope = f" of type {entity_type!r}" if entity_type else ""
    base = _DEFAULT_PROMPT_TEMPLATE.format(scope=scope)
    if extra_guidance:
        return f"{base}\nAdditional guidance:\n{extra_guidance}"
    return base
