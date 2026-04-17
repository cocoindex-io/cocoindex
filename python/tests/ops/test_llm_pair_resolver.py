"""Tests for cocoindex.ops.entity_resolution.llm_resolver.LlmPairResolver.

Mocks ``litellm.acompletion`` to script model responses without hitting any
real provider.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("litellm", reason="litellm not installed")
pytest.importorskip("instructor", reason="instructor not installed")

from litellm import ModelResponse  # noqa: E402

from cocoindex.ops.entity_resolution import CanonicalSide, PairDecision  # noqa: E402
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver  # noqa: E402


def _make_response(matched: str | None, canonical: str = "matched") -> ModelResponse:
    """Build a litellm-compatible response carrying a JSON PairDecision."""
    payload = json.dumps({"matched": matched, "canonical": canonical})
    return ModelResponse(
        choices=[
            {
                "finish_reason": "stop",
                "message": {"content": payload, "role": "assistant"},
            }
        ]
    )


def _mock_completion(matched: str | None, canonical: str = "matched") -> AsyncMock:
    """Return a mock that simulates a litellm completion."""
    resp = _make_response(matched, canonical)

    async def _acompletion(**kwargs: object) -> ModelResponse:
        return resp

    return AsyncMock(side_effect=_acompletion)


@pytest.mark.asyncio
async def test_llm_happy_path_no_match() -> None:
    mock = _mock_completion(matched=None)
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini")
        result = await resolver("foo", ["bar", "baz"])
    assert result == PairDecision()
    assert mock.call_count >= 1


@pytest.mark.asyncio
async def test_llm_happy_path_match() -> None:
    mock = _mock_completion(matched="bar", canonical="matched")
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini")
        result = await resolver("foo", ["bar", "baz"])
    assert result == PairDecision(matched="bar", canonical=CanonicalSide.MATCHED)


@pytest.mark.asyncio
async def test_llm_match_new_canonical() -> None:
    mock = _mock_completion(matched="bar", canonical="new")
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini")
        result = await resolver("foo", ["bar", "baz"])
    assert result == PairDecision(matched="bar", canonical=CanonicalSide.NEW)


@pytest.mark.asyncio
async def test_llm_hallucinated_matched_retries_then_succeeds() -> None:
    call_count = {"n": 0}

    async def _acompletion(**kwargs: object) -> ModelResponse:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _make_response(matched="ghost")
        return _make_response(matched="bar")

    mock = AsyncMock(side_effect=_acompletion)
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini")
        result = await resolver("foo", ["bar", "baz"])

    assert result == PairDecision(matched="bar", canonical=CanonicalSide.MATCHED)
    assert call_count["n"] == 2


@pytest.mark.asyncio
async def test_llm_retry_exhaustion_returns_no_match() -> None:
    mock = _mock_completion(matched="ghost")
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini", retries=1)
        result = await resolver("foo", ["bar", "baz"])
    assert result == PairDecision()


@pytest.mark.asyncio
async def test_llm_provider_error_propagates() -> None:
    async def _explode(**kwargs: object) -> ModelResponse:
        raise RuntimeError("provider down")

    mock = AsyncMock(side_effect=_explode)
    with patch(
        "cocoindex.ops.entity_resolution.llm_resolver._litellm.acompletion", mock
    ):
        resolver = LlmPairResolver(model="openai/gpt-4o-mini")
        with pytest.raises(Exception):
            await resolver("foo", ["bar", "baz"])
