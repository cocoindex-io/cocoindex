"""Verify the single-text ``embed`` public API on the shipped embedders.

The batching decorator's own correctness (batching, memoization, GPU runner) is
covered by ``python/tests/core/test_function_batching.py``. These tests verify
the thin wrapper added on top — that ``await embedder.embed("text")`` returns a
single ``NDArray[np.float32]`` rather than a ``list[NDArray]``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import numpy as np
import pytest

pytest.importorskip("litellm", reason="litellm not installed")

from cocoindex.ops.litellm import LiteLLMEmbedder  # noqa: E402
from cocoindex.resources.embedder import Embedder  # noqa: E402


@pytest.mark.asyncio
async def test_litellm_embedder_single_text_api() -> None:
    # Patch litellm.aembedding to return a deterministic 4-d vector.
    fake_response = type(
        "R",
        (),
        {"data": [{"embedding": [0.1, 0.2, 0.3, 0.4]}]},
    )()
    embedder = LiteLLMEmbedder("fake-model")

    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(return_value=fake_response),
    ) as mocked:
        vec = await embedder.embed("hello")

    # Single NDArray, not a list
    assert isinstance(vec, np.ndarray)
    assert vec.dtype == np.float32
    assert vec.shape == (4,)
    # Exactly one underlying call with our single text in the batch
    mocked.assert_called_once()
    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["input"] == ["hello"]


def test_litellm_embedder_satisfies_embedder_protocol() -> None:
    embedder = LiteLLMEmbedder("fake-model")
    assert isinstance(embedder, Embedder)


@pytest.mark.parametrize(
    "model, expects_float_hint",
    [
        ("text-embedding-3-small", True),
        ("openai/text-embedding-3-small", True),
        ("voyage/voyage-code-3", False),
        ("voyage/voyage-3-large", False),
        ("bedrock/amazon.titan-embed-text-v2:0", False),
    ],
)
@pytest.mark.asyncio
async def test_litellm_encoding_format_gated_by_provider(
    model: str, expects_float_hint: bool
) -> None:
    fake_response = type(
        "R",
        (),
        {"data": [{"embedding": [0.1, 0.2, 0.3, 0.4]}]},
    )()
    embedder = LiteLLMEmbedder(model)

    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(return_value=fake_response),
    ) as mocked:
        await embedder.embed("hello")

    call_kwargs = mocked.call_args.kwargs
    if expects_float_hint:
        assert call_kwargs.get("encoding_format") == "float"
        assert call_kwargs.get("drop_params") is True
    else:
        assert "encoding_format" not in call_kwargs
        assert "drop_params" not in call_kwargs
