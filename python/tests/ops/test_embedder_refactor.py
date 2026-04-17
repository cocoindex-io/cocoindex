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

from cocoindex.ops.litellm import LiteLLMEmbedder
from cocoindex.resources.embedder import Embedder


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
