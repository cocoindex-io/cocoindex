"""Verify the single-text ``embed`` public API on the shipped embedders.

The batching decorator's own correctness (batching, memoization, GPU runner) is
covered by ``python/tests/core/test_function_batching.py``. These tests verify
the thin wrapper added on top — that ``await embedder.embed("text")`` returns a
single ``NDArray[np.float32]`` rather than a ``list[NDArray]``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, call, patch

import numpy as np
import pytest

pytest.importorskip("litellm", reason="litellm not installed")

from litellm.exceptions import AuthenticationError  # noqa: E402

import cocoindex as coco  # noqa: E402
from cocoindex.ops.litellm import LiteLLMEmbedder  # noqa: E402

# Note on the sleep patch target below: retry sleeps now happen inside
# cocoindex._internal.deadline via a late `asyncio.sleep` lookup. Patching
# `cocoindex.ops.litellm._asyncio.sleep` still intercepts them because
# `_asyncio` is an alias of the stdlib module object, so the patch mutates
# the same `asyncio.sleep` attribute the deadline helper reads.
from cocoindex.resources.embedder import Embedder  # noqa: E402


class _FakeHTTPError(Exception):
    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


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


@pytest.mark.asyncio
async def test_litellm_embedder_retries_transient_embedding_errors() -> None:
    fake_response = type(
        "R",
        (),
        {"data": [{"embedding": [0.1, 0.2, 0.3, 0.4]}]},
    )()
    embedder = LiteLLMEmbedder("fake-model")
    mocked_embedding = AsyncMock(
        side_effect=[
            _FakeHTTPError(429),
            _FakeHTTPError(503),
            fake_response,
        ]
    )

    with (
        patch("cocoindex.ops.litellm.litellm.aembedding", new=mocked_embedding),
        patch("cocoindex.ops.litellm._asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        vec = await embedder.embed("hello")

    assert vec.tolist() == pytest.approx([0.1, 0.2, 0.3, 0.4])
    assert mocked_embedding.call_count == 3
    sleep.assert_has_awaits([call(1.0), call(2.0)])


@pytest.mark.asyncio
async def test_litellm_embedder_does_not_retry_non_transient_embedding_errors() -> None:
    embedder = LiteLLMEmbedder("fake-model")
    mocked_embedding = AsyncMock(side_effect=_FakeHTTPError(400))

    with (
        patch("cocoindex.ops.litellm.litellm.aembedding", new=mocked_embedding),
        patch("cocoindex.ops.litellm._asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        with pytest.raises(_FakeHTTPError):
            await embedder.embed("hello")

    mocked_embedding.assert_awaited_once()
    sleep.assert_not_called()


# ============================================================================
# RetryWithSmallerBatch: over-limit batches are split, global errors are not
# ============================================================================


def _fake_embedding_response(texts: list[str]) -> Any:
    # Embedding derived from the text so tests can verify item alignment.
    return SimpleNamespace(data=[{"embedding": [float(len(t))]} for t in texts])


@pytest.mark.asyncio
async def test_litellm_embedder_splits_oversized_batch() -> None:
    """A provider batch-size rejection splits the batch; every text succeeds
    with its own embedding (results stay aligned through the split)."""
    started = asyncio.Event()
    release = asyncio.Event()
    call_inputs: list[list[str]] = []

    async def fake_aembedding(*, model: str, input: list[str], **kwargs: Any) -> Any:
        call_inputs.append(list(input))
        if len(call_inputs) == 1:
            started.set()
            await release.wait()
        if len(input) > 2:
            raise _FakeHTTPError(400, "TOO_MANY_TOKENS_IN_BATCH")
        return _fake_embedding_response(input)

    embedder = LiteLLMEmbedder("fake-model")
    with patch("cocoindex.ops.litellm.litellm.aembedding", new=fake_aembedding):
        # First call runs inline and blocks, so the next four coalesce into
        # one batch of 4 — which the fake provider rejects.
        task0 = asyncio.create_task(embedder.embed("a"))
        await started.wait()
        texts = ["bb", "ccc", "dddd", "eeeee"]
        tasks = [asyncio.create_task(embedder.embed(t)) for t in texts]
        await asyncio.sleep(0.05)  # let them enqueue behind the inline call
        release.set()
        results = await asyncio.gather(task0, *tasks)

    for text, vec in zip(["a", *texts], results):
        assert vec.tolist() == [float(len(text))]
    # Inline [1], rejected [4], then the two halves of 2.
    assert [len(c) for c in call_inputs[:2]] == [1, 4]
    assert sorted(len(c) for c in call_inputs[2:]) == [2, 2]


@pytest.mark.asyncio
async def test_litellm_embedder_raises_retry_with_smaller_batch_on_400() -> None:
    """A non-retryable, non-global error on a multi-text batch becomes the
    RetryWithSmallerBatch signal (with the original error as its cause)."""
    embedder = LiteLLMEmbedder("fake-model")
    provider_error = _FakeHTTPError(400, "batch exceeds maximum context length")
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(side_effect=provider_error),
    ):
        with pytest.raises(coco.RetryWithSmallerBatch) as exc_info:
            await embedder._embed._execute_orig_async_fn(["a", "b"])
    assert exc_info.value.__cause__ is provider_error


@pytest.mark.asyncio
async def test_litellm_embedder_single_text_error_surfaces_original() -> None:
    """With one text there is nothing to split — the caller sees the original
    provider error (the engine unwraps the size-1 signal)."""
    embedder = LiteLLMEmbedder("fake-model")
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(side_effect=_FakeHTTPError(400, "input too large")),
    ):
        with pytest.raises(_FakeHTTPError):
            await embedder.embed("only")


@pytest.mark.asyncio
async def test_litellm_embedder_splits_after_transient_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient error that survives the same-size retry budget is still
    splittable — a smaller request may pass where the large one timed out."""
    # Near-zero retry budget: the retry wrapper exhausts its deadline almost
    # immediately (DeadlineExceededError, a TimeoutError subclass).
    monkeypatch.setattr("cocoindex.ops.litellm._EMBEDDING_RETRY_TIMEOUT_SECONDS", 0.05)
    embedder = LiteLLMEmbedder("fake-model")
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(side_effect=_FakeHTTPError(429)),
    ):
        with pytest.raises(coco.RetryWithSmallerBatch) as exc_info:
            await embedder._embed._execute_orig_async_fn(["a", "b"])
    assert isinstance(exc_info.value.__cause__, TimeoutError)


@pytest.mark.asyncio
async def test_litellm_embedder_does_not_split_global_errors() -> None:
    """Credential / auth errors can't be fixed by splitting — they propagate
    as-is even for multi-text batches."""
    embedder = LiteLLMEmbedder("fake-model")
    auth_error = AuthenticationError(
        message="access denied", llm_provider="openai", model="fake-model"
    )
    for error in (
        _FakeHTTPError(401, "Invalid API key provided"),
        auth_error,
    ):
        with patch(
            "cocoindex.ops.litellm.litellm.aembedding",
            new=AsyncMock(side_effect=error),
        ):
            with pytest.raises(type(error)):
                await embedder._embed._execute_orig_async_fn(["a", "b"])


@pytest.mark.asyncio
async def test_litellm_embedder_does_not_retry_missing_credentials_server_error() -> (
    None
):
    embedder = LiteLLMEmbedder("fake-model")
    missing_credentials_error = _FakeHTTPError(
        500,
        "litellm.InternalServerError: OpenAIException - Missing credentials. "
        "Please pass an `api_key`, `workload_identity`, `admin_api_key`, or set "
        "the `OPENAI_API_KEY` or `OPENAI_ADMIN_KEY` environment variable.",
    )
    mocked_embedding = AsyncMock(side_effect=missing_credentials_error)

    with (
        patch("cocoindex.ops.litellm.litellm.aembedding", new=mocked_embedding),
        patch("cocoindex.ops.litellm._asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        with pytest.raises(_FakeHTTPError):
            await embedder.embed("hello")

    mocked_embedding.assert_awaited_once()
    sleep.assert_not_called()
