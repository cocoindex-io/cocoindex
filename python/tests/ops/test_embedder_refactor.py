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
# Arrives with litellm; imported through importorskip so this module skips
# rather than failing collection where neither is installed.
httpx = pytest.importorskip("httpx", reason="httpx not installed")

from litellm.exceptions import (  # noqa: E402
    APIConnectionError,
    AuthenticationError,
    Timeout,
)

import cocoindex as coco  # noqa: E402
from cocoindex.ops.litellm import LiteLLMEmbedder, _aligned_embeddings  # noqa: E402

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


@pytest.mark.parametrize(
    ("error", "expected_calls"),
    [
        # litellm.Timeout is terminal even though it reports HTTP 408 and
        # descends from openai's APIConnectionError — both of which the
        # classification would otherwise treat as retryable.
        pytest.param(
            Timeout(message="too slow", model="fake-model", llm_provider="openai"),
            1,
            id="litellm-timeout",
        ),
        # A refused/reset connection is a fast failure: still retried.
        pytest.param(
            APIConnectionError(
                message="connection refused", model="fake-model", llm_provider="openai"
            ),
            3,
            id="litellm-connection-error",
        ),
        # httpx timeouts subclass neither TimeoutError nor litellm.Timeout;
        # they are matched by name.
        pytest.param(httpx.ReadTimeout("stalled"), 1, id="httpx-read-timeout"),
        pytest.param(httpx.PoolTimeout("no free slot"), 1, id="httpx-pool-timeout"),
        pytest.param(httpx.ConnectError("refused"), 3, id="httpx-connect-error"),
    ],
)
@pytest.mark.asyncio
async def test_litellm_embedder_retries_fast_failures_but_not_timeouts(
    error: Exception, expected_calls: int
) -> None:
    fake_response = type("R", (), {"data": [{"embedding": [0.1]}]})()
    embedder = LiteLLMEmbedder("fake-model")
    mocked_embedding = AsyncMock(side_effect=[error, error, fake_response])

    with (
        patch("cocoindex.ops.litellm.litellm.aembedding", new=mocked_embedding),
        patch("cocoindex.ops.litellm._asyncio.sleep", new=AsyncMock()),
    ):
        if expected_calls == 1:
            with pytest.raises(type(error)):
                await embedder.embed("hello")
        else:
            await embedder.embed("hello")

    assert mocked_embedding.await_count == expected_calls


@pytest.mark.asyncio
async def test_litellm_embedder_does_not_split_timeouts() -> None:
    """A multi-text batch that times out propagates the timeout; splitting
    would only re-spend a budget the backend already failed to meet."""
    embedder = LiteLLMEmbedder("fake-model")
    timeout_error = Timeout(
        message="too slow", model="fake-model", llm_provider="openai"
    )
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding",
        new=AsyncMock(side_effect=timeout_error),
    ):
        with pytest.raises(Timeout):
            await embedder._embed._execute_orig_async_fn(["a", "b"])


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


# ============================================================================
# Response items are aligned to inputs by `index` when the provider sends one
# ============================================================================


def test_aligned_embeddings_positional_when_no_index() -> None:
    # A missing key and an explicit None are both "no index".
    data = [{"embedding": [1.0]}, {"index": None, "embedding": [2.0]}]
    assert [v.tolist() for v in _aligned_embeddings(data, 2)] == [[1.0], [2.0]]


def test_aligned_embeddings_reorders_by_index() -> None:
    data = [
        {"index": 2, "embedding": [2.0]},
        {"index": 0, "embedding": [0.0]},
        {"index": 1, "embedding": [1.0]},
    ]
    assert [v.tolist() for v in _aligned_embeddings(data, 3)] == [[0.0], [1.0], [2.0]]


@pytest.mark.asyncio
async def test_litellm_embedder_aligns_batch_by_index() -> None:
    """The batch body maps a provider response that arrives out of order
    back onto the input texts by ``index``."""
    texts = ["a", "bb", "ccc"]
    response = SimpleNamespace(
        data=[{"index": i, "embedding": [float(len(texts[i]))]} for i in (2, 0, 1)]
    )
    embedder = LiteLLMEmbedder("fake-model")
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding", new=AsyncMock(return_value=response)
    ):
        vecs = await embedder._embed._execute_orig_async_fn(texts)
    assert [v.tolist() for v in vecs] == [[1.0], [2.0], [3.0]]


@pytest.mark.parametrize(
    ("indices", "n", "match"),
    [
        pytest.param([0, None], 2, "with and without", id="partial"),
        pytest.param([0, 0], 2, "not a permutation", id="duplicate"),
        pytest.param([0, 1], 3, "2 items for 3 inputs", id="missing"),
        pytest.param([-1, 0], 2, "not a permutation", id="negative"),
        pytest.param([0, 2], 2, "not a permutation", id="out-of-range"),
        pytest.param([0, "1"], 2, "not a permutation", id="non-int"),
    ],
)
def test_aligned_embeddings_rejects_bad_indices(
    indices: list[Any], n: int, match: str
) -> None:
    data = [{"index": i, "embedding": [0.0]} for i in indices]
    with pytest.raises(RuntimeError, match=match):
        _aligned_embeddings(data, n)


@pytest.mark.asyncio
async def test_litellm_embedder_bad_index_fails_whole_batch() -> None:
    """A malformed index set is a provider bug: it surfaces as-is rather than
    as RetryWithSmallerBatch, since splitting would only hide the misorder."""
    response = SimpleNamespace(data=[{"index": 0, "embedding": [0.0]}] * 2)
    embedder = LiteLLMEmbedder("fake-model")
    with patch(
        "cocoindex.ops.litellm.litellm.aembedding", new=AsyncMock(return_value=response)
    ):
        with pytest.raises(RuntimeError, match="not a permutation"):
            await embedder._embed._execute_orig_async_fn(["a", "b"])
