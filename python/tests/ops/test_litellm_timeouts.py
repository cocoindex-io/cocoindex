"""Timeouts are terminal for embedding: never retried, never split.

A timeout has by definition already spent its duration, so retrying it inside
the request's own time bound re-spends what is left — and splitting the batch
on one is worse still, because every sub-batch takes a *fresh* full timeout.
That turns one expired request into a tree of them aimed at a backend that is
already too slow, which is exactly the amplification these tests pin shut.

``LiteLLMEmbedder(timeout=...)`` is the one clock on a request: litellm's own
per-request ``timeout`` kwarg is no longer forwarded.

Runs against the stub ``litellm`` module from the ``litellm_module`` fixture,
so CI (which does not install the optional dependency) actually executes
them. The cases that need real provider classes — ``litellm.Timeout``, the
httpx timeouts — live in ``test_embedder_refactor.py``, since a hand-rolled
stand-in for those would only prove the stand-in.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("numpy")

import cocoindex as coco  # noqa: E402
from cocoindex._internal.batching import wrap_batch_fn_async  # noqa: E402


class _FakeHTTPError(Exception):
    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


class _CallRecorder:
    """Fake ``litellm.aembedding`` that always fails, recording batch sizes."""

    def __init__(self, error: BaseException) -> None:
        self._error = error
        self.batch_sizes: list[int] = []

    async def __call__(self, *, model: str, input: list[str], **kwargs: Any) -> Any:
        self.batch_sizes.append(len(input))
        raise self._error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            coco.DeadlineExceededError("CocoIndex timeout deadline exceeded"),
            id="deadline-exceeded",
        ),
        # asyncio.TimeoutError is builtin TimeoutError on 3.11+.
        pytest.param(asyncio.TimeoutError("slow"), id="asyncio-timeout"),
    ],
)
@pytest.mark.asyncio
async def test_timeout_is_not_retried(
    litellm_module: Any, caplog: pytest.LogCaptureFixture, error: BaseException
) -> None:
    """Terminal on the first attempt — and silent. The bogus "retrying in
    0.0s" warning was the visible symptom of the old classification."""
    embedder = litellm_module.LiteLLMEmbedder("fake-model")
    mocked = AsyncMock(side_effect=error)

    with (
        caplog.at_level(logging.WARNING),
        patch.object(litellm_module.litellm, "aembedding", new=mocked),
        patch("asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        with pytest.raises(type(error)):
            await embedder.embed("hello")

    mocked.assert_awaited_once()
    sleep.assert_not_called()
    assert "failed with transient error" not in caplog.text


@pytest.mark.asyncio
async def test_timeout_propagates_instead_of_splitting(litellm_module: Any) -> None:
    """The batch body raises the timeout rather than the split signal."""
    embedder = litellm_module.LiteLLMEmbedder("fake-model")
    error = coco.DeadlineExceededError("CocoIndex timeout deadline exceeded")

    with patch.object(
        litellm_module.litellm, "aembedding", new=AsyncMock(side_effect=error)
    ):
        with pytest.raises(coco.DeadlineExceededError):
            await embedder._embed._execute_orig_async_fn(["a", "b"])


@pytest.mark.asyncio
async def test_non_timeout_error_still_splits(litellm_module: Any) -> None:
    """Control: a non-timeout, non-global failure is still splittable — a
    smaller request may pass a provider's token or payload cap."""
    embedder = litellm_module.LiteLLMEmbedder("fake-model")
    provider_error = _FakeHTTPError(400, "batch exceeds maximum context length")

    with patch.object(
        litellm_module.litellm, "aembedding", new=AsyncMock(side_effect=provider_error)
    ):
        with pytest.raises(coco.RetryWithSmallerBatch) as exc_info:
            await embedder._embed._execute_orig_async_fn(["a", "b"])
    assert exc_info.value.__cause__ is provider_error


# Seconds as a number is the form litellm's `timeout` kwarg took while it was
# the one passing through, so existing `timeout=30` calls keep working.
@pytest.mark.parametrize(
    "timeout",
    [
        pytest.param(timedelta(seconds=0.05), id="timedelta"),
        pytest.param(0.05, id="seconds"),
    ],
)
@pytest.mark.asyncio
async def test_expired_timeout_does_not_fan_out(
    litellm_module: Any, timeout: timedelta | float
) -> None:
    """The bug this fixes end to end: a batch whose timeout expires must not
    become a split tree, each node taking a fresh full timeout.

    Driven through the engine's split driver — the same thing one batcher
    dispatch runs — so the tree is exact. A backend that is merely slow (429
    forever, timeout too short) must be hit at the original batch size only.
    """
    recorder = _CallRecorder(_FakeHTTPError(429))
    embedder = litellm_module.LiteLLMEmbedder("fake-model", timeout=timeout)

    with patch.object(litellm_module.litellm, "aembedding", new=recorder):
        with pytest.raises(coco.DeadlineExceededError):
            await asyncio.wait_for(
                wrap_batch_fn_async(embedder._embed._execute_orig_async_fn)(
                    ["a", "b", "c", "d"]
                ),
                timeout=10.0,
            )

    # Retries within the one timeout are fine; sub-batches are not.
    assert recorder.batch_sizes, "the backend was never called"
    assert set(recorder.batch_sizes) == {4}, (
        f"expected only full-size attempts, saw sizes {recorder.batch_sizes}"
    )


@pytest.mark.asyncio
async def test_timeout_bounds_a_slow_backend(litellm_module: Any) -> None:
    """A short timeout cuts a hung request where the 10-minute default would
    still be waiting."""
    embedder = litellm_module.LiteLLMEmbedder(
        "fake-model", timeout=timedelta(seconds=0.05)
    )
    attempts = 0

    async def too_slow(*, model: str, input: list[str], **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        # Far longer than the timeout: the attempt is cut at the deadline, so
        # this never returns.
        await asyncio.sleep(5.0)

    with patch.object(litellm_module.litellm, "aembedding", new=too_slow):
        with pytest.raises(coco.DeadlineExceededError):
            await asyncio.wait_for(embedder.embed("hello"), timeout=10.0)

    assert attempts == 1


@pytest.mark.asyncio
async def test_timeout_is_not_forwarded_to_litellm(litellm_module: Any) -> None:
    """One clock per request. `timeout` is CocoIndex's bound on the whole
    request; forwarding it to litellm as well would put a second, per-attempt
    clock on the same call."""
    seen: list[dict[str, Any]] = []

    async def record(*, model: str, input: list[str], **kwargs: Any) -> Any:
        seen.append(kwargs)
        return SimpleNamespace(data=[{"embedding": [1.0]} for _ in input])

    embedder = litellm_module.LiteLLMEmbedder(
        "fake-model", timeout=timedelta(seconds=30), api_base="http://x"
    )
    with patch.object(litellm_module.litellm, "aembedding", new=record):
        await embedder.embed("hello")

    assert len(seen) == 1
    assert "timeout" not in seen[0]
    assert seen[0]["api_base"] == "http://x"  # other kwargs still pass through


@pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1), 0, -1.5])
def test_rejects_non_positive_timeout(
    litellm_module: Any, value: timedelta | float
) -> None:
    with pytest.raises(ValueError, match="timeout must be positive"):
        litellm_module.LiteLLMEmbedder("fake-model", timeout=value)
