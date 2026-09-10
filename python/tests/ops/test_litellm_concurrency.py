"""``LiteLLMEmbedder(max_inflight_requests=...)``: bound concurrent requests
to the embedding backend.

Batching bounds how many texts ride in one request, never how many requests
are open at once — and a batch that fails splits into halves that run
concurrently *inside* the same batch dispatch. So the permit is taken per
backend request, inside the batch body and inside the retry loop. These tests
pin that placement down.

Most of them drive the split wrapper directly rather than going through the
batcher. Coalescing N concurrent ``embed()`` callers into one batch is
timing-dependent — under load the batcher may dispatch a subset — which makes
any assertion about the resulting split tree flaky. Driving the wrapper gives
an exact tree, and ``test_limit_holds_through_the_public_api`` covers the real
``embed()`` path with a flake-proof upper-bound assertion.

Runs against the stub ``litellm`` module from the ``litellm_module`` fixture
so it exercises the real split driver without the optional dependency.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

pytest.importorskip("numpy")

from cocoindex._internal.batching import (  # noqa: E402
    BatchItemFailure,
    wrap_batch_fn_async,
)


class _FakeHTTPError(Exception):
    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


def _fake_embedding_response(texts: list[str]) -> Any:
    # Embedding derived from the text so tests can verify item alignment.
    return SimpleNamespace(data=[{"embedding": [float(len(t))]} for t in texts])


def _texts(n: int) -> list[str]:
    """``n`` texts of distinct length, so each maps to a distinct embedding."""
    return ["x" * (i + 1) for i in range(n)]


class _ConcurrencyProbe:
    """Fake ``litellm.aembedding`` that records how many calls overlap."""

    def __init__(self, *, split_above: int) -> None:
        self._split_above = split_above
        self._inflight = 0
        self.peak_inflight = 0
        self.batch_sizes: list[int] = []

    async def __call__(self, *, model: str, input: list[str], **kwargs: Any) -> Any:
        self.batch_sizes.append(len(input))
        self._inflight += 1
        self.peak_inflight = max(self.peak_inflight, self._inflight)
        try:
            # A real suspension point, so concurrent calls genuinely overlap
            # and `peak_inflight` measures something.
            await asyncio.sleep(0.02)
            if len(input) > self._split_above:
                raise _FakeHTTPError(400, "TOO_MANY_TOKENS_IN_BATCH")
            return _fake_embedding_response(input)
        finally:
            self._inflight -= 1


def _split_runner(embedder: Any) -> Any:
    """The batch body wrapped in the engine's split-and-retry driver.

    This is what a single batcher dispatch runs, minus the batcher — so the
    split tree is exact instead of depending on how many callers coalesced.
    """
    return wrap_batch_fn_async(embedder._embed._execute_orig_async_fn)


async def _run_batch(
    module: Any, embedder: Any, probe: Any, texts: list[str]
) -> list[float]:
    with patch.object(module.litellm, "aembedding", new=probe):
        out = await asyncio.wait_for(_split_runner(embedder)(texts), timeout=10.0)
    return [vec.tolist()[0] for vec in out]


@pytest.mark.asyncio
async def test_bounds_inflight_requests_across_split(litellm_module: Any) -> None:
    """The split tree is the fan-out that floods a backend: one batch of 16
    rejected above size 2 becomes 8 leaf requests, all open at once — while
    the batcher sees a single dispatch. `max_inflight_requests` bounds them;
    the default (None) leaves them unbounded."""
    texts = _texts(16)
    expected = [float(len(t)) for t in texts]

    unbounded_probe = _ConcurrencyProbe(split_above=2)
    unbounded = await _run_batch(
        litellm_module,
        litellm_module.LiteLLMEmbedder("fake-model"),
        unbounded_probe,
        texts,
    )
    assert unbounded == expected
    assert unbounded_probe.peak_inflight == 8  # 16 -> 8+8 -> 4s -> eight 2s

    bounded_probe = _ConcurrencyProbe(split_above=2)
    bounded = await _run_batch(
        litellm_module,
        litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=2),
        bounded_probe,
        texts,
    )
    assert bounded == expected
    assert bounded_probe.peak_inflight == 2
    # Same requests, only paced differently — the split tree is untouched.
    assert bounded_probe.batch_sizes == unbounded_probe.batch_sizes


@pytest.mark.asyncio
async def test_inflight_limit_of_one_survives_split(litellm_module: Any) -> None:
    """Regression: at a limit of 1, a batch that splits must still finish.

    A permit held across the split would deadlock — the split driver waits on
    both halves, and the halves would wait on the permit their parent only
    releases once they are done. Taking the permit per request avoids it: the
    parent's body has already raised, and released, before the halves start.
    """
    texts = _texts(8)
    probe = _ConcurrencyProbe(split_above=1)  # split all the way to singletons
    embedder = litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=1)

    values = await _run_batch(litellm_module, embedder, probe, texts)

    assert values == [float(len(t)) for t in texts]
    assert probe.peak_inflight == 1
    # 8 -> 4+4 -> four 2s -> eight singletons, each request taken one at a time.
    assert probe.batch_sizes == [8, 4, 4, 2, 2, 2, 2] + [1] * 8


@pytest.mark.asyncio
async def test_releases_permit_during_retry_backoff(
    litellm_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The permit wraps one request, not the retry loop around it.

    A sub-batch sleeping off a 429 backoff holds no slot, so at a limit of 1
    the sibling the split dispatched alongside it runs during that sleep.
    Taken around the retry loop instead, the permit would keep that sibling
    out until the first sub-batch had finished retrying — turning a 30s
    backoff into 30s of stalled throughput for everything else.
    """
    monkeypatch.setattr(litellm_module, "_RETRY_INITIAL_BACKOFF_SECONDS", 0.3)
    order: list[str] = []
    rate_limited = False

    async def fake_aembedding(*, model: str, input: list[str], **kwargs: Any) -> Any:
        nonlocal rate_limited
        order.append(input[0])
        if len(input) > 2:  # the batch of 4: too big, so it gets split
            raise _FakeHTTPError(400, "TOO_MANY_TOKENS_IN_BATCH")
        if not rate_limited:  # first half: rate-limited once, backs off, retries
            rate_limited = True
            raise _FakeHTTPError(429)
        return _fake_embedding_response(input)

    embedder = litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=1)
    texts = ["a", "bb", "ccc", "dddd"]
    with patch.object(litellm_module.litellm, "aembedding", new=fake_aembedding):
        out = await asyncio.wait_for(_split_runner(embedder)(texts), timeout=10.0)

    assert [vec.tolist()[0] for vec in out] == [1.0, 2.0, 3.0, 4.0]
    # The batch of 4, the rate-limited half ["a","bb"], then its sibling
    # ["ccc","dddd"] *during* the backoff, and only then the half's retry.
    assert order == ["a", "a", "ccc", "a"]


@pytest.mark.asyncio
async def test_limit_holds_through_the_public_api(litellm_module: Any) -> None:
    """End-to-end over the real `embed()` path, batcher included.

    Nothing is asserted about the shape of the split tree: how many callers
    the batcher coalesces is timing-dependent. Only that every text comes
    back correct, that no scheduling pushed concurrency past the limit, and
    that nothing deadlocked.
    """
    texts = _texts(8)
    probe = _ConcurrencyProbe(split_above=1)
    embedder = litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=1)

    with patch.object(litellm_module.litellm, "aembedding", new=probe):
        results = await asyncio.wait_for(
            asyncio.gather(*(embedder.embed(t) for t in texts)), timeout=10.0
        )

    assert [vec.tolist()[0] for vec in results] == [float(len(t)) for t in texts]
    assert probe.peak_inflight == 1


def test_permit_survives_a_new_event_loop(litellm_module: Any) -> None:
    """Regression: an embedder outlives any one event loop.

    An `asyncio.Semaphore` binds to the loop that first blocks on it, and a
    module-level embedder — the documented usage — is reused across the fresh
    loop each `Environment` (or pytest-asyncio test) brings up. Without
    rebinding, the second loop raised `RuntimeError: ... is bound to a
    different event loop`, surfacing as a failure for every text.

    Sync on purpose: it needs to drive two `asyncio.run` loops itself.
    """
    texts = _texts(8)
    expected = [float(len(t)) for t in texts]
    embedder = litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=1)

    async def one_pass() -> list[float]:
        # split_above=1 with a limit of 1 guarantees real waiters, which is
        # what makes the semaphore latch onto the running loop.
        probe = _ConcurrencyProbe(split_above=1)
        with patch.object(litellm_module.litellm, "aembedding", new=probe):
            out = await asyncio.wait_for(_split_runner(embedder)(texts), timeout=10.0)
        failed = [v for v in out if isinstance(v, BatchItemFailure)]
        assert not failed, f"sub-batch failed: {failed[0].error!r}"
        assert probe.peak_inflight == 1
        return [vec.tolist()[0] for vec in out]

    assert asyncio.run(one_pass()) == expected  # loop A latches the semaphore
    assert asyncio.run(one_pass()) == expected  # loop B: fresh loop, same embedder


@pytest.mark.parametrize("value", [0, -1])
def test_rejects_non_positive_inflight_limit(litellm_module: Any, value: int) -> None:
    with pytest.raises(ValueError, match="max_inflight_requests"):
        litellm_module.LiteLLMEmbedder("fake-model", max_inflight_requests=value)
