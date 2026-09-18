"""Tests for SentenceTransformerEmbedder's OOM split-and-retry behavior.

The real sentence-transformers package is not needed: ``_get_model`` is
stubbed with a fake model, so these tests exercise the op's error
classification and the batching engine's RetryWithSmallerBatch path
(including the GPU runner) without loading any model.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import numpy as np
import pytest

import cocoindex as coco
from cocoindex._internal import runner as runner_module
from cocoindex.ops import sentence_transformers as sentence_transformers_module
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

_OOM_MESSAGE = "CUDA out of memory. Tried to allocate 20.00 GiB"


class _FakeModel:
    """Fake SentenceTransformer: OOMs on batches larger than ``oom_above``.

    Embeddings are derived from the text length so tests can verify that
    results stay aligned to their inputs through a split.
    """

    def __init__(self, oom_above: int = 2) -> None:
        self.oom_above = oom_above
        self.encode_sizes: list[int] = []
        self.first_call_started = threading.Event()
        self.release_first_call = threading.Event()
        self._lock = threading.Lock()

    def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
        with self._lock:
            first = not self.encode_sizes
            self.encode_sizes.append(len(texts))
        if first:
            self.first_call_started.set()
            assert self.release_first_call.wait(timeout=5)
        if len(texts) > self.oom_above:
            raise RuntimeError(_OOM_MESSAGE)
        return np.array([[float(len(t))] for t in texts], dtype=np.float32)


def _make_embedder(model: Any) -> SentenceTransformerEmbedder:
    # Keep the existing batching tests on the generic in-process path even when
    # the suite runs on macOS. MPS routing has focused tests below.
    embedder = SentenceTransformerEmbedder("fake-model", device="cpu")
    embedder._get_model = lambda: model  # type: ignore[method-assign]
    return embedder


@pytest.mark.asyncio
async def test_sentence_transformer_splits_oom_batch() -> None:
    """An OOM on a large batch splits it; every text succeeds with its own
    embedding, end to end through the batcher and the GPU runner."""
    fake = _FakeModel(oom_above=2)
    embedder = _make_embedder(fake)

    # First call runs inline and blocks inside encode (on a GPU runner
    # thread), so the next four coalesce into one batch of 4 — which OOMs.
    task0 = asyncio.create_task(embedder.embed("a"))
    assert await asyncio.to_thread(fake.first_call_started.wait, 5)
    texts = ["bb", "ccc", "dddd", "eeeee"]
    tasks = [asyncio.create_task(embedder.embed(t)) for t in texts]
    await asyncio.sleep(0.05)  # let them enqueue behind the inline call
    fake.release_first_call.set()
    results = await asyncio.gather(task0, *tasks)

    for text, vec in zip(["a", *texts], results):
        assert vec.tolist() == [float(len(text))]
    # Inline [1], the OOMing batch of 4, then its two halves.
    assert fake.encode_sizes == [1, 4, 2, 2]


class _AlwaysFailModel:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
        raise self.error


@pytest.mark.parametrize(
    "error",
    [RuntimeError(_OOM_MESSAGE), MemoryError("host allocation failed")],
    ids=["cuda", "host"],
)
def test_sentence_transformer_oom_on_multi_text_raises_signal(
    error: BaseException,
) -> None:
    embedder = _make_embedder(_AlwaysFailModel(error))
    with pytest.raises(coco.RetryWithSmallerBatch) as exc_info:
        embedder._embed._execute_orig_sync_fn(["a", "b", "c"])
    assert exc_info.value.__cause__ is error


@pytest.mark.asyncio
async def test_sentence_transformer_oom_on_single_text_surfaces_original() -> None:
    """A single text that doesn't fit is its own failure — the caller sees the
    OOM error (the engine unwraps the size-1 signal)."""
    embedder = _make_embedder(_AlwaysFailModel(RuntimeError(_OOM_MESSAGE)))
    with pytest.raises(RuntimeError, match="out of memory"):
        await embedder.embed("only")


def test_sentence_transformer_non_oom_error_propagates() -> None:
    """Config/model errors aren't composition-dependent — no split."""
    embedder = _make_embedder(_AlwaysFailModel(KeyError("unknown prompt_name")))
    with pytest.raises(KeyError):
        embedder._embed._execute_orig_sync_fn(["a", "b"])


@pytest.mark.parametrize(
    ("device", "platform", "expected"),
    [
        ("mps", "linux", True),
        ("cpu", "darwin", False),
        ("cuda", "darwin", False),
        (None, "darwin", True),
        (None, "linux", False),
    ],
)
def test_sentence_transformer_detects_mps_device(
    monkeypatch: pytest.MonkeyPatch,
    device: str | None,
    platform: str,
    expected: bool,
) -> None:
    monkeypatch.setattr(sys, "platform", platform)

    assert sentence_transformers_module._is_mps_device(device) is expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("device", "uses_mps"), [("mps", True), ("cpu", False)])
async def test_sentence_transformer_routes_to_device_runner(
    monkeypatch: pytest.MonkeyPatch,
    device: str,
    uses_mps: bool,
) -> None:
    monkeypatch.setattr(
        runner_module,
        "_configure_mps_allocator_defaults",
        Mock(),
    )
    embedder = SentenceTransformerEmbedder("fake-model", device=device)
    generic_embed = AsyncMock(return_value=np.array([1.0], dtype=np.float32))
    mps_embed = AsyncMock(return_value=np.array([2.0], dtype=np.float32))
    generic_dimension = AsyncMock(return_value=128)
    mps_dimension = AsyncMock(return_value=256)
    monkeypatch.setattr(embedder, "_embed", generic_embed)
    monkeypatch.setattr(embedder, "_embed_mps", mps_embed)
    monkeypatch.setattr(embedder, "_dimension", generic_dimension)
    monkeypatch.setattr(embedder, "_dimension_mps", mps_dimension)

    result = await embedder.embed._execute_orig_async_fn("text")
    dimension = await embedder.dimension._execute_orig_async_fn()

    assert result.tolist() == ([2.0] if uses_mps else [1.0])
    assert dimension == (256 if uses_mps else 128)
    assert mps_embed.await_count == int(uses_mps)
    assert generic_embed.await_count == int(not uses_mps)
    assert mps_dimension.await_count == int(uses_mps)
    assert generic_dimension.await_count == int(not uses_mps)


class _SuccessModel:
    def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
        return np.array([[float(len(text))] for text in texts], dtype=np.float32)

    def get_sentence_embedding_dimension(self) -> int:
        return 384


def _install_fake_mps(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    *,
    synchronize_error: BaseException | None = None,
    allocated_memory: int = 50,
    recommended_memory: int = 100,
    low_watermark: str = "0.4",
    high_watermark: str = "0.5",
) -> None:
    monkeypatch.setenv("PYTORCH_MPS_LOW_WATERMARK_RATIO", low_watermark)
    monkeypatch.setenv("PYTORCH_MPS_HIGH_WATERMARK_RATIO", high_watermark)

    def synchronize() -> None:
        events.append("synchronize")
        if synchronize_error is not None:
            raise synchronize_error

    fake_torch: Any = SimpleNamespace(
        backends=SimpleNamespace(mps=SimpleNamespace(is_available=lambda: True)),
        cuda=SimpleNamespace(is_available=lambda: False),
        mps=SimpleNamespace(
            synchronize=synchronize,
            empty_cache=lambda: events.append("empty_cache"),
            driver_allocated_memory=lambda: allocated_memory,
            recommended_max_memory=lambda: recommended_memory,
        ),
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setattr(
        gc,
        "collect",
        lambda: events.append("gc_collect"),
    )


def test_sentence_transformer_mps_clears_cache_after_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(monkeypatch, events)
    embedder = _make_embedder(_SuccessModel())

    result = embedder._embed_mps._execute_orig_sync_fn(["a", "bb"])

    assert [item.tolist() for item in result] == [[1.0], [2.0]]
    assert events == ["synchronize", "gc_collect", "empty_cache", "synchronize"]


def test_sentence_transformer_mps_skips_cleanup_below_pressure_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(monkeypatch, events, allocated_memory=39)
    embedder = _make_embedder(_SuccessModel())

    result = embedder._embed_mps._execute_orig_sync_fn(["a"])

    assert result[0].tolist() == [1.0]
    assert events == []


def test_sentence_transformer_mps_cleanup_uses_explicit_watermark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(
        monkeypatch,
        events,
        allocated_memory=30,
        low_watermark="0.25",
        high_watermark="0.3",
    )
    embedder = _make_embedder(_SuccessModel())

    embedder._embed_mps._execute_orig_sync_fn(["a"])

    assert events == ["synchronize", "gc_collect", "empty_cache", "synchronize"]


def test_sentence_transformer_mps_oom_clears_cache_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(monkeypatch, events, allocated_memory=1)
    error = RuntimeError(_OOM_MESSAGE)
    embedder = _make_embedder(_AlwaysFailModel(error))

    with pytest.raises(coco.RetryWithSmallerBatch) as exc_info:
        embedder._embed_mps._execute_orig_sync_fn(["a", "b"])

    assert exc_info.value.__cause__ is error
    assert events == ["empty_cache"]


def test_sentence_transformer_mps_cleanup_failure_does_not_mask_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(
        monkeypatch,
        events,
        synchronize_error=RuntimeError("cleanup failed"),
    )
    embedder = _make_embedder(_SuccessModel())

    result = embedder._embed_mps._execute_orig_sync_fn(["a"])

    assert result[0].tolist() == [1.0]
    assert events == ["synchronize"]


def test_sentence_transformer_mps_cleanup_does_not_mask_model_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(
        monkeypatch,
        events,
        synchronize_error=RuntimeError("cleanup failed"),
    )
    embedder = _make_embedder(_AlwaysFailModel(KeyError("unknown prompt_name")))

    with pytest.raises(KeyError, match="unknown prompt_name"):
        embedder._embed_mps._execute_orig_sync_fn(["a"])
    assert events == ["synchronize"]


def test_sentence_transformer_mps_dimension_clears_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_fake_mps(monkeypatch, events)
    embedder = _make_embedder(_SuccessModel())

    dimension = embedder._dimension_mps._execute_orig_sync_fn()

    assert dimension == 384
    assert events == ["synchronize", "gc_collect", "empty_cache", "synchronize"]


_subprocess_model_initializations = 0


class _SubprocessFakeModel(_SuccessModel):
    def __init__(self, initialization: int) -> None:
        self._initialization = initialization

    def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
        return np.array(
            [[float(len(text)), float(self._initialization)] for text in texts],
            dtype=np.float32,
        )

    def get_sentence_embedding_dimension(self) -> int:
        return 2


class _SubprocessFakeMPSEmbedder(SentenceTransformerEmbedder):
    def _get_model(self) -> Any:
        global _subprocess_model_initializations
        if self._model is None:
            _subprocess_model_initializations += 1
            self._model = _SubprocessFakeModel(  # type: ignore[assignment]
                _subprocess_model_initializations
            )
        return self._model


@pytest.mark.asyncio
async def test_sentence_transformer_mps_subprocess_keeps_model_warm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS", raising=False)
    monkeypatch.setenv("PYTORCH_MPS_LOW_WATERMARK_RATIO", "0.4")
    monkeypatch.setenv("PYTORCH_MPS_HIGH_WATERMARK_RATIO", "0.5")
    monkeypatch.setattr(runner_module._MPS_GPU, "_use_subprocess", None)
    embedder = _SubprocessFakeMPSEmbedder("fake-model", device="mps")

    dimension = await embedder.dimension()
    first = await embedder.embed("a")
    second = await embedder.embed("bb")

    assert dimension == 2
    assert first.tolist() == [1.0, 1.0]
    assert second.tolist() == [2.0, 1.0]
