"""Tests for SentenceTransformerEmbedder's OOM split-and-retry behavior.

The real sentence-transformers package is not needed: ``_get_model`` is
stubbed with a fake model, so these tests exercise the op's error
classification and the batching engine's RetryWithSmallerBatch path
(including the GPU runner) without loading any model.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import numpy as np
import pytest

import cocoindex as coco
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
    embedder = SentenceTransformerEmbedder("fake-model")
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
