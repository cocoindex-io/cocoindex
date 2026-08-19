"""Sentence Transformers integration for text embeddings.

This module provides a wrapper around the sentence-transformers library
that implements VectorSchemaProvider for easy integration with CocoIndex connectors.
"""

from __future__ import annotations

__all__ = ["SentenceTransformerEmbedder"]

import gc as _gc
import os as _os
import sys as _sys
import threading as _threading
import typing as _typing
from typing import Any as _Any

import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex._internal import runner as _runner
from cocoindex.resources import schema as _schema

if _typing.TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


def _is_oom_error(error: BaseException) -> bool:
    """Whether ``error`` is an accelerator/host out-of-memory failure.

    Matches by message rather than type so it covers ``torch.OutOfMemoryError``
    ("CUDA out of memory..."), MPS ("MPS backend out of memory..."), and older
    torch versions that raise plain ``RuntimeError`` — without importing torch.
    """
    return isinstance(error, MemoryError) or "out of memory" in str(error).lower()


def _is_mps_device(device: str | None) -> bool:
    return device == "mps" or (device is None and _sys.platform == "darwin")


def _clear_mps_allocator_cache() -> None:
    """Reclaim unused PyTorch MPS memory when driver allocations reach low watermark.

    Performs garbage collection and flushes the MPS cache only under memory pressure
    to avoid per-batch synchronization overhead on Apple Silicon.
    """

    try:
        import torch

        if not torch.backends.mps.is_available():
            return
        try:
            configured_ratios = (
                float(
                    _os.environ.get(
                        "PYTORCH_MPS_LOW_WATERMARK_RATIO",
                        _runner._MPS_LOW_WATERMARK_RATIO,
                    )
                ),
                float(
                    _os.environ.get(
                        "PYTORCH_MPS_HIGH_WATERMARK_RATIO",
                        _runner._MPS_HIGH_WATERMARK_RATIO,
                    )
                ),
            )
            positive_ratios = [ratio for ratio in configured_ratios if ratio > 0]
            if not positive_ratios:
                return
            cleanup_ratio = min(positive_ratios)
            if (
                torch.mps.driver_allocated_memory()
                < torch.mps.recommended_max_memory() * cleanup_ratio
            ):
                return
        except Exception:
            # Fall back to cleanup if the PyTorch version lacks memory telemetry APIs.
            pass
        torch.mps.synchronize()
        _gc.collect()
        torch.mps.empty_cache()
        torch.mps.synchronize()
    except Exception:  # pragma: no cover - defensive
        pass


def _empty_accelerator_cache() -> None:
    """Flush GPU/MPS memory caches prior to retrying a failed batch after an OOM.

    Releases cached allocations to reduce fragmentation before batch splitting.
    Swallows exceptions defensively to preserve the original exception context.
    """
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        elif torch.backends.mps.is_available():
            # Clear MPS cache before splitting and retrying the batch.
            torch.mps.empty_cache()
    except Exception:  # pragma: no cover - defensive
        pass


class SentenceTransformerEmbedder(_schema.VectorSchemaProvider):
    """Wrapper for SentenceTransformer models that implements VectorSchemaProvider.

    This class provides a thread-safe interface to SentenceTransformer models
    and automatically provides vector schema information for CocoIndex connectors.

    Args:
        model_name_or_path: Name of a pre-trained model from HuggingFace or path
            to a local model directory.
        device: Device to load the model on (e.g., ``"cuda"``, ``"mps"``, ``"cpu"``).
            Defaults to ``None`` to let SentenceTransformer auto-detect.
        trust_remote_code: Whether to allow loading models with custom code
            from the HuggingFace Hub (e.g., Jina models with custom pooling).

    Example:
        >>> from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
        >>> embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
        >>>
        >>> # Get vector schema for database column definitions
        >>> schema = await embedder.__coco_vector_schema__()
        >>> print(f"Embedding dimension: {schema.size}, dtype: {schema.dtype}")
        >>>
        >>> # Embed text
        >>> embedding = await embedder.embed("Hello, world!")
        >>> print(f"Shape: {embedding.shape}, dtype: {embedding.dtype}")
    """

    def __init__(
        self,
        model_name_or_path: str,
        *,
        device: str | None = None,
        trust_remote_code: bool = False,
    ) -> None:
        """Initialize the SentenceTransformer embedder."""
        self._model_name_or_path = model_name_or_path
        self._device = device
        self._trust_remote_code = trust_remote_code
        self._model: SentenceTransformer | None = None
        self._lock = _threading.Lock()
        if self._uses_mps():
            _runner._configure_mps_allocator_defaults()

    def __getstate__(self) -> dict[str, _Any]:
        return {
            "model_name_or_path": self._model_name_or_path,
            "device": self._device,
            "trust_remote_code": self._trust_remote_code,
        }

    def __setstate__(self, state: dict[str, _Any]) -> None:
        self._model_name_or_path = state["model_name_or_path"]
        self._device = state["device"]
        self._trust_remote_code = state["trust_remote_code"]
        self._model = None
        self._lock = _threading.Lock()

    def _get_model(self) -> SentenceTransformer:
        """Lazy-load the model (thread-safe)."""
        if self._model is None:
            with self._lock:
                if self._model is None:
                    if self._uses_mps():
                        _runner._configure_mps_allocator_defaults()
                    from sentence_transformers import SentenceTransformer

                    self._model = SentenceTransformer(
                        self._model_name_or_path,
                        device=self._device,
                        trust_remote_code=self._trust_remote_code,
                    )
        return self._model

    def _uses_mps(self) -> bool:
        return _is_mps_device(self._device)

    def _encode(
        self,
        texts: list[str],
        prompt_name: str | None = None,
        normalize_embeddings: bool = True,
    ) -> list[_NDArray[_np.float32]]:
        model = self._get_model()
        try:
            embeddings: _NDArray[_np.float32] = model.encode(
                texts,
                prompt_name=prompt_name,
                convert_to_numpy=True,
                normalize_embeddings=normalize_embeddings,
                show_progress_bar=False,
            )  # type: ignore[assignment]
        except Exception as e:
            # Memory consumption scales with batch_size * padded_seq_len.
            # Signal the engine to split the batch and retry on OOM errors.
            if _is_oom_error(e):
                _empty_accelerator_cache()
                raise coco.RetryWithSmallerBatch() from e
            raise
        return list(embeddings)

    @coco.fn.as_async(batching=True, runner=coco.GPU, max_batch_size=64)
    def _embed(
        self,
        texts: list[str],
        prompt_name: str | None = None,
        normalize_embeddings: bool = True,
    ) -> list[_NDArray[_np.float32]]:
        """Execute non-MPS embedding batches on the default GPU runner."""

        return self._encode(texts, prompt_name, normalize_embeddings)

    @coco.fn.as_async(batching=True, runner=_runner._MPS_GPU, max_batch_size=64)
    def _embed_mps(
        self,
        texts: list[str],
        prompt_name: str | None = None,
        normalize_embeddings: bool = True,
    ) -> list[_NDArray[_np.float32]]:
        """Execute MPS embedding batches inside an isolated worker with memory pressure checks."""

        try:
            return self._encode(texts, prompt_name, normalize_embeddings)
        finally:
            _clear_mps_allocator_cache()

    @coco.fn(memo=True, version=1, logic_tracking="self")
    async def embed(
        self,
        text: str,
        prompt_name: str | None = None,
        normalize_embeddings: bool = True,
    ) -> _NDArray[_np.float32]:
        """Embed a single text into a float32 vector.

        Concurrent calls with the same ``prompt_name`` and ``normalize_embeddings``
        are automatically batched by the underlying :meth:`_embed` decorator.

        Args:
            text: Text string to embed.
            prompt_name: Prompt name for instruction following models that use
                different prompts for queries vs documents.
            normalize_embeddings: Whether to normalize embeddings to unit length.

        Returns:
            Numpy array of shape ``(dim,)`` containing the embedding vector.
        """
        scheduled_embed = self._embed_mps if self._uses_mps() else self._embed
        result: _NDArray[_np.float32] = await scheduled_embed(  # type: ignore[arg-type]
            text, prompt_name, normalize_embeddings
        )
        return result

    async def __coco_vector_schema__(self) -> _schema.VectorSchema:
        """Return vector schema information for this model.

        Returns:
            VectorSchema with the embedding dimension and dtype.

        Raises:
            RuntimeError: If the model's embedding dimension cannot be determined.
        """
        dim = await self.dimension()
        return _schema.VectorSchema(dtype=_np.dtype(_np.float32), size=dim)

    @coco.fn.as_async(runner=coco.GPU)
    def _dimension(self) -> int:
        model = self._get_model()
        dim = model.get_sentence_embedding_dimension()
        if dim is None:
            raise RuntimeError(
                f"Embedding dimension is unknown for model {self._model_name_or_path}."
            )
        return int(dim)

    @coco.fn.as_async(runner=_runner._MPS_GPU)
    def _dimension_mps(self) -> int:
        try:
            model = self._get_model()
            dim = model.get_sentence_embedding_dimension()
            if dim is None:
                raise RuntimeError(
                    f"Embedding dimension is unknown for model {self._model_name_or_path}."
                )
            return int(dim)
        finally:
            _clear_mps_allocator_cache()

    @coco.fn(memo=True)
    async def dimension(self) -> int:
        """Return the embedding dimension for this model.

        Returns:
            The embedding dimension as an integer.

        Raises:
            RuntimeError: If the model's embedding dimension cannot be determined.
        """
        scheduled_dimension = (
            self._dimension_mps if self._uses_mps() else self._dimension
        )
        return _typing.cast(int, await scheduled_dimension())

    def __coco_memo_key__(self) -> object:
        return (self._model_name_or_path, self._device, self._trust_remote_code)
