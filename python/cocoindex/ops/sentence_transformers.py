"""Sentence Transformers integration for text embeddings.

This module provides a wrapper around the sentence-transformers library
that implements VectorSchemaProvider for easy integration with CocoIndex connectors.
"""

from __future__ import annotations

__all__ = ["SentenceTransformerEmbedder"]

import threading as _threading
import typing as _typing
from typing import Any as _Any

import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex.resources import schema as _schema

if _typing.TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


class SentenceTransformerEmbedder(_schema.VectorSchemaProvider):
    """Wrapper for SentenceTransformer models that implements VectorSchemaProvider.

    This class provides a thread-safe interface to SentenceTransformer models
    and automatically provides vector schema information for CocoIndex connectors.

    Args:
        model_name_or_path: Name of a pre-trained model from HuggingFace or path
            to a local model directory.
        device: Device to load the model on (e.g., ``"cuda"``, ``"cpu"``).
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
                    from sentence_transformers import SentenceTransformer

                    self._model = SentenceTransformer(
                        self._model_name_or_path,
                        device=self._device,
                        trust_remote_code=self._trust_remote_code,
                    )
        return self._model

    @coco.fn.as_async(batching=True, runner=coco.GPU, max_batch_size=64)
    def _embed(
        self,
        texts: list[str],
        prompt_name: str | None = None,
        normalize_embeddings: bool = True,
    ) -> list[_NDArray[_np.float32]]:
        """Batched embedding. Concurrent single-text calls into :meth:`embed`
        are grouped by the ``@coco.fn.as_async(batching=True)`` decorator;
        this method is the per-batch body invoked by the decorator.

        Args:
            texts: Batch of text strings to embed (handled by the engine).
            prompt_name: Prompt name for instruction following models that use
                different prompts for queries vs documents.
            normalize_embeddings: Whether to normalize embeddings to unit length.
                Defaults to ``True`` for compatibility with cosine similarity.

        Note:
            Pass ``prompt_name`` and ``normalize_embeddings`` consistently across
            calls — mixing explicit values with defaults creates separate batchers.
        """
        model = self._get_model()
        embeddings: _NDArray[_np.float32] = model.encode(
            texts,
            prompt_name=prompt_name,
            convert_to_numpy=True,
            normalize_embeddings=normalize_embeddings,
            show_progress_bar=False,
        )  # type: ignore[assignment]
        return list(embeddings)

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
        result: _NDArray[_np.float32] = await self._embed(  # type: ignore[arg-type]
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

    @coco.fn.as_async(runner=coco.GPU, memo=True)
    def dimension(self) -> int:
        """Return the embedding dimension for this model.

        Returns:
            The embedding dimension as an integer.

        Raises:
            RuntimeError: If the model's embedding dimension cannot be determined.
        """
        model = self._get_model()
        dim = model.get_sentence_embedding_dimension()
        if dim is None:
            raise RuntimeError(
                f"Embedding dimension is unknown for model {self._model_name_or_path}."
            )
        return int(dim)

    def __coco_memo_key__(self) -> object:
        return (self._model_name_or_path, self._device, self._trust_remote_code)
