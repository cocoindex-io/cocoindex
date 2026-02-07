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
import cocoindex.asyncio as coco_aio
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
        normalize_embeddings: Whether to normalize embeddings to unit length.
            Defaults to True for compatibility with cosine similarity.

    Example:
        >>> from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
        >>> embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
        >>>
        >>> # Get vector schema for database column definitions
        >>> schema = await embedder.__coco_vector_schema__()
        >>> print(f"Embedding dimension: {schema.size}, dtype: {schema.dtype}")
        >>>
        >>> # Embed text to embedding
        >>> embedding = embedder.embed("Hello, world!")
        >>> print(f"Shape: {embedding.shape}, dtype: {embedding.dtype}")
    """

    def __init__(
        self,
        model_name_or_path: str,
        *,
        normalize_embeddings: bool = True,
    ) -> None:
        """Initialize the SentenceTransformer embedder."""
        self._model_name_or_path = model_name_or_path
        self._normalize_embeddings = normalize_embeddings
        self._model: SentenceTransformer | None = None
        self._lock = _threading.Lock()

    def __getstate__(self) -> dict[str, _Any]:
        return {
            "model_name_or_path": self._model_name_or_path,
            "normalize_embeddings": self._normalize_embeddings,
        }

    def __setstate__(self, state: dict[str, _Any]) -> None:
        self._model_name_or_path = state["model_name_or_path"]
        self._normalize_embeddings = state["normalize_embeddings"]
        self._model = None
        self._lock = _threading.Lock()

    def _get_model(self) -> SentenceTransformer:
        """Lazy-load the model (thread-safe)."""
        if self._model is None:
            with self._lock:
                # Double-check pattern
                if self._model is None:
                    from sentence_transformers import SentenceTransformer

                    self._model = SentenceTransformer(self._model_name_or_path)
        return self._model

    @coco_aio.function(batching=True, runner=coco.GPU, max_batch_size=64)
    def embed(self, texts: list[str]) -> list[_NDArray[_np.float32]]:
        """Embed texts to embedding vectors.

        With batching enabled, this function receives a batch of texts and returns
        a batch of embeddings. The external signature is still single text -> single embedding.

        Args:
            texts: List of text strings to embed (batched input).

        Returns:
            List of numpy arrays, each of shape (dim,) containing an embedding vector.
        """
        model = self._get_model()
        embeddings: _NDArray[_np.float32] = model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=self._normalize_embeddings,
        )  # type: ignore[assignment]
        return list(embeddings)

    @coco_aio.function(runner=coco.GPU)
    def __coco_vector_schema__(self) -> _schema.VectorSchema:
        """Return vector schema information for this model.

        Returns:
            VectorSchema with the embedding dimension and dtype.

        Raises:
            RuntimeError: If the model's embedding dimension cannot be determined.
        """
        model = self._get_model()
        dim = model.get_sentence_embedding_dimension()
        if dim is None:
            raise RuntimeError(
                f"Embedding dimension is unknown for model {self._model_name_or_path}."
            )
        return _schema.VectorSchema(dtype=_np.dtype(_np.float32), size=dim)

    def __coco_memo_key__(self) -> object:
        return (self._model_name_or_path, self._normalize_embeddings)
