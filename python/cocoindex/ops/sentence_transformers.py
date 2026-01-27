"""Sentence Transformers integration for text embeddings.

This module provides a wrapper around the sentence-transformers library
that implements VectorSchemaProvider for easy integration with CocoIndex connectors.
"""

from __future__ import annotations

__all__ = ["SentenceTransformerEmbedder"]

import asyncio

import threading as _threading
import typing as _typing

import numpy as _np
from numpy.typing import NDArray as _NDArray

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
        >>> schema = embedder.__coco_vector_schema__()
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

    def _get_model(self) -> SentenceTransformer:
        """Lazy-load the model (thread-safe)."""
        if self._model is None:
            with self._lock:
                # Double-check pattern
                if self._model is None:
                    from sentence_transformers import SentenceTransformer

                    self._model = SentenceTransformer(
                        self._model_name_or_path,
                    )
        return self._model

    def embed(self, text: str) -> _NDArray[_np.float32]:
        """Embed text to an embedding vector.

        Args:
            text: The text string to embed.

        Returns:
            Numpy array of shape (dim,) containing the embedding vector.
        """
        model = self._get_model()

        # Use the lock to prevent concurrent GPU access
        with self._lock:
            embeddings: _NDArray[_np.float32] = model.encode(
                [text],
                convert_to_numpy=True,
                normalize_embeddings=self._normalize_embeddings,
            )  # type: ignore[assignment]

        result: _NDArray[_np.float32] = embeddings[0]  # type: ignore[assignment]
        return result

    async def embed_async(self, text: str) -> _NDArray[_np.float32]:
        """Embed text to an embedding vector asynchronously.

        Args:
            text: The text string to embed.

        Returns:
            Numpy array of shape (dim,) containing the embedding vector.
        """
        return await asyncio.to_thread(self.embed, text)

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
