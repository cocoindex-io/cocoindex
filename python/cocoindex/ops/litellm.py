"""LiteLLM integration for text embeddings.

This module provides a wrapper around the LiteLLM library
that implements VectorSchemaProvider for easy integration with CocoIndex connectors.
"""

from __future__ import annotations

__all__ = ["LiteLLMEmbedder", "litellm"]

import asyncio as _asyncio
from typing import Any as _Any

import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex.resources import schema as _schema

import litellm as litellm


class LiteLLMEmbedder(_schema.VectorSchemaProvider):
    """Wrapper for LiteLLM embedding models that implements VectorSchemaProvider.

    This class provides an async interface to LiteLLM's embedding API
    and automatically provides vector schema information for CocoIndex connectors.

    Args:
        model: LiteLLM model name (e.g., ``"text-embedding-ada-002"``,
            ``"vertex_ai/textembedding-gecko"``).
        **kwargs: Additional keyword arguments passed through to every
            ``litellm.aembedding`` call (e.g., ``api_key``, ``api_base``,
            ``dimensions``).

    Example:
        >>> from cocoindex.ops.litellm import LiteLLMEmbedder
        >>> embedder = LiteLLMEmbedder("text-embedding-ada-002")
        >>>
        >>> # Get vector schema for database column definitions
        >>> schema = await embedder.__coco_vector_schema__()
        >>> print(f"Embedding dimension: {schema.size}, dtype: {schema.dtype}")
        >>>
        >>> # Embed text
        >>> embedding = await embedder.embed("Hello, world!")
        >>> print(f"Shape: {embedding.shape}, dtype: {embedding.dtype}")
    """

    def __init__(self, model: str, **kwargs: _Any) -> None:
        """Initialize the LiteLLM embedder."""
        self._model = model
        self._kwargs = kwargs
        self._dim: int | None = None
        self._lock: _asyncio.Lock | None = None

    def _get_lock(self) -> _asyncio.Lock:
        """Get or create the asyncio lock (must be called from async context)."""
        if self._lock is None:
            self._lock = _asyncio.Lock()
        return self._lock

    async def _get_dim(self) -> int:
        """Get embedding dimension, caching the result.

        Embeds a short test text to determine the dimension since LiteLLM
        does not provide a dedicated API for querying embedding dimensions.
        """
        if self._dim is not None:
            return self._dim
        async with self._get_lock():
            if self._dim is not None:
                return self._dim
            response = await litellm.aembedding(
                model=self._model,
                input=["hello"],
                **self._kwargs,
            )
            embedding = response.data[0]["embedding"]
            self._dim = len(embedding)
            return self._dim

    @coco.fn.as_async(batching=True, memo=True, max_batch_size=64, version=1)
    async def embed(self, texts: list[str]) -> list[_NDArray[_np.float32]]:
        """Embed texts to embedding vectors.

        With batching enabled, this function receives a batch of texts and returns
        a batch of embeddings. The external signature is still single text -> single embedding.

        Args:
            texts: List of text strings to embed (batched input).

        Returns:
            List of numpy arrays, each of shape (dim,) containing an embedding vector.
        """
        response = await litellm.aembedding(
            model=self._model,
            input=texts,
            **self._kwargs,
        )
        return [
            _np.array(item["embedding"], dtype=_np.float32) for item in response.data
        ]

    @coco.fn.as_async(memo=True)
    async def __coco_vector_schema__(self) -> _schema.VectorSchema:
        """Return vector schema information for this model.

        Returns:
            VectorSchema with the embedding dimension and dtype.
        """
        dim = await self._get_dim()
        return _schema.VectorSchema(dtype=_np.dtype(_np.float32), size=dim)

    def __coco_memo_key__(self) -> object:
        return (self._model, self._kwargs)
