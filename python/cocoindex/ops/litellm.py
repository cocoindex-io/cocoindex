"""LiteLLM integration for text embeddings and speech-to-text.

This module provides thin wrappers around the LiteLLM library: ``LiteLLMEmbedder``
implements ``VectorSchemaProvider`` for connector vector columns, and
``LiteLLMTranscriber`` exposes speech-to-text via LiteLLM's transcription API.
"""

from __future__ import annotations

__all__ = [
    "LiteLLMEmbedder",
    "LiteLLMTranscriber",
    "litellm",
]

import asyncio as _asyncio
import io as _io
from typing import Any as _Any

import litellm as litellm
import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex.resources import file as _file
from cocoindex.resources import schema as _schema


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
                encoding_format="float",
                **self._kwargs,
            )
            embedding = response.data[0]["embedding"]
            self._dim = len(embedding)
            return self._dim

    @coco.fn.as_async(batching=True, max_batch_size=64)  # type: ignore[arg-type]
    async def _embed(
        self,
        texts: list[str],
        input_type: str | None = None,
    ) -> list[_NDArray[_np.float32]]:
        """Batched embedding. Concurrent single-text calls into :meth:`embed`
        are grouped by the ``@coco.fn.as_async(batching=True)`` decorator;
        this method is the per-batch body invoked by the decorator.

        Args:
            texts: Batch of text strings to embed (handled by the engine).
            input_type: Input type for asymmetric embedding models (e.g.,
                Cohere's ``"search_query"`` / ``"search_document"``).

        Note:
            Pass ``input_type`` consistently across calls — mixing explicit
            values with the default creates separate batchers.
        """
        kwargs = dict(self._kwargs)
        if input_type is not None:
            kwargs["input_type"] = input_type
        response = await litellm.aembedding(
            model=self._model,
            input=texts,
            encoding_format="float",
            **kwargs,
        )
        return [
            _np.array(item["embedding"], dtype=_np.float32) for item in response.data
        ]

    @coco.fn(memo=True, version=1, logic_tracking="self")
    async def embed(
        self,
        text: str,
        input_type: str | None = None,
    ) -> _NDArray[_np.float32]:
        """Embed a single text into a float32 vector.

        Concurrent calls with the same ``input_type`` are automatically
        batched by the underlying :meth:`_embed` decorator.

        Args:
            text: Text string to embed.
            input_type: Input type for asymmetric embedding models (e.g.,
                Cohere's ``"search_query"`` / ``"search_document"``).

        Returns:
            Numpy array of shape ``(dim,)`` containing the embedding vector.
        """
        result: _NDArray[_np.float32] = await self._embed(text, input_type)  # type: ignore[arg-type]
        return result

    @coco.fn(memo=True)
    async def __coco_vector_schema__(self) -> _schema.VectorSchema:
        """Return vector schema information for this model.

        Returns:
            VectorSchema with the embedding dimension and dtype.
        """
        dim = await self._get_dim()
        return _schema.VectorSchema(dtype=_np.dtype(_np.float32), size=dim)

    def __coco_memo_key__(self) -> object:
        return (self._model, self._kwargs)


class LiteLLMTranscriber:
    def __init__(self, model: str, **kwargs: _Any) -> None:
        self._model = model
        self._kwargs = kwargs

    @coco.fn(memo=True, version=1, logic_tracking="self")
    async def transcribe(self, file: _file.FileLike[_Any], **kwargs: _Any) -> str:
        audio = _io.BytesIO(await file.read())
        audio.name = file.file_path.name
        return await _asyncio.to_thread(self._transcribe_sync, audio, kwargs)

    def _transcribe_sync(self, file: _Any, per_call_kwargs: dict[str, _Any]) -> str:
        call_kwargs = dict(self._kwargs)
        call_kwargs.update(per_call_kwargs)

        response = litellm.transcription(
            model=self._model,
            file=file,
            **call_kwargs,
        )
        return response.text  # type: ignore[no-any-return]

    def __coco_memo_key__(self) -> object:
        return (self._model, self._kwargs)
