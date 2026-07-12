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
import logging as _logging
from datetime import timedelta as _timedelta
from collections.abc import Awaitable as _Awaitable
from collections.abc import Callable as _Callable
from typing import Any as _Any
from typing import TypeVar as _TypeVar

import litellm as litellm

from cocoindex._internal import deadline as _deadline
import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex.resources import file as _file
from cocoindex.resources import schema as _schema

_logger = _logging.getLogger(__name__)

_T = _TypeVar("_T")
_EMBEDDING_RETRY_TIMEOUT_SECONDS = 10 * 60
_EMBEDDING_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
_EMBEDDING_RETRY_MAX_BACKOFF_SECONDS = 30.0
_RETRYABLE_HTTP_STATUS_CODES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_RETRYABLE_TRANSPORT_ERROR_CLASS_NAMES = frozenset(
    {
        "ClientConnectorError",
        "ConnectError",
        "ConnectTimeout",
        "PoolTimeout",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "ServerDisconnectedError",
        "WriteError",
        "WriteTimeout",
    }
)


def _message_indicates_non_retryable_credentials_error(message: str) -> bool:
    normalized = message.lower()
    if any(
        fragment in normalized
        for fragment in (
            "missing credentials",
            "no api key",
            "invalid api key",
            "unauthorized",
        )
    ):
        return True
    if "api key" not in normalized and "api_key" not in normalized:
        return False
    return any(
        fragment in normalized
        for fragment in ("missing", "must be set", "not set", "required", "invalid")
    )


def _litellm_exception_classes(*names: str) -> tuple[type[BaseException], ...]:
    classes: list[type[BaseException]] = []
    for name in names:
        obj = getattr(litellm, name, None)
        if isinstance(obj, type) and issubclass(obj, BaseException):
            classes.append(obj)
    return tuple(classes)


_RETRYABLE_LITELLM_EXCEPTION_CLASSES = _litellm_exception_classes(
    "APIConnectionError",
    "BadGatewayError",
    "InternalServerError",
    "RateLimitError",
    "ServiceUnavailableError",
    "Timeout",
)

# Errors about who we are or what we asked for (credentials, permissions,
# unknown model, exhausted budget) — batch composition can't affect them, so
# splitting the batch can't help.
_GLOBAL_LITELLM_EXCEPTION_CLASSES = _litellm_exception_classes(
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "BudgetExceededError",
)


def _is_global_litellm_error(error: BaseException) -> bool:
    return isinstance(
        error, _GLOBAL_LITELLM_EXCEPTION_CLASSES
    ) or _message_indicates_non_retryable_credentials_error(str(error))


def _http_status_code(error: BaseException) -> int | None:
    for attr in ("status_code", "exception_status_code"):
        value = getattr(error, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    if isinstance(value, int):
        return value
    return None


def _is_transport_error(error: BaseException) -> bool:
    if isinstance(error, (TimeoutError, ConnectionError)):
        return True
    error_type = type(error)
    module = error_type.__module__
    return (
        module.startswith(("aiohttp.", "httpcore.", "httpx."))
        and error_type.__name__ in _RETRYABLE_TRANSPORT_ERROR_CLASS_NAMES
    )


def _is_retryable_litellm_error(error: BaseException) -> bool:
    if _message_indicates_non_retryable_credentials_error(str(error)):
        return False
    status_code = _http_status_code(error)
    if status_code is not None:
        return status_code in _RETRYABLE_HTTP_STATUS_CODES or 500 <= status_code < 600
    return isinstance(
        error, _RETRYABLE_LITELLM_EXCEPTION_CLASSES
    ) or _is_transport_error(error)


async def _retry_litellm_call(
    operation: _Callable[[], _Awaitable[_T]],
    operation_name: str,
) -> _T:
    # Time is the brake here (no attempt cap): retry transient failures
    # inside a 10-minute deadline scope, with each in-flight attempt
    # bounded to the remaining time. An ambient coco.timeout() merges by
    # min-nesting and can only stop retries sooner. Exhaustion raises
    # DeadlineExceededError (one time concept: the deadline system).
    return await _deadline.retry_transient(
        operation,
        retry_on=_is_retryable_litellm_error,
        timeout=_timedelta(seconds=_EMBEDDING_RETRY_TIMEOUT_SECONDS),
        backoff=_deadline.exponential_backoff(
            initial=_EMBEDDING_RETRY_INITIAL_BACKOFF_SECONDS,
            multiplier=2.0,
            max_delay=_EMBEDDING_RETRY_MAX_BACKOFF_SECONDS,
        ),
        bound_attempt=True,
        operation_name=operation_name,
    )


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

    def _build_call_kwargs(self, **extra: _Any) -> dict[str, _Any]:
        # voyage/ and bedrock/ reject `encoding_format="float"` (voyage requires
        # base64); leave them with their native defaults. For everyone else,
        # ask for the float-decoded payload and let litellm drop unsupported
        # params on a per-call basis.
        kwargs = dict(self._kwargs)
        kwargs.update(extra)
        if not self._model.startswith(("voyage/", "bedrock/")):
            kwargs.setdefault("encoding_format", "float")
            kwargs.setdefault("drop_params", True)
        return kwargs

    async def _aembedding_with_retry(self, texts: list[str], **extra: _Any) -> _Any:
        async def _call() -> _Any:
            return await litellm.aembedding(
                model=self._model,
                input=texts,
                **self._build_call_kwargs(**extra),
            )

        return await _retry_litellm_call(_call, "litellm.aembedding")

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
            response = await self._aembedding_with_retry(["hello"])
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
        extra: dict[str, _Any] = {}
        if input_type is not None:
            extra["input_type"] = input_type
        try:
            response = await self._aembedding_with_retry(texts, **extra)
        except Exception as e:
            # Anything reaching here is either global (credentials/model —
            # splitting can't help) or has already exhausted its same-size
            # retry budget above. For the latter, ask the engine to halve the
            # batch and retry: smaller requests may pass where the big one
            # couldn't (a provider's token/payload cap, one rejected input,
            # or a timeout on an oversized payload). If the error is actually
            # global after all, splitting still terminates: every item fails
            # with it at size 1, at the cost of the sub-batches' retries.
            # (No batch-size check needed — at size 1 the engine unwraps the
            # signal and raises the original error.)
            if not _is_global_litellm_error(e):
                raise coco.RetryWithSmallerBatch() from e
            raise
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
    """Wrapper for LiteLLM speech-to-text transcription models.

    This class provides an async interface to LiteLLM's transcription API
    for CocoIndex ``FileLike`` inputs.

    Args:
        model: LiteLLM transcription model name (e.g., ``"whisper-1"``,
            ``"elevenlabs/scribe_v1"``).
        **kwargs: Additional keyword arguments passed through to every
            ``litellm.atranscription`` call (e.g., ``api_key``, ``api_base``,
            ``language``, ``extra_body``).

    Example:
        >>> from cocoindex.ops.litellm import LiteLLMTranscriber
        >>> transcriber = LiteLLMTranscriber("whisper-1")
        >>> transcript = await transcriber.transcribe(audio_file)
        >>> print(transcript)
    """

    def __init__(self, model: str, **kwargs: _Any) -> None:
        """Initialize the LiteLLM transcriber."""
        self._model = model
        self._kwargs = kwargs

    @coco.fn(memo=True, version=1, logic_tracking="self")
    async def transcribe(self, file: _file.FileLike[_Any], **kwargs: _Any) -> str:
        """Transcribe audio content from a ``FileLike`` object into text.

        ``FileLike`` provides async read methods. The content is read into a
        binary file-like object before calling LiteLLM.

        Args:
            file: ``FileLike`` object containing audio data.
            **kwargs: Additional keyword arguments passed through to this
                ``litellm.atranscription`` call.

        Returns:
            The transcribed text.

        Note:
            Per-call keyword arguments override defaults provided when the
            transcriber was initialized.
        """
        audio = _io.BytesIO(await file.read())
        audio.name = file.file_path.name
        call_kwargs = dict(self._kwargs)
        call_kwargs.update(kwargs)
        response = await litellm.atranscription(
            model=self._model,
            file=audio,
            **call_kwargs,
        )
        return response.text  # type: ignore[no-any-return]

    def __coco_memo_key__(self) -> object:
        return (self._model, self._kwargs)
