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
import contextlib as _contextlib
import io as _io
import logging as _logging
from contextlib import AbstractAsyncContextManager as _AbstractAsyncContextManager
from datetime import timedelta as _timedelta
from collections.abc import Awaitable as _Awaitable
from collections.abc import Callable as _Callable
from typing import Any as _Any
from typing import TypeVar as _TypeVar
from typing import cast as _cast

import litellm as litellm

from cocoindex._internal import deadline as _deadline
import numpy as _np
from numpy.typing import NDArray as _NDArray

import cocoindex as coco
from cocoindex.resources import file as _file
from cocoindex.resources import schema as _schema

_logger = _logging.getLogger(__name__)

_T = _TypeVar("_T")
_DEFAULT_TIMEOUT = _timedelta(minutes=10)
_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
_RETRY_MAX_BACKOFF_SECONDS = 30.0
_RETRYABLE_HTTP_STATUS_CODES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
# HTTP client packages whose exception classes the two name sets below refer to.
_TRANSPORT_ERROR_PACKAGES = frozenset({"aiohttp", "httpcore", "httpx"})
# Fast transport failures — a refused or reset connection, a broken read or
# write — worth another try. Timeouts are deliberately absent from this set;
# they live in `_TIMEOUT_TRANSPORT_ERROR_CLASS_NAMES` and are terminal.
_RETRYABLE_TRANSPORT_ERROR_CLASS_NAMES = frozenset(
    {
        "ClientConnectorError",
        "ConnectError",
        "ReadError",
        "RemoteProtocolError",
        "ServerDisconnectedError",
        "WriteError",
    }
)
# httpx/httpcore timeout classes, matched by name because they subclass
# neither builtin `TimeoutError` nor anything else we can key on. (aiohttp's
# timeout classes need no entry here: they subclass `TimeoutError` already.)
_TIMEOUT_TRANSPORT_ERROR_CLASS_NAMES = frozenset(
    {
        "ConnectTimeout",
        "PoolTimeout",
        "ReadTimeout",
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
)

_TIMEOUT_LITELLM_EXCEPTION_CLASSES = _litellm_exception_classes("Timeout")

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


def _is_transport_error_named(error: BaseException, names: frozenset[str]) -> bool:
    # Matched on the root package because these libraries are inconsistent
    # about where their exceptions claim to live: httpx and httpcore rewrite
    # `__module__` to the bare package name, aiohttp leaves it on the
    # defining submodule.
    error_type = type(error)
    return (
        error_type.__module__.partition(".")[0] in _TRANSPORT_ERROR_PACKAGES
        and error_type.__name__ in names
    )


def _is_timeout_error(error: BaseException) -> bool:
    """Every timeout this module can recognize, in one place.

    Timeouts are terminal for embedding — never retried, never a reason to
    split the batch. A timeout has by definition already spent its duration,
    so retrying it inside a time budget mostly re-spends what is left, and
    against an overloaded backend it amplifies the very load that caused it.
    The remedy for a genuine timeout is a longer ``LiteLLMEmbedder(timeout=...)``,
    not more requests.

    Covers CocoIndex's own deadline expiry (``DeadlineExceededError``), raw
    asyncio timeouts and aiohttp's timeouts through builtin ``TimeoutError``,
    litellm's ``Timeout``, and the httpx/httpcore timeout classes that
    subclass neither.
    """
    return (
        isinstance(error, TimeoutError)
        or isinstance(error, _TIMEOUT_LITELLM_EXCEPTION_CLASSES)
        or _is_transport_error_named(error, _TIMEOUT_TRANSPORT_ERROR_CLASS_NAMES)
    )


def _is_transport_error(error: BaseException) -> bool:
    return isinstance(error, ConnectionError) or _is_transport_error_named(
        error, _RETRYABLE_TRANSPORT_ERROR_CLASS_NAMES
    )


def _is_retryable_litellm_error(error: BaseException) -> bool:
    # Timeouts first, ahead of every other rule: each one below would
    # otherwise let a timeout back in — `litellm.Timeout` reports HTTP 408,
    # and both it and the httpx timeouts descend from retryable parents.
    if _is_timeout_error(error):
        return False
    if _message_indicates_non_retryable_credentials_error(str(error)):
        return False
    status_code = _http_status_code(error)
    if status_code is not None:
        return status_code in _RETRYABLE_HTTP_STATUS_CODES or 500 <= status_code < 600
    return isinstance(
        error, _RETRYABLE_LITELLM_EXCEPTION_CLASSES
    ) or _is_transport_error(error)


def _resolve_timeout(timeout: _timedelta | float | None) -> _timedelta:
    """Normalize a caller-supplied request bound to a positive ``timedelta``.

    Shared by both classes so ``timeout`` means one thing across the module:
    a bound on the whole request, seconds when given as a bare number.
    """
    if timeout is None:
        return _DEFAULT_TIMEOUT
    if not isinstance(timeout, _timedelta):
        # Seconds — the form litellm's own `timeout` kwarg took while it was
        # the one passing through here.
        timeout = _timedelta(seconds=timeout)
    if timeout <= _timedelta(0):
        raise ValueError(f"timeout must be positive, got {timeout!r}")
    return timeout


async def _retry_litellm_call(
    operation: _Callable[[], _Awaitable[_T]],
    operation_name: str,
    timeout: _timedelta,
) -> _T:
    # Time is the brake here (no attempt cap): retry fast transient failures
    # inside a `timeout` deadline scope, with each in-flight attempt bounded
    # to the remaining time. Timeouts are not among them — see
    # `_is_timeout_error`. An ambient coco.timeout() merges by min-nesting
    # and can only stop retries sooner. Exhaustion raises
    # DeadlineExceededError (one time concept: the deadline system).
    return await _deadline.retry_transient(
        operation,
        retry_on=_is_retryable_litellm_error,
        timeout=timeout,
        backoff=_deadline.exponential_backoff(
            initial=_RETRY_INITIAL_BACKOFF_SECONDS,
            multiplier=2.0,
            max_delay=_RETRY_MAX_BACKOFF_SECONDS,
        ),
        bound_attempt=True,
        operation_name=operation_name,
    )


def _aligned_embeddings(data: list[_Any], n: int) -> list[_NDArray[_np.float32]]:
    """Map embedding response items back to the ``n`` inputs they embed.

    Items carrying an ``index`` are placed by it; if no item carries one
    (missing or ``None``), the response is taken positionally. Mixing the two,
    or an index set that is not a permutation of ``0..n-1``, raises so a
    misordered response fails loudly instead of silently misaligning
    embeddings with their texts.
    """
    if len(data) != n:
        raise RuntimeError(
            f"litellm embedding response has {len(data)} items for {n} inputs"
        )
    out: list[_NDArray[_np.float32] | None] = [None] * n
    indexed = n > 0 and data[0].get("index") is not None
    for pos, item in enumerate(data):
        index = item.get("index")
        if (index is not None) != indexed:
            raise RuntimeError(
                "litellm embedding response mixes items with and without `index`"
            )
        if not indexed:
            index = pos
        elif type(index) is not int or not 0 <= index < n or out[index] is not None:
            raise RuntimeError(
                "litellm embedding response indices are not a permutation of "
                f"0..{n - 1}: got {[item.get('index') for item in data]}"
            )
        out[index] = _np.array(item["embedding"], dtype=_np.float32)
    return _cast(list[_NDArray[_np.float32]], out)


class LiteLLMEmbedder(_schema.VectorSchemaProvider):
    """Wrapper for LiteLLM embedding models that implements VectorSchemaProvider.

    This class provides an async interface to LiteLLM's embedding API
    and automatically provides vector schema information for CocoIndex connectors.

    Args:
        model: LiteLLM model name (e.g., ``"text-embedding-ada-002"``,
            ``"vertex_ai/textembedding-gecko"``).
        max_inflight_requests: Cap on how many requests this embedder keeps
            open against the backend at once, or ``None`` (the default) for
            no cap. Useful for self-hosted endpoints that reject bursts with
            429s. The cap is per instance.
        timeout: Bound on each embedding request, end to end: fast failures
            (429, 5xx, a refused or reset connection) are retried with
            backoff inside it, a request that times out is not retried, and
            expiry raises ``DeadlineExceededError``. A ``timedelta``, or
            seconds as a number. Defaults to 10 minutes; raise it for a slow
            endpoint, since one request carries up to 64 texts. This is the
            only clock on the request — litellm's own per-request ``timeout``
            is not forwarded.
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

    def __init__(
        self,
        model: str,
        *,
        max_inflight_requests: int | None = None,
        timeout: _timedelta | float | None = None,
        **kwargs: _Any,
    ) -> None:
        """Initialize the LiteLLM embedder."""
        if max_inflight_requests is not None and max_inflight_requests < 1:
            raise ValueError(
                "max_inflight_requests must be a positive int or None, got "
                f"{max_inflight_requests!r}"
            )
        self._model = model
        self._timeout = _resolve_timeout(timeout)
        self._kwargs = kwargs
        self._max_inflight_requests = max_inflight_requests
        self._dim: int | None = None
        self._lock: _asyncio.Lock | None = None
        self._inflight_semaphore: _asyncio.Semaphore | None = None
        self._inflight_loop: _asyncio.AbstractEventLoop | None = None

    def _get_lock(self) -> _asyncio.Lock:
        """Get or create the asyncio lock (must be called from async context)."""
        if self._lock is None:
            self._lock = _asyncio.Lock()
        return self._lock

    def _inflight_permit(self) -> _AbstractAsyncContextManager[None]:
        """A permit to hold for one in-flight request to the backend.

        The semaphore is built on first use, and rebuilt whenever the running
        loop changes. An ``asyncio.Semaphore`` binds to the loop that first
        blocks on it and raises ``RuntimeError`` if it is then awaited from
        another — and an embedder routinely outlives a loop: the documented
        usage constructs one at module scope, while each ``Environment`` gets
        a fresh loop (and, under pytest-asyncio, each test does).

        Rebinding makes the cap per loop rather than per instance. That is
        exact for the sequential case this guards (one loop replaced by the
        next, nothing left in flight on the old one). Two loops running
        *concurrently* against one embedder would get a budget each, which is
        the same way the instance-scoped cap already behaves for two
        embedders sharing a backend.
        """
        if self._max_inflight_requests is None:
            return _contextlib.nullcontext()
        loop = _asyncio.get_running_loop()
        if self._inflight_semaphore is None or self._inflight_loop is not loop:
            self._inflight_semaphore = _asyncio.Semaphore(self._max_inflight_requests)
            self._inflight_loop = loop
        return self._inflight_semaphore

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
            # The permit is scoped to one backend request, deliberately as
            # tight as possible around it:
            #
            # * Inside the retry loop, not around it — an attempt sleeping
            #   out a 30s backoff would otherwise hold a slot it isn't using.
            # * Inside the batch body, not at the batcher — the whole
            #   `RetryWithSmallerBatch` split tree runs within one batch
            #   dispatch (the split wrapper is applied before the runner
            #   reaches the batcher), so a batcher-level bound would cap
            #   top-level batches while their sub-batches fanned out freely.
            # * Never held across a split. `_run_split_async` only gathers
            #   the two halves after `await fn(inputs)` has already raised,
            #   so the parent's permit is released before the halves ask for
            #   theirs. Holding it across the split would deadlock outright
            #   at a limit of 1.
            async with self._inflight_permit():
                return await litellm.aembedding(
                    model=self._model,
                    input=texts,
                    **self._build_call_kwargs(**extra),
                )

        return await _retry_litellm_call(_call, "litellm.aembedding", self._timeout)

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
            # Two kinds of error propagate untouched. Global ones
            # (credentials/model) can't be fixed by batch composition. And a
            # timeout — including this batch exhausting its own `timeout` —
            # must not fan out: every sub-batch would take a fresh full
            # timeout, turning one expired request into a tree of them
            # against a backend that is already too slow. A longer `timeout`
            # (or lower concurrency) is the remedy.
            #
            # Anything else gets halved and retried: a smaller request may
            # pass where the big one couldn't (a provider's token/payload cap
            # or one rejected input). Splitting terminates either way — at
            # size 1 the engine unwraps the signal and raises the original
            # error, so no batch-size check is needed here.
            if _is_timeout_error(e) or _is_global_litellm_error(e):
                raise
            raise coco.RetryWithSmallerBatch() from e
        return _aligned_embeddings(response.data, len(texts))

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
        # `max_inflight_requests` and `timeout` are deliberately absent: they
        # pace and bound requests but cannot change an embedding, so tuning
        # either must not invalidate every cached vector.
        return (self._model, self._kwargs)


class LiteLLMTranscriber:
    """Wrapper for LiteLLM speech-to-text transcription models.

    This class provides an async interface to LiteLLM's transcription API
    for CocoIndex ``FileLike`` inputs.

    Args:
        model: LiteLLM transcription model name (e.g., ``"whisper-1"``,
            ``"elevenlabs/scribe_v1"``).
        timeout: Bound on each transcription request, end to end: fast
            failures (429, 5xx, a refused or reset connection) are retried
            with backoff inside it, a request that times out is not retried,
            and expiry raises ``DeadlineExceededError``. A ``timedelta``, or
            seconds as a number. Defaults to 10 minutes; raise it for long
            audio, since one request carries a whole file. This is the only
            clock on the request — litellm's own per-request ``timeout`` is
            not forwarded.
        **kwargs: Additional keyword arguments passed through to every
            ``litellm.atranscription`` call (e.g., ``api_key``, ``api_base``,
            ``language``, ``extra_body``).

    Example:
        >>> from cocoindex.ops.litellm import LiteLLMTranscriber
        >>> transcriber = LiteLLMTranscriber("whisper-1")
        >>> transcript = await transcriber.transcribe(audio_file)
        >>> print(transcript)
    """

    def __init__(
        self,
        model: str,
        *,
        timeout: _timedelta | float | None = None,
        **kwargs: _Any,
    ) -> None:
        """Initialize the LiteLLM transcriber."""
        self._model = model
        self._timeout = _resolve_timeout(timeout)
        self._kwargs = kwargs

    @coco.fn(memo=True, version=1, logic_tracking="self")
    async def transcribe(self, file: _file.FileLike[_Any], **kwargs: _Any) -> str:
        """Transcribe audio content from a ``FileLike`` object into text.

        ``FileLike`` provides async read methods. The content is read into a
        binary file-like object before calling LiteLLM.

        Fast failures (429, 5xx, a dropped connection) are retried with
        backoff inside the instance's ``timeout``; a request that times out
        is terminal — see :func:`_is_timeout_error`.

        Args:
            file: ``FileLike`` object containing audio data.
            **kwargs: Additional keyword arguments passed through to this
                ``litellm.atranscription`` call.

        Returns:
            The transcribed text.

        Note:
            Per-call keyword arguments override defaults provided when the
            transcriber was initialized. ``timeout`` is not among them: it is
            CocoIndex's clock on the whole request rather than a provider
            argument, and is settable only on the constructor.
        """
        if "timeout" in kwargs:
            raise TypeError(
                "timeout is not a per-call argument — pass it to "
                "LiteLLMTranscriber(...) to bound every request"
            )
        content = await file.read()
        name = file.file_path.name
        call_kwargs = dict(self._kwargs)
        call_kwargs.update(kwargs)

        async def _call() -> _Any:
            # A fresh buffer per attempt: litellm reads the audio to EOF, so
            # replaying one BytesIO would upload an empty body on the retry.
            # The bytes themselves are read once, above.
            audio = _io.BytesIO(content)
            audio.name = name
            return await litellm.atranscription(
                model=self._model,
                file=audio,
                **call_kwargs,
            )

        response = await _retry_litellm_call(
            _call, "litellm.atranscription", self._timeout
        )
        return response.text  # type: ignore[no-any-return]

    def __coco_memo_key__(self) -> object:
        # `timeout` is deliberately absent: it bounds the request but cannot
        # change a transcript, so tuning it must not invalidate every cached
        # transcription.
        return (self._model, self._kwargs)
