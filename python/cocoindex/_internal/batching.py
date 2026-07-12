"""Batch splitting: retry a failed batch as smaller batches.

``RetryWithSmallerBatch`` is the control-flow signal a batched function body
(``@coco.fn(batching=True)``) raises to tell the engine that the batch failed
in a way that may depend on the batch's composition — e.g. a provider's
per-request token/payload limit, or a single input the provider rejects. The
engine reacts by halving the batch and re-running each half, recursing down
to single items; at size 1 the wrapped original error becomes that item's own
failure. Sub-batches that succeed along the way keep their results, so one
bad input fails only its own caller instead of the whole batch.

The signal is always consumed by the engine — it never propagates to callers.
Raising it is safe for *any* deterministic batch failure: if the error was
actually global (would fail at any size), the recursion terminates at single
items and every caller receives the underlying error, at the bounded cost of
extra attempts (at most ``2n - 1`` for a batch of ``n``). Transient errors
(rate limits, network blips) should first be retried at the same batch size;
once that retry budget is exhausted, splitting is a reasonable last resort —
a smaller request may pass where the large one timed out.
"""

from __future__ import annotations

import asyncio
import pickle
from typing import Any, Awaitable, Callable, Coroutine

__all__ = ["RetryWithSmallerBatch"]


class RetryWithSmallerBatch(Exception):
    """Raise from a batched function body to retry the work as smaller batches.

    Use ``raise RetryWithSmallerBatch() from err`` inside the ``except`` block
    handling the batch-level error ``err``. Safe to raise at any batch size —
    no need to special-case single-item batches: when the recursion bottoms
    out at one item, the engine unwraps the signal and raises ``err`` as that
    item's failure.
    """

    # Set only when the signal is restored from a pickle round-trip (e.g.
    # crossing a subprocess-runner boundary). Plain exception pickling drops
    # ``__cause__`` — and ``concurrent.futures`` then overwrites it with its
    # remote-traceback marker on the parent side — so the cause travels in
    # regular state instead, where nothing touches it.
    _restored_cause: BaseException | None = None

    def __reduce__(self) -> tuple[Any, ...]:
        cause = self.__cause__ or self.__context__ or self._restored_cause
        if cause is not None:
            try:
                pickle.dumps(cause)
            except Exception:
                # An unpicklable cause would fail the whole transit; dropping
                # it degrades to the pre-pickling behavior (signal without
                # cause) instead.
                cause = None
        return (_restore_signal, (self.args, cause))


def _restore_signal(
    args: tuple[Any, ...], cause: BaseException | None
) -> RetryWithSmallerBatch:
    exc = RetryWithSmallerBatch(*args)
    exc._restored_cause = cause
    return exc


class BatchItemFailure:
    """Sentinel in a batch output list marking that single item's failure.

    Produced by the split drivers below when a sub-batch fails after a split;
    unwrapped (re-raised) on the caller side by ``AsyncFunction._execute``.
    Items of one failed sub-batch share one exception object — same behavior
    as multiple awaiters of a single ``asyncio.Future``.
    """

    __slots__ = ("error",)

    error: BaseException

    def __init__(self, error: BaseException) -> None:
        self.error = error


def split_cause(e: RetryWithSmallerBatch) -> BaseException:
    """The per-item error a leaf-level ``RetryWithSmallerBatch`` stands for.

    ``_restored_cause`` is checked first: after a subprocess round-trip it
    holds the true cause, while ``__cause__`` holds the executor's
    remote-traceback marker (see ``RetryWithSmallerBatch.__reduce__``).
    """
    return e._restored_cause or e.__cause__ or e.__context__ or e


def wrap_batch_fn_async(
    fn: Callable[[list[Any]], Awaitable[list[Any]]],
) -> Callable[[list[Any]], Coroutine[Any, Any, list[Any]]]:
    """Add ``RetryWithSmallerBatch`` split-and-retry around an async batch fn."""

    async def run_with_split(inputs: list[Any]) -> list[Any]:
        return await _run_split_async(fn, inputs, is_root=True)

    return run_with_split


def wrap_batch_fn_sync(
    fn: Callable[[list[Any]], list[Any]],
) -> Callable[[list[Any]], list[Any]]:
    """Add ``RetryWithSmallerBatch`` split-and-retry around a sync batch fn."""

    def run_with_split(inputs: list[Any]) -> list[Any]:
        return _run_split_sync(fn, inputs, is_root=True)

    return run_with_split


# Both drivers share this structure (see `_run_split_async` for comments):
# the split/raise happens *outside* the `except` blocks so the consumed
# `RetryWithSmallerBatch` signal doesn't get chained into the `__context__`
# of errors surfaced to callers.


async def _run_split_async(
    fn: Callable[[list[Any]], Awaitable[list[Any]]],
    inputs: list[Any],
    is_root: bool,
) -> list[Any]:
    root_leaf_error: BaseException | None = None
    try:
        return await fn(inputs)
    except RetryWithSmallerBatch as e:
        if len(inputs) > 1:
            pass  # split below
        elif is_root:
            root_leaf_error = split_cause(e)
        else:
            return [BatchItemFailure(split_cause(e))]
    except Exception as e:
        if is_root:
            # No split happened; propagate as a whole-batch failure — the
            # engine fans it out to every caller with proper per-caller
            # error replication.
            raise
        # Deterministic failure of a sub-batch after a split (e.g. the body
        # re-raised the original error for a single bad item): confine it to
        # this sub-batch's items so sibling sub-batches keep their results.
        return [BatchItemFailure(e) for _ in inputs]
    if root_leaf_error is not None:
        raise root_leaf_error
    mid = len(inputs) // 2
    first, second = await asyncio.gather(
        _run_split_async(fn, inputs[:mid], is_root=False),
        _run_split_async(fn, inputs[mid:], is_root=False),
    )
    return [*first, *second]


def _run_split_sync(
    fn: Callable[[list[Any]], list[Any]],
    inputs: list[Any],
    is_root: bool,
) -> list[Any]:
    root_leaf_error: BaseException | None = None
    try:
        return fn(inputs)
    except RetryWithSmallerBatch as e:
        if len(inputs) > 1:
            pass  # split below
        elif is_root:
            root_leaf_error = split_cause(e)
        else:
            return [BatchItemFailure(split_cause(e))]
    except Exception as e:
        if is_root:
            raise
        return [BatchItemFailure(e) for _ in inputs]
    if root_leaf_error is not None:
        raise root_leaf_error
    mid = len(inputs) // 2
    first = _run_split_sync(fn, inputs[:mid], is_root=False)
    second = _run_split_sync(fn, inputs[mid:], is_root=False)
    return [*first, *second]
