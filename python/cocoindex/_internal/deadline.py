from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from datetime import timedelta
from typing import TypeAlias, TypeVar

from . import core


DeadlineExceededError = core.DeadlineExceededError

DeadlineSnapshot = core.DeadlineContext

_current_deadline: ContextVar[DeadlineSnapshot] = ContextVar(
    "coco_deadline", default=core.deadline_none()
)

_logger = logging.getLogger(__name__)

_RetryResultT = TypeVar("_RetryResultT")

# Which failures are safe to retry: an exception-type tuple (isinstance
# semantics, like an `except` clause) or a predicate for classifications
# that inspect status codes or messages.
RetryOn: TypeAlias = tuple[type[Exception], ...] | Callable[[Exception], bool]


@contextlib.contextmanager
def timeout(duration: timedelta) -> Iterator[None]:
    """Apply a cooperative timeout deadline to CocoIndex checkpoints."""
    if not isinstance(duration, timedelta):
        raise TypeError("timeout() requires a datetime.timedelta")

    token = _current_deadline.set(
        _current_deadline.get().with_timeout(duration.total_seconds())
    )
    try:
        yield
    finally:
        _current_deadline.reset(token)


def check_cancellation() -> None:
    """Raise if the current work has been asked to stop.

    Deadline expiry is the first cancellation source (raising
    ``DeadlineExceededError``); future sources (e.g. a batched call whose
    callers have all been cancelled) will surface through the same
    checkpoint.
    """
    _current_deadline.get().check()


def deadline_for_engine() -> DeadlineSnapshot:
    """The only sanctioned way to obtain the deadline for a core engine call.

    Engine entry points that perform a Rust-side deadline check take the
    deadline as a required argument and must be passed this value. Entry
    points that isolate (mount, mount_each, live) take no argument at all.
    """
    return _current_deadline.get()


@contextlib.contextmanager
def restore(snapshot: DeadlineSnapshot) -> Iterator[None]:
    """Temporarily restore a previously captured deadline snapshot."""
    token = _current_deadline.set(snapshot)
    try:
        yield
    finally:
        _current_deadline.reset(token)


@contextlib.contextmanager
def without_deadline() -> Iterator[None]:
    """Temporarily clear any active deadline."""
    with restore(core.deadline_none()):
        yield


def remaining_seconds() -> float | None:
    """Return the remaining deadline budget, or None when no deadline is active."""
    return _current_deadline.get().remaining_secs()


def has_deadline() -> bool:
    """Return whether a deadline is currently active."""
    return _current_deadline.get().has_deadline()


def exponential_backoff(
    initial: float = 1.0,
    multiplier: float = 2.0,
    max_delay: float = 30.0,
) -> Callable[[int], float]:
    """Return a backoff strategy: attempt index (0-based) to delay seconds."""
    return lambda attempt: min(initial * multiplier**attempt, max_delay)


def _should_retry(retry_on: RetryOn, error: Exception) -> bool:
    if isinstance(retry_on, tuple):
        return isinstance(error, retry_on)
    return retry_on(error)


async def retry_transient(
    fn: Callable[[], Awaitable[_RetryResultT]],
    *,
    retry_on: RetryOn,
    max_attempts: int | None = 4,
    budget: timedelta | None = None,
    backoff: Callable[[int], float] | None = None,
    bound_attempt: bool = False,
    operation_name: str | None = None,
) -> _RetryResultT:
    """Retry ``fn`` on transient failures. The only retry loop in the tree.

    Two kinds of wall, with different exhaustion semantics:

    - Policy walls (``max_attempts``, ``budget``): when exhausted, the last
      transient error is re-raised. These belong to the call site.
    - The ambient deadline (a ``coco.timeout(...)`` scope transferred from
      the caller): raises ``DeadlineExceededError``. It fires only if the
      user actually set one, and can only stop retries sooner.

    No attempt starts past any wall, no result is accepted past the ambient
    deadline (checked after each attempt completes), and backoff sleeps
    never exceed the tightest remaining wall. ``bound_attempt=True``
    additionally cancels an in-flight attempt at the remaining POLICY budget
    via ``asyncio.wait_for`` (local policy over the caller's own coroutine).
    The ambient deadline is deliberately never used to cancel an in-flight
    attempt: CocoIndex timeouts are cooperative, so an attempt always runs
    to completion and the deadline is enforced at the checkpoints around it.
    Cancellation, ``KeyboardInterrupt``, and ``SystemExit`` always propagate
    untouched.

    ``max_attempts=None`` means no attempt cap and requires ``budget``.
    """
    if max_attempts is None and budget is None:
        raise ValueError("retry_transient(max_attempts=None) requires a budget")
    if max_attempts is not None and max_attempts < 1:
        raise ValueError("retry_transient requires max_attempts >= 1")
    if budget is not None and budget <= timedelta(0):
        raise ValueError("retry_transient requires a positive budget")
    if backoff is None:
        backoff = exponential_backoff()

    budget_ctx = (
        None
        if budget is None
        else core.deadline_none().with_timeout(budget.total_seconds())
    )

    def _budget_expired() -> bool:
        if budget_ctx is None:
            return False
        return (budget_ctx.remaining_secs() or 0.0) <= 0.0

    def _walls_remaining() -> float | None:
        """Tightest remaining time across the budget and ambient deadline."""
        remains = [
            r
            for r in (
                remaining_seconds(),
                budget_ctx.remaining_secs() if budget_ctx is not None else None,
            )
            if r is not None
        ]
        return min(remains) if remains else None

    # Exception, not BaseException: doubles as a type-level guard — if the
    # except clause below ever widens back to BaseException, this assignment
    # becomes a mypy error.
    last_error: Exception | None = None
    attempt_index = 0
    while True:
        # Never start an attempt past a wall. The ambient deadline is the
        # user's clock and wins the exception when both are expired. A
        # remaining budget of exactly zero counts as expired, so a sleep
        # clipped to the wall cannot spin at the boundary.
        check_cancellation()
        ambient_remaining = remaining_seconds()
        if ambient_remaining is not None and ambient_remaining <= 0:
            raise DeadlineExceededError("CocoIndex timeout deadline exceeded")
        if last_error is not None and _budget_expired():
            raise last_error

        try:
            if bound_attempt and budget_ctx is not None:
                # Bound only by the policy budget, never the ambient
                # deadline: hard-cancelling at the user's deadline would
                # break the cooperative contract and surface TimeoutError
                # instead of DeadlineExceededError.
                bound = budget_ctx.remaining_secs()
                result = (
                    await asyncio.wait_for(fn(), timeout=bound)
                    if bound is not None
                    else await fn()
                )
            else:
                result = await fn()
        except Exception as error:
            # Deliberately Exception, not BaseException: cancellation,
            # KeyboardInterrupt, and SystemExit always propagate untouched,
            # regardless of how broad the retry_on classification is.
            if not _should_retry(retry_on, error):
                raise
            last_error = error
        else:
            # The helper is itself a cooperative checkpoint: a result that
            # completed past the ambient deadline raises instead of
            # returning, same as the coco.fn post-return checkpoint. The
            # attempt was never cancelled; its completion is simply not
            # accepted past the user's clock. Raising in `else` keeps the
            # checkpoint out of retry classification.
            check_cancellation()
            return result

        attempt_index += 1
        if max_attempts is not None and attempt_index >= max_attempts:
            raise last_error
        if _budget_expired():
            raise last_error

        delay = backoff(attempt_index - 1)
        wall = _walls_remaining()
        sleep_for = max(0.0, delay if wall is None else min(delay, wall))
        if operation_name is not None:
            _logger.warning(
                "%s failed with transient error on attempt %d; retrying in %.1fs: %s",
                operation_name,
                attempt_index,
                sleep_for,
                last_error,
            )
        # Late attribute lookup so test fixtures that patch asyncio.sleep
        # (to record sleeps and drive the virtual clock) take effect.
        await asyncio.sleep(sleep_for)
