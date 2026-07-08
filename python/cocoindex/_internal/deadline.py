from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from datetime import timedelta
from typing import TypeAlias, TypeVar

from . import core


DeadlineExceededError = core.DeadlineExceededError

DeadlineContext = (
    core.DeadlineContext
)  # re-export of the core type; one name everywhere

_current_deadline: ContextVar[DeadlineContext] = ContextVar(
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


def deadline_for_engine() -> DeadlineContext:
    """The only sanctioned way to obtain the deadline for a core engine call.

    Engine entry points that perform a Rust-side deadline check take the
    deadline as a required argument and must be passed this value. Entry
    points that isolate (mount, mount_each, live) take no argument at all.
    """
    return _current_deadline.get()


@contextlib.contextmanager
def restore(deadline: DeadlineContext) -> Iterator[None]:
    """Temporarily restore a previously captured deadline context."""
    token = _current_deadline.set(deadline)
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
    """Return the remaining deadline time, or None when no deadline is active."""
    return _current_deadline.get().remaining_secs()


def has_deadline() -> bool:
    """Return whether a deadline is currently active."""
    return _current_deadline.get().has_deadline()


def exponential_backoff(
    initial: float = 1.0,
    multiplier: float = 2.0,
    max_delay: float = 30.0,
    jitter: float = 0.0,
) -> Callable[[int], float]:
    """Return a backoff strategy: attempt index (0-based) to delay seconds.

    The returned strategy is STATEFUL (it advances its own delay on each
    call, avoiding a power computation per attempt) — construct a fresh one
    per retry operation. ``jitter`` scales each delay by a random factor in
    ``[1 - jitter, 1 + jitter]``; the default of 0 keeps schedules exact.
    """
    delay = initial / multiplier

    def next_delay(_attempt: int) -> float:
        nonlocal delay
        delay = min(delay * multiplier, max_delay)
        if jitter:
            return delay * random.uniform(1.0 - jitter, 1.0 + jitter)
        return delay

    return next_delay


def _should_retry(retry_on: RetryOn, error: Exception) -> bool:
    if isinstance(retry_on, tuple):
        return isinstance(error, retry_on)
    return retry_on(error)


# The module-level context manager, aliased so retry_transient's `timeout`
# parameter doesn't shadow it.
_timeout_scope = timeout


async def retry_transient(
    fn: Callable[[], Awaitable[_RetryResultT]],
    *,
    retry_on: RetryOn,
    max_attempts: int | None = None,
    timeout: timedelta | None = None,
    backoff: Callable[[int], float] | None = None,
    bound_attempt: bool = False,
    operation_name: str | None = None,
) -> _RetryResultT:
    """Retry ``fn`` on transient failures. The only retry loop in the tree.

    Exhaustion semantics:

    - ``max_attempts`` (the only policy wall): when exhausted, the last
      transient error is re-raised. Default ``None`` = no cap.
    - Time limits are deadlines, and there is exactly one time concept:
      ``timeout`` is sugar for running the loop inside a
      ``coco.timeout(...)`` scope, merging with any ambient deadline by
      min-nesting. Expiry raises ``DeadlineExceededError``.

    Deadline enforcement is best-effort-or-better: no attempt starts past
    the deadline, no result is accepted past it (checked after each attempt
    completes), backoff sleeps never exceed the remaining time — and with
    ``bound_attempt=True``, an in-flight attempt is additionally cancelled
    at the effective deadline via ``asyncio.wait_for`` and surfaces as
    ``DeadlineExceededError``.

    With neither ``max_attempts`` nor a deadline, retries are unbounded;
    this helper is internal and every call site sets at least one limit.
    Cancellation, ``KeyboardInterrupt``, and ``SystemExit`` always propagate
    untouched.
    """
    if max_attempts is not None and max_attempts < 1:
        raise ValueError("retry_transient requires max_attempts >= 1")
    if timeout is not None and timeout <= timedelta(0):
        raise ValueError("retry_transient requires a positive timeout")
    if backoff is None:
        backoff = exponential_backoff()

    scope = _timeout_scope(timeout) if timeout is not None else contextlib.nullcontext()
    with scope:
        # Exception, not BaseException: doubles as a type-level guard — if
        # the except clause below ever widens back to BaseException, this
        # assignment becomes a mypy error.
        last_error: Exception | None = None
        attempt_index = 0
        while True:
            # Never start an attempt past the deadline. A remaining time of
            # exactly zero counts as expired, so a sleep clipped to the
            # deadline cannot spin at the boundary.
            check_cancellation()
            remaining = remaining_seconds()
            if remaining is not None and remaining <= 0:
                raise DeadlineExceededError("CocoIndex timeout deadline exceeded")

            try:
                if bound_attempt and remaining is not None:
                    try:
                        result = await asyncio.wait_for(fn(), timeout=remaining)
                    except TimeoutError as timeout_error:
                        # Translate only a wait_for cancellation at the
                        # deadline; a TimeoutError raised by fn itself
                        # before the deadline re-raises as-is.
                        remaining_now = remaining_seconds()
                        if remaining_now is not None and remaining_now <= 0:
                            raise DeadlineExceededError(
                                "CocoIndex timeout deadline exceeded"
                            ) from timeout_error
                        raise
                else:
                    result = await fn()
            except Exception as error:
                # Deliberately Exception, not BaseException: cancellation,
                # KeyboardInterrupt, and SystemExit always propagate
                # untouched, regardless of how broad retry_on is.
                if not _should_retry(retry_on, error):
                    raise
                last_error = error
            else:
                # The helper is itself a checkpoint: a result completing
                # past the deadline raises instead of returning, same as
                # the coco.fn post-return checkpoint. Raising in `else`
                # keeps the checkpoint out of retry classification.
                check_cancellation()
                return result

            attempt_index += 1
            if max_attempts is not None and attempt_index >= max_attempts:
                raise last_error

            delay = backoff(attempt_index - 1)
            remaining = remaining_seconds()
            sleep_for = max(0.0, delay if remaining is None else min(delay, remaining))
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
