from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from datetime import timedelta
from typing import Final, TypeAlias, TypeGuard, TypeVar, final


class DeadlineExceededError(TimeoutError):
    """Raised when the current CocoIndex timeout deadline has passed."""


DeadlineSnapshot = float | None


@final
class NoDeadlinePropagation:
    """Sentinel for boundaries that must leave deadline context untouched."""

    __slots__ = ()


NO_DEADLINE_PROPAGATION: Final = NoDeadlinePropagation()
DeadlinePropagation: TypeAlias = DeadlineSnapshot | NoDeadlinePropagation

_current_deadline: ContextVar[DeadlineSnapshot] = ContextVar(
    "coco_deadline", default=None
)
_monotonic_now: Callable[[], float] = time.monotonic
_sleep_for: Callable[[float], Awaitable[None]] = asyncio.sleep

_RetryResultT = TypeVar("_RetryResultT")


@contextlib.contextmanager
def timeout(duration: timedelta) -> Iterator[None]:
    """Apply a cooperative timeout deadline to CocoIndex checkpoints."""
    if not isinstance(duration, timedelta):
        raise TypeError("timeout() requires a datetime.timedelta")

    new_deadline = _monotonic_now() + duration.total_seconds()
    current = _current_deadline.get()
    effective = new_deadline if current is None else min(current, new_deadline)
    token = _current_deadline.set(effective)
    try:
        yield
    finally:
        _current_deadline.reset(token)


def check_deadline() -> None:
    """Raise DeadlineExceededError if the current deadline has passed."""
    deadline = _current_deadline.get()
    if deadline is not None and _monotonic_now() > deadline:
        raise DeadlineExceededError("CocoIndex timeout deadline exceeded")


def capture() -> DeadlineSnapshot:
    """Capture the current deadline for explicit cross-task propagation."""
    return _current_deadline.get()


def is_deadline_snapshot(
    propagation: DeadlinePropagation,
) -> TypeGuard[DeadlineSnapshot]:
    """Return true when a propagation value is an explicit deadline snapshot."""
    return not isinstance(propagation, NoDeadlinePropagation)


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
    with restore(None):
        yield


def remaining_seconds() -> float | None:
    """Return the remaining deadline budget, or None when no deadline is active."""
    deadline = _current_deadline.get()
    if deadline is None:
        return None
    return max(0.0, deadline - _monotonic_now())


def has_deadline() -> bool:
    """Return whether a deadline is currently active."""
    return _current_deadline.get() is not None


def _check_retry_budget() -> None:
    check_deadline()
    remaining = remaining_seconds()
    if remaining is not None and remaining <= 0:
        raise DeadlineExceededError("CocoIndex timeout deadline exceeded")


async def _sleep_until_deadline(delay_seconds: float) -> None:
    """Sleep without exceeding the current deadline."""
    _check_retry_budget()
    if delay_seconds <= 0:
        await _sleep_for(0)
        _check_retry_budget()
        return

    remaining = remaining_seconds()
    sleep_for = delay_seconds if remaining is None else min(delay_seconds, remaining)
    await _sleep_for(sleep_for)
    _check_retry_budget()


async def retry_until_deadline(
    attempt: Callable[[], Awaitable[_RetryResultT | None]],
    *,
    backoff_seconds: float,
) -> _RetryResultT:
    """Retry an async operation until it succeeds or the current deadline passes.

    The attempt returns ``None`` to request another try. This helper is only used
    when a deadline exists; callers without a deadline keep their existing retry
    policy.
    """
    while True:
        _check_retry_budget()
        result = await attempt()
        if result is not None:
            return result
        await _sleep_until_deadline(backoff_seconds)
