from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from datetime import timedelta
from typing import TypeVar

from . import core


DeadlineExceededError = core.DeadlineExceededError

DeadlineSnapshot = core.DeadlineContext

_current_deadline: ContextVar[DeadlineSnapshot] = ContextVar(
    "coco_deadline", default=core.deadline_none()
)

_RetryResultT = TypeVar("_RetryResultT")


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


def check_deadline() -> None:
    """Raise DeadlineExceededError if the current deadline has passed."""
    _current_deadline.get().check()


def capture() -> DeadlineSnapshot:
    """Capture the current deadline for explicit cross-task propagation."""
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


def _check_retry_budget() -> None:
    check_deadline()
    remaining = remaining_seconds()
    if remaining is not None and remaining <= 0:
        raise DeadlineExceededError("CocoIndex timeout deadline exceeded")


async def _sleep_until_deadline(delay_seconds: float) -> None:
    """Sleep without exceeding the current deadline."""
    _check_retry_budget()
    if delay_seconds <= 0:
        await asyncio.sleep(0)
        _check_retry_budget()
        return

    remaining = remaining_seconds()
    sleep_for = delay_seconds if remaining is None else min(delay_seconds, remaining)
    await asyncio.sleep(sleep_for)
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
