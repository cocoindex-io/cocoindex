"""Error classes shared by tools and handles, and the one retry helper.

Three classes of failure, decided at the tool boundary (DESIGN.md §7):

- ``ToolError`` — the model's mistake. The registry turns it into a
  ``ToolResult(ok=False)`` the model reads and recovers from.
- ``TransientError`` — a failure worth retrying below the model: a connector
  hiccup, a rate limit. Code that does I/O wraps it with ``retrying`` *before*
  any side effect, so the model never sees it. One that escapes is an
  environment failure like any other.
- anything else — an environment failure. Contained at the nearest open
  session as a result; fails a closed session, whose parent then sees the
  failure as a result (``session.py``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

# The engine's retry loop and backoff live in an internal module today; the
# harness is their second consumer. Promote them to the public API when the
# harness moves to its own repository.
from cocoindex._internal.deadline import exponential_backoff, retry_transient

_T = TypeVar("_T")

_RETRY_MAX_ATTEMPTS = 5
_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
_RETRY_MAX_BACKOFF_SECONDS = 30.0


class ToolError(Exception):
    """A model-facing error: bad arguments, unknown handle, invalid reference."""


class TransientError(Exception):
    """A failure that a retry may cure. Raise it before any side effect."""


async def retrying(
    operation: Callable[[], Awaitable[_T]],
    *,
    backoff: Callable[[int], float] | None = None,
) -> _T:
    """Run ``operation``, retrying ``TransientError`` with exponential backoff.

    At most five attempts; backoff grows from one second to thirty. The
    deadline in scope (``coco.timeout``) bounds the whole loop and is never
    retried past: expiry raises ``DeadlineExceededError``. The engine's
    ``retry_transient`` is the only retry loop in the tree; this is its harness
    policy.
    """
    return await retry_transient(
        operation,
        retry_on=(TransientError,),
        max_attempts=_RETRY_MAX_ATTEMPTS,
        backoff=backoff
        or exponential_backoff(
            initial=_RETRY_INITIAL_BACKOFF_SECONDS, max_delay=_RETRY_MAX_BACKOFF_SECONDS
        ),
    )
