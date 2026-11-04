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

A contained failure is **recorded** in the session that absorbed it
(``record_contained_failure``): a data dependency that never validates, so
the session's memo is not reused and the tool runs again next run, as the
failure contract promises. A session that absorbed none records none.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import cocoindex as coco

# The engine's retry loop and backoff live in an internal module today; the
# harness is their second consumer. Promote them to the public API when the
# harness moves to its own repository.
from cocoindex._internal.deadline import exponential_backoff, retry_transient

from .trace import TraceSink

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


def _never_valid(key: str, prev: str) -> coco.MemoStateOutcome:
    return coco.MemoStateOutcome(state=prev)


# Keyed by where the failure was absorbed (a tool, a child key); the state is
# the error as the model saw it. Always invalid: the memo of a session that
# absorbed a failure is never reused.
CONTAINED_FAILURE = coco.DataDep(
    "coco_harness.contained-failure", validate=_never_valid
)


def record_contained_failure(sink: TraceSink, where: str, exc: BaseException) -> None:
    """Mark the current session as holding a fact about the environment, and note it in its trace."""
    coco.use_data_dep(
        CONTAINED_FAILURE, key=where, state=f"{type(exc).__name__}: {exc}"
    )
    sink.note_failure(where)
