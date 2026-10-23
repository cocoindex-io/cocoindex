"""One connection per event loop, for clients bound to the loop that created them.

Internal: shared by the built-in graph connectors' ``ConnectionFactory``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Generic

from typing_extensions import TypeVar

_T = TypeVar("_T")

_logger = logging.getLogger(__name__)


@dataclass
class _LoopEntry(Generic[_T]):
    """The connection of one event loop, and the lock that opens it once."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    conn: _T | None = None


class PerLoopConnections(Generic[_T]):
    """Connections keyed by event loop, for a client that is bound to the loop
    it was created on (an async driver, a WebSocket).

    ``open`` runs once per event loop, on that loop's first :meth:`acquire`;
    every later :meth:`acquire` on the loop returns the same connection, also
    when callers run concurrently. ``close`` runs on :meth:`close`, for the
    running loop's connection only. ``owner`` names the factory in the warning
    about connections of other loops left open.

    :meth:`close` closes the running loop's connection under the caller's
    feet: call it once every user of the factory is done (in a lifespan,
    after ``yield``), not while components are still applying.

    An entry lives until its loop is closed: the connection references the
    loop, so the loop cannot be collected first. :meth:`acquire` and
    :meth:`close` drop the entries of closed loops; their connection cannot be
    closed on its loop anymore, so it is dropped unclosed, without a warning.
    """

    def __init__(
        self,
        open: Callable[[], Awaitable[_T]],
        close: Callable[[_T], Awaitable[None]],
        *,
        owner: str,
    ) -> None:
        self._open = open
        self._close = close
        self._owner = owner
        self._lock = threading.Lock()
        self._entries: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, _LoopEntry[_T]
        ] = weakref.WeakKeyDictionary()

    def _drop_closed_loops(self) -> None:
        """Under ``self._lock``."""
        for closed_loop in [loop for loop in self._entries if loop.is_closed()]:
            del self._entries[closed_loop]

    async def acquire(self) -> _T:
        """The running event loop's connection, opened on its first call."""
        loop = asyncio.get_running_loop()
        with self._lock:
            self._drop_closed_loops()
            entry = self._entries.get(loop)
            if entry is None:
                entry = self._entries[loop] = _LoopEntry()
        # Callers run concurrently: open once, not once per caller.
        async with entry.lock:
            if entry.conn is None:
                entry.conn = await self._open()
            return entry.conn

    async def close(self) -> None:
        """Close the running event loop's connection, if any; a later
        :meth:`acquire` opens a new one. Connections of other loops can't be
        closed here: they are left open, with a warning."""
        loop = asyncio.get_running_loop()
        with self._lock:
            self._drop_closed_loops()
            entry = self._entries.get(loop)
            # A held lock is an open in progress: that connection stays too.
            num_others = sum(
                1
                for other_loop, other in self._entries.items()
                if other_loop is not loop
                and (other.conn is not None or other.lock.locked())
            )
        if num_others:
            _logger.warning(
                "%s.close(): %d connection(s) opened on other event loops were "
                "not closed; close() must run on the event loop that opened them",
                self._owner,
                num_others,
            )
        if entry is None:
            return
        # Waits for an open in progress, so its connection is closed too.
        async with entry.lock:
            conn, entry.conn = entry.conn, None
        if conn is not None:
            await self._close(conn)
