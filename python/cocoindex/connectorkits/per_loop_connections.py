"""One connection per event loop, for clients bound to the loop that created them."""

from __future__ import annotations

__all__ = ["PerLoopConnections"]

import asyncio as _asyncio
import logging as _logging
import threading as _threading
import weakref as _weakref
from collections.abc import Awaitable as _Awaitable
from collections.abc import Callable as _Callable
from dataclasses import dataclass as _dataclass
from dataclasses import field as _field
from typing import Generic as _Generic
from typing_extensions import TypeVar as _TypeVar

_T = _TypeVar("_T")

_logger = _logging.getLogger(__name__)


@_dataclass
class _LoopEntry(_Generic[_T]):
    """The connection of one event loop, and the lock that opens it once."""

    lock: _asyncio.Lock = _field(default_factory=_asyncio.Lock)
    conn: _T | None = None


class PerLoopConnections(_Generic[_T]):
    """Connections keyed by event loop, for a client that is bound to the loop
    it was created on (an async driver, a WebSocket).

    ``open`` runs once per event loop, on that loop's first :meth:`acquire`;
    every later :meth:`acquire` on the loop returns the same connection, also
    when callers run concurrently. ``close`` runs on :meth:`close`, for the
    running loop's connection only. ``owner`` names the factory in the warning
    about connections of other loops left open.

    Entries are held weakly by loop. An open connection references its loop,
    so an entry is only freed once nothing else references the loop either.
    """

    def __init__(
        self,
        open: _Callable[[], _Awaitable[_T]],
        close: _Callable[[_T], _Awaitable[None]],
        *,
        owner: str,
    ) -> None:
        self._open = open
        self._close = close
        self._owner = owner
        self._lock = _threading.Lock()
        self._entries: _weakref.WeakKeyDictionary[
            _asyncio.AbstractEventLoop, _LoopEntry[_T]
        ] = _weakref.WeakKeyDictionary()

    async def acquire(self) -> _T:
        """The running event loop's connection, opened on its first call."""
        loop = _asyncio.get_running_loop()
        with self._lock:
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
        loop = _asyncio.get_running_loop()
        with self._lock:
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
