"""
Thread-safe execution runtime for running coroutines from sync code.
"""

from __future__ import annotations

import asyncio
import threading
import warnings
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


class _ExecutionContext:
    _lock: threading.Lock
    _event_loop: asyncio.AbstractEventLoop | None = None

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """A long-lived background event loop owned by CocoIndex."""
        with self._lock:
            if self._event_loop is None or self._event_loop.is_closed():
                loop = asyncio.new_event_loop()
                self._event_loop = loop

                def _runner(loop: asyncio.AbstractEventLoop) -> None:
                    asyncio.set_event_loop(loop)
                    loop.run_forever()

                threading.Thread(target=_runner, args=(loop,), daemon=True).start()
            return self._event_loop

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """
        Run a coroutine on the CocoIndex background loop, blocking until it finishes.
        """
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        loop = self.event_loop
        if running_loop is not None:
            if running_loop is loop:
                raise RuntimeError(
                    "CocoIndex sync API was called from inside CocoIndex's async context. "
                    "Use the async variant of this method instead."
                )
            warnings.warn(
                "CocoIndex sync API was called inside an existing event loop. "
                "This may block other tasks. Prefer the async method.",
                RuntimeWarning,
                stacklevel=2,
            )

        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return fut.result()
        except KeyboardInterrupt:
            fut.cancel()
            raise


execution_context = _ExecutionContext()
