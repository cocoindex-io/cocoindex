"""
This module provides a standalone execution runtime for executing coroutines in a thread-safe
manner.
"""

import threading
import asyncio
from typing import Any, Coroutine


class _ExecutionContext:
    _lock: threading.Lock
    _event_loop: asyncio.AbstractEventLoop | None = None

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop for the cocoindex library."""
        with self._lock:
            if self._event_loop is None:
                self._event_loop = asyncio.new_event_loop()
                threading.Thread(
                    target=self._event_loop.run_forever, daemon=True
                ).start()
            return self._event_loop

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run a coroutine in the event loop, blocking until it finishes. Return its result."""
        return asyncio.run_coroutine_threadsafe(coro, self.event_loop).result()


execution_context = _ExecutionContext()
