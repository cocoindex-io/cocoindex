"""
This module provides a standalone execution runtime for executing coroutines in a thread-safe
manner.
"""

import threading
import asyncio
import inspect
import warnings

from typing import Any, Callable, Awaitable, TypeVar, Coroutine

T = TypeVar("T")


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
                loop = asyncio.new_event_loop()
                self._event_loop = loop

                def _runner(l: asyncio.AbstractEventLoop) -> None:
                    asyncio.set_event_loop(l)
                    l.run_forever()

                threading.Thread(target=_runner, args=(loop,), daemon=True).start()
            return self._event_loop

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine in the event loop, blocking until it finishes. Return its result."""
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


def to_async_call(call: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    if isinstance(call, (staticmethod, classmethod)):
        base_call = call.__func__
    else:
        base_call = call
    if inspect.iscoroutinefunction(base_call):
        return call
    return lambda *args, **kwargs: asyncio.to_thread(lambda: call(*args, **kwargs))
