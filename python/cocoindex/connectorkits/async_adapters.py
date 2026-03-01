"""Utilities for adapting between synchronous and asynchronous interfaces."""

from __future__ import annotations

__all__ = ["async_to_sync_iter", "sync_to_async_iter"]

import asyncio as _asyncio
import queue as _queue
import threading as _threading
from typing import (
    AsyncIterator,
    Callable,
    Iterator,
)
from typing_extensions import TypeVar as _TypeVar

_T = _TypeVar("_T")

DEFAULT_QUEUE_SIZE = 1024


async def sync_to_async_iter(
    sync_iter_fn: Callable[[], Iterator[_T]],
    *,
    max_queue_size: int = DEFAULT_QUEUE_SIZE,
) -> AsyncIterator[_T]:
    """
    Adapt a synchronous iterator function to an asynchronous iterator.

    This function takes a callable that returns a synchronous iterator and
    converts it to an async iterator. The sync iteration runs in a separate
    thread to avoid blocking the event loop.

    Args:
        sync_iter_fn: A callable that returns a synchronous iterator (e.g., a
            generator function or lambda). Takes no arguments.
        max_queue_size: Maximum number of items to buffer in the queue between
            the producer thread and async consumer. Defaults to 1024.

    Yields:
        Values produced by the synchronous iterator.

    Raises:
        Any exception raised by the synchronous iterator is re-raised in the
        async context.

    Example:
        >>> def sync_generator(start: int, end: int):
        ...     for i in range(start, end):
        ...         yield i
        ...
        >>> async def main():
        ...     async for value in sync_to_async_iter(lambda: sync_generator(0, 5)):
        ...         print(value)
    """
    # Queue to communicate values/exceptions from sync thread to async consumer.
    # Each item is (is_done_or_error, value_or_exception).
    q: _queue.Queue[tuple[bool, _T | Exception]] = _queue.Queue(maxsize=max_queue_size)
    stop_event = _threading.Event()

    def producer() -> None:
        try:
            for item in sync_iter_fn():
                if stop_event.is_set():
                    break
                q.put((False, item))
        except Exception as e:  # pylint: disable=broad-except
            q.put((True, e))
        finally:
            q.put((True, StopIteration()))

    loop = _asyncio.get_running_loop()
    thread = _threading.Thread(target=producer, daemon=True)
    thread.start()

    try:
        while True:
            # Wait for items from the queue without blocking the event loop
            is_done_or_error, value = await loop.run_in_executor(None, q.get)
            if is_done_or_error:
                if isinstance(value, StopIteration):
                    break
                raise value  # type: ignore[misc]
            yield value  # type: ignore[misc]
    finally:
        # Signal the producer to stop if consumer exits early
        stop_event.set()
        # Drain the queue to unblock producer if it's blocked on put()
        try:
            while True:
                q.get_nowait()
        except _queue.Empty:
            pass
        thread.join(timeout=1.0)


def async_to_sync_iter(
    async_iter_fn: Callable[[], AsyncIterator[_T]],
    *,
    max_queue_size: int = DEFAULT_QUEUE_SIZE,
) -> Iterator[_T]:
    """
    Adapt an asynchronous iterator function to a synchronous iterator.

    This function takes a callable that returns an async iterator and
    converts it to a sync iterator. The async iteration runs in a separate
    thread with its own event loop.

    Args:
        async_iter_fn: A callable that returns an async iterator.
            Takes no arguments.
        max_queue_size: Maximum number of items to buffer in the queue between
            the producer thread and sync consumer. Defaults to 1024.

    Yields:
        Values produced by the async iterator.

    Raises:
        RuntimeError: If called from within a running event loop.
        Any exception raised by the async iterator is re-raised.

    Example:
        >>> async def async_generator(start: int, end: int):
        ...     for i in range(start, end):
        ...         yield i
        ...
        >>> for value in async_to_sync_iter(lambda: async_generator(0, 5)):
        ...     print(value)
    """
    try:
        _asyncio.get_running_loop()
    except RuntimeError:
        pass  # No running loop, which is what we want
    else:
        raise RuntimeError(
            "async_to_sync_iter must not be called from a running event loop"
        )

    q: _queue.Queue[tuple[bool, _T | Exception]] = _queue.Queue(maxsize=max_queue_size)
    stop_event = _threading.Event()

    def producer() -> None:
        async def run_async() -> None:
            try:
                async for item in async_iter_fn():
                    if stop_event.is_set():
                        break
                    q.put((False, item))
            except Exception as e:  # pylint: disable=broad-except
                q.put((True, e))
            finally:
                q.put((True, StopIteration()))

        _asyncio.run(run_async())

    thread = _threading.Thread(target=producer, daemon=True)
    thread.start()

    try:
        while True:
            is_done_or_error, value = q.get()
            if is_done_or_error:
                if isinstance(value, StopIteration):
                    break
                raise value  # type: ignore[misc]
            yield value  # type: ignore[misc]
    finally:
        # Signal the producer to stop if consumer exits early
        stop_event.set()
        # Drain the queue to unblock producer if it's blocked on put()
        try:
            while True:
                q.get_nowait()
        except _queue.Empty:
            pass
        thread.join(timeout=1.0)
