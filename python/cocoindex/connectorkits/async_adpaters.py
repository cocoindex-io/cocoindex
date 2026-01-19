"""Utilities for adapting synchronous APIs to async interfaces."""

from __future__ import annotations

import asyncio
import queue
import threading
from typing import (
    AsyncIterator,
    Callable,
    Iterator,
)
from typing_extensions import TypeVar

T = TypeVar("T")

DEFAULT_QUEUE_SIZE = 1024


async def sync_to_async_iter(
    sync_iter_fn: Callable[[], Iterator[T]],
    *,
    max_queue_size: int = DEFAULT_QUEUE_SIZE,
) -> AsyncIterator[T]:
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
    q: queue.Queue[tuple[bool, T | Exception]] = queue.Queue(maxsize=max_queue_size)
    stop_event = threading.Event()

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

    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=producer, daemon=True)
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
        except queue.Empty:
            pass
        thread.join(timeout=1.0)


__all__ = ["sync_to_async_iter"]
