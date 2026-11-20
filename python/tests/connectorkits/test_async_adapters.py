from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Generator, Iterator
import concurrent.futures
import contextvars
import subprocess
import sys
import textwrap
import threading
import time
from typing import cast
from unittest.mock import patch

import pytest

from cocoindex.connectorkits.async_adapters import (
    async_to_sync_iter,
    sync_to_async_iter,
)


@pytest.mark.parametrize("finish", ["exhaust", "error", "close", "cancel"])
def test_executor_submissions_do_not_inherit_consumer_context(finish: str) -> None:
    marker = contextvars.ContextVar("adapter_consumer", default="empty")
    submissions: list[str] = []
    producer_contexts: list[str] = []
    release = threading.Event()
    started = threading.Event()

    def source() -> Iterator[int]:
        producer_contexts.append(marker.get())
        started.set()
        if finish == "cancel":
            release.wait()
        yield 1
        if finish == "error":
            raise ValueError("source failed")

    async def consume() -> None:
        loop = asyncio.get_running_loop()
        # A dedicated executor ensures this test covers submissions that start
        # a worker, not just submissions to an already-warm default pool.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            loop.set_default_executor(executor)
            original_submit = executor.submit

            def record_submit(
                fn: Callable[..., object], /, *args: object, **kwargs: object
            ) -> concurrent.futures.Future[object]:
                submissions.append(marker.get())
                return original_submit(fn, *args, **kwargs)

            token = marker.set("consumer")
            try:
                with patch.object(executor, "submit", side_effect=record_submit):
                    iterator = sync_to_async_iter(source, max_queue_size=1)
                    if finish == "cancel":
                        pending = asyncio.create_task(anext(iterator))
                        while not started.is_set():
                            await asyncio.sleep(0)
                        pending.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await pending
                    elif finish == "close":
                        assert await anext(iterator) == 1
                        await iterator.aclose()
                    elif finish == "error":
                        with pytest.raises(ValueError, match="source failed"):
                            async for _ in iterator:
                                pass
                    else:
                        assert [item async for item in iterator] == [1]
                    assert marker.get() == "consumer"
            finally:
                marker.reset(token)
                release.set()

    asyncio.run(consume())
    assert producer_contexts == ["consumer"]
    # Covers both blocking queue reads and the join in the finally block.
    assert len(submissions) >= 2
    assert submissions == ["empty"] * len(submissions)


def test_cancelled_read_does_not_hang_asyncio_shutdown() -> None:
    # A subprocess bounds the failure: an abandoned executor worker would keep
    # asyncio.run() alive indefinitely, even after the task has been cancelled.
    script = textwrap.dedent(
        """
        import asyncio
        import threading
        from cocoindex.connectorkits.async_adapters import sync_to_async_iter

        started = threading.Event()
        release = threading.Event()

        def source():
            started.set()
            release.wait()
            yield 1

        async def main():
            iterator = sync_to_async_iter(source, max_queue_size=1)
            pending = asyncio.create_task(anext(iterator))
            while not started.is_set():
                await asyncio.sleep(0.01)
            pending.cancel()
            try:
                await pending
            except asyncio.CancelledError:
                pass

        asyncio.run(main())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("source_is_async", [False, True])
def test_producer_error_after_close_does_not_leak_thread(source_is_async: bool) -> None:
    release = threading.Event()
    producers: list[threading.Thread] = []

    def source() -> Iterator[int]:
        producers.append(threading.current_thread())
        yield 1
        release.wait()
        raise ValueError("source failed after close")

    async def async_source() -> AsyncIterator[int]:
        for item in source():
            yield item

    async def consume_first() -> None:
        iterator = sync_to_async_iter(source, max_queue_size=1)
        assert await anext(iterator) == 1
        await iterator.aclose()

    try:
        if source_is_async:
            iterator = cast(
                Generator[int, None, None],
                async_to_sync_iter(async_source, max_queue_size=1),
            )
            assert next(iterator) == 1
            iterator.close()
        else:
            asyncio.run(consume_first())
    finally:
        release.set()
        for producer in producers:
            producer.join(timeout=2)

    assert producers
    assert all(not producer.is_alive() for producer in producers)


@pytest.mark.parametrize("source_is_async", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_bounded_queue_preserves_values_and_errors(
    source_is_async: bool, fail: bool
) -> None:
    received: list[int] = []

    def source() -> Iterator[int]:
        yield from range(5)
        if fail:
            raise ValueError("source failed")

    async def async_source() -> AsyncIterator[int]:
        for item in source():
            yield item

    async def consume() -> None:
        async for item in sync_to_async_iter(source, max_queue_size=1):
            received.append(item)
            if item == 0:
                # Longer than the producer's put timeout, forcing a retry.
                await asyncio.sleep(0.3)

    def run() -> None:
        if source_is_async:
            for item in async_to_sync_iter(async_source, max_queue_size=1):
                received.append(item)
                if item == 0:
                    time.sleep(0.3)
        else:
            asyncio.run(consume())

    if fail:
        with pytest.raises(ValueError, match="source failed"):
            run()
    else:
        run()
    assert received == list(range(5))


@pytest.mark.asyncio
async def test_sync_to_async_iter_cleanup_does_not_block_event_loop() -> None:
    producer_release = threading.Event()
    loop_progressed = asyncio.Event()

    def sync_iter() -> Iterator[int]:
        yield 1
        producer_release.wait()
        yield 2

    async_iter = sync_to_async_iter(sync_iter)
    assert await anext(async_iter) == 1

    async def release_producer_after_loop_turn() -> None:
        await asyncio.sleep(0)
        loop_progressed.set()
        producer_release.set()

    release_task = asyncio.create_task(release_producer_after_loop_turn())
    try:
        await async_iter.aclose()
        assert loop_progressed.is_set()
    finally:
        producer_release.set()
        await release_task
