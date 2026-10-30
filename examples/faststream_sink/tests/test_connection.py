from __future__ import annotations

import asyncio

import pytest

from examples.faststream_sink.connection import drain_cleanup


async def test_cleanup_is_owned_and_drained_under_repeated_cancellation() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    calls = 0

    async def cleanup() -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        completed.set()

    task = asyncio.create_task(drain_cleanup(cleanup()))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert not completed.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == 1
    assert completed.is_set()


async def test_cleanup_error_propagates() -> None:
    async def cleanup() -> None:
        raise OSError("cleanup failed")

    with pytest.raises(OSError, match="cleanup failed"):
        await drain_cleanup(cleanup())
