from __future__ import annotations

import asyncio
from collections.abc import Iterator
import threading

import pytest

from cocoindex.connectorkits.async_adapters import sync_to_async_iter


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
