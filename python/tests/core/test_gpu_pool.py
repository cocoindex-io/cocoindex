"""Tests for GPUPool and GPURunner multi-GPU / fractional allocation."""

from __future__ import annotations

import asyncio
import subprocess
from collections.abc import Iterator
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal import runner as _runner_mod
from cocoindex._internal.runner import (
    GPURunner,
    configure_gpu_pool,
    current_gpu,
    current_gpus,
    current_gpu_fraction,
)


@pytest.fixture(autouse=True)
def _reset_gpu_pool() -> Iterator[None]:
    old = _runner_mod._default_gpu_pool
    _runner_mod._default_gpu_pool = None
    yield
    _runner_mod._default_gpu_pool = old


@pytest.mark.asyncio
async def test_runner_sets_current_gpu_sync() -> None:
    configure_gpu_pool(2)
    seen: list[int | None] = []
    runner = GPURunner(fraction=1.0)

    def fn(x: int) -> int:
        seen.append(current_gpu())
        return x + 1

    result = await runner.run_sync_fn(fn, 5)
    assert result == 6
    assert seen[0] is not None
    assert 0 <= seen[0] < 2


@pytest.mark.asyncio
async def test_runner_sets_current_gpu_async() -> None:
    configure_gpu_pool(2)
    seen: list[int | None] = []
    runner = GPURunner(fraction=1.0)

    async def fn(x: int) -> int:
        seen.append(current_gpu())
        return x + 1

    result = await runner.run(fn, 5)
    assert result == 6
    assert seen[0] is not None


@pytest.mark.asyncio
async def test_gpu_call_factory_creates_fraction() -> None:
    base = GPURunner(fraction=1.0)
    half = base(0.5)
    assert isinstance(half, GPURunner)
    assert half._fraction == 0.5
    assert base._fraction == 1.0


@pytest.mark.asyncio
async def test_invalid_fraction_raises() -> None:
    with pytest.raises(ValueError):
        GPURunner(fraction=0.0)
    with pytest.raises(ValueError):
        GPURunner(fraction=1.5)


@pytest.mark.asyncio
async def test_parallel_runners_assign_different_gpus() -> None:
    configure_gpu_pool(2)
    runner = GPURunner(fraction=1.0)
    seen: list[int | None] = []

    async def fn(tag: int) -> int:
        g = current_gpu()
        seen.append(g)
        await asyncio.sleep(0.02)
        return tag

    results = await asyncio.gather(runner.run(fn, 100), runner.run(fn, 200))
    assert sorted(results) == [100, 200]
    assert len(set(seen)) == 2


@pytest.mark.asyncio
async def test_fractional_runners_share_gpu() -> None:
    configure_gpu_pool(1)
    runner = GPURunner(fraction=0.5)
    seen: list[int | None] = []

    async def fn(tag: int) -> int:
        seen.append(current_gpu())
        return tag

    results = await asyncio.gather(runner.run(fn, 1), runner.run(fn, 2))
    assert sorted(results) == [1, 2]
    assert all(g == seen[0] for g in seen)


@pytest.mark.asyncio
async def test_default_pool_single_gpu_serializes() -> None:
    runner = GPURunner(fraction=1.0)
    order: list[str] = []

    async def fn(tag: str) -> str:
        order.append(f"start:{tag}")
        await asyncio.sleep(0.02)
        order.append(f"end:{tag}")
        return tag

    await asyncio.gather(runner.run(fn, "a"), runner.run(fn, "b"))
    assert order.index("end:a") < order.index("start:b") or order.index(
        "end:b"
    ) < order.index("start:a")


@pytest.mark.asyncio
async def test_coco_fn_runner_multi_gpu_parallel() -> None:
    configure_gpu_pool(2)
    seen_gpus: list[int | None] = []
    seen_threads: list[int] = []

    @coco.fn.as_async(runner=coco.GPU)
    def _gpu_work(x: int) -> int:
        import time

        seen_gpus.append(coco.current_gpu())
        seen_threads.append(__import__("threading").get_ident())
        time.sleep(0.05)
        return x + 1

    results = await asyncio.gather(_gpu_work(10), _gpu_work(20))
    assert sorted(results) == [11, 21]
    assert len(set(seen_gpus)) == 2
    assert len(set(seen_threads)) == 2


@pytest.mark.asyncio
async def test_coco_fn_runner_single_gpu_serializes() -> None:
    order: list[str] = []

    @coco.fn.as_async(runner=coco.GPU)
    def _gpu_serial(x: int) -> int:
        import time

        order.append(f"start:{x}")
        time.sleep(0.02)
        order.append(f"end:{x}")
        return x

    await asyncio.gather(_gpu_serial(1), _gpu_serial(2))
    starts = [i for i, s in enumerate(order) if s.startswith("start")]
    ends = [i for i, s in enumerate(order) if s.startswith("end")]
    assert ends[0] < starts[1]


@pytest.mark.asyncio
async def test_coco_fn_runner_multi_gpu_parallel_async() -> None:
    configure_gpu_pool(2)
    seen_gpus: list[int | None] = []

    @coco.fn.as_async(runner=coco.GPU)
    async def _gpu_work_async(x: int) -> int:
        seen_gpus.append(coco.current_gpu())
        await asyncio.sleep(0.05)
        return x + 1

    results = await asyncio.gather(_gpu_work_async(10), _gpu_work_async(20))
    assert sorted(results) == [11, 21]
    assert len(set(seen_gpus)) == 2
    assert all(g is not None for g in seen_gpus)


@pytest.mark.asyncio
async def test_coco_fn_fractional_gpu_shares_single_gpu() -> None:
    configure_gpu_pool(1)
    seen_gpus: list[int | None] = []
    started: list[int] = []
    finished: list[int] = []

    @coco.fn.as_async(runner=coco.GPU(0.5))
    async def _half_gpu(x: int) -> int:
        seen_gpus.append(coco.current_gpu())
        started.append(x)
        await asyncio.sleep(0.05)
        finished.append(x)
        return x

    results = await asyncio.gather(_half_gpu(1), _half_gpu(2))
    assert sorted(results) == [1, 2]
    assert all(g == 0 for g in seen_gpus)
    assert len(started) == 2
    assert len(finished) == 2


@pytest.mark.asyncio
async def test_coco_fn_fractional_gpu_blocked_when_full() -> None:
    configure_gpu_pool(1)
    in_flight = 0
    max_in_flight = 0

    @coco.fn.as_async(runner=coco.GPU(0.5))
    async def _half_gpu(x: int) -> int:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1
        return x

    results = await asyncio.gather(_half_gpu(1), _half_gpu(2), _half_gpu(3))
    assert sorted(results) == [1, 2, 3]
    assert max_in_flight == 2


@pytest.mark.asyncio
async def test_runner_current_gpus_and_fraction_sync() -> None:
    configure_gpu_pool(2)
    runner = GPURunner(fraction=0.5)

    def fn(x: int) -> int:
        assert current_gpus() == [current_gpu()]
        assert current_gpu_fraction() == 0.5
        return x + 1

    result = await runner.run_sync_fn(fn, 5)
    assert result == 6


@pytest.mark.asyncio
async def test_runner_current_gpus_and_fraction_async() -> None:
    configure_gpu_pool(2)
    runner = GPURunner(fraction=0.5)

    async def fn(x: int) -> int:
        assert current_gpus() == [current_gpu()]
        assert current_gpu_fraction() == 0.5
        return x + 1

    result = await runner.run(fn, 5)
    assert result == 6


@pytest.mark.asyncio
async def test_coco_fn_current_gpus_and_fraction() -> None:
    configure_gpu_pool(2)
    seen: list[tuple[list[int], float | None]] = []

    @coco.fn.as_async(runner=coco.GPU(0.5))
    def _gpu_work(x: int) -> int:
        seen.append((coco.current_gpus(), coco.current_gpu_fraction()))
        return x + 1

    results = await asyncio.gather(_gpu_work(10), _gpu_work(20))
    assert sorted(results) == [11, 21]
    assert len(seen) == 2
    for gpus, fraction in seen:
        assert len(gpus) == 1
        assert 0 <= gpus[0] < 2
        assert fraction == 0.5

