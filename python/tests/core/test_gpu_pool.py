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
    GPUPool,
    GPURunner,
    configure_gpu_pool,
    current_gpu,
    current_gpus,
    current_gpu_fraction,
    _detect_num_gpus,
)


@pytest.fixture(autouse=True)
def _reset_gpu_pool() -> Iterator[None]:
    old = _runner_mod._default_gpu_pool
    _runner_mod._default_gpu_pool = None
    yield
    _runner_mod._default_gpu_pool = old


@pytest.mark.asyncio
async def test_acquire_returns_gpu_id() -> None:
    pool = GPUPool(num_gpus=2)
    gpu = await pool.acquire(1.0)
    assert 0 <= gpu < 2
    await pool.release(gpu, 1.0)


@pytest.mark.asyncio
async def test_acquire_different_gpus() -> None:
    pool = GPUPool(num_gpus=2)
    gpu0 = await pool.acquire(1.0)
    gpu1 = await pool.acquire(1.0)
    assert gpu0 != gpu1
    await pool.release(gpu0, 1.0)
    await pool.release(gpu1, 1.0)


@pytest.mark.asyncio
async def test_acquire_blocks_when_capacity_full() -> None:
    pool = GPUPool(num_gpus=1)
    gpu = await pool.acquire(1.0)

    task = asyncio.create_task(pool.acquire(1.0))
    await asyncio.sleep(0.02)
    assert not task.done()

    await pool.release(gpu, 1.0)
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == 0
    await pool.release(result, 1.0)


@pytest.mark.asyncio
async def test_fractional_shares_same_gpu() -> None:
    pool = GPUPool(num_gpus=1)
    gpu0 = await pool.acquire(0.5)
    gpu1 = await pool.acquire(0.5)
    assert gpu0 == gpu1

    task = asyncio.create_task(pool.acquire(0.5))
    await asyncio.sleep(0.02)
    assert not task.done()

    await pool.release(gpu0, 0.5)
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == gpu0
    await pool.release(gpu1, 0.5)
    await pool.release(result, 0.5)


@pytest.mark.asyncio
async def test_multi_gpu_all_parallel() -> None:
    pool = GPUPool(num_gpus=3)
    tasks = [asyncio.create_task(pool.acquire(1.0)) for _ in range(3)]
    results = await asyncio.gather(*tasks)
    assert len(set(results)) == 3
    for g in results:
        await pool.release(g, 1.0)


@pytest.mark.asyncio
async def test_invalid_num_gpus_raises() -> None:
    with pytest.raises(ValueError):
        GPUPool(num_gpus=0)


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


def test_detect_num_gpus_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COCOINDEX_NUM_GPUS", "4")
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    assert _detect_num_gpus() == 4


def test_detect_num_gpus_cuda_visible_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,2,3")
    assert _detect_num_gpus() == 3


def test_detect_num_gpus_cuda_visible_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_explicit_env_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COCOINDEX_NUM_GPUS", "0")
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_explicit_env_overrides_cuda_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COCOINDEX_NUM_GPUS", "2")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2,3")
    assert _detect_num_gpus() == 2


def test_detect_num_gpus_cuda_visible_single_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_cuda_visible_with_whitespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0, 1 , 2")
    assert _detect_num_gpus() == 3


def test_detect_num_gpus_nvidia_smi_returns_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def _mock_run(*args: Any, **kwargs: Any) -> Any:
        class _Completed:
            returncode = 0
            stdout = "8\n"

        return _Completed()

    monkeypatch.setattr(subprocess, "run", _mock_run)
    assert _detect_num_gpus() == 8


def test_detect_num_gpus_nvidia_smi_empty_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def _mock_run(*args: Any, **kwargs: Any) -> Any:
        class _Completed:
            returncode = 0
            stdout = ""

        return _Completed()

    monkeypatch.setattr(subprocess, "run", _mock_run)
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_nvidia_smi_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def _mock_run(*args: Any, **kwargs: Any) -> Any:
        class _Completed:
            returncode = 1
            stdout = ""

        return _Completed()

    monkeypatch.setattr(subprocess, "run", _mock_run)
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_nvidia_smi_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def _mock_run(*args: Any, **kwargs: Any) -> Any:
        raise FileNotFoundError("nvidia-smi not found")

    monkeypatch.setattr(subprocess, "run", _mock_run)
    assert _detect_num_gpus() == 1


def test_detect_num_gpus_all_missing_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COCOINDEX_NUM_GPUS", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def _mock_run(*args: Any, **kwargs: Any) -> Any:
        class _Completed:
            returncode = 1
            stdout = ""

        return _Completed()

    monkeypatch.setattr(subprocess, "run", _mock_run)
    assert _detect_num_gpus() == 1
