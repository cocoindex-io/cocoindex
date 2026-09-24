"""Tests for GPU subprocess mode: the GPU assignment reaches the child process."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures.process import BrokenProcessPool

import pytest

from cocoindex._internal import runner as _runner_mod
from cocoindex._internal.runner import GPURunner, configure_gpu_pool

# Each test starts `spawn` interpreters that import the Rust extension, which the
# project-wide 30s budget does not cover on a cold runner.
pytestmark = pytest.mark.timeout(90)


@pytest.fixture(autouse=True)
def _subprocess_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run GPU work in subprocesses, from a fresh pool, and clean up after."""
    monkeypatch.setenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS", "1")
    monkeypatch.setattr(_runner_mod, "_default_gpu_pool", None)
    monkeypatch.setattr(_runner_mod, "_pool", None)
    try:
        yield
    finally:
        pool = _runner_mod._pool
        if pool is not None:
            # `wait=False`: joining workers has no timeout, and a wedged one
            # would hang teardown until pytest-timeout kills the process.
            pool.shutdown(wait=False, cancel_futures=True)


def _probe() -> tuple[int | None, float | None, int]:
    """Report the GPU context and pid visible inside the subprocess."""
    import cocoindex as coco

    return coco.current_gpu(), coco.current_gpu_fraction(), os.getpid()


async def _aprobe() -> tuple[int | None, float | None, int]:
    """Async probe: goes through `GPURunner.run` and the child's `asyncio.run`."""
    import cocoindex as coco

    return coco.current_gpu(), coco.current_gpu_fraction(), os.getpid()


def _kill_worker() -> None:
    """Take the worker process down, the way a native crash would."""
    os._exit(7)


def _cuda_probe() -> tuple[int | None, int | None, int]:
    """Run a real CUDA kernel on the assigned device; report where it landed."""
    import torch

    import cocoindex as coco

    assigned = coco.current_gpu()
    if assigned is None:
        return None, None, os.getpid()
    x = torch.randn(256, 256, device=f"cuda:{assigned}")
    (x @ x.T).sum().item()  # force a kernel launch
    return assigned, x.device.index, os.getpid()


def _cuda_device_count() -> int:
    try:
        import torch
    except ImportError:
        return 0
    return torch.cuda.device_count() if torch.cuda.is_available() else 0


@pytest.mark.asyncio
async def test_subprocess_receives_gpu_assignment() -> None:
    """current_gpu() in the subprocess is the id the GPU pool handed out."""
    configure_gpu_pool(1)
    gpu, fraction, pid = await GPURunner(fraction=1.0).run_sync_fn(_probe)
    assert gpu == 0
    assert fraction == 1.0
    assert pid != os.getpid()  # it really did run in a subprocess


@pytest.mark.asyncio
async def test_async_fn_receives_gpu_assignment() -> None:
    """`run()` propagates the assignment too, not just `run_sync_fn()`."""
    configure_gpu_pool(2)
    gpu, fraction, pid = await GPURunner(fraction=1.0).run(_aprobe)
    assert gpu in (0, 1)
    assert fraction == 1.0
    assert pid != os.getpid()


@pytest.mark.asyncio
async def test_worker_reports_each_calls_own_assignment() -> None:
    """A reused worker must not report the previous call's context."""
    configure_gpu_pool(1)
    half = GPURunner(fraction=0.5)
    full = GPURunner(fraction=1.0)

    seen = [
        (await runner.run_sync_fn(_probe))[1] for runner in (half, full, half, full)
    ]
    assert seen == [0.5, 1.0, 0.5, 1.0]


@pytest.mark.asyncio
async def test_crashing_call_surfaces_instead_of_respawning_forever() -> None:
    """A call that kills its worker every time must fail, not retry endlessly."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    with pytest.raises(BrokenProcessPool):
        await runner.run_sync_fn(_kill_worker)
    # The pool is usable again afterwards.
    gpu, _, _ = await runner.run_sync_fn(_probe)
    assert gpu == 0


@pytest.mark.asyncio
@pytest.mark.skipif(_cuda_device_count() == 0, reason="needs a CUDA device")
async def test_real_cuda_work_lands_on_the_assigned_device() -> None:
    """End to end on real hardware: the tensor lands on the assigned GPU."""
    n = _cuda_device_count()
    configure_gpu_pool(n)
    runner = GPURunner(fraction=1.0)
    assigned, ran_on, pid = await runner.run_sync_fn(_cuda_probe)
    assert assigned is not None
    assert ran_on == assigned
    assert pid != os.getpid()
