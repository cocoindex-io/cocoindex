"""Tests for GPU subprocess mode: assignment propagation and parallelism."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import pytest

from cocoindex._internal import runner as _runner_mod
from cocoindex._internal.runner import GPURunner, configure_gpu_pool

# Seconds a call waits for its peers before declaring them serialized.
_RENDEZVOUS_TIMEOUT = 30.0

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


def _probe_together(rendezvous: str, expected: int) -> tuple[int | None, int]:
    """Report the assignment, but only once every peer has also started.

    Each call announces itself in `rendezvous` and waits for the others, so it
    cannot return unless all `expected` calls are resident at the same time. Were
    the pool to serialize them this raises, rather than merely running slowly.
    """
    import time
    import uuid

    import cocoindex as coco

    gpu = coco.current_gpu()
    open(os.path.join(rendezvous, uuid.uuid4().hex), "w").close()
    deadline = time.monotonic() + _RENDEZVOUS_TIMEOUT
    while len(os.listdir(rendezvous)) < expected:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"only {len(os.listdir(rendezvous))} of {expected} calls "
                "were running at once"
            )
        time.sleep(0.01)
    return gpu, os.getpid()


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
async def test_subprocess_multi_gpu_runs_concurrently(tmp_path: Path) -> None:
    """The motivating bug: one shared worker serialized every GPU behind it."""
    configure_gpu_pool(4)
    runner = GPURunner(fraction=1.0)
    results = await asyncio.gather(
        *(runner.run_sync_fn(_probe_together, str(tmp_path), 4) for _ in range(4))
    )
    assert sorted(gpu for gpu, _ in results if gpu is not None) == [0, 1, 2, 3]
    assert len({pid for _, pid in results}) == 4


@pytest.mark.asyncio
async def test_fractional_calls_share_a_gpu_concurrently(tmp_path: Path) -> None:
    """Calls sharing one GPU by fraction must also run at the same time."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=0.25)
    results = await asyncio.gather(
        *(runner.run_sync_fn(_probe_together, str(tmp_path), 4) for _ in range(4))
    )
    assert all(gpu == 0 for gpu, _ in results)
    assert len({pid for _, pid in results}) == 4


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
async def test_pool_stays_wide_after_a_crash(tmp_path: Path) -> None:
    """Restarting a broken pool must not narrow it back to one worker."""
    configure_gpu_pool(2)
    runner = GPURunner(fraction=1.0)
    with pytest.raises(BrokenProcessPool):
        await runner.run_sync_fn(_kill_worker)

    results = await asyncio.gather(
        *(runner.run_sync_fn(_probe_together, str(tmp_path), 2) for _ in range(2))
    )
    assert len({pid for _, pid in results}) == 2


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
