"""Tests for the GPU runner's subprocess mode.

Covers the GPU assignment reaching the subprocess, calls running in parallel, one
GPU per worker, crashes and dead workers, and when a cancelled call gives its GPU
back. The last of these also holds in-process, so it is tested in both modes.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal import runner as _runner_mod
from cocoindex._internal.runner import (
    GPURunner,
    _get_default_gpu_pool,
    configure_gpu_pool,
)

# Seconds a call in a worker waits for the test, or for its peers, before giving up.
# Kept under the module timeout so a stuck call fails its own test with a message.
_CHILD_WAIT_TIMEOUT = 75.0

# Seconds the test itself waits for a condition or a call.
_WAIT_TIMEOUT = 60.0

# Each test starts `spawn` interpreters that import the Rust extension, which the
# project-wide 30s budget does not cover on a cold runner.
pytestmark = pytest.mark.timeout(90)


@pytest.fixture(autouse=True)
def _subprocess_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run GPU work in subprocesses, from fresh pools, and clean up after."""
    monkeypatch.setenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS", "1")
    monkeypatch.setattr(_runner_mod, "_default_gpu_pool", None)
    monkeypatch.setattr(_runner_mod, "_workers", {})
    try:
        yield
    finally:
        for workers in _runner_mod._workers.values():
            for worker in workers:
                # `wait=False`: joining a worker has no timeout, and a wedged one
                # would hang teardown until pytest-timeout kills the process.
                worker.executor.shutdown(wait=False, cancel_futures=True)


def _wait_for_file(path: str) -> None:
    """In a worker: block until `path` exists."""
    deadline = time.monotonic() + _CHILD_WAIT_TIMEOUT
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise TimeoutError(f"{path} never appeared")
        time.sleep(0.01)


def _probe() -> tuple[int | None, float | None, int]:
    """Report the GPU context and pid visible inside the subprocess."""
    return coco.current_gpu(), coco.current_gpu_fraction(), os.getpid()


async def _aprobe() -> tuple[int | None, float | None, int]:
    """Async probe: goes through `GPURunner.run` and the child's `asyncio.run`."""
    return coco.current_gpu(), coco.current_gpu_fraction(), os.getpid()


def _probe_together(rendezvous: str, expected: int) -> tuple[int | None, int]:
    """Report the assignment, but only once every peer has also started.

    Each call announces itself in `rendezvous` and waits for the others, so it
    cannot return unless all `expected` calls are resident at the same time. Were
    they to run one after another this raises, rather than merely running slowly.
    """
    gpu = coco.current_gpu()
    open(os.path.join(rendezvous, uuid.uuid4().hex), "w").close()
    deadline = time.monotonic() + _CHILD_WAIT_TIMEOUT
    while len(os.listdir(rendezvous)) < expected:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"only {len(os.listdir(rendezvous))} of {expected} calls "
                "were running at once"
            )
        time.sleep(0.01)
    return gpu, os.getpid()


def _hold_until(started: str, release: str) -> str:
    """Mark that the call started, then stay on the worker until `release` exists."""
    open(started, "w").close()
    _wait_for_file(release)
    return "held"


def _log_span(log: str, tag: str, release: str | None = None) -> str:
    """Log when the call starts and ends; in between, wait for `release` if given."""
    with open(log, "a") as handle:
        print(f"{tag}:start", time.monotonic(), file=handle)
    if release is not None:
        _wait_for_file(release)
    with open(log, "a") as handle:
        print(f"{tag}:end", time.monotonic(), file=handle)
    return tag


async def _await_condition(
    condition: Callable[[], object], what: str, task: asyncio.Task[Any] | None = None
) -> None:
    """Block until `condition()` holds.

    If `task` finishes first, its exception is raised here rather than the test
    waiting out its timeout and reporting only `what`.
    """
    deadline = time.monotonic() + _WAIT_TIMEOUT
    while not condition():
        if task is not None and task.done():
            task.result()
            raise AssertionError(f"{what}: the call returned early")
        assert time.monotonic() < deadline, what
        await asyncio.sleep(0.05)


def _kill_worker() -> None:
    """Take the worker process down, the way a native crash would."""
    os._exit(7)


def _raise_broken_process_pool() -> None:
    """A user function that raises `BrokenProcessPool` itself, from a healthy worker."""
    raise BrokenProcessPool("raised by the function, not by a dying worker")


def _identity(value: object) -> object:
    return value


def _echo_kwargs(**kwargs: object) -> dict[str, object]:
    return kwargs


async def _aecho_kwargs(**kwargs: object) -> dict[str, object]:
    return kwargs


def _cuda_probe() -> tuple[int | None, int | None, int]:
    """Run a real CUDA kernel on the assigned device; report where it landed."""
    import torch

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


async def _assert_gpu_held_until(release: Path) -> None:
    """Check the one GPU stays fully taken until `release` is created, then frees up.

    A full-GPU acquire is the direct test: it can only succeed once the cancelled
    call's capacity is back in the pool.
    """
    gpu_pool = _get_default_gpu_pool()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(gpu_pool.acquire(1.0), 1.0)
    release.touch()
    gpu = await asyncio.wait_for(gpu_pool.acquire(1.0), _WAIT_TIMEOUT)
    gpu_pool.release(gpu, 1.0)


# A runner of its own: functions cache their batch queue, and the queue holds a
# reference to the event loop it was first used on.
_FN_RUNNER = GPURunner(fraction=1.0)


@coco.fn.as_async(runner=_FN_RUNNER)
def _held_via_fn(log: str, release: str) -> str:
    return _log_span(log, "held", release)


@coco.fn.as_async(runner=_FN_RUNNER)
def _probe_via_fn(log: str) -> tuple[int | None, int]:
    return coco.current_gpu(), os.getpid()


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
@pytest.mark.parametrize("in_subprocess", [True, False])
@pytest.mark.parametrize("method", ["run_sync_fn", "run"])
async def test_kwargs_named_like_runner_parameters_reach_the_function(
    monkeypatch: pytest.MonkeyPatch, in_subprocess: bool, method: str
) -> None:
    """A function may take keyword arguments named like the runner's own parameters.

    They must reach the function, and must not fail the call after the GPU was
    acquired, which would leak it and hang every later call.
    """
    if not in_subprocess:
        monkeypatch.delenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS")
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    call = (
        runner.run_sync_fn(_echo_kwargs, gpu_ids=1, fraction=2, release=3)
        if method == "run_sync_fn"
        else runner.run(_aecho_kwargs, gpu_ids=1, fraction=2, release=3)
    )
    assert await call == {"gpu_ids": 1, "fraction": 2, "release": 3}
    # The GPU came back, so a second full-GPU call is admitted.
    assert await asyncio.wait_for(runner.run_sync_fn(_echo_kwargs), _WAIT_TIMEOUT) == {}


@pytest.mark.asyncio
async def test_an_unpicklable_argument_fails_without_leaking_the_gpu() -> None:
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    with pytest.raises(TypeError):
        await runner.run_sync_fn(_identity, threading.Lock())
    assert await asyncio.wait_for(runner.run_sync_fn(_identity, 1), _WAIT_TIMEOUT) == 1


@pytest.mark.asyncio
async def test_crashing_call_surfaces_instead_of_respawning_forever() -> None:
    """A call that kills its worker every time must fail, not retry endlessly."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    with pytest.raises(BrokenProcessPool):
        await runner.run_sync_fn(_kill_worker)
    # A new worker takes the next call.
    gpu, _, _ = await runner.run_sync_fn(_probe)
    assert gpu == 0


@pytest.mark.asyncio
async def test_real_cuda_work_lands_on_the_assigned_device() -> None:
    """End to end on real hardware: the tensor lands on the assigned GPU."""
    # Checked here rather than in `skipif`: every worker imports this module to
    # unpickle the helpers, and a module-level check would import torch in each.
    n = _cuda_device_count()
    if n == 0:
        pytest.skip("needs a CUDA device")
    configure_gpu_pool(n)
    runner = GPURunner(fraction=1.0)
    assigned, ran_on, pid = await runner.run_sync_fn(_cuda_probe)
    assert assigned is not None
    assert ran_on == assigned
    assert pid != os.getpid()


@pytest.mark.asyncio
async def test_calls_on_different_gpus_run_at_the_same_time(tmp_path: Path) -> None:
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
@pytest.mark.parametrize("fraction, max_workers", [(1.0, 1), (0.5, 2)])
async def test_a_gpu_starts_a_worker_only_when_its_workers_are_busy(
    fraction: float, max_workers: int
) -> None:
    """A GPU must not run more workers than it ever has calls at once.

    Every worker holds a CUDA context and its own copy of any model the function
    caches. Many callers queue here, so each finished call is followed at once by
    the call its release admits; that call must find the idle worker rather than
    start another. A `ProcessPoolExecutor` shared by the GPU's calls does start
    another: it marks a worker idle only after the finished call's callbacks have
    released the GPU.
    """
    configure_gpu_pool(1)
    runner = GPURunner(fraction=fraction)
    pids: set[int] = set()

    async def caller() -> None:
        for _ in range(25):
            pids.add((await runner.run_sync_fn(_probe))[2])

    await asyncio.gather(*(caller() for _ in range(16)))
    assert len(pids) <= max_workers
    assert len(_runner_mod._workers[0]) <= max_workers


@pytest.mark.asyncio
async def test_pool_stays_wide_after_a_crash(tmp_path: Path) -> None:
    """After a crash, calls sharing the GPU must still run at the same time."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=0.5)
    with pytest.raises(BrokenProcessPool):
        await runner.run_sync_fn(_kill_worker)

    results = await asyncio.gather(
        *(runner.run_sync_fn(_probe_together, str(tmp_path), 2) for _ in range(2))
    )
    assert len({pid for _, pid in results}) == 2


@pytest.mark.asyncio
async def test_a_crash_fails_only_the_call_on_that_worker(tmp_path: Path) -> None:
    """A dying worker must not take down a call running beside it on the same GPU."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=0.5)
    started, release = tmp_path / "started", tmp_path / "release"

    survivor = asyncio.create_task(
        runner.run_sync_fn(_hold_until, str(started), str(release))
    )
    try:
        await _await_condition(
            started.exists, "the surviving call never started", survivor
        )
        # The survivor's worker is busy, so this call gets a worker of its own.
        with pytest.raises(BrokenProcessPool):
            await runner.run_sync_fn(_kill_worker)
    finally:
        release.touch()
    assert await survivor == "held"


@pytest.mark.asyncio
async def test_a_function_raising_broken_process_pool_keeps_its_worker() -> None:
    """Only a worker whose process died is dropped, not one whose call raised."""
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    _, _, pid = await runner.run_sync_fn(_probe)
    with pytest.raises(BrokenProcessPool, match="raised by the function"):
        await runner.run_sync_fn(_raise_broken_process_pool)
    assert (await runner.run_sync_fn(_probe))[2] == pid
    assert len(_runner_mod._workers[0]) == 1


@pytest.mark.asyncio
async def test_a_worker_that_fails_to_take_a_call_is_taken_out_of_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A submit that fails must not leave a phantom call on the worker.

    Its worker would never look idle again, so the GPU would start a second worker
    for calls that never overlap.
    """
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    await runner.run_sync_fn(_probe)

    submit = ProcessPoolExecutor.submit
    failures = iter([OSError("could not start the worker")])

    def flaky_submit(self: ProcessPoolExecutor, *args: Any, **kwargs: Any) -> Any:
        for error in failures:
            raise error
        return submit(self, *args, **kwargs)

    monkeypatch.setattr(ProcessPoolExecutor, "submit", flaky_submit)
    with pytest.raises(OSError, match="could not start the worker"):
        await runner.run_sync_fn(_probe)
    assert _runner_mod._workers[0] == []

    for _ in range(3):
        await runner.run_sync_fn(_probe)
    assert [worker.calls for worker in _runner_mod._workers[0]] == [0]


@pytest.mark.asyncio
async def test_each_worker_serves_only_one_gpu() -> None:
    """A worker must not move between GPUs.

    A process that has run work on a GPU keeps a CUDA context there, plus its
    allocator's cache and any model it caches per device, until it exits. A worker
    serving every GPU would hold one of each on every card. Holding one GPU from the
    parent steers each call onto the other, so the calls alternate between them.
    """
    configure_gpu_pool(2)
    gpu_pool = _get_default_gpu_pool()
    runner = GPURunner(fraction=1.0)
    served: dict[int, set[int | None]] = {}
    held = await gpu_pool.acquire(1.0)
    for _ in range(4):
        gpu, _, pid = await runner.run_sync_fn(_probe)
        assert gpu is not None and gpu != held
        served.setdefault(pid, set()).add(gpu)
        # The call's GPU is free again: hold it, so the next call gets the other one.
        other = await gpu_pool.acquire(1.0)
        gpu_pool.release(held, 1.0)
        held = other
    gpu_pool.release(held, 1.0)

    assert set().union(*served.values()) == {0, 1}
    assert all(len(gpus) == 1 for gpus in served.values()), (
        f"a worker served more than one GPU: {served}"
    )


@pytest.mark.asyncio
async def test_a_call_after_an_idle_worker_died_still_runs() -> None:
    """A call must not fail on a worker that died while idle, since it never ran.

    This covers a death the worker's executor has already noticed. A call that
    reaches the worker in the moment before that still fails, which is why the
    test waits for the executor first.
    """
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    _, _, pid = await runner.run_sync_fn(_probe)

    # Kill the idle worker the way the OOM killer would.
    os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
    executor = _runner_mod._workers[0][0].executor
    await _await_condition(
        lambda: executor._broken, "the executor never noticed its worker died"
    )

    gpu, _, new_pid = await runner.run_sync_fn(_probe)
    assert gpu == 0
    assert new_pid != pid


@pytest.mark.asyncio
async def test_cancelled_call_keeps_the_gpu_until_its_worker_stops(
    tmp_path: Path,
) -> None:
    """A cancelled call must not hand its GPU back while the worker is still on it.

    The child cannot be interrupted once it has started. Releasing the capacity at
    cancellation time therefore admits the next call onto a card that is still busy,
    which is how two full-GPU jobs end up on one device. The caller itself does not
    wait for the worker: the GPU is held, not the caller.
    """
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    log, release = tmp_path / "timeline.txt", tmp_path / "release"

    first = asyncio.create_task(
        runner.run_sync_fn(_log_span, str(log), "cancelled", str(release))
    )
    try:
        await _await_condition(
            lambda: log.exists() and "cancelled:start" in log.read_text(),
            "the call never started",
            first,
        )
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first  # returns while the worker is still held on `release`
        assert "cancelled:end" not in log.read_text()
        await _assert_gpu_held_until(release)
    finally:
        release.touch()


@pytest.mark.asyncio
async def test_cancelled_call_through_coco_fn_keeps_the_gpu(tmp_path: Path) -> None:
    """The same, through `@coco.fn`, whose batcher cancels the call without awaiting it."""
    configure_gpu_pool(1)
    log, release = tmp_path / "timeline.txt", tmp_path / "release"

    first = asyncio.create_task(_held_via_fn(str(log), str(release)))
    try:
        await _await_condition(
            lambda: log.exists() and "held:start" in log.read_text(),
            "the call never started",
            first,
        )
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await _assert_gpu_held_until(release)
    finally:
        release.touch()
    # The runner still works afterwards.
    gpu, pid = await _probe_via_fn(str(log))
    assert gpu == 0
    assert pid != os.getpid()


@pytest.mark.asyncio
async def test_in_process_cancelled_call_keeps_the_gpu_until_its_thread_stops(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """In-process, a started thread cannot be interrupted either."""
    monkeypatch.delenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS")
    configure_gpu_pool(1)
    runner = GPURunner(fraction=1.0)
    log, release = tmp_path / "timeline.txt", tmp_path / "release"

    first = asyncio.create_task(
        runner.run_sync_fn(_log_span, str(log), "cancelled", str(release))
    )
    try:
        await _await_condition(
            lambda: log.exists() and "cancelled:start" in log.read_text(),
            "the call never started",
            first,
        )
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await _assert_gpu_held_until(release)
    finally:
        release.touch()


@pytest.mark.asyncio
async def test_a_late_release_goes_back_to_the_pool_it_came_from(
    tmp_path: Path,
) -> None:
    """A cancelled call's GPU must return to the pool it was taken from.

    The GPU is released when the worker finishes, which can be after the caller was
    cancelled and `configure_gpu_pool` swapped in a new pool. Releasing into the new
    pool would hand out capacity it never gave, double-booking its GPU.
    """
    configure_gpu_pool(1)
    old_pool = _get_default_gpu_pool()
    runner = GPURunner(fraction=1.0)
    started, release = tmp_path / "started", tmp_path / "release"

    call = asyncio.create_task(
        runner.run_sync_fn(_hold_until, str(started), str(release))
    )
    try:
        await _await_condition(started.exists, "the call never started", call)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call

        configure_gpu_pool(1)
        new_pool = _get_default_gpu_pool()
        held = await new_pool.acquire(1.0)
    finally:
        release.touch()

    # The worker finishing hands the GPU back to the old pool...
    assert await asyncio.wait_for(old_pool.acquire(1.0), _WAIT_TIMEOUT) == 0
    # ...and not to the new one, whose only GPU is still held.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(new_pool.acquire(1.0), 0.5)
    new_pool.release(held, 1.0)
