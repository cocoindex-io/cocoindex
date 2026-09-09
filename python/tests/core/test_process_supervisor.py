"""Real-process tests for the MPS-only owned subprocess supervisor."""

from __future__ import annotations

import asyncio
import os
import pickle
import time
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import cocoindex as coco
import psutil
import pytest
import pytest_asyncio
from cocoindex._internal import runner as runner_module
from cocoindex._internal.batching import RetryWithSmallerBatch
from cocoindex._internal.process_supervisor import (
    _ProcessSupervisorClosedError,
    _RemoteExecutionError,
    _RemoteTraceback,
    _SingleProcessSupervisor,
    _WorkerCrashedError,
)


_worker_call_count = 0


class _WorkerUserError(Exception):
    pass


class _UnpicklableWorkerError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.unpicklable = lambda: None


def _worker_identity() -> tuple[int, int]:
    global _worker_call_count
    _worker_call_count += 1
    return os.getpid(), _worker_call_count


def _return_pid() -> int:
    return os.getpid()


def _record_attempt(path: str) -> int:
    pid = os.getpid()
    with Path(path).open("a", encoding="utf-8") as file:
        file.write(f"{pid}\n")
        file.flush()
        os.fsync(file.fileno())
    return pid


def _crash_once(attempts_path: str) -> int:
    path = Path(attempts_path)
    should_crash = not path.exists()
    pid = _record_attempt(attempts_path)
    if should_crash:
        os._exit(86)
    return pid


def _always_crash(attempts_path: str) -> None:
    _record_attempt(attempts_path)
    os._exit(87)


def _hang(attempts_path: str) -> None:
    _record_attempt(attempts_path)
    while True:
        time.sleep(60)


def _wait_for_release(attempts_path: str, release_path: str) -> int:
    pid = _record_attempt(attempts_path)
    while not Path(release_path).exists():
        time.sleep(0.01)
    return pid


def _raise_user_error(attempts_path: str) -> None:
    _record_attempt(attempts_path)
    raise _WorkerUserError("worker failure")


def _raise_unpicklable_error() -> None:
    raise _UnpicklableWorkerError("cannot pickle me")


def _raise_retry_with_smaller_batch() -> None:
    try:
        raise ValueError("original batch failure")
    except ValueError as error:
        raise RetryWithSmallerBatch() from error


async def _execute(
    supervisor: _SingleProcessSupervisor,
    fn: Any,
    *args: Any,
) -> Any:
    payload = pickle.dumps((fn, args, {}), protocol=pickle.HIGHEST_PROTOCOL)
    result = await supervisor.execute(payload)
    return pickle.loads(result)


def _read_attempts(path: Path) -> list[int]:
    if not path.exists():
        return []
    return [int(line) for line in path.read_text().splitlines() if line]


async def _wait_for_attempts(
    path: Path, count: int, *, timeout: float = 5.0
) -> list[int]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        attempts = _read_attempts(path)
        if len(attempts) >= count:
            return attempts
        await asyncio.sleep(0.01)
    raise TimeoutError(f"Expected {count} attempts in {path}")


async def _assert_pid_gone(pid: int, *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if not psutil.pid_exists(pid):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Worker PID {pid} is still alive")


@pytest_asyncio.fixture
async def supervisor() -> AsyncIterator[_SingleProcessSupervisor]:
    instance = _SingleProcessSupervisor(
        runner_module._execute_in_subprocess,
        process_name="cocoindex-supervisor-test-worker",
    )
    try:
        yield instance
    finally:
        await instance.aclose()


@pytest.fixture
def mps_runner() -> Iterator[None]:
    old_use_subprocess = runner_module._MPS_GPU._use_subprocess
    runner_module._shutdown_mps_process_supervisor()
    runner_module._MPS_GPU._use_subprocess = True
    try:
        yield
    finally:
        runner_module._shutdown_mps_process_supervisor()
        runner_module._MPS_GPU._use_subprocess = old_use_subprocess


@pytest.mark.asyncio
async def test_success_reuses_worker_state(
    supervisor: _SingleProcessSupervisor,
) -> None:
    first_pid, first_count = await _execute(supervisor, _worker_identity)
    second_pid, second_count = await _execute(supervisor, _worker_identity)

    assert second_pid == first_pid
    assert (first_count, second_count) == (1, 2)


@pytest.mark.asyncio
async def test_requests_from_different_event_loops_share_worker(
    supervisor: _SingleProcessSupervisor,
) -> None:
    def run_from_thread() -> int:
        result = asyncio.run(_execute(supervisor, _return_pid))
        assert isinstance(result, int)
        return result

    first_pid, second_pid = await asyncio.gather(
        asyncio.to_thread(run_from_thread),
        asyncio.to_thread(run_from_thread),
    )

    assert second_pid == first_pid


@pytest.mark.asyncio
async def test_user_exception_keeps_worker(
    supervisor: _SingleProcessSupervisor, tmp_path: Path
) -> None:
    first_pid = await _execute(supervisor, _return_pid)
    attempts_path = tmp_path / "user-error-attempts"

    with pytest.raises(_WorkerUserError, match="worker failure") as exc_info:
        await _execute(supervisor, _raise_user_error, str(attempts_path))

    assert isinstance(exc_info.value.__cause__, _RemoteTraceback)
    assert _read_attempts(attempts_path) == [first_pid]
    assert await _execute(supervisor, _return_pid) == first_pid


@pytest.mark.asyncio
async def test_unpicklable_exception_falls_back_without_recycling(
    supervisor: _SingleProcessSupervisor,
) -> None:
    first_pid = await _execute(supervisor, _return_pid)

    with pytest.raises(_RemoteExecutionError, match="cannot pickle me"):
        await _execute(supervisor, _raise_unpicklable_error)

    assert await _execute(supervisor, _return_pid) == first_pid


@pytest.mark.asyncio
async def test_queued_cancellation_does_not_kill_active_worker(
    supervisor: _SingleProcessSupervisor, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "active-attempts"
    release_path = tmp_path / "release"
    active = asyncio.create_task(
        _execute(
            supervisor,
            _wait_for_release,
            str(attempts_path),
            str(release_path),
        )
    )
    active_pid = (await _wait_for_attempts(attempts_path, 1))[0]

    queued = asyncio.create_task(_execute(supervisor, _return_pid))
    await asyncio.sleep(0)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(queued, timeout=1)

    release_path.touch()
    assert await asyncio.wait_for(active, timeout=5) == active_pid
    assert await _execute(supervisor, _return_pid) == active_pid


@pytest.mark.asyncio
async def test_running_cancellation_reaps_worker_before_propagating(
    supervisor: _SingleProcessSupervisor, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "cancel-attempts"
    task = asyncio.create_task(_execute(supervisor, _hang, str(attempts_path)))
    cancelled_pid = (await _wait_for_attempts(attempts_path, 1))[0]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)

    await _assert_pid_gone(cancelled_pid)
    assert await _execute(supervisor, _return_pid) != cancelled_pid


@pytest.mark.asyncio
async def test_close_reaps_active_worker(
    supervisor: _SingleProcessSupervisor, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "close-attempts"
    task = asyncio.create_task(_execute(supervisor, _hang, str(attempts_path)))
    active_pid = (await _wait_for_attempts(attempts_path, 1))[0]

    await supervisor.aclose()

    with pytest.raises(_ProcessSupervisorClosedError):
        await task
    await _assert_pid_gone(active_pid)
    await supervisor.aclose()


@pytest.mark.asyncio
async def test_close_racing_queued_cancellations_drains_every_request(
    supervisor: _SingleProcessSupervisor, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "close-race-attempts"
    active = asyncio.create_task(_execute(supervisor, _hang, str(attempts_path)))
    active_pid = (await _wait_for_attempts(attempts_path, 1))[0]
    queued = [asyncio.create_task(_execute(supervisor, _return_pid)) for _ in range(50)]
    await asyncio.sleep(0)

    close_task = asyncio.create_task(supervisor.aclose())
    for task in queued:
        task.cancel()

    queued_outcomes = await asyncio.wait_for(
        asyncio.gather(*queued, return_exceptions=True), timeout=5
    )
    await asyncio.wait_for(close_task, timeout=5)

    assert all(isinstance(error, asyncio.CancelledError) for error in queued_outcomes)
    with pytest.raises(_ProcessSupervisorClosedError):
        await active
    await _assert_pid_gone(active_pid)


@pytest.mark.asyncio
async def test_mps_hard_crash_replays_once(mps_runner: None, tmp_path: Path) -> None:
    attempts_path = tmp_path / "crash-once-attempts"

    result_pid = await runner_module._MPS_GPU.run_sync_fn(
        _crash_once, str(attempts_path)
    )

    attempts = _read_attempts(attempts_path)
    assert len(attempts) == 2
    assert result_pid == attempts[1]
    assert attempts[0] != attempts[1]
    await _assert_pid_gone(attempts[0])


@pytest.mark.asyncio
async def test_mps_repeated_crash_is_bounded_and_recovers(
    mps_runner: None, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "repeated-crash-attempts"

    with pytest.raises(_WorkerCrashedError) as exc_info:
        await runner_module._MPS_GPU.run_sync_fn(_always_crash, str(attempts_path))

    attempts = _read_attempts(attempts_path)
    assert len(attempts) == 2
    assert exc_info.value.exit_code == 87
    for pid in attempts:
        await _assert_pid_gone(pid)
    assert await runner_module._MPS_GPU.run_sync_fn(_return_pid) not in attempts


@pytest.mark.asyncio
async def test_mps_timeout_does_not_replay_and_recovers(
    mps_runner: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    warm_pid = await runner_module._MPS_GPU.run_sync_fn(_return_pid)
    attempts_path = tmp_path / "timeout-attempts"
    monkeypatch.setattr(
        runner_module, "_MPS_FALLBACK_TIMEOUT", timedelta(milliseconds=200)
    )

    with pytest.raises(coco.DeadlineExceededError):
        await asyncio.wait_for(
            runner_module._MPS_GPU.run_sync_fn(_hang, str(attempts_path)),
            timeout=5,
        )

    assert _read_attempts(attempts_path) == [warm_pid]
    await _assert_pid_gone(warm_pid)
    assert await runner_module._MPS_GPU.run_sync_fn(_return_pid) != warm_pid


@pytest.mark.asyncio
async def test_mps_cancellation_reaps_without_replay(
    mps_runner: None, tmp_path: Path
) -> None:
    attempts_path = tmp_path / "mps-cancel-attempts"
    task = asyncio.create_task(
        runner_module._MPS_GPU.run_sync_fn(_hang, str(attempts_path))
    )
    cancelled_pid = (await _wait_for_attempts(attempts_path, 1))[0]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)

    assert _read_attempts(attempts_path) == [cancelled_pid]
    await _assert_pid_gone(cancelled_pid)
    assert await runner_module._MPS_GPU.run_sync_fn(_return_pid) != cancelled_pid


@pytest.mark.asyncio
async def test_mps_user_exception_is_not_replayed_or_recycled(
    mps_runner: None, tmp_path: Path
) -> None:
    worker_pid = await runner_module._MPS_GPU.run_sync_fn(_return_pid)
    attempts_path = tmp_path / "mps-user-error-attempts"

    with pytest.raises(_WorkerUserError, match="worker failure"):
        await runner_module._MPS_GPU.run_sync_fn(_raise_user_error, str(attempts_path))

    assert _read_attempts(attempts_path) == [worker_pid]
    assert await runner_module._MPS_GPU.run_sync_fn(_return_pid) == worker_pid


@pytest.mark.asyncio
async def test_mps_preserves_retry_with_smaller_batch_cause(
    mps_runner: None,
) -> None:
    with pytest.raises(RetryWithSmallerBatch) as exc_info:
        await runner_module._MPS_GPU.run_sync_fn(_raise_retry_with_smaller_batch)

    assert isinstance(exc_info.value._restored_cause, ValueError)
    assert str(exc_info.value._restored_cause) == "original batch failure"


@pytest.mark.asyncio
async def test_generic_gpu_keeps_existing_subprocess_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generic_runner = runner_module.GPURunner()
    generic_runner._use_subprocess = True

    def fail_if_mps_supervisor_is_used() -> None:
        raise AssertionError("generic GPU used the MPS supervisor")

    monkeypatch.setattr(
        runner_module, "_get_mps_process_supervisor", fail_if_mps_supervisor_is_used
    )

    assert await generic_runner.run_sync_fn(_return_pid) != os.getpid()
