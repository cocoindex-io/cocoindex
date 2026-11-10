"""
Runner base class and GPU runner implementation.

Runners execute functions in specific contexts. Each runner owns a BatchQueue.

The GPU runner supports multiple GPUs and fractional GPU allocations.
Configure the GPU pool size via the COCOINDEX_NUM_GPUS environment variable
(default: 1). Set COCOINDEX_RUN_GPU_IN_SUBPROCESS=1 for subprocess isolation.
"""

from __future__ import annotations

import asyncio
import functools
import os
import pickle
import threading
import multiprocessing as mp
from abc import ABC, abstractmethod
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextvars import Context, ContextVar
from typing import Any, Callable, Coroutine, NamedTuple, TypeVar, ParamSpec

from . import core

P = ParamSpec("P")
R = TypeVar("R")

# Flag indicating if we're running inside a subprocess (GPU runner)
# When True, @coco.fn decorators should execute the raw function
# without batching/runner/memo since those are already handled by the parent.
_in_subprocess: bool = False


class Runner(ABC):
    """Base class for runners that execute functions.

    Each runner owns a BatchQueue, created lazily on first use. The queue is
    shared with functions using this runner for batch aggregation; concurrency
    control is handled by the runner's run/run_sync_fn implementations.

    Subclasses must implement:
    - run(): Execute an async function
    - run_sync_fn(): Execute a sync function
    """

    _queue: core.BatchQueue | None
    _queue_lock: threading.Lock

    def __init__(self) -> None:
        self._queue = None
        self._queue_lock = threading.Lock()

    def get_queue(self) -> core.BatchQueue:
        """Get or create the BatchQueue for this runner.

        All functions using this runner share this queue for batch aggregation.
        """
        if self._queue is None:
            with self._queue_lock:
                if self._queue is None:
                    self._queue = core.BatchQueue()
        return self._queue

    @abstractmethod
    async def run(
        self, fn: Callable[P, Coroutine[Any, Any, R]], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute an async function with args/kwargs.

        This is async because it needs to await the async function's result.
        Caller must be in an async context.
        """
        ...

    @abstractmethod
    async def run_sync_fn(
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute a sync function with args/kwargs.

        This is async to avoid blocking the event loop while waiting for execution.
        The function itself is sync but execution may involve I/O (e.g., subprocess).
        """
        ...


# ============================================================================
# Subprocess execution infrastructure
# ============================================================================

_WATCHDOG_INTERVAL_SECONDS = 10.0

# Ceiling on subprocess workers per GPU. `GPUPool` decides how many calls run at once,
# and admits at most `1/fraction` calls on one GPU; this only stops a tiny fraction from
# spawning processes without bound. Past it, a call queues behind the least busy worker.
_MAX_SUBPROCESS_WORKERS = 32


class _Worker:
    """One subprocess, as a single-worker executor, and the calls currently on it."""

    __slots__ = ("executor", "calls")

    def __init__(self) -> None:
        self.executor = ProcessPoolExecutor(
            max_workers=1,
            initializer=_subprocess_init,
            initargs=(os.getpid(),),
            mp_context=mp.get_context("spawn"),
        )
        self.calls = 0


_workers_lock = threading.Lock()
# Workers per GPU id. A worker only ever serves the GPU its calls were given, so it
# holds a CUDA context (and any model it caches) on that device only. Each worker is
# its own executor, and a call goes to an idle one when there is one: a GPU gets a new
# worker only when every worker it has is busy, and a worker that dies fails only the
# calls on it.
_workers: dict[int, list[_Worker]] = {}


def _take_worker(gpu_id: int) -> _Worker:
    """Assign the next call on `gpu_id` to an idle worker, else a new one, else (at the
    cap) the least busy one."""
    with _workers_lock:
        workers = _workers.setdefault(gpu_id, [])
        worker = min(workers, key=lambda w: w.calls, default=None)
        if worker is None or (
            worker.calls > 0 and len(workers) < _MAX_SUBPROCESS_WORKERS
        ):
            worker = _Worker()
            workers.append(worker)
        worker.calls += 1
        return worker


def _finish_call(gpu_id: int, worker: _Worker) -> None:
    """Take a finished call off `worker`, and drop the worker if its process died.

    Runs on the worker's own management thread when a call completes. A dead worker's
    executor has already terminated its process and failed the calls on it, so all
    that is left is to drop it. Shutting it down here would wait on this very thread:
    3.14 holds the executor's shutdown lock while it fails the calls.
    """
    with _workers_lock:
        worker.calls -= 1
        workers = _workers.get(gpu_id, [])
        # Whether the process died, not whether the call raised `BrokenProcessPool`,
        # which a user function may do itself. The executor sets this before it fails
        # the calls.
        if worker.executor._broken and worker in workers:
            workers.remove(worker)


def _discard_worker(gpu_id: int, worker: _Worker) -> None:
    """Take a worker that failed to accept a call out of service."""
    with _workers_lock:
        workers = _workers.get(gpu_id, [])
        if worker in workers:
            workers.remove(worker)
    # `wait=False`: do not block the event loop; calls already on it still finish.
    worker.executor.shutdown(wait=False)


def _subprocess_init(parent_pid: int) -> None:
    """Initialize the subprocess with watchdog and signal handling."""
    import signal
    import faulthandler

    global _in_subprocess
    _in_subprocess = True

    faulthandler.enable()
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass

    _start_parent_watchdog(parent_pid)


def _start_parent_watchdog(parent_pid: int) -> None:
    """Terminate subprocess if parent exits."""
    import time

    try:
        import psutil
    except ImportError:
        return  # psutil not available, skip watchdog

    try:
        p = psutil.Process(parent_pid)
        created = p.create_time()
    except psutil.Error:
        os._exit(1)

    def _watch() -> None:
        while True:
            try:
                if not (p.is_running() and p.create_time() == created):
                    os._exit(1)
            except psutil.NoSuchProcess:
                os._exit(1)
            time.sleep(_WATCHDOG_INTERVAL_SECONDS)

    threading.Thread(target=_watch, name="parent-watchdog", daemon=True).start()


def _execute_in_subprocess(payload_bytes: bytes) -> bytes:
    """Run in subprocess: unpack, execute, return pickled result."""
    fn, args, kwargs, gpu_ids, fraction = pickle.loads(payload_bytes)
    # ContextVars do not cross a process boundary, so the parent's assignment
    # travels in the payload and is re-established here.
    _current_gpus.set(gpu_ids)
    _current_gpu_fraction.set(fraction)
    result = fn(*args, **kwargs)
    # Handle async callables (functions or callable objects with async __call__)
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)


class _Submission(NamedTuple):
    worker: _Worker
    future: Future[bytes]


def _submit(gpu_id: int, payload: bytes) -> _Submission:
    """Queue `payload` on a worker for `gpu_id`."""
    while True:
        worker = _take_worker(gpu_id)
        try:
            return _Submission(
                worker, worker.executor.submit(_execute_in_subprocess, payload)
            )
        except BrokenProcessPool:
            # The worker died while idle and its executor has noticed, so the call never
            # reached it: drop the worker and give the call to another.
            _finish_call(gpu_id, worker)
        except BaseException:
            _finish_call(gpu_id, worker)
            _discard_worker(gpu_id, worker)
            raise


async def execute_in_subprocess(
    gpu_ids: list[int],
    fraction: float,
    release: Callable[[], None],
    fn: Callable[..., R],
    /,
    *args: Any,
    **kwargs: Any,
) -> R:
    """Execute a function in a subprocess and return the result.

    The function and all arguments must be picklable. ``gpu_ids`` and ``fraction``
    are the caller's GPU assignment; the call runs on a worker of its first GPU. The
    parameters before ``*args`` are positional-only, so the submitted function's
    keyword arguments cannot collide with them.

    ``release`` hands the assignment back to the GPU pool, and is called exactly
    once. Once the call is queued, that happens when its future completes, not when
    this coroutine returns. A started worker cannot be interrupted, so a cancelled
    call that a worker has started keeps its GPU until the work is off the device,
    and the next call is not admitted onto a card that is still busy.

    A call whose worker dies fails with ``BrokenProcessPool`` and is not retried, so
    a function that reliably crashes fails once instead of respawning forever. The
    engine handles that like any failed component: nothing is written or memoized
    for it, and the next update retries it. Only a call that never reached its
    worker, because the worker had already died while idle, is given to another.
    """
    gpu_id = gpu_ids[0]
    try:
        payload = pickle.dumps(
            (fn, args, kwargs, gpu_ids, fraction), protocol=pickle.HIGHEST_PROTOCOL
        )
        worker, future = _submit(gpu_id, payload)
    except BaseException:
        release()
        raise

    def _on_done(_: Future[bytes]) -> None:
        try:
            # The worker is marked idle before the GPU goes back, so the call that the
            # release admits finds it idle instead of starting another worker.
            _finish_call(gpu_id, worker)
        finally:
            release()

    # On the `concurrent.futures.Future`, not the asyncio wrapper: the wrapper is
    # cancelled as soon as the caller is, which would release the GPU while the
    # worker is still on it. Added before `wrap_future` adds its own callback, so
    # the GPU is back in the pool by the time the caller sees the result.
    future.add_done_callback(_on_done)
    result_bytes = await asyncio.wrap_future(future)
    return pickle.loads(result_bytes)  # type: ignore[no-any-return]


def in_subprocess() -> bool:
    """Check if we're running in a subprocess."""
    return _in_subprocess


# ============================================================================
# GPU Pool — fractional capacity tracking across multiple GPUs
# ============================================================================


# ============================================================================
# GPU identity propagation
# ============================================================================

_current_gpus: ContextVar[list[int]] = ContextVar("coco_current_gpus", default=[])
_current_gpu_fraction: ContextVar[float | None] = ContextVar(
    "coco_current_gpu_fraction", default=None
)


def current_gpu() -> int | None:
    """Return the first physical GPU id assigned to the current call, or None."""
    gpus = _current_gpus.get()
    return gpus[0] if gpus else None


def current_gpus() -> list[int]:
    """Return the physical GPU ids assigned to the current call."""
    return list(_current_gpus.get())


def current_gpu_fraction() -> float | None:
    """Return the fractional GPU amount assigned to the current call, or None."""
    return _current_gpu_fraction.get()


def _run_with_gpu_context(
    gpu_ids: list[int],
    fraction: float,
    fn: Callable[..., R],
    /,
    *args: Any,
    **kwargs: Any,
) -> R:
    tok_gpus = _current_gpus.set(gpu_ids)
    tok_frac = _current_gpu_fraction.set(fraction)
    try:
        return fn(*args, **kwargs)
    finally:
        _current_gpus.reset(tok_gpus)
        _current_gpu_fraction.reset(tok_frac)


# ============================================================================
# Default GPU pool
# ============================================================================

_default_gpu_pool: core.GPUPool | None = None
_default_gpu_pool_lock = threading.Lock()


def _get_default_gpu_pool() -> core.GPUPool:
    global _default_gpu_pool
    with _default_gpu_pool_lock:
        if _default_gpu_pool is None:
            _default_gpu_pool = core.GPUPool.default()
        return _default_gpu_pool


def configure_gpu_pool(num_gpus: int) -> None:
    """Override the default GPU pool. Must be called before any GPU function runs."""
    global _default_gpu_pool
    with _default_gpu_pool_lock:
        _default_gpu_pool = core.GPUPool(num_gpus=num_gpus)


# ============================================================================
# GPU Runner
# ============================================================================


class EmptyContextThreadPoolExecutor(ThreadPoolExecutor):
    """A thread pool whose worker threads start in an empty context.

    Where threads inherit the starter's context (free-threaded 3.14+), a worker
    started while a component runs would otherwise keep that component's context,
    and with it the environment, alive as long as the pool lives.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        return Context().run(super().submit, fn, *args, **kwargs)


class _GPUAssignment(NamedTuple):
    gpu_id: int
    # Gives the GPU back to the pool it was acquired from.
    release: Callable[[], None]


class GPURunner(Runner):
    """Runner for GPU workloads with fractional allocation support.

    ``coco.GPU`` is shorthand for ``GPURunner(fraction=1.0)``.
    ``coco.GPU(0.5)`` creates a runner requesting half a GPU.

    The assigned GPU id(s) are available inside the function via
    ``coco.current_gpu()`` (first id) and ``coco.current_gpus()`` (full list).
    The allocated fraction is available via ``coco.current_gpu_fraction()``.
    Subprocess mode reports the same assignment as in-process mode: the id is
    carried in the payload and re-established in the child. Each GPU has its own
    workers, so a worker only serves the GPU its calls were given.
    ``CUDA_VISIBLE_DEVICES`` is left unset, so ``cuda:{coco.current_gpu()}`` names
    the same device in both modes.
    """

    _fraction: float
    _use_subprocess: bool | None
    _gpu_executor: ThreadPoolExecutor | None

    def __init__(self, fraction: float = 1.0) -> None:
        super().__init__()
        if not (0 < fraction <= 1.0):
            raise ValueError(f"fraction must be in (0, 1.0], got {fraction}")
        self._fraction = fraction
        self._use_subprocess = None
        self._gpu_executor = None

    def __call__(self, fraction: float = 1.0) -> GPURunner:
        return GPURunner(fraction=fraction)

    def _should_use_subprocess(self) -> bool:
        """Check if subprocess mode is enabled (reads env var lazily on first call)."""
        if self._use_subprocess is None:
            self._use_subprocess = (
                os.environ.get("COCOINDEX_RUN_GPU_IN_SUBPROCESS") == "1"
            )
        return self._use_subprocess

    def _get_gpu_executor(self) -> ThreadPoolExecutor:
        """Get or create the dedicated GPU thread pool."""
        if self._gpu_executor is None:
            self._gpu_executor = EmptyContextThreadPoolExecutor(
                thread_name_prefix="gpu"
            )
        return self._gpu_executor

    async def _acquire_gpu(self) -> _GPUAssignment:
        """Acquire a GPU.

        The release is bound to the pool the GPU came from, so it returns the capacity
        there even if `configure_gpu_pool` has replaced the default pool since.
        """
        gpu_pool = _get_default_gpu_pool()
        gpu_id = await gpu_pool.acquire(self._fraction)
        return _GPUAssignment(
            gpu_id, functools.partial(gpu_pool.release, gpu_id, self._fraction)
        )

    async def run(
        self, fn: Callable[P, Coroutine[Any, Any, R]], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute an async function.

        Acquires a GPU from the pool, sets current GPU context, then:
        - In-process (default): runs directly on the event loop.
        - Subprocess mode: via execute_in_subprocess (asyncio.run() in subprocess).
        """
        assignment = await self._acquire_gpu()
        gpu_ids = [assignment.gpu_id]
        if self._should_use_subprocess():
            return await execute_in_subprocess(
                gpu_ids,
                self._fraction,
                assignment.release,
                # The subprocess runs a coroutine function through `asyncio.run()`.
                fn,  # type: ignore[arg-type]
                *args,
                **kwargs,
            )
        tok_gpus = _current_gpus.set(gpu_ids)
        tok_frac = _current_gpu_fraction.set(self._fraction)
        try:
            return await fn(*args, **kwargs)
        finally:
            _current_gpus.reset(tok_gpus)
            _current_gpu_fraction.reset(tok_frac)
            assignment.release()

    async def run_sync_fn(
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute a sync function.

        Acquires a GPU from the pool, sets current GPU context, then:
        - In-process (default): offloads to the dedicated GPU thread pool.
        - Subprocess mode: via execute_in_subprocess.
        """
        assignment = await self._acquire_gpu()
        gpu_ids = [assignment.gpu_id]
        if self._should_use_subprocess():
            return await execute_in_subprocess(
                gpu_ids,
                self._fraction,
                assignment.release,
                fn,
                *args,
                **kwargs,
            )
        try:
            future = self._get_gpu_executor().submit(
                _run_with_gpu_context, gpu_ids, self._fraction, fn, *args, **kwargs
            )
        except BaseException:
            assignment.release()
            raise
        # As in subprocess mode: a started thread cannot be interrupted, so the GPU goes
        # back when the thread finishes, not when this call returns or is cancelled.
        future.add_done_callback(lambda _: assignment.release())
        return await asyncio.wrap_future(future)


GPU = GPURunner(fraction=1.0)
