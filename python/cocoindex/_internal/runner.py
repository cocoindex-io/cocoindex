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
import subprocess
import threading
import multiprocessing as mp
import warnings
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextvars import ContextVar
from typing import Any, Callable, Coroutine, TypeVar, ParamSpec

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
_pool_lock = threading.Lock()
_pool: ProcessPoolExecutor | None = None


def _get_pool() -> ProcessPoolExecutor:
    """Get or create the singleton subprocess pool."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ProcessPoolExecutor(
                max_workers=1,
                initializer=_subprocess_init,
                initargs=(os.getpid(),),
                mp_context=mp.get_context("spawn"),
            )
        return _pool


def _restart_pool(old_pool: ProcessPoolExecutor | None = None) -> None:
    """Restart the subprocess pool if it died."""
    global _pool
    with _pool_lock:
        if old_pool is not None and _pool is not old_pool:
            return  # Another thread already restarted
        prev_pool = _pool
        _pool = ProcessPoolExecutor(
            max_workers=1,
            initializer=_subprocess_init,
            initargs=(os.getpid(),),
            mp_context=mp.get_context("spawn"),
        )
        if prev_pool is not None:
            prev_pool.shutdown(cancel_futures=True)


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
    fn, args, kwargs = pickle.loads(payload_bytes)
    result = fn(*args, **kwargs)
    # Handle async callables (functions or callable objects with async __call__)
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)


async def _submit_to_pool_async(fn: Callable[..., Any], *args: Any) -> Any:
    """Submit work to pool and wait asynchronously."""
    loop = asyncio.get_running_loop()
    while True:
        pool = _get_pool()
        try:
            return await loop.run_in_executor(pool, fn, *args)
        except BrokenProcessPool:
            _restart_pool(old_pool=pool)


async def execute_in_subprocess(fn: Callable[..., R], *args: Any, **kwargs: Any) -> R:
    """Execute a function in a subprocess and return the result.

    The function and all arguments must be picklable.
    """
    payload = pickle.dumps((fn, args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
    result_bytes = await _submit_to_pool_async(_execute_in_subprocess, payload)
    return pickle.loads(result_bytes)  # type: ignore[no-any-return]


def in_subprocess() -> bool:
    """Check if we're running in a subprocess."""
    return _in_subprocess

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
    gpu_ids: list[int], fraction: float, fn: Callable[..., R], *args: Any, **kwargs: Any
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


class GPURunner(Runner):
    """Runner for GPU workloads with fractional allocation support.

    ``coco.GPU`` is shorthand for ``GPURunner(fraction=1.0)``.
    ``coco.GPU(0.5)`` creates a runner requesting half a GPU.

    The assigned GPU id(s) are available inside the function via
    ``coco.current_gpu()`` (first id) and ``coco.current_gpus()`` (full list).
    The allocated fraction is available via ``coco.current_gpu_fraction()``.
    For multi-GPU subprocess mode (where ``CUDA_VISIBLE_DEVICES`` must be set
    per-process), use in-process mode (the default) until per-GPU subprocess
    pools are implemented.
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
            self._gpu_executor = ThreadPoolExecutor(thread_name_prefix="gpu")
        return self._gpu_executor

    async def _acquire_gpu(self) -> int:
        return await _get_default_gpu_pool().acquire(self._fraction)

    async def _release_gpu(self, gpu_id: int) -> None:
        await _get_default_gpu_pool().release(gpu_id, self._fraction)

    async def run(
        self, fn: Callable[P, Coroutine[Any, Any, R]], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute an async function.

        Acquires a GPU from the pool, sets current GPU context, then:
        - In-process (default): runs directly on the event loop.
        - Subprocess mode: via execute_in_subprocess (asyncio.run() in subprocess).
        """
        gpu_id = await self._acquire_gpu()
        gpu_ids = [gpu_id]
        tok_gpus = _current_gpus.set(gpu_ids)
        tok_frac = _current_gpu_fraction.set(self._fraction)
        try:
            if self._should_use_subprocess():
                _warn_subprocess_multi_gpu()
                # Type ignore: execute_in_subprocess handles async fns via asyncio.run() internally
                return await execute_in_subprocess(fn, *args, **kwargs)  # type: ignore[arg-type]
            return await fn(*args, **kwargs)
        finally:
            _current_gpus.reset(tok_gpus)
            _current_gpu_fraction.reset(tok_frac)
            await self._release_gpu(gpu_id)

    async def run_sync_fn(
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute a sync function.

        Acquires a GPU from the pool, sets current GPU context, then:
        - In-process (default): offloads to the dedicated GPU thread pool.
        - Subprocess mode: via execute_in_subprocess.
        """
        gpu_id = await self._acquire_gpu()
        gpu_ids = [gpu_id]
        try:
            if self._should_use_subprocess():
                _warn_subprocess_multi_gpu()
                return await execute_in_subprocess(fn, *args, **kwargs)
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._get_gpu_executor(),
                functools.partial(
                    _run_with_gpu_context, gpu_ids, self._fraction, fn, *args, **kwargs
                ),
            )
        finally:
            await self._release_gpu(gpu_id)


GPU = GPURunner(fraction=1.0)

_subprocess_multi_gpu_warned = False


def _warn_subprocess_multi_gpu() -> None:
    global _subprocess_multi_gpu_warned
    if _subprocess_multi_gpu_warned:
        return
    _subprocess_multi_gpu_warned = True
    pool = _get_default_gpu_pool()
    if pool.num_gpus > 1:
        warnings.warn(
            f"COCOINDEX_RUN_GPU_IN_SUBPROCESS=1 with num_gpus={pool.num_gpus}: "
            "subprocess mode does not yet support per-GPU CUDA_VISIBLE_DEVICES. "
            "All subprocess calls run on the same GPU regardless of pool "
            "assignment. Use in-process mode for multi-GPU support.",
            UserWarning,
            stacklevel=4,
        )
