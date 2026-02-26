"""
Runner base class and GPU runner implementation.

Runners execute functions in specific contexts. Each runner owns a BatchQueue
that serializes execution.

The GPU runner runs in-process by default with an async lock for serialization.
Set COCOINDEX_RUN_GPU_IN_SUBPROCESS=1 for subprocess isolation.
"""

from __future__ import annotations

import asyncio
import functools
import pickle
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Callable, Coroutine, TypeVar, ParamSpec
import threading
import os
import multiprocessing as mp
from . import core

P = ParamSpec("P")
R = TypeVar("R")

# Flag indicating if we're running inside a subprocess (GPU runner)
# When True, @coco.fn decorators should execute the raw function
# without batching/runner/memo since those are already handled by the parent.
_in_subprocess: bool = False


class Runner(ABC):
    """Base class for runners that execute functions.

    Each runner owns a BatchQueue that serializes execution of all functions
    using this runner. The queue is created lazily on first use.

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

        All functions using this runner share this queue, ensuring
        serial execution of workloads.
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
# GPU Runner
# ============================================================================


class GPURunner(Runner):
    """Singleton runner for GPU workloads.

    By default, runs in-process. Serialization is handled by the BatchQueue
    (inherited from Runner) and the dedicated single-worker thread pool for
    sync functions. Set COCOINDEX_RUN_GPU_IN_SUBPROCESS=1 for subprocess isolation.
    """

    _instance: GPURunner | None = None
    _use_subprocess: bool | None
    _gpu_executor: ThreadPoolExecutor | None

    def __new__(cls) -> GPURunner:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Only initialize once (singleton)
        if not hasattr(self, "_queue"):
            super().__init__()
            self._use_subprocess = None
            self._gpu_executor = None

    def _should_use_subprocess(self) -> bool:
        """Check if subprocess mode is enabled (reads env var lazily on first call)."""
        if self._use_subprocess is None:
            self._use_subprocess = (
                os.environ.get("COCOINDEX_RUN_GPU_IN_SUBPROCESS") == "1"
            )
        return self._use_subprocess

    def _get_gpu_executor(self) -> ThreadPoolExecutor:
        """Get or create the dedicated GPU thread pool (single worker)."""
        if self._gpu_executor is None:
            self._gpu_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="gpu"
            )
        return self._gpu_executor

    async def run(
        self, fn: Callable[P, Coroutine[Any, Any, R]], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute an async function.

        Default: in-process directly.
        Subprocess mode: via execute_in_subprocess (asyncio.run() in subprocess).
        """
        if self._should_use_subprocess():
            # Type ignore: execute_in_subprocess handles async fns via asyncio.run() internally
            return await execute_in_subprocess(fn, *args, **kwargs)  # type: ignore[arg-type]
        return await fn(*args, **kwargs)

    async def run_sync_fn(
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute a sync function.

        Default: in-process on a dedicated single-worker GPU thread.
        Subprocess mode: via execute_in_subprocess.
        """
        if self._should_use_subprocess():
            return await execute_in_subprocess(fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_gpu_executor(), functools.partial(fn, *args, **kwargs)
        )


# Singleton instance for public use
GPU = GPURunner()
