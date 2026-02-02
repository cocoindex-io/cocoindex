"""
Runner base class and GPU runner implementation.

Runners execute functions in specific contexts (e.g., subprocess for GPU isolation).
Each runner owns a BatchQueue that serializes execution.
"""

from __future__ import annotations

import asyncio
import pickle
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Callable, Coroutine, TypeVar, TYPE_CHECKING
import threading
import os
import multiprocessing as mp

if TYPE_CHECKING:
    from . import core

R = TypeVar("R")


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
                    from . import core

                    self._queue = core.BatchQueue()
        return self._queue

    @abstractmethod
    async def run(
        self, fn: Callable[..., Coroutine[Any, Any, R]], *args: Any, **kwargs: Any
    ) -> R:
        """Execute an async function with args/kwargs.

        This is async because it needs to await the async function's result.
        Caller must be in an async context.
        """
        ...

    @abstractmethod
    def run_sync_fn(self, fn: Callable[..., R], *args: Any, **kwargs: Any) -> R:
        """Execute a sync function with args/kwargs.

        This is sync because the function itself is sync.
        Works from both sync and async contexts.
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
    if asyncio.iscoroutinefunction(fn):
        result = asyncio.run(fn(*args, **kwargs))
    else:
        result = fn(*args, **kwargs)
    return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)


def _submit_to_pool_sync(fn: Callable[..., Any], *args: Any) -> Any:
    """Submit work to pool and wait synchronously."""
    while True:
        pool = _get_pool()
        try:
            future = pool.submit(fn, *args)
            return future.result()
        except BrokenProcessPool:
            _restart_pool(old_pool=pool)


def execute_in_subprocess(fn: Callable[..., R], *args: Any, **kwargs: Any) -> R:
    """Execute a function in a subprocess and return the result.

    The function and all arguments must be picklable.
    """
    payload = pickle.dumps((fn, args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
    result_bytes = _submit_to_pool_sync(_execute_in_subprocess, payload)
    return pickle.loads(result_bytes)  # type: ignore[no-any-return]


# ============================================================================
# GPU Runner
# ============================================================================


class GPURunner(Runner):
    """Singleton runner that executes in subprocess for GPU isolation.

    All functions using this runner share the same queue (inherited from Runner),
    ensuring serial execution of GPU workloads.
    """

    _instance: GPURunner | None = None

    def __new__(cls) -> GPURunner:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Only initialize once (singleton)
        if not hasattr(self, "_queue"):
            super().__init__()

    async def run(
        self, fn: Callable[..., Coroutine[Any, Any, R]], *args: Any, **kwargs: Any
    ) -> R:
        """Execute an async function in subprocess.

        The async function is run via asyncio.run() in the subprocess.
        """
        # Type ignore: execute_in_subprocess handles async fns by running asyncio.run() internally
        return execute_in_subprocess(fn, *args, **kwargs)  # type: ignore[return-value]

    def run_sync_fn(self, fn: Callable[..., R], *args: Any, **kwargs: Any) -> R:
        """Execute a sync function in subprocess."""
        return execute_in_subprocess(fn, *args, **kwargs)


# Singleton instance for public use
GPU = GPURunner()
