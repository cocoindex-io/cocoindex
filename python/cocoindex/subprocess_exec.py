"""
Lightweight subprocess-backed executor stub.

- Uses a single global ProcessPoolExecutor (max_workers=1), created lazily.
- In the subprocess, maintains a registry of executor instances keyed by
  (executor_factory, pickled spec) to enable reuse.
  even if key collision happens.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from typing import Any, Callable
import pickle
import threading
import asyncio
import os
import time
from .user_app_loader import load_user_app
import logging
import multiprocessing as mp

WATCHDOG_INTERVAL_SECONDS = 10.0

# ---------------------------------------------
# Main process: single, lazily-created pool
# ---------------------------------------------
_pool_lock = threading.Lock()
_pool: ProcessPoolExecutor | None = None
_user_apps: list[str] = []
_logger = logging.getLogger(__name__)


def _get_pool() -> ProcessPoolExecutor:
    global _pool  # pylint: disable=global-statement
    with _pool_lock:
        if _pool is None:
            # Single worker process as requested
            _pool = ProcessPoolExecutor(
                max_workers=1,
                initializer=_subprocess_init,
                initargs=(_user_apps, os.getpid()),
                mp_context=mp.get_context("spawn"),
            )
        return _pool


def add_user_app(app_target: str) -> None:
    with _pool_lock:
        _user_apps.append(app_target)


def _restart_pool(old_pool: ProcessPoolExecutor | None = None) -> None:
    """Safely restart the global ProcessPoolExecutor.

    Thread-safe via `_pool_lock`. Shuts down the old pool and re-creates a new
    one with the same initializer/args.
    """
    global _pool
    with _pool_lock:
        # If another thread already swapped the pool, skip restart
        if old_pool is not None and _pool is not old_pool:
            return
        _logger.error("Detected dead subprocess pool; restarting and retrying.")
        prev_pool = _pool
        _pool = ProcessPoolExecutor(
            max_workers=1,
            initializer=_subprocess_init,
            initargs=(_user_apps, os.getpid()),
            mp_context=mp.get_context("spawn"),
        )
        if prev_pool is not None:
            # Best-effort shutdown of previous pool; letting exceptions bubble up
            # is acceptable here and signals irrecoverable executor state.
            prev_pool.shutdown(cancel_futures=True)


async def _submit_with_restart(fn: Callable[..., Any], *args: Any) -> Any:
    """Submit and await work, restarting the subprocess until it succeeds.

    Retries on BrokenProcessPool or pool-shutdown RuntimeError; re-raises other
    exceptions.
    """
    while True:
        pool = _get_pool()
        try:
            fut = pool.submit(fn, *args)
            return await asyncio.wrap_future(fut)
        except BrokenProcessPool:
            _restart_pool(old_pool=pool)
            # loop and retry


# ---------------------------------------------
# Subprocess: executor registry and helpers
# ---------------------------------------------


def _start_parent_watchdog(
    parent_pid: int, interval_seconds: float = WATCHDOG_INTERVAL_SECONDS
) -> None:
    """Terminate this process if the parent process exits or PPID changes.

    This runs in a background daemon thread so it never blocks pool work.
    """

    import psutil

    if parent_pid is None:
        parent_pid = os.getppid()

    try:
        p = psutil.Process(parent_pid)
        # Cache create_time to defeat PID reuse.
        created = p.create_time()
    except psutil.Error:
        # Parent already gone or not accessible
        os._exit(1)

    def _watch() -> None:
        while True:
            try:
                # is_running() + same create_time => same process and still alive
                if not (p.is_running() and p.create_time() == created):
                    os._exit(1)
            except psutil.NoSuchProcess:
                os._exit(1)
            time.sleep(interval_seconds)

    threading.Thread(target=_watch, name="parent-watchdog", daemon=True).start()


def _subprocess_init(user_apps: list[str], parent_pid: int) -> None:
    import signal
    import faulthandler

    faulthandler.enable()
    # Ignore SIGINT in the subprocess on best-effort basis.
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass

    _start_parent_watchdog(parent_pid)

    # In case any user app is already in this subprocess, e.g. the subprocess is forked, we need to avoid loading it again.
    with _pool_lock:
        already_loaded_apps = set(_user_apps)

    loaded_apps = []
    for app_target in user_apps:
        if app_target not in already_loaded_apps:
            load_user_app(app_target)
            loaded_apps.append(app_target)

    with _pool_lock:
        _user_apps.extend(loaded_apps)


class _OnceResult:
    _result: Any = None
    _done: bool = False

    def run_once(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if self._done:
            return self._result
        self._result = _call_method(method, *args, **kwargs)
        self._done = True
        return self._result


@dataclass
class _ExecutorEntry:
    executor: Any


_SUBPROC_EXECUTORS: dict[bytes, _ExecutorEntry] = {}


def _call_method(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run an awaitable/coroutine to completion synchronously, otherwise return as-is."""
    try:
        if asyncio.iscoroutinefunction(method):
            return asyncio.run(method(*args, **kwargs))
        else:
            return method(*args, **kwargs)
    except Exception as e:
        raise RuntimeError(
            f"Error calling method `{method.__name__}` from subprocess"
        ) from e


def _get_or_create_entry(key_bytes: bytes) -> _ExecutorEntry:
    entry = _SUBPROC_EXECUTORS.get(key_bytes)
    if entry is None:
        executor_factory, spec = pickle.loads(key_bytes)
        inst = executor_factory()
        inst.spec = spec
        entry = _ExecutorEntry(executor=inst)
        _SUBPROC_EXECUTORS[key_bytes] = entry
    return entry


def _sp_call(key_bytes: bytes, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    entry = _get_or_create_entry(key_bytes)
    return _call_method(entry.executor.__call__, *args, **kwargs)


# ---------------------------------------------
# Public stub
# ---------------------------------------------


class _ExecutorStub:
    _key_bytes: bytes

    def __init__(self, executor_factory: type[Any], spec: Any) -> None:
        self._key_bytes = pickle.dumps(
            (executor_factory, spec), protocol=pickle.HIGHEST_PROTOCOL
        )

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await _submit_with_restart(_sp_call, self._key_bytes, args, kwargs)


def executor_stub(executor_factory: type[Any], spec: Any) -> Any:
    """
    Create a subprocess-backed stub for the given executor class/spec.

    - Lazily initializes a singleton ProcessPoolExecutor (max_workers=1).
    - Returns a stub object exposing async __call__ and async prepare; analyze is
      exposed if present on the original class.
    """
    return _ExecutorStub(executor_factory, spec)
