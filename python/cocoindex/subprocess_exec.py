"""
Lightweight subprocess-backed executor stub.

- Uses a single global ProcessPoolExecutor (max_workers=1), created lazily.
- In the subprocess, maintains a registry of executor instances keyed by
  (executor_factory, pickled spec) to enable reuse.
- Caches analyze() and prepare() results per key to avoid repeated calls
  even if key collision happens.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable
import pickle
import threading
import asyncio
from .user_app_loader import load_user_app


# ---------------------------------------------
# Main process: single, lazily-created pool
# ---------------------------------------------
_pool_lock = threading.Lock()
_pool: ProcessPoolExecutor | None = None
_user_apps: list[str] = []


def _get_pool() -> ProcessPoolExecutor:
    global _pool
    with _pool_lock:
        if _pool is None:
            # Single worker process as requested
            _pool = ProcessPoolExecutor(
                max_workers=1, initializer=_subprocess_init, initargs=(_user_apps,)
            )
        return _pool


def add_user_app(app_target: str) -> None:
    with _pool_lock:
        _user_apps.append(app_target)


# ---------------------------------------------
# Subprocess: executor registry and helpers
# ---------------------------------------------


def _subprocess_init(user_apps: list[str]) -> None:
    for app_target in user_apps:
        load_user_app(app_target)


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
    prepare: _OnceResult = field(default_factory=_OnceResult)
    analyze: _OnceResult = field(default_factory=_OnceResult)
    ready_to_call: bool = False


_SUBPROC_EXECUTORS: dict[bytes, _ExecutorEntry] = {}


def _call_method(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run an awaitable/coroutine to completion synchronously, otherwise return as-is."""
    if asyncio.iscoroutinefunction(method):
        return asyncio.run(method(*args, **kwargs))
    else:
        return method(*args, **kwargs)


def _get_or_create_entry(key_bytes: bytes) -> _ExecutorEntry:
    entry = _SUBPROC_EXECUTORS.get(key_bytes)
    if entry is None:
        executor_factory, spec = pickle.loads(key_bytes)
        inst = executor_factory()
        inst.spec = spec
        entry = _ExecutorEntry(executor=inst)
        _SUBPROC_EXECUTORS[key_bytes] = entry
    return entry


def _sp_analyze(key_bytes: bytes) -> Any:
    entry = _get_or_create_entry(key_bytes)
    return entry.analyze.run_once(entry.executor.analyze)


def _sp_prepare(key_bytes: bytes) -> Any:
    entry = _get_or_create_entry(key_bytes)
    return entry.prepare.run_once(entry.executor.prepare)


def _sp_call(key_bytes: bytes, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    entry = _get_or_create_entry(key_bytes)
    # There's a chance that the subprocess crashes and restarts in the middle.
    # So we want to always make sure the executor is ready before each call.
    if not entry.ready_to_call:
        if analyze_fn := getattr(entry.executor, "analyze", None):
            entry.analyze.run_once(analyze_fn)
        if prepare_fn := getattr(entry.executor, "prepare", None):
            entry.prepare.run_once(prepare_fn)
        entry.ready_to_call = True
    return _call_method(entry.executor.__call__, *args, **kwargs)


# ---------------------------------------------
# Public stub
# ---------------------------------------------


class _ExecutorStub:
    _pool: ProcessPoolExecutor
    _key_bytes: bytes

    def __init__(self, executor_factory: type[Any], spec: Any) -> None:
        self._pool = _get_pool()
        self._key_bytes = pickle.dumps(
            (executor_factory, spec), protocol=pickle.HIGHEST_PROTOCOL
        )

        # Conditionally expose analyze if underlying class has it (sync-only in caller)
        if hasattr(executor_factory, "analyze"):
            # Bind as attribute so getattr(..., "analyze", None) works upstream
            def _analyze() -> Any:
                fut = self._pool.submit(_sp_analyze, self._key_bytes)
                return fut.result()

            # Attach method
            setattr(self, "analyze", _analyze)

        if hasattr(executor_factory, "prepare"):

            async def prepare() -> Any:
                fut = self._pool.submit(_sp_prepare, self._key_bytes)
                return await asyncio.wrap_future(fut)

            setattr(self, "prepare", prepare)

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        fut = self._pool.submit(_sp_call, self._key_bytes, args, kwargs)
        return await asyncio.wrap_future(fut)


def executor_stub(executor_factory: type[Any], spec: Any) -> Any:
    """
    Create a subprocess-backed stub for the given executor class/spec.

    - Lazily initializes a singleton ProcessPoolExecutor (max_workers=1).
    - Returns a stub object exposing async __call__ and async prepare; analyze is
      exposed if present on the original class.
    """
    return _ExecutorStub(executor_factory, spec)
