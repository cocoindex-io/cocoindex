"""
Environment module.
"""

from __future__ import annotations

from inspect import isasyncgenfunction
import asyncio
import threading
import warnings
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Iterator,
    AsyncIterator,
    cast,
    overload,
)


from . import core  # type: ignore
from . import setting
from ..engine_object import dump_engine_object


class _LoopRunner:
    """
    Owns an event loop and optionally a daemon thread running it.

    This is used both for:
    - Per-Environment loops (when a non-running loop is provided or created)
    - The global background loop used for sync / cross-thread scheduling
    """

    _loop: asyncio.AbstractEventLoop
    _thread: threading.Thread | None

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._thread = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    def ensure_running(self) -> None:
        if self._loop.is_running() or self._loop.is_closed():
            return

        def _runner(loop: asyncio.AbstractEventLoop) -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._thread = threading.Thread(target=_runner, args=(self._loop,), daemon=True)
        self._thread.start()

    @classmethod
    def from_running_loop(cls, loop: asyncio.AbstractEventLoop) -> "_LoopRunner":
        runner = cls(loop)
        # Already running; no thread needed.
        return runner

    @classmethod
    def create_new_running(cls) -> "_LoopRunner":
        runner = cls(asyncio.new_event_loop())
        runner.ensure_running()
        return runner


class EnvironmentBuilder:
    """Builder for the Environment."""

    _settings: setting.Settings

    def __init__(self, settings: setting.Settings | None = None):
        self._settings = settings or setting.Settings.from_env()

    @property
    def settings(self) -> setting.Settings:
        return self._settings


LifespanFn = (
    Callable[[EnvironmentBuilder], Iterator[None]]
    | Callable[[EnvironmentBuilder], AsyncIterator[None]]
)


def _noop_lifespan_fn(_builder: EnvironmentBuilder) -> Iterator[None]:
    yield


class Environment:
    """
    CocoIndex runtime environment.

    Note: lifecycle is NOT driven by this class. Use `start()` / `stop()` (or the
    API `runtime()` context managers) to control the default environment lifespan.
    """

    _core_env: core.Environment
    _settings: setting.Settings
    _loop_runner: _LoopRunner

    def __init__(
        self,
        settings: setting.Settings,
        event_loop: asyncio.AbstractEventLoop | None = None,
    ):
        if not settings.db_path:
            raise ValueError("Settings.db_path must be provided")
        self._settings = settings

        if event_loop is None:
            try:
                event_loop = asyncio.get_running_loop()
            except RuntimeError:
                event_loop = asyncio.new_event_loop()

        if event_loop.is_running():
            self._loop_runner = _LoopRunner.from_running_loop(event_loop)
        else:
            # Keep a loop running for sync users (needed for async callbacks).
            runner = _LoopRunner(event_loop)
            runner.ensure_running()
            self._loop_runner = runner

        async_context = core.AsyncContext(self._loop_runner.loop)
        self._core_env = core.Environment(dump_engine_object(settings), async_context)

    @property
    def settings(self) -> setting.Settings:
        return self._settings

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop_runner.loop


_default_env_lifespan_fn_lock: threading.Lock = threading.Lock()
_default_env_lifespan_fn: LifespanFn | None = None

_default_env_start_stop_lock: asyncio.Lock = asyncio.Lock()
_default_env_lifespan_iter: Iterator[None] | None = None
_default_env_async_lifespan_iter: AsyncGenerator[None, None] | None = None
_default_env_lock: threading.Lock = threading.Lock()
_default_env: Environment | None = None


@overload
def lifespan(fn: LifespanFn) -> LifespanFn: ...
@overload
def lifespan(fn: None) -> Callable[[LifespanFn], LifespanFn]: ...
def lifespan(fn: LifespanFn | None = None) -> Any:
    """
    Decorate a function that returns a lifespan.
    It registers the function as a lifespan provider.
    """

    def _inner(fn: LifespanFn) -> LifespanFn:
        global _default_env_lifespan_fn  # pylint: disable=global-statement
        with _default_env_lifespan_fn_lock:
            if _default_env_lifespan_fn is not None:
                warnings.warn(
                    f"Overriding the default lifespan function {_default_env_lifespan_fn} with {fn}."
                )
            _default_env_lifespan_fn = fn
        return fn

    if fn is not None:
        return _inner(fn)
    else:
        return _inner


async def start() -> Environment:
    """
    Start the default environment (executes on the default environment's event loop).
    """
    global _default_env  # pylint: disable=global-statement
    global _default_env_lifespan_iter  # pylint: disable=global-statement
    global _default_env_async_lifespan_iter  # pylint: disable=global-statement

    async with _default_env_start_stop_lock:
        with _default_env_lock:
            if _default_env is not None:
                return _default_env
        with _default_env_lifespan_fn_lock:
            fn = _default_env_lifespan_fn or _noop_lifespan_fn

        env_builder = EnvironmentBuilder()
        if isasyncgenfunction(fn):
            ait = cast(AsyncGenerator[None, None], fn(env_builder))
            _default_env_async_lifespan_iter = ait
            await anext(ait)
        else:
            it = cast(Iterator[None], fn(env_builder))
            _default_env_lifespan_iter = it
            next(it)

        built_settings = env_builder.settings
        if not built_settings.db_path:
            raise ValueError("Environment settings must provide Settings.db_path")

        loop = asyncio.get_running_loop()
        env = Environment(built_settings, event_loop=loop)
        with _default_env_lock:
            _default_env = env
        return env


async def stop() -> None:
    """
    Stop the default environment (executes on the default environment's event loop).
    """
    global _default_env  # pylint: disable=global-statement
    global _default_env_lifespan_iter  # pylint: disable=global-statement
    global _default_env_async_lifespan_iter  # pylint: disable=global-statement

    async with _default_env_start_stop_lock:
        it = _default_env_lifespan_iter
        _default_env_lifespan_iter = None
        ait = _default_env_async_lifespan_iter
        _default_env_async_lifespan_iter = None
        with _default_env_lock:
            _default_env = None

    if it is not None:
        try:
            next(it)
        except StopIteration:
            pass
        finally:
            close = getattr(it, "close", None)
            if callable(close):
                close()

    if ait is not None:
        try:
            await anext(ait)
        except StopAsyncIteration:
            pass
        finally:
            await ait.aclose()


_bg_loop_lock: threading.Lock = threading.Lock()
_bg_loop_runner: _LoopRunner | None = None


def _default_env_loop() -> asyncio.AbstractEventLoop:
    """
    Ensure we have a long-lived background event loop for sync / cross-loop callers.

    Important: we do NOT reuse a "currently running" loop here, because callers
    (e.g. pytest-asyncio) may create short-lived loops that get closed.
    """
    global _bg_loop_runner  # pylint: disable=global-statement

    with _bg_loop_lock:
        if _bg_loop_runner is not None and not _bg_loop_runner.loop.is_closed():
            return _bg_loop_runner.loop

        _bg_loop_runner = _LoopRunner.create_new_running()
        return _bg_loop_runner.loop


async def default_env() -> Environment:
    """
    Get the default environment.
    """
    return await start()


def start_sync() -> Environment:
    loop = _default_env_loop()
    fut = asyncio.run_coroutine_threadsafe(start(), loop)
    return fut.result()


def stop_sync() -> None:
    with _default_env_lock:
        env = _default_env
    if env is None:
        return
    loop = env.event_loop
    fut = asyncio.run_coroutine_threadsafe(stop(), loop)
    fut.result()


def default_env_sync() -> Environment:
    return start_sync()


def reset_default_lifespan_for_tests() -> None:
    """
    Reset the registered default lifespan function.

    This is intended for tests so lifespan registration does not leak across test modules.
    """
    global _default_env_lifespan_fn  # pylint: disable=global-statement
    with _default_env_lifespan_fn_lock:
        _default_env_lifespan_fn = None
