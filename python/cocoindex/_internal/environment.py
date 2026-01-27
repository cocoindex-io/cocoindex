"""
Environment module.
"""

from __future__ import annotations

from inspect import isasyncgenfunction
import asyncio
import threading
import warnings
import weakref
from contextlib import AsyncExitStack
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Callable,
    ContextManager,
    Iterator,
    AsyncIterator,
    overload,
)

from . import core
from . import setting
from ..engine_object import dump_engine_object
from .context_keys import ContextKey, ContextProvider

if TYPE_CHECKING:
    from cocoindex._internal.app import AppBase


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
    _context_provider: ContextProvider

    def __init__(self, settings: setting.Settings | None = None):
        self._settings = settings or setting.Settings.from_env()
        self._context_provider = ContextProvider()

    @property
    def settings(self) -> setting.Settings:
        return self._settings

    def provide(self, key: ContextKey[Any], value: Any) -> Any:
        return self._context_provider.provide(key, value)

    def provide_with(self, key: ContextKey[Any], cm: ContextManager[Any]) -> Any:
        return self._context_provider.provide_with(key, cm)

    async def provide_async_with(
        self, key: ContextKey[Any], cm: AsyncContextManager[Any]
    ) -> Any:
        return await self._context_provider.provide_async_with(key, cm)


LifespanFn = (
    Callable[[EnvironmentBuilder], Iterator[None]]
    | Callable[[EnvironmentBuilder], AsyncIterator[None]]
)


def _noop_lifespan_fn(_builder: EnvironmentBuilder) -> Iterator[None]:
    yield


_environment_info_lock = threading.Lock()
_environment_infos: list[EnvironmentInfo] = []


class EnvironmentInfo:
    """
    Per-environment information, for both already initialized and not-yet (lazily) initialized environments.
    """

    __slots__ = ("_env_ref", "_app_registry", "_app_registry_lock")

    _env_ref: weakref.ReferenceType[Environment | LazyEnvironment]
    _app_registry: weakref.WeakValueDictionary[str, AppBase[Any, Any]]
    _app_registry_lock: threading.Lock

    def __init__(self, env: Environment | LazyEnvironment) -> None:
        self._env_ref = weakref.ref(env)
        self._app_registry = weakref.WeakValueDictionary()
        self._app_registry_lock = threading.Lock()
        with _environment_info_lock:
            _environment_infos.append(self)

    def register_app(self, name: str, app: AppBase[Any, Any]) -> None:
        """Register an app with this environment."""
        with self._app_registry_lock:
            if name in self._app_registry:
                raise ValueError(
                    f"An app named '{name}' is already registered in this environment."
                )
            self._app_registry[name] = app

    def get_apps(self) -> list[AppBase[Any, Any]]:
        """Get all registered apps for this environment."""
        with self._app_registry_lock:
            return list(self._app_registry.values())

    @property
    def env(self) -> Environment | LazyEnvironment | None:
        """The environment, or None if it has been garbage collected."""
        return self._env_ref()

    @property
    def env_name(self) -> str | None:
        """The environment name, or None if it has been garbage collected."""
        env = self.env
        return env.name if env else None


def get_registered_environment_infos() -> list[EnvironmentInfo]:
    """Get all registered environment infos (that haven't been garbage collected)."""
    with _environment_info_lock:
        # Filter out infos whose environments have been garbage collected
        return [info for info in _environment_infos if info.env is not None]


class Environment:
    """
    CocoIndex runtime environment.

    Note: lifecycle is NOT driven by this class. Use `start()` / `stop()` (or the
    API `runtime()` context managers) to control the default environment lifespan.
    """

    __slots__ = (
        "_name",
        "_core_env",
        "_settings",
        "_context_provider",
        "_loop_runner",
        "_info",
        "__weakref__",
    )

    _name: str
    _core_env: core.Environment
    _settings: setting.Settings
    _context_provider: ContextProvider
    _loop_runner: _LoopRunner
    _info: EnvironmentInfo

    def __init__(
        self,
        settings: setting.Settings,
        *,
        name: str | None = None,
        context_provider: ContextProvider | None = None,
        event_loop: asyncio.AbstractEventLoop | None = None,
        info: EnvironmentInfo | None = None,
    ):
        if not settings.db_path:
            raise ValueError("Settings.db_path must be provided")
        self._name = name or ""
        self._settings = settings
        self._context_provider = context_provider or ContextProvider()

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
        self._info = info or EnvironmentInfo(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def settings(self) -> setting.Settings:
        return self._settings

    @property
    def context_provider(self) -> ContextProvider:
        return self._context_provider

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop_runner.loop

    async def _get_env(self) -> "Environment":
        return self

    def _get_env_sync(self) -> "Environment":
        return self


class LazyEnvironment:
    """
    Lazy-initialized environment. To be initialized using lifespan function.
    """

    __slots__ = (
        "_name",
        "_lifespan_fn_lock",
        "_lifespan_fn",
        "_start_stop_lock",
        "_exit_stack",
        "_env",
        "_info",
        "__weakref__",
    )

    _name: str
    _lifespan_fn_lock: threading.Lock
    _lifespan_fn: LifespanFn | None
    _start_stop_lock: asyncio.Lock
    _exit_stack: AsyncExitStack | None
    _env: Environment | None
    _info: EnvironmentInfo

    def __init__(self, name: str = "default") -> None:
        self._name = name
        self._lifespan_fn_lock = threading.Lock()
        self._start_stop_lock = asyncio.Lock()
        self._lifespan_fn = None
        self._exit_stack = None
        self._env = None
        self._info = EnvironmentInfo(self)

    @property
    def name(self) -> str:
        return self._name

    def lifespan(self, fn: LifespanFn) -> None:
        with self._lifespan_fn_lock:
            if self._lifespan_fn is not None:
                warnings.warn(
                    f"Overriding the default lifespan function {self._lifespan_fn} with {fn}."
                )
            self._lifespan_fn = fn

    async def _reset(self) -> None:
        await self.stop()
        with self._lifespan_fn_lock:
            self._lifespan_fn = None

    async def _get_env(self) -> Environment:
        """
        Start the default environment (executes on the default environment's event loop).
        """
        async with self._start_stop_lock:
            if self._env is not None:
                return self._env
            with self._lifespan_fn_lock:
                fn = self._lifespan_fn or _noop_lifespan_fn

            env_builder = EnvironmentBuilder()
            exit_stack = AsyncExitStack()
            self._exit_stack = exit_stack

            try:
                if isasyncgenfunction(fn):
                    # Start async generator and register cleanup
                    async_gen: AsyncGenerator[None, None] = fn(env_builder)  # type: ignore[assignment]
                    await anext(async_gen)

                    async def _aclose() -> None:
                        try:
                            await anext(async_gen)
                        except StopAsyncIteration:
                            pass
                        finally:
                            await async_gen.aclose()

                    exit_stack.push_async_callback(_aclose)
                else:
                    # Start sync generator and register cleanup
                    sync_gen: Iterator[None] = fn(env_builder)  # type: ignore[assignment]
                    next(sync_gen)

                    def _close() -> None:
                        try:
                            next(sync_gen)
                        except StopIteration:
                            pass
                        finally:
                            close_fn = getattr(sync_gen, "close", None)
                            if callable(close_fn):
                                close_fn()

                    exit_stack.callback(_close)

                built_settings = env_builder.settings
                if not built_settings.db_path:
                    default_db_path = setting.get_default_db_path()
                    if default_db_path:
                        built_settings.db_path = default_db_path
                    else:
                        raise ValueError(
                            "Environment settings must provide Settings.db_path "
                            "(or set COCOINDEX_DB environment variable)"
                        )

                context_provider = env_builder._context_provider
                self._exit_stack.push_async_callback(context_provider.aclose)

                loop = asyncio.get_running_loop()
                env = Environment(
                    built_settings,
                    name=self._name,
                    context_provider=context_provider,
                    event_loop=loop,
                    info=self._info,
                )
                self._env = env
                return env
            except:
                await exit_stack.aclose()
                self._exit_stack = None
                raise

    def _get_env_sync(self) -> Environment:
        if self._env is not None:
            return self._env
        env_loop = default_env_loop()
        fut = asyncio.run_coroutine_threadsafe(self._get_env(), env_loop)
        return fut.result()

    async def start(self) -> Environment:
        """
        Start the default environment (executes on the default environment's event loop).
        """
        return await self._get_env()

    async def stop(self) -> None:
        """
        Stop the default environment (executes on the default environment's event loop).
        """
        async with self._start_stop_lock:
            exit_stack = self._exit_stack
            self._exit_stack = None
            self._env = None

        if exit_stack is not None:
            await exit_stack.aclose()


_default_env = LazyEnvironment()


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
        _default_env.lifespan(fn)
        return fn

    if fn is not None:
        return _inner(fn)
    else:
        return _inner


async def start() -> Environment:
    """
    Start the default environment (executes on the default environment's event loop).
    """
    return await _default_env._get_env()


async def stop() -> None:
    """
    Stop the default environment (executes on the default environment's event loop).
    """
    await _default_env.stop()


_bg_loop_lock: threading.Lock = threading.Lock()
_bg_loop_runner: _LoopRunner | None = None


def default_env_loop() -> asyncio.AbstractEventLoop:
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


def start_sync() -> Environment:
    loop = default_env_loop()
    fut = asyncio.run_coroutine_threadsafe(_default_env.start(), loop)
    return fut.result()


def stop_sync() -> None:
    env = _default_env._env
    if env is None:
        return
    loop = env.event_loop
    fut = asyncio.run_coroutine_threadsafe(_default_env.stop(), loop)
    fut.result()


def default_env() -> LazyEnvironment:
    """Get the default lazy environment."""
    return _default_env


def default_env_sync() -> Environment:
    return start_sync()


def reset_default_env_for_tests() -> None:
    """
    Reset the registered default lifespan function.

    This is intended for tests so lifespan registration does not leak across test modules.
    """
    asyncio.run(_default_env._reset())
