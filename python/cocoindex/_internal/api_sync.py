from __future__ import annotations

import asyncio
import threading
from typing import (
    Any,
    Generic,
    Mapping,
    ParamSpec,
    Sequence,
    TypeVar,
    overload,
)

from . import core  # type: ignore
from .app import AppBase
from .scope import Scope
from .function import Function
from .pending_marker import ResolvesTo
from . import environment as _env
from contextlib import contextmanager


P = ParamSpec("P")
K = TypeVar("K")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")

_NOT_SET = object()


class ComponentMountRunHandle(Generic[ReturnT]):
    """Handle for a component that was started with `mount_run()`. Allows getting the result."""

    __slots__ = ("_core", "_lock", "_cached_result", "_parent_ctx")

    _core: core.ComponentMountRunHandle
    _lock: threading.Lock
    _cached_result: ReturnT | object
    _parent_ctx: core.ComponentProcessorContext

    def __init__(
        self,
        core_handle: core.ComponentMountRunHandle,
        parent_ctx: core.ComponentProcessorContext,
    ) -> None:
        self._core = core_handle
        self._lock = threading.Lock()
        self._cached_result = _NOT_SET
        self._parent_ctx = parent_ctx

    def result(self) -> ReturnT:
        """Get the result of the component. Can be called multiple times."""
        with self._lock:
            if self._cached_result is _NOT_SET:
                self._cached_result = self._core.result(self._parent_ctx)
            return self._cached_result  # type: ignore


class ComponentMountHandle:
    """Handle for a component that was started with `mount()`. Allows waiting until ready."""

    __slots__ = ("_core", "_lock", "_ready_called")

    _core: core.ComponentMountHandle
    _lock: threading.Lock
    _ready_called: bool

    def __init__(self, core_handle: core.ComponentMountHandle) -> None:
        self._core = core_handle
        self._lock = threading.Lock()
        self._ready_called = False

    def ready(self) -> None:
        """Wait until the component is ready. Can be called multiple times."""
        with self._lock:
            if not self._ready_called:
                self._core.ready()
                self._ready_called = True


@overload
def mount_run(
    processor_fn: Function[P, ResolvesTo[ReturnT]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
@overload
def mount_run(
    processor_fn: Function[P, Sequence[ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[P, Sequence[ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[P, Mapping[K, ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Mapping[K, ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[P, ReturnT],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
def mount_run(
    processor_fn: Function[P, ReturnT],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Any]:
    """
    Mount and run a component, returning a handle to await its result.

    Args:
        processor_fn: The function to run as the component processor.
        scope: The scope for the component (includes stable path and processor context).
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to get the result.
    """
    parent_ctx = scope._core_processor_ctx
    processor = processor_fn._as_core_component_processor(
        scope._core_path, *args, **kwargs
    )
    core_handle = core.mount_run(processor, scope._core_path, parent_ctx)
    return ComponentMountRunHandle(core_handle, parent_ctx)


def mount(
    processor_fn: Function[P, ReturnT],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
    """
    Mount a component in the background and return a handle to wait until ready.

    Args:
        processor_fn: The function to run as the component processor.
        scope: The scope for the component (includes stable path and processor context).
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to wait until the component is ready.
    """
    parent_ctx = scope._core_processor_ctx
    processor = processor_fn._as_core_component_processor(
        scope._core_path, *args, **kwargs
    )
    core_handle = core.mount(processor, scope._core_path, parent_ctx)
    return ComponentMountHandle(core_handle)


class App(AppBase[P, ReturnT]):
    def run(self) -> ReturnT:
        root_path = core.StablePath()
        processor = self._main_fn._as_core_component_processor(
            root_path,
            *self._app_args,
            **self._app_kwargs,  # type: ignore[arg-type]
        )
        if self._environment is not None:
            loop = self._environment.event_loop
        else:
            loop = _env.default_env_sync().event_loop
        core_app = asyncio.run_coroutine_threadsafe(self._get_core(), loop).result()
        return core_app.run(processor)  # type: ignore


def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    _env.start_sync()


def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    _env.stop_sync()


def default_env() -> _env.Environment:
    """Get the default environment (starting it if needed)."""
    return _env.default_env_sync()


@contextmanager
def runtime() -> Any:
    """
    Context manager that calls `start()` on enter and `stop()` on exit.
    """
    start()
    try:
        yield
    finally:
        stop()


__all__ = [
    "App",
    "ComponentMountHandle",
    "ComponentMountRunHandle",
    "mount",
    "mount_run",
    "start",
    "stop",
    "default_env",
    "runtime",
]
