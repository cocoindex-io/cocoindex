from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import (
    Any,
    Concatenate,
    Generic,
    Mapping,
    Sequence,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .app import AppBase
from .pending_marker import ResolvesTo
from .scope import Scope
from .function import Function
from .typing import NOT_SET, NotSetType
from . import environment as _env


P = ParamSpec("P")
K = TypeVar("K")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")


class ComponentMountRunHandle(Generic[ReturnT]):
    """Handle for a component that was started with `mount_run()`. Allows awaiting the result."""

    __slots__ = ("_core", "_lock", "_cached_result", "_parent_ctx")

    _core: core.ComponentMountRunHandle[ReturnT]
    _lock: asyncio.Lock
    _cached_result: ReturnT | NotSetType
    _parent_ctx: core.ComponentProcessorContext

    def __init__(
        self,
        core_handle: core.ComponentMountRunHandle[ReturnT],
        parent_ctx: core.ComponentProcessorContext,
    ) -> None:
        self._core = core_handle
        self._lock = asyncio.Lock()
        self._cached_result = NOT_SET
        self._parent_ctx = parent_ctx

    async def result(self) -> ReturnT:
        """Get the result of the component. Can be called multiple times."""
        async with self._lock:
            if isinstance(self._cached_result, NotSetType):
                self._cached_result = await self._core.result_async(self._parent_ctx)
            return self._cached_result


class ComponentMountHandle:
    """Handle for a component that was started with `mount()`. Allows waiting until ready."""

    __slots__ = ("_core", "_lock", "_ready_called")

    _core: core.ComponentMountHandle
    _lock: asyncio.Lock
    _ready_called: bool

    def __init__(self, core_handle: core.ComponentMountHandle) -> None:
        self._core = core_handle
        self._lock = asyncio.Lock()
        self._ready_called = False

    async def ready(self) -> None:
        """Wait until the component is ready. Can be called multiple times."""
        async with self._lock:
            if not self._ready_called:
                await self._core.ready_async()
                self._ready_called = True


@overload
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], ResolvesTo[ReturnT]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
@overload
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], Sequence[ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], Sequence[ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], Mapping[K, ResolvesTo[ReturnT]]],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Mapping[K, ReturnT]]: ...
@overload
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
    scope: Scope,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
def mount_run(
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
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
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
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
    async def run(self) -> ReturnT:
        root_path = core.StablePath()
        processor = self._main_fn._as_core_component_processor(
            root_path,
            *self._app_args,
            **self._app_kwargs,
        )
        core_app = await self._get_core()
        return await core_app.run_async(processor)


async def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    await _env.start()


async def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    await _env.stop()


async def default_env() -> _env.Environment:
    """Get the default environment (starting it if needed)."""
    return await _env.default_env()


@asynccontextmanager
async def runtime() -> Any:
    """
    Async context manager that calls `start()` on enter and `stop()` on exit.
    """
    await start()
    try:
        yield
    finally:
        await stop()


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
