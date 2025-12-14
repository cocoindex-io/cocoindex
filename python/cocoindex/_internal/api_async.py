import asyncio
from typing import (
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
)

from . import core
from .app import AppBase
from .context import component_ctx_var
from .function import Function
from .stable_path import StablePath


P = ParamSpec("P")
R = TypeVar("R")

_NOT_SET = object()


class ComponentMountRunHandle(Generic[R]):
    """Handle for a component that was started with `mount_run()`. Allows awaiting the result."""

    __slots__ = ("_core", "_lock", "_cached_result")

    _core: core.ComponentMountRunHandle
    _lock: asyncio.Lock
    _cached_result: R | object

    def __init__(self, core_handle: core.ComponentMountRunHandle) -> None:
        self._core = core_handle
        self._lock = asyncio.Lock()
        self._cached_result = _NOT_SET

    async def result(self) -> R:
        """Get the result of the component. Can be called multiple times."""
        parent_ctx = component_ctx_var.get()
        async with self._lock:
            if self._cached_result is _NOT_SET:
                self._cached_result = await self._core.result_async(parent_ctx)
            return self._cached_result  # type: ignore


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


def mount_run(
    processor_fn: Function[Concatenate[StablePath, P], R],
    stable_path: StablePath,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[R]:
    """
    Mount and run a component, returning a handle to await its result.

    Args:
        processor_fn: The function to run as the component processor.
        stable_path: The stable path for the component.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to get the result.
    """
    parent_ctx = component_ctx_var.get()
    processor = processor_fn._as_core_component_processor(stable_path, *args, **kwargs)
    core_handle = core.mount_run(processor, stable_path._core, parent_ctx)
    return ComponentMountRunHandle(core_handle)


def mount(
    processor_fn: Function[Concatenate[StablePath, P], R],
    stable_path: StablePath,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
    """
    Mount a component in the background and return a handle to wait until ready.

    Args:
        processor_fn: The function to run as the component processor.
        stable_path: The stable path for the component.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to wait until the component is ready.
    """
    parent_ctx = component_ctx_var.get()
    processor = processor_fn._as_core_component_processor(stable_path, *args, **kwargs)
    core_handle = core.mount(processor, stable_path._core, parent_ctx)
    return ComponentMountHandle(core_handle)


class App(AppBase[P, R]):
    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        processor = self._main_fn._as_core_component_processor(
            StablePath(), *args, **kwargs
        )
        return await self._core.run_async(processor)  # type: ignore


__all__ = [
    "App",
    "ComponentMountHandle",
    "ComponentMountRunHandle",
    "mount",
    "mount_run",
]
