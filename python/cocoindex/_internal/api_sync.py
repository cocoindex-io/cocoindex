from __future__ import annotations

import threading
from typing import (
    Any,
    Concatenate,
    Generic,
    Mapping,
    ParamSpec,
    Sequence,
    TypeVar,
    overload,
)

from . import core
from .app import AppBase
from .scope import Scope
from .function import Function
from .pending_marker import ResolvesTo


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
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
@overload
def mount_run(
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], Sequence[ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], Sequence[ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], Mapping[K, ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Mapping[K, ReturnT]]: ...
@overload
def mount_run(
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
def mount_run(
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Any]:
    """
    Mount and run a component, returning a handle to await its result.

    Args:
        scope: The scope for the component (includes stable path and processor context).
        processor_fn: The function to run as the component processor.
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
    scope: Scope,
    processor_fn: Function[Concatenate[Scope, P], ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
    """
    Mount a component in the background and return a handle to wait until ready.

    Args:
        scope: The scope for the component (includes stable path and processor context).
        processor_fn: The function to run as the component processor.
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
    def run(self, *args: P.args, **kwargs: P.kwargs) -> ReturnT:
        root_path = core.StablePath()
        processor = self._main_fn._as_core_component_processor(
            root_path, *args, **kwargs
        )
        return self._core.run(processor)  # type: ignore


__all__ = [
    "App",
    "ComponentMountHandle",
    "ComponentMountRunHandle",
    "mount",
    "mount_run",
]
