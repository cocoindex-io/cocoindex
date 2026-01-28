from __future__ import annotations

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

from . import core
from .app import AppBase
from .component_ctx import (
    ComponentSubpath,
    build_child_path,
    get_context_from_ctx,
)
from .function import AnyCallable, create_core_component_processor
from .pending_marker import ResolvesTo
from . import environment as _environment
from .typing import NOT_SET, NotSetType
from contextlib import contextmanager


P = ParamSpec("P")
K = TypeVar("K")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")


class ProcessingUnitMountRunHandle(Generic[ReturnT]):
    """Handle for a processing unit that was started with `mount_run()`. Allows getting the result."""

    __slots__ = ("_core", "_lock", "_cached_result", "_parent_ctx")

    _core: core.ComponentMountRunHandle[ReturnT]
    _lock: threading.Lock
    _cached_result: ReturnT | NotSetType
    _parent_ctx: core.ComponentProcessorContext

    def __init__(
        self,
        core_handle: core.ComponentMountRunHandle[ReturnT],
        parent_ctx: core.ComponentProcessorContext,
    ) -> None:
        self._core = core_handle
        self._lock = threading.Lock()
        self._cached_result = NOT_SET
        self._parent_ctx = parent_ctx

    def result(self) -> ReturnT:
        """Get the result of the processing unit. Can be called multiple times."""
        with self._lock:
            if isinstance(self._cached_result, NotSetType):
                self._cached_result = self._core.result(self._parent_ctx)
            return self._cached_result


class ProcessingUnitMountHandle:
    """Handle for a processing unit that was started with `mount()`. Allows waiting until ready."""

    __slots__ = ("_core", "_lock", "_ready_called")

    _core: core.ComponentMountHandle
    _lock: threading.Lock
    _ready_called: bool

    def __init__(self, core_handle: core.ComponentMountHandle) -> None:
        self._core = core_handle
        self._lock = threading.Lock()
        self._ready_called = False

    def wait_until_ready(self) -> None:
        """Wait until the processing unit is ready. Can be called multiple times."""
        with self._lock:
            if not self._ready_called:
                self._core.wait_until_ready()
                self._ready_called = True


@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountRunHandle[ReturnT]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Sequence[ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Mapping[K, ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountRunHandle[Mapping[K, ReturnT]]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountRunHandle[ReturnT]: ...
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountRunHandle[Any]:
    """
    Mount and run a processing unit, returning a handle to await its result.

    Args:
        subpath: The component subpath (from component_subpath()).
        processor_fn: The function to run as the processing unit processor.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to get the result.

    Example:
        target = coco.mount_run(
            coco.component_subpath("setup"), declare_dir_target, outdir
        ).result()
    """
    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    core_handle = core.mount_run(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
    )
    return ProcessingUnitMountRunHandle(core_handle, parent_ctx._core_processor_ctx)


def mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ProcessingUnitMountHandle:
    """
    Mount a processing unit in the background and return a handle to wait until ready.

    Args:
        subpath: The component subpath (from component_subpath()).
        processor_fn: The function to run as the processing unit processor.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        A handle that can be used to wait until the processing unit is ready.

    Example:
        with coco.component_subpath("process"):
            for f in files:
                coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)
    """
    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    core_handle = core.mount(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
    )
    return ProcessingUnitMountHandle(core_handle)


class App(AppBase[P, ReturnT]):
    def update(self, *, report_to_stdout: bool = False) -> ReturnT:
        """
        Update the app (run the app once to process all pending changes).

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.

        Returns:
            The result of the main function.
        """
        env, core_app = self._get_core_env_app_sync()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        return core_app.update(processor, report_to_stdout=report_to_stdout)

    def drop(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        _env, core_app = self._get_core_env_app_sync()
        core_app.drop(report_to_stdout=report_to_stdout)


def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    _environment.start_sync()


def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    _environment.stop_sync()


def default_env() -> _environment.Environment:
    """Get the default environment (starting it if needed)."""
    return _environment.default_env_sync()


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
    "ProcessingUnitMountHandle",
    "ProcessingUnitMountRunHandle",
    "mount",
    "mount_run",
    "start",
    "stop",
    "default_env",
    "runtime",
]
