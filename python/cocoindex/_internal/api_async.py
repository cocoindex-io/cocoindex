from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Concatenate,
    Callable,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core, environment
from .app import AppBase
from .pending_marker import ResolvesTo
from .component_ctx import (
    ComponentSubpath,
    build_child_path,
    get_context_from_ctx,
)
from .stable_path import StableKey
from .function import (
    AnyCallable,
    create_core_component_processor,
    async_function as function,
)
from .typing import NOT_SET, NotSetType


P = ParamSpec("P")
K = TypeVar("K")
T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")


class ComponentMountRunHandle(Generic[ReturnT]):
    """Handle for a processing unit that was started with `mount_run()`. Allows awaiting the result."""

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
        """Get the result of the processing unit. Can be called multiple times."""
        async with self._lock:
            if isinstance(self._cached_result, NotSetType):
                self._cached_result = await self._core.result_async(self._parent_ctx)
            return self._cached_result


class ComponentMountHandle:
    """Handle for processing unit(s) started with `mount()` or `mount_each()`. Allows waiting until ready."""

    __slots__ = ("_cores", "_lock", "_ready_called")

    _cores: list[core.ComponentMountHandle]
    _lock: asyncio.Lock
    _ready_called: bool

    def __init__(self, core_handles: list[core.ComponentMountHandle]) -> None:
        self._cores = core_handles
        self._lock = asyncio.Lock()
        self._ready_called = False

    async def ready(self) -> None:
        """Wait until all processing units are ready. Can be called multiple times."""
        async with self._lock:
            if not self._ready_called:
                for c in self._cores:
                    await c.ready_async()
                self._ready_called = True


@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Sequence[ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Sequence[ReturnT]]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Mapping[K, ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Mapping[K, ReturnT]]: ...
@overload
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[ReturnT]: ...
def mount_run(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountRunHandle[Any]:
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
        target = await coco.mount_run(
            coco.component_subpath("setup"), declare_table_target, table_name
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
    return ComponentMountRunHandle(core_handle, parent_ctx._core_processor_ctx)


def mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
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
    return ComponentMountHandle([core_handle])


def mount_each(
    fn: AnyCallable[Concatenate[T, P], Any],
    items: Iterable[tuple[StableKey, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
    """
    Mount one independent component per item in a keyed iterable.

    Sugar over a loop of mount() calls. Each item's key is used as the component subpath.

    Args:
        fn: The function to run for each item. The item value is passed as the first argument.
        items: A keyed iterable of (key, value) pairs. The key becomes the component subpath.
        *args: Additional arguments passed to fn after the item value.
        **kwargs: Additional keyword arguments passed to fn.

    Returns:
        A handle that can be used to wait until all processing units are ready.

    Example:
        coco_aio.mount_each(process_file, files.items(), target_table)

        # Equivalent to:
        # for key, item in files.items():
        #     coco_aio.mount(coco.component_subpath(key), process_file, item, target_table)
    """
    parent_ctx = get_context_from_ctx()
    core_handles: list[core.ComponentMountHandle] = []
    for key, item in items:
        child_path = build_child_path(parent_ctx, ComponentSubpath(key))
        processor = create_core_component_processor(
            fn, parent_ctx._env, child_path, (item, *args), kwargs
        )
        core_handle = core.mount(
            processor,
            child_path,
            parent_ctx._core_processor_ctx,
            parent_ctx._core_fn_call_ctx,
        )
        core_handles.append(core_handle)
    return ComponentMountHandle(core_handles)


async def map(
    fn: Callable[Concatenate[T, P], Awaitable[ReturnT]],
    items: Iterable[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> list[ReturnT]:
    """
    Run a function concurrently on each item in an iterable.
    No processing components are created — this is pure concurrent execution
    (async tasks) within the current component.

    Args:
        fn: The function to apply to each item. The item is passed as the first argument.
        items: The items to iterate.
        *args: Additional passthrough arguments to fn (appended after the item).
        **kwargs: Additional passthrough keyword arguments to fn.

    Returns:
        Results from each invocation.
    """
    return list(await asyncio.gather(*(fn(item, *args, **kwargs) for item in items)))


class App(AppBase[P, ReturnT]):
    async def update(
        self, *, report_to_stdout: bool = False, full_reprocess: bool = False
    ) -> ReturnT:
        """
        Update the app (run the app once to process all pending changes).

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
            full_reprocess: If True, reprocess everything and invalidate existing caches.

        Returns:
            The result of the main function.
        """
        env, core_app = await self._get_core_env_app()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        return await core_app.update_async(
            processor, report_to_stdout=report_to_stdout, full_reprocess=full_reprocess
        )

    async def drop(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        _env, core_app = await self._get_core_env_app()
        await core_app.drop_async(report_to_stdout=report_to_stdout)


async def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    await environment.start()


async def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    await environment.stop()


async def default_env() -> environment.Environment:
    """Get the default environment (starting it if needed)."""
    return await environment.start()


@asynccontextmanager
async def runtime() -> AsyncIterator[None]:
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
    "function",
    "map",
    "mount",
    "mount_each",
    "mount_run",
    "start",
    "stop",
    "default_env",
    "runtime",
]
