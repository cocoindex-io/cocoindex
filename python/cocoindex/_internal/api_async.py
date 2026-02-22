from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterable, Coroutine
from typing import (
    Any,
    AsyncIterator,
    Concatenate,
    Callable,
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
from .stable_path import Symbol
from .target_state import (
    TargetState,
    TargetStateProvider,
    TargetHandler,
    declare_target_state_with_child,
)
from .typing import NOT_SET, NotSetType


P = ParamSpec("P")
K = TypeVar("K")
T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")

_ValueT = TypeVar("_ValueT")
_ChildHandlerT = TypeVar("_ChildHandlerT", bound="TargetHandler[Any, Any, Any] | None")


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
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Sequence[ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Sequence[ReturnT]: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Mapping[K, ResolvesTo[ReturnT]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Mapping[K, ReturnT]: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Any:
    """
    Mount a dependent processing component and return its result.

    The child component cannot refresh independently — re-executing the child
    requires re-executing the parent. The ``use_`` prefix (consistent with
    ``use_context()``) signals that the caller creates a dependency on the
    child's result.

    Args:
        subpath: The component subpath (from component_subpath()).
        processor_fn: The function to run as the processing unit processor.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The return value of processor_fn.

    Example:
        target = await coco_aio.use_mount(
            coco.component_subpath("setup"), declare_table_target, table_name
        )
    """
    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    core_handle = await core.mount_run_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
    )
    return await core_handle.result_async(parent_ctx._core_processor_ctx)


async def mount(
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
                await coco_aio.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)
    """
    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    core_handle = await core.mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
    )
    return ComponentMountHandle([core_handle])


async def mount_each(
    fn: AnyCallable[Concatenate[T, P], Any],
    items: Iterable[tuple[StableKey, T]] | AsyncIterable[tuple[StableKey, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle:
    """
    Mount one independent component per item in a keyed iterable.

    Sugar over a loop of mount() calls. Each item's key is used as the component subpath.
    Accepts both sync and async iterables; prefers async iteration when available.

    Args:
        fn: The function to run for each item. The item value is passed as the first argument.
        items: A keyed iterable of (key, value) pairs (sync or async). The key becomes the
            component subpath.
        *args: Additional arguments passed to fn after the item value.
        **kwargs: Additional keyword arguments passed to fn.

    Returns:
        A handle that can be used to wait until all processing units are ready.

    Example:
        await coco_aio.mount_each(process_file, files.items(), target_table)

        # Equivalent to:
        # for key, item in files.items():
        #     coco_aio.mount(coco.component_subpath(key), process_file, item, target_table)
    """
    parent_ctx = get_context_from_ctx()
    core_handles: list[core.ComponentMountHandle] = []

    async def _mount_one(key: StableKey, item: Any) -> None:
        child_path = build_child_path(parent_ctx, ComponentSubpath(key))
        processor = create_core_component_processor(
            fn, parent_ctx._env, child_path, (item, *args), kwargs
        )
        core_handle = await core.mount_async(
            processor,
            child_path,
            parent_ctx._core_processor_ctx,
            parent_ctx._core_fn_call_ctx,
        )
        core_handles.append(core_handle)

    if isinstance(items, AsyncIterable):
        async for key, item in items:
            await _mount_one(key, item)
    else:
        for key, item in items:
            await _mount_one(key, item)
    return ComponentMountHandle(core_handles)


async def map(
    fn: Callable[Concatenate[T, P], Coroutine[Any, Any, ReturnT]],
    items: Iterable[T] | AsyncIterable[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> list[ReturnT]:
    """
    Run a function concurrently on each item in an iterable.
    No processing components are created — this is pure concurrent execution
    (async tasks) within the current component.

    Args:
        fn: The function to apply to each item. The item is passed as the first argument.
        items: The items to iterate (sync or async).
        *args: Additional passthrough arguments to fn (appended after the item).
        **kwargs: Additional passthrough keyword arguments to fn.

    Returns:
        Results from each invocation.
    """
    tasks: list[asyncio.Task[ReturnT]] = []
    async with asyncio.TaskGroup() as tg:
        if isinstance(items, AsyncIterable):
            async for item in items:
                tasks.append(tg.create_task(fn(item, *args, **kwargs)))
        else:
            for item in items:
                tasks.append(tg.create_task(fn(item, *args, **kwargs)))
    return [t.result() for t in tasks]


_MOUNT_TARGET_SYMBOL = Symbol("cocoindex/mount_target")


async def mount_target(
    target_state: TargetState[TargetHandler[_ValueT, Any, _ChildHandlerT]],
) -> TargetStateProvider[_ValueT, _ChildHandlerT]:
    """
    Mount a target, ensuring its container target state is applied before returning
    the child TargetStateProvider.

    Sugar over ``use_mount()`` combined with ``declare_target_state_with_child()``.
    The component subpath is derived automatically from the target's globally unique key.

    Args:
        target_state: A TargetState with a child handler, as created by
            ``TargetStateProvider.target_state(key, value)``. The key must be globally
            unique (target connectors ensure this by construction).

    Returns:
        The resolved child TargetStateProvider, ready to use for declaring child
        target states.

    Example::

        provider = await coco_aio.mount_target(
            target_db.table_target(table_name=TABLE_NAME, table_schema=schema)
        )
    """
    subpath = ComponentSubpath(_MOUNT_TARGET_SYMBOL) / (
        *target_state._provider._core.stable_key_chain(),
        target_state._key,
    )
    return await use_mount(subpath, declare_target_state_with_child, target_state)  # type: ignore[no-any-return, return-value]


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
    "function",
    "map",
    "mount",
    "mount_each",
    "mount_target",
    "use_mount",
    "start",
    "stop",
    "default_env",
    "runtime",
]
