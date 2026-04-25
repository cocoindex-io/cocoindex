from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterable, Coroutine
from typing import (
    Any,
    Awaitable,
    Concatenate,
    Callable,
    Iterable,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core, environment
from .app import App, AppConfig, DropHandle, UpdateHandle, show_progress
from .update_stats import ComponentStats, UpdateSnapshot, UpdateStats, UpdateStatus
from .pending_marker import ResolvesTo
from .component_ctx import (
    ComponentSubpath,
    ExceptionContext,
    ExceptionHandler,
    ExceptionHandlerChain,
    MountKind,
    build_child_path,
    get_context_from_ctx,
    exception_handler,
)

_logger = logging.getLogger(__name__)


def _resolve_handler(
    handler_chain: ExceptionHandlerChain,
    *,
    env_name: str,
    stable_path: str,
    processor_name: str | None,
    mount_kind: MountKind,
    parent_stable_path: str | None,
) -> Callable[[str], Awaitable[None]]:
    """
    Wrap a handler chain into a single async callable invoked by Rust.

    The returned callable takes a stringified error (from Rust) and runs:
    - innermost handler first (head of the chain)
    - if a handler raises, calls the next outer handler with the new exception
    - if all handlers raise, logs the original component error (built-in fallback)
    """

    async def _run(err_str: str) -> None:
        original_exc: BaseException = RuntimeError(err_str)
        current_exc: BaseException = original_exc
        source: Literal["component", "handler"] = "component"
        node: ExceptionHandlerChain | None = handler_chain
        while node is not None:
            ctx = ExceptionContext(
                env_name=env_name,
                stable_path=stable_path,
                processor_name=processor_name,
                mount_kind=mount_kind,
                parent_stable_path=parent_stable_path,
                is_background=True,
                source=source,
                original_exception=None if source == "component" else original_exc,
            )
            try:
                ret = node.handler(current_exc, ctx)
                if inspect.isawaitable(ret):
                    await ret
                return
            except BaseException as handler_exc:
                current_exc = handler_exc
                source = "handler"
                node = node.base
        # All handlers raised — fall back to built-in log, matching the no-handler path.
        _logger.error("component build failed:\n%s", err_str, exc_info=current_exc)

    return _run


from .stable_path import StableKey
from .function import (
    AnyCallable,
    AsyncCallable,
    LogicTracking,
    create_core_component_processor,
    fn,
    fn_ret_deserializer,
)
from .stable_path import Symbol
from .target_state import (
    TargetState,
    TargetStateProvider,
    TargetHandler,
    declare_target_state_with_child,
)
from .live_component import (
    LiveComponent,
    LiveComponentOperator,
    LiveMapFeed,
    LiveMapView,
    LiveMapSubscriber,
    _MountEachLiveComponent,
    is_live_component_class,
)

# ============================================================================
# Re-exports from internal modules (shared types)
# ============================================================================

from .context_keys import ContextKey, ContextProvider

from .target_state import (
    ChildTargetDef,
    TargetReconcileOutput,
    TargetActionSink,
    PendingTargetStateProvider,
    declare_target_state,
    register_root_target_states_provider,
)

from .environment import Environment, EnvironmentBuilder, LifespanFn
from .environment import lifespan

from .runner import GPU, Runner

from .memo_fingerprint import (
    memo_fingerprint,
    register_memo_key_function,
    NotMemoKeyable,
)

from .serde import unpickle_safe, serialize_by_pickle

from .pending_marker import PendingS, ResolvedS, MaybePendingS

from .component_ctx import (
    ComponentContext,
    component_subpath,
    use_context,
    get_component_context,
)

from .setting import Settings

from .stable_path import ROOT_PATH, StablePath

from .typing import NonExistenceType, NON_EXISTENCE, is_non_existence, MemoStateOutcome


# ============================================================================
# Mount APIs (async only)
# ============================================================================

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
    processor_fn: AsyncCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: Callable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: AsyncCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    subpath: ComponentSubpath,
    processor_fn: Callable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    processor_fn: AsyncCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    processor_fn: Callable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    processor_fn: AsyncCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def use_mount(
    processor_fn: Callable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
async def use_mount(*pos_args: Any, **kwargs: Any) -> Any:
    """
    Mount a dependent processing component and return its result.

    The child component cannot refresh independently — re-executing the child
    requires re-executing the parent. The ``use_`` prefix (consistent with
    ``use_context()``) signals that the caller creates a dependency on the
    child's result.

    Accepts an optional ``ComponentSubpath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        processor_fn: The function to run as the processing unit processor.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The return value of processor_fn.

    Example:
        target = await coco.use_mount(declare_table_target, table_name)

        # With explicit subpath:
        target = await coco.use_mount(
            coco.component_subpath("setup"), declare_table_target, table_name
        )
    """
    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = getattr(processor_fn, "__name__", None)
        if name is None:
            raise TypeError(
                "use_mount() requires a ComponentSubpath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = ComponentSubpath(Symbol(name))

    if is_live_component_class(processor_fn):
        raise TypeError(
            "LiveComponent classes cannot be used with use_mount(). "
            "Use mount() instead."
        )

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    core_handle = await core.use_mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
    )
    pyvalue = await core_handle.result_async(parent_ctx._core_processor_ctx)
    return pyvalue.get(fn_ret_deserializer(processor_fn))


async def _mount_live_component(
    parent_ctx: ComponentContext,
    child_path: core.StablePath,
    instance: Any,
) -> ComponentMountHandle:
    """Mount a pre-constructed LiveComponent instance."""
    controller, readiness_handle = await core.mount_live_async(
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        parent_ctx._core_processor_ctx.live,
    )

    operator = LiveComponentOperator(controller, instance, parent_ctx._env, child_path)

    process_live_coro = instance.process_live(operator)
    controller.start(process_live_coro)

    return ComponentMountHandle([readiness_handle])


@overload
async def mount(
    subpath: ComponentSubpath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle: ...


@overload
async def mount(
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle: ...


async def mount(*pos_args: Any, **kwargs: Any) -> ComponentMountHandle:
    """
    Mount a processing unit in the background and return a handle to wait until ready.

    Accepts an optional ``ComponentSubpath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        processor_fn: The function to run as the processing unit processor.
            Can also be a LiveComponent class.
        *args: Arguments to pass to the function (or LiveComponent constructor).
        **kwargs: Keyword arguments to pass to the function (or LiveComponent constructor).

    Returns:
        A handle that can be used to wait until the processing unit is ready.

    Example:
        await coco.mount(process_file, file, target)

        # With explicit subpath:
        await coco.mount(coco.component_subpath("process", filename), process_file, file, target)
    """
    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = getattr(processor_fn, "__name__", None)
        if name is None:
            raise TypeError(
                "mount() requires a ComponentSubpath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = ComponentSubpath(Symbol(name))

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    if is_live_component_class(processor_fn):
        instance = processor_fn(*args, **kwargs)
        return await _mount_live_component(parent_ctx, child_path, instance)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    resolved = (
        _resolve_handler(
            parent_ctx._exception_handler_chain,
            env_name=parent_ctx._env.name,
            stable_path=child_path.to_string(),
            processor_name=getattr(processor_fn, "__qualname__", None),
            mount_kind="mount",
            parent_stable_path=parent_ctx._core_path.to_string(),
        )
        if parent_ctx._exception_handler_chain
        else None
    )
    core_handle = await core.mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        resolved,
    )
    return ComponentMountHandle([core_handle])


_ItemsType = (
    Iterable[tuple[StableKey, T]]
    | AsyncIterable[tuple[StableKey, T]]
    | LiveMapFeed[StableKey, T]
)


@overload
async def mount_each(
    subpath: ComponentSubpath,
    fn: AnyCallable[Concatenate[T, P], Any],
    items: _ItemsType[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle: ...


@overload
async def mount_each(
    fn: AnyCallable[Concatenate[T, P], Any],
    items: _ItemsType[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ComponentMountHandle: ...


async def mount_each(*pos_args: Any, **kwargs: Any) -> ComponentMountHandle:
    """
    Mount one independent component per item in a keyed iterable.

    Accepts an optional ``ComponentSubpath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    When *items* is a ``LiveMapFeed`` or ``LiveMapView``, an internal
    ``LiveComponent`` is created to handle live watching automatically.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        fn: The function to run for each item. The item value is passed as the first argument.
        items: A keyed iterable of (key, value) pairs, or a LiveMapFeed/LiveMapView for live mode.
        *args: Additional arguments passed to fn after the item value.
        **kwargs: Additional keyword arguments passed to fn.

    Returns:
        A handle that can be used to wait until all processing units are ready.
    """
    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        fn = pos_args[1]
        items = pos_args[2]
        extra_args = pos_args[3:]
    else:
        fn = pos_args[0]
        items = pos_args[1]
        extra_args = pos_args[2:]
        name = getattr(fn, "__name__", None)
        if name is None:
            raise TypeError(
                "mount_each() requires a ComponentSubpath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = ComponentSubpath(Symbol(name))

    if is_live_component_class(fn):
        raise TypeError(
            "LiveComponent classes cannot be used with mount_each(). "
            "Use mount() instead."
        )

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    if isinstance(items, LiveMapFeed):
        instance = _MountEachLiveComponent(items, fn, extra_args, kwargs)
        return await _mount_live_component(parent_ctx, child_path, instance)

    core_handles: list[core.ComponentMountHandle] = []

    async def _mount_one(key: StableKey, item: Any) -> None:
        item_path = child_path.concat(key)
        processor = create_core_component_processor(
            fn, parent_ctx._env, item_path, (item, *extra_args), kwargs
        )
        resolved = (
            _resolve_handler(
                parent_ctx._exception_handler_chain,
                env_name=parent_ctx._env.name,
                stable_path=item_path.to_string(),
                processor_name=getattr(fn, "__qualname__", None),
                mount_kind="mount_each",
                parent_stable_path=parent_ctx._core_path.to_string(),
            )
            if parent_ctx._exception_handler_chain
            else None
        )
        core_handle = await core.mount_async(
            processor,
            item_path,
            parent_ctx._core_processor_ctx,
            parent_ctx._core_fn_call_ctx,
            resolved,
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

        provider = await coco.mount_target(
            target_db.table_target(table_name=TABLE_NAME, table_schema=schema)
        )
    """
    subpath = ComponentSubpath(_MOUNT_TARGET_SYMBOL) / (
        *target_state._provider._core.stable_key_chain(),
        target_state._key,
    )
    return await use_mount(subpath, declare_target_state_with_child, target_state)  # type: ignore[no-any-return, return-value]


# ============================================================================
# Start / Stop / Runtime
# ============================================================================


async def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    await environment.start()


async def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    await environment.stop()


def start_blocking() -> None:
    """Start the default environment synchronously (and enter its lifespan, if any)."""
    environment.start_sync()


def stop_blocking() -> None:
    """Stop the default environment synchronously (and exit its lifespan, if any)."""
    environment.stop_sync()


async def default_env() -> environment.Environment:
    """Get the default environment (starting it if needed)."""
    return await environment.start()


class _DualModeRuntime:
    """Context manager that works with both `with` and `async with`."""

    def __enter__(self) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass  # No running loop — sync usage is fine
        else:
            raise RuntimeError(
                "Cannot use sync 'with coco.runtime()' from within an async event loop. "
                "Use 'async with coco.runtime()' instead."
            )
        start_blocking()
        return None

    def __exit__(self, *exc: Any) -> None:
        stop_blocking()

    async def __aenter__(self) -> None:
        await start()
        return None

    async def __aexit__(self, *exc: Any) -> None:
        await stop()


def runtime() -> _DualModeRuntime:
    """
    Dual-mode context manager that calls start/stop.

    Use ``with coco.runtime():`` for sync code, or
    ``async with coco.runtime():`` for async code.
    """
    return _DualModeRuntime()


# ============================================================================
# __all__
# ============================================================================

__all__ = [
    # .app
    "App",
    "AppConfig",
    "DropHandle",
    "UpdateHandle",
    "show_progress",
    # .update_stats
    "ComponentStats",
    "UpdateSnapshot",
    "UpdateStats",
    "UpdateStatus",
    # .function
    "fn",
    "LogicTracking",
    # .context_keys
    "ContextKey",
    "ContextProvider",
    # .target_state
    "ChildTargetDef",
    "TargetState",
    "TargetStateProvider",
    "TargetReconcileOutput",
    "TargetHandler",
    "TargetActionSink",
    "PendingTargetStateProvider",
    "declare_target_state",
    "declare_target_state_with_child",
    "register_root_target_states_provider",
    # .environment
    "Environment",
    "EnvironmentBuilder",
    "LifespanFn",
    "lifespan",
    # .runner
    "GPU",
    "Runner",
    # .serde
    "unpickle_safe",
    "serialize_by_pickle",
    # .memo_fingerprint
    "memo_fingerprint",
    "register_memo_key_function",
    "NotMemoKeyable",
    # .pending_marker
    "MaybePendingS",
    "PendingS",
    "ResolvedS",
    "ResolvesTo",
    # .component_ctx
    "ComponentContext",
    "ComponentSubpath",
    "ExceptionContext",
    "ExceptionHandler",
    "component_subpath",
    "exception_handler",
    "use_context",
    "get_component_context",
    # .setting
    "Settings",
    # .stable_path
    "ROOT_PATH",
    "StablePath",
    "StableKey",
    "Symbol",
    # .typing
    "NON_EXISTENCE",
    "NonExistenceType",
    "is_non_existence",
    "MemoStateOutcome",
    # .live_component
    "LiveComponent",
    "LiveComponentOperator",
    "LiveMapFeed",
    "LiveMapView",
    "LiveMapSubscriber",
    # Mount APIs
    "ComponentMountHandle",
    "mount",
    "mount_each",
    "mount_target",
    "map",
    "use_mount",
    # Start/stop/runtime
    "start",
    "stop",
    "start_blocking",
    "stop_blocking",
    "default_env",
    "runtime",
]
