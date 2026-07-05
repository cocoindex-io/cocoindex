from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Coroutine
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Concatenate,
    Generic,
    Iterable,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from . import core, environment
from .app import App, AppConfig, DropHandle, UpdateHandle, show_progress
from .deadline import (
    DeadlineExceededError,
    capture as _capture_deadline,
    check_deadline,
    timeout,
)
from .update_stats import (
    ComponentStats,
    StatsGroupHandle,
    UpdateSnapshot,
    UpdateStats,
    UpdateStatus,
)
from .pending_marker import ResolvesTo
from .component_ctx import (
    ComponentSubpath,
    ExceptionContext,
    ExceptionHandler,
    build_child_path,
    get_context_from_ctx,
    exception_handler,
    stats_group,
)


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
    auto_refresh,
    check_not_in_process_live,
    is_live_component_class,
)
from cocoindex.connectorkits import default_subpath_name as _default_subpath_name

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

from .runner import (
    GPU,
    GPUPool,
    GPURunner,
    Runner,
    configure_gpu_pool,
    current_gpu,
    current_gpus,
    current_gpu_fraction,
)

from .memo_fingerprint import (
    memo_fingerprint,
    register_memo_key_function,
    NotMemoKeyable,
)

from .serde import (
    unpickle_safe,
    serialize_by_pickle,
    make_deserialize_fn,
    get_deserialize_fn,
    DeserializeFn,
)

from .pending_marker import PendingS, ResolvedS, MaybePendingS

from .component_ctx import (
    ComponentContext,
    component_subpath,
    use_context,
    get_component_context,
)

from .setting import Settings, LmdbSettings

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

    __slots__ = ("_cores", "_lock", "_next_ready_index")

    _cores: list[core.ComponentMountHandle]
    _lock: asyncio.Lock
    _next_ready_index: int

    def __init__(self, core_handles: list[core.ComponentMountHandle]) -> None:
        self._cores = core_handles
        self._lock = asyncio.Lock()
        self._next_ready_index = 0

    async def ready(self) -> None:
        """Wait until all processing units are ready. Can be called multiple times."""
        # Fail fast before waiting behind another ready() caller.
        check_deadline()
        async with self._lock:
            # The deadline may have expired while we were waiting for the lock.
            check_deadline()
            while self._next_ready_index < len(self._cores):
                # ready_async() consumes the Rust handle, so if a deadline fires
                # after this await, the next ready() call must resume here.
                await self._cores[self._next_ready_index].ready_async()
                self._next_ready_index += 1
                # The child is ready; now check the caller's own timeout before
                # it continues or waits on the next child.
                check_deadline()


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
    check_deadline()
    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = _default_subpath_name(processor_fn)
        if name is None:
            raise TypeError(
                "use_mount() requires a ComponentSubpath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = ComponentSubpath(Symbol(name))

    check_not_in_process_live("coco.use_mount")

    if is_live_component_class(processor_fn):
        raise TypeError(
            "LiveComponent classes cannot be used with use_mount(). "
            "Use mount() instead."
        )

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    processor = create_core_component_processor(
        processor_fn,
        parent_ctx._env,
        child_path,
        args,
        kwargs,
        deadline_snapshot=_capture_deadline(),
    )
    core_handle = await core.use_mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        _capture_deadline(),
    )
    pyvalue = await core_handle.result_async(parent_ctx._core_processor_ctx)
    return pyvalue.get(fn_ret_deserializer(processor_fn))


async def _mount_live_component(
    parent_ctx: ComponentContext,
    child_path: core.StablePath,
    instance: Any,
) -> ComponentMountHandle:
    """Mount a pre-constructed LiveComponent instance.

    Wraps `instance.process_live(operator)` in `_process_live_wrapper` so
    `_in_process_live = True` is set inside the asyncio Task that runs
    the body (the wrapper Coroutine inherits this `Context` value through
    asyncio's standard Task-context inheritance, and any `coco.mount*`
    call within will raise).
    """
    from .live_component import _process_live_wrapper

    controller, readiness_handle = await core.mount_live_async(
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        parent_ctx._core_processor_ctx.live,
    )

    operator = LiveComponentOperator(controller, instance, parent_ctx._env, child_path)

    controller.start(_process_live_wrapper(instance, operator))

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
    check_deadline()
    check_not_in_process_live("coco.mount")

    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = _default_subpath_name(processor_fn)
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
    resolved = parent_ctx.resolve_exception_handler(
        stable_path=child_path.to_string(),
        processor_name=getattr(processor_fn, "__qualname__", None),
        mount_kind="mount",
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
        fn: The function to run for each item — the item value is passed as the
            first argument. May also be a LiveComponent class, in which case one
            live component instance is created per item (the item value is passed
            as the first constructor argument, mirroring the plain-function shape).
        items: A keyed iterable of (key, value) pairs, or a LiveMapFeed/LiveMapView for live mode.
        *args: Additional arguments passed to fn after the item value.
        **kwargs: Additional keyword arguments passed to fn.

    Returns:
        A handle that can be used to wait until all processing units are ready.
    """
    check_deadline()
    check_not_in_process_live("coco.mount_each")

    if pos_args and isinstance(pos_args[0], ComponentSubpath):
        subpath = pos_args[0]
        fn = pos_args[1]
        items = pos_args[2]
        extra_args = pos_args[3:]
    else:
        fn = pos_args[0]
        items = pos_args[1]
        extra_args = pos_args[2:]
        name = _default_subpath_name(fn)
        if name is None:
            raise TypeError(
                "mount_each() requires a ComponentSubpath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = ComponentSubpath(Symbol(name))

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    if isinstance(items, LiveMapFeed):
        # Live data source: the per-item `fn` (whether a plain function or a
        # LiveComponent class) is dispatched through `mount()` / `operator.update()`
        # inside `_MountEachLiveComponent`, both of which already handle live
        # component classes — so no special-casing of `fn` is needed here.
        instance = _MountEachLiveComponent(items, fn, extra_args, kwargs)
        return await _mount_live_component(parent_ctx, child_path, instance)

    # Static data source: mount one component per item. When `fn` is a
    # LiveComponent class, each item gets its own live component instance
    # (same path as `mount(LiveCompClass)`, just looped per item).
    fn_is_live = is_live_component_class(fn)
    core_handles: list[core.ComponentMountHandle] = []

    async def _mount_one(key: StableKey, item: Any) -> None:
        item_path = child_path.concat(key)
        if fn_is_live:
            instance = fn(item, *extra_args, **kwargs)
            handle = await _mount_live_component(parent_ctx, item_path, instance)
            core_handles.extend(handle._cores)
            return
        processor = create_core_component_processor(
            fn, parent_ctx._env, item_path, (item, *extra_args), kwargs
        )
        resolved = parent_ctx.resolve_exception_handler(
            stable_path=item_path.to_string(),
            processor_name=getattr(fn, "__qualname__", None),
            mount_kind="mount_each",
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


# Keep map task failures wrapped so a user function can return an Exception
# object as a normal value without being confused with a failed task.
@dataclass(frozen=True, slots=True)
class _MapTaskSuccess(Generic[ReturnT]):
    value: ReturnT


@dataclass(frozen=True, slots=True)
class _MapTaskFailure:
    error: Exception


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

    Once a task is started, ``map`` waits for it to finish even if another
    task hits a deadline. If multiple tasks fail, the first failure in input
    order is raised.

    Args:
        fn: The function to apply to each item. The item is passed as the first argument.
        items: The items to iterate (sync or async).
        *args: Additional passthrough arguments to fn (appended after the item).
        **kwargs: Additional passthrough keyword arguments to fn.

    Returns:
        Results from each invocation.
    """
    check_deadline()

    async def _run_one(item: T) -> _MapTaskSuccess[ReturnT] | _MapTaskFailure:
        try:
            # A task may start after the caller's deadline moved forward while
            # earlier tasks were being scheduled.
            check_deadline()
            result = await fn(item, *args, **kwargs)
            # Do not let a value returned after the deadline look successful to
            # the map caller.
            check_deadline()
            return _MapTaskSuccess(result)
        except Exception as exc:
            return _MapTaskFailure(exc)

    # Keep handles for every task that made it into the TaskGroup, so schedule
    # failures do not prevent us from draining already-started work.
    tasks: list[asyncio.Task[_MapTaskSuccess[ReturnT] | _MapTaskFailure]] = []
    # Raised after the TaskGroup exits, so started tasks finish first.
    schedule_error: Exception | None = None

    async with asyncio.TaskGroup() as tg:

        def _schedule_one(item: T) -> None:
            # Fail before enqueueing new work. Already-started tasks are still
            # drained by the TaskGroup below.
            check_deadline()
            tasks.append(tg.create_task(_run_one(item)))

        try:
            if isinstance(items, AsyncIterable):
                async for item in items:
                    _schedule_one(item)
            else:
                for item in items:
                    _schedule_one(item)
        except Exception as exc:
            schedule_error = exc

    results = [task.result() for task in tasks]

    if schedule_error is not None:
        raise schedule_error

    for outcome in results:
        if not isinstance(outcome, _MapTaskFailure):
            continue
        if isinstance(outcome.error, DeadlineExceededError):
            raise DeadlineExceededError(
                "CocoIndex timeout deadline exceeded"
            ) from outcome.error
        raise outcome.error
    # All started tasks completed successfully; check the caller's deadline
    # before returning their values.
    check_deadline()
    return [cast(_MapTaskSuccess[ReturnT], outcome).value for outcome in results]


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
    check_deadline()
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
# use_state
# ============================================================================

_StateT = TypeVar("_StateT")

# State has no static type hint (the handle is generic), so decode as Any.
_DESERIALIZE_ANY = make_deserialize_fn(Any)


class StateHandle(Generic[_StateT]):
    """
    Handle for a persistent per-component state value.

    Returned by `coco.use_state()`. Read the current value via `.value`;
    assign to `.value` to persist a new value for the next run.
    """

    __slots__ = ("_key", "_stored", "_deserializer", "_core_processor_ctx")

    def __init__(
        self,
        key: StableKey,
        stored: core.StoredValue,
        deserializer: DeserializeFn,
        core_processor_ctx: core.ComponentProcessorContext,
    ) -> None:
        self._key = key
        self._stored = stored
        self._deserializer = deserializer
        self._core_processor_ctx = core_processor_ctx

    @property
    def value(self) -> _StateT:
        # Lazy read: object-backed returns directly; bytes-backed deserializes once, cached.
        return self._stored.get(self._deserializer)  # type: ignore[no-any-return]

    @value.setter
    def value(self, new_value: _StateT) -> None:
        # Hand the object to Rust without serializing (serialization is deferred
        # to commit). Keep the returned object-backed cell so later in-run reads
        # return this value directly.
        self._stored = self._core_processor_ctx.update_user_state(self._key, new_value)


@overload
def use_state(key: StableKey, initial_value: Any = None) -> StateHandle[Any]: ...
@overload
def use_state(
    key: StableKey,
    initial_value: _StateT | None = None,
    *,
    type_hint: type[_StateT],
) -> StateHandle[_StateT]: ...
def use_state(
    key: StableKey,
    initial_value: Any = None,
    *,
    type_hint: type[Any] | None = None,
) -> StateHandle[Any]:
    """
    Declare a persistent state for the current component.

    On the first run, the returned handle's `.value` is `initial_value`
    (or `None` if omitted). On subsequent runs, `.value` is the value
    stored at the end of the previous run. Assign to `handle.value`
    during the run to persist a new value.

    The value is serialized lazily, once, when the component commits — not at
    assignment. Two consequences: (1) if the value is not serializable, the
    error surfaces at commit (identifying the state key) rather than at the
    `handle.value = ...` line; (2) the persisted value reflects the object as it
    is at commit, so mutating it in place after assignment is captured.

    Args:
        key: Unique StableKey within this component (None, bool, int, str,
             bytes, uuid.UUID, Symbol, or a tuple of these). Must be declared
             at most once per component run.
        initial_value: Value to use when no stored state exists for `key`.
                       Defaults to `None`.
        type_hint: Optional type to deserialize the stored value into. When
                   provided, `.value` is decoded via the registered
                   serialization framework (msgspec for dataclasses /
                   NamedTuples / msgspec.Structs / primitives, pickle for
                   types decorated with ``@coco.serialize_by_pickle``, pydantic
                   for ``BaseModel`` subclasses). When omitted, the value is
                   decoded generically (``Any``) — i.e. whatever object the
                   deserializer produces from the stored bytes.

    Returns:
        A StateHandle wrapping the current value.

    Raises:
        RuntimeError: In the following cases, which surface as component build
                      failures — logged by default but not propagated to
                      `app.update()` unless a custom exception handler re-raises:

                      - Inside a `with coco.component_subpath()` block: state
                        is owned by the component's stable path, not the shifted
                        subpath, so the key would silently read/write under the
                        wrong identity.
                      - Inside a memoized function body: on a cache hit the body
                        is skipped entirely, so the key would never be declared
                        and would be garbage-collected as stale on the next commit.
                      - If `key` is declared more than once in the same component
                        run: each key maps to exactly one state slot; a second
                        declaration would be ambiguous.

    Example::

        # Plain (value typed as Any)
        counter = coco.use_state("counter", 0)

        # Typed — handle.value is Cursor, with full type inference
        @dataclass
        class Cursor:
            pos: int
            tag: str

        cur = coco.use_state("cursor", type_hint=Cursor, initial_value=Cursor(0, "init"))
        cur.value.pos += 1
        cur.value = Cursor(cur.value.pos, "next")
    """
    ctx = get_context_from_ctx()
    if ctx._core_path != ctx._core_processor_ctx.stable_path:
        raise RuntimeError(
            "coco.use_state() cannot be called inside a `with coco.component_subpath()` block"
        )

    if ctx._in_memo_fn:
        raise RuntimeError(
            "coco.use_state() cannot be called inside a memoized function"
        )
    try:
        # initial_value passed unserialized; engine core drops it if a value is
        # already stored on the previous run for this key.
        stored = ctx._core_processor_ctx.use_state(key, initial_value)
    except ValueError as e:
        # Rust client errors surface as ValueError; normalize to RuntimeError so
        # all use_state usage errors have a consistent type for callers.
        raise RuntimeError(str(e)) from None
    if type_hint is not None:
        deserializer = get_deserialize_fn(
            type_hint,  # type: ignore[arg-type]  # type objects are hashable at runtime
            source_label=f"use_state key {key!r}",
        )
    else:
        deserializer = _DESERIALIZE_ANY
    return StateHandle(key, stored, deserializer, ctx._core_processor_ctx)


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
    "StatsGroupHandle",
    "UpdateSnapshot",
    "UpdateStats",
    "UpdateStatus",
    # .function
    "fn",
    "LogicTracking",
    "timeout",
    "check_deadline",
    "DeadlineExceededError",
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
    "GPUPool",
    "GPURunner",
    "Runner",
    "configure_gpu_pool",
    "current_gpu",
    "current_gpus",
    "current_gpu_fraction",
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
    "stats_group",
    "use_context",
    "get_component_context",
    # .setting
    "Settings",
    "LmdbSettings",
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
    "auto_refresh",
    # use_state
    "StateHandle",
    "use_state",
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
