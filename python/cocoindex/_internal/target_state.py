from __future__ import annotations

from typing import (
    Collection,
    Callable,
    Generic,
    Literal,
    NamedTuple,
    Protocol,
    Any,
    Sequence,
    TypeAlias,
    overload,
)
import weakref
from typing_extensions import TypeVar

from . import core
from .component_ctx import get_context_from_ctx
from .context_keys import ContextProvider
from .pending_marker import PendingS, MaybePendingS, ResolvesTo
from .serde import (
    make_deserialize_fn,
    get_param_annotation,
    qualified_name,
    unwrap_element_type,
)
from .typing import NonExistenceType, StableKey


ActionT = TypeVar("ActionT")
ActionT_co = TypeVar("ActionT_co", covariant=True)
ActionT_contra = TypeVar("ActionT_contra", contravariant=True)

ValueT = TypeVar("ValueT", default=Any)
ValueT_contra = TypeVar("ValueT_contra", contravariant=True, default=Any)
TrackingRecordT = TypeVar("TrackingRecordT", default=Any)
TrackingRecordT_co = TypeVar("TrackingRecordT_co", covariant=True, default=Any)
HandlerT_co = TypeVar(
    "HandlerT_co", covariant=True, bound="TargetHandler[Any, Any, Any]"
)
OptChildHandlerT = TypeVar(
    "OptChildHandlerT",
    bound="TargetHandler[Any, Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildHandlerT_co = TypeVar(
    "OptChildHandlerT_co",
    bound="TargetHandler[Any, Any, Any] | None",
    default=None,
    covariant=True,
)


class _TypedTargetHandlerWrapper:
    """Wraps a TargetHandler to auto-deserialize tracking records (StoredValue → typed objects)."""

    __slots__ = ("_handler", "_deserializer")

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        # reconcile(self, key, desired, prev_possible_records, ...) — position 3
        reconcile_label = qualified_name(type(handler).reconcile)
        try:
            ann = get_param_annotation(type(handler).reconcile, 3)
            record_type = unwrap_element_type(ann)
        except Exception:
            record_type = Any
        self._deserializer = make_deserialize_fn(
            record_type,
            source_label=f"prev_possible_records param of {reconcile_label}()",
        )

    def reconcile(
        self,
        key: Any,
        desired: Any,
        prev_possible_records: Any,
        prev_may_be_missing: bool,
        /,
    ) -> Any:
        records = [r.get(self._deserializer) for r in prev_possible_records]
        return self._handler.reconcile(key, desired, records, prev_may_be_missing)

    def attachments(self) -> dict[str, Any]:
        if not hasattr(self._handler, "attachments"):
            return {}
        return {
            k: _TypedTargetHandlerWrapper(v)
            for k, v in self._handler.attachments().items()
        }


class ChildTargetDef(Generic[HandlerT_co], NamedTuple):
    handler: HandlerT_co


class TargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, None],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, HandlerT_co],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class AsyncTargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, None],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, HandlerT_co],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    async def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class TargetActionSink(Generic[ActionT_contra, OptChildHandlerT_co]):
    __slots__ = ("_core",)
    _core: core.TargetActionSink

    def __init__(self, core_action_sink: core.TargetActionSink):
        self._core = core_action_sink

    @staticmethod
    def from_fn(
        fn: TargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        return _get_cached_sink(fn, "sync", core.TargetActionSink.new_sync)

    @staticmethod
    def from_async_fn(
        fn: AsyncTargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        return _get_cached_sink(fn, "async", core.TargetActionSink.new_async)


_SINK_CACHE_ATTR = "_cocoindex_target_action_sinks"


def _get_cached_sink(
    fn: Any,
    mode: Literal["sync", "async"],
    new_core: Callable[[Any], core.TargetActionSink],
) -> TargetActionSink[Any, Any]:
    """Reuse one core sink per callable owner so the Rust-side batcher is shared."""
    owner = getattr(fn, "__self__", None)
    func = getattr(fn, "__func__", None)
    cache_key = (mode, func) if owner is not None and func is not None else (mode, None)
    if owner is None:
        owner = fn
        func = None

    try:
        owner_ref = weakref.ref(owner)
    except TypeError:
        return TargetActionSink(new_core(fn))

    cache = getattr(owner, _SINK_CACHE_ATTR, None)
    if cache is None:
        cache = {}
        try:
            setattr(owner, _SINK_CACHE_ATTR, cache)
        except (AttributeError, TypeError):
            return TargetActionSink(new_core(fn))

    value = cache.get(cache_key)
    if value is None:
        callback = _make_weak_sink_callback(owner_ref, func, mode)
        value = TargetActionSink(new_core(callback))
        cache[cache_key] = value
    return value


def _make_weak_sink_callback(
    owner_ref: weakref.ReferenceType[Any],
    func: Any,
    mode: Literal["sync", "async"],
) -> Any:
    if mode == "sync":

        def sync_callback(
            context_provider: ContextProvider, actions: Sequence[Any]
        ) -> Any:
            owner = owner_ref()
            if owner is None:
                raise RuntimeError("target action sink owner has been released")
            if func is None:
                return owner(context_provider, actions)
            return func(owner, context_provider, actions)

        return sync_callback

    async def async_callback(
        context_provider: ContextProvider, actions: Sequence[Any]
    ) -> Any:
        owner = owner_ref()
        if owner is None:
            raise RuntimeError("target action sink owner has been released")
        if func is None:
            return await owner(context_provider, actions)
        return await func(owner, context_provider, actions)

    return async_callback


class TargetReconcileOutput(
    Generic[ActionT, TrackingRecordT_co, OptChildHandlerT_co], NamedTuple
):
    action: ActionT
    sink: TargetActionSink[ActionT, OptChildHandlerT_co]
    tracking_record: TrackingRecordT_co | NonExistenceType
    child_invalidation: Literal["destructive", "lossy"] | None = None


class TargetHandler(Protocol[ValueT_contra, TrackingRecordT, OptChildHandlerT_co]):
    def reconcile(
        self,
        key: StableKey,
        desired_target_state: ValueT_contra | NonExistenceType,
        prev_possible_records: Collection[TrackingRecordT],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[Any, TrackingRecordT, OptChildHandlerT_co] | None: ...


class TargetStateProvider(
    Generic[ValueT, OptChildHandlerT, MaybePendingS],
    ResolvesTo["TargetStateProvider[ValueT, OptChildHandlerT]"],
):
    __slots__ = ("_core",)
    _core: core.TargetStateProvider

    def __init__(self, core_provider: core.TargetStateProvider):
        self._core = core_provider

    @property
    def memo_key(self) -> str:
        return self._core.coco_memo_key()

    def target_state(
        self: TargetStateProvider[ValueT, OptChildHandlerT],
        key: StableKey,
        value: ValueT,
    ) -> "TargetState[OptChildHandlerT]":
        return TargetState(self, key, value)

    def attachment(
        self: TargetStateProvider[ValueT, OptChildHandlerT],
        att_type: str,
    ) -> "TargetStateProvider":
        ctx = get_context_from_ctx()
        provider = self._core.register_attachment_provider(
            ctx._core_processor_ctx, att_type
        )
        return TargetStateProvider(provider)

    def __coco_memo_key__(self) -> str:
        return self._core.coco_memo_key()


PendingTargetStateProvider: TypeAlias = TargetStateProvider[
    ValueT, OptChildHandlerT, PendingS
]


class TargetState(Generic[OptChildHandlerT]):
    __slots__ = ("_provider", "_key", "_value")
    _provider: TargetStateProvider[Any, OptChildHandlerT]
    _key: Any
    _value: Any

    def __init__(
        self,
        provider: TargetStateProvider[ValueT, OptChildHandlerT],
        key: StableKey,
        value: ValueT,
    ):
        self._provider = provider
        self._key = key
        self._value = value


def declare_target_state(target_state: TargetState[None]) -> None:
    """
    Declare a target state within the current component context.

    Args:
        target_state: The target state to declare.
    """
    ctx = get_context_from_ctx()
    core.declare_target_state(
        ctx._core_processor_ctx,
        ctx._core_fn_call_ctx,
        target_state._provider._core,
        target_state._key,
        target_state._value,
    )


def declare_target_state_with_child(
    target_state: TargetState[TargetHandler[ValueT, Any, OptChildHandlerT]],
) -> PendingTargetStateProvider[ValueT, OptChildHandlerT]:
    """
    Declare a target state with a child handler within the current component context.

    Args:
        target_state: The target state to declare.

    Returns:
        A TargetStateProvider for the child target states.
    """
    ctx = get_context_from_ctx()
    provider = core.declare_target_state_with_child(
        ctx._core_processor_ctx,
        ctx._core_fn_call_ctx,
        target_state._provider._core,
        target_state._key,
        target_state._value,
    )
    return TargetStateProvider(provider)


def register_root_target_states_provider(
    name: str, handler: TargetHandler[ValueT, Any, OptChildHandlerT]
) -> TargetStateProvider[ValueT, OptChildHandlerT]:
    wrapped = _TypedTargetHandlerWrapper(handler)
    provider = core.register_root_target_states_provider(name, wrapped)
    return TargetStateProvider(provider)
