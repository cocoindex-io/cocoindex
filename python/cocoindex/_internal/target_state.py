from __future__ import annotations

from typing import (
    Collection,
    Generic,
    Hashable,
    NamedTuple,
    Protocol,
    Any,
    Sequence,
    TypeAlias,
    overload,
)
import threading
import weakref
from typing_extensions import TypeVar

from . import core
from .component_ctx import get_context_from_ctx
from .pending_marker import PendingS, MaybePendingS, ResolvesTo
from .typing import NonExistenceType


ActionT = TypeVar("ActionT")
ActionT_co = TypeVar("ActionT_co", covariant=True)
ActionT_contra = TypeVar("ActionT_contra", contravariant=True)
KeyT = TypeVar("KeyT", bound=Hashable)
KeyT_contra = TypeVar("KeyT_contra", contravariant=True, bound=Hashable)
ValueT = TypeVar("ValueT", default=Any)
ValueT_contra = TypeVar("ValueT_contra", contravariant=True, default=Any)
TrackingRecordT = TypeVar("TrackingRecordT", default=Any)
TrackingRecordT_co = TypeVar("TrackingRecordT_co", covariant=True, default=Any)
HandlerT_co = TypeVar(
    "HandlerT_co", covariant=True, bound="TargetHandler[Any, Any, Any, Any]"
)
OptChildHandlerT = TypeVar(
    "OptChildHandlerT",
    bound="TargetHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildHandlerT_co = TypeVar(
    "OptChildHandlerT_co",
    bound="TargetHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)


class ChildTargetDef(Generic[HandlerT_co], NamedTuple):
    handler: HandlerT_co


class TargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, None],
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, HandlerT_co],
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    def __call__(
        self, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class AsyncTargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, None],
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, HandlerT_co],
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    async def __call__(
        self, actions: Sequence[ActionT_contra], /
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
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(core.TargetActionSink.new_sync(canonical))

    @staticmethod
    def from_async_fn(
        fn: AsyncTargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(core.TargetActionSink.new_async(canonical))


class _ObjectDeduper:
    __slots__ = ("_lock", "_map")
    _lock: threading.Lock
    _map: weakref.WeakValueDictionary[Any, Any]

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._map = weakref.WeakValueDictionary()

    def get_canonical(self, obj: Any) -> Any:
        with self._lock:
            value = self._map.get(obj)
            if value is not None:
                return value

            self._map[obj] = obj
            return obj


_SYNC_FN_DEDUPER = _ObjectDeduper()
_ASYNC_FN_DEDUPER = _ObjectDeduper()


class TargetReconcileOutput(
    Generic[ActionT, TrackingRecordT_co, OptChildHandlerT_co], NamedTuple
):
    action: ActionT
    sink: TargetActionSink[ActionT, OptChildHandlerT_co]
    tracking_record: TrackingRecordT_co | NonExistenceType


class TargetHandler(
    Protocol[KeyT_contra, ValueT_contra, TrackingRecordT, OptChildHandlerT_co]
):
    def reconcile(
        self,
        key: KeyT_contra,
        desired_target_state: ValueT_contra | NonExistenceType,
        prev_possible_states: Collection[TrackingRecordT],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[Any, TrackingRecordT, OptChildHandlerT_co] | None: ...


class TargetStateProvider(
    Generic[KeyT, ValueT, OptChildHandlerT, MaybePendingS],
    ResolvesTo["TargetStateProvider[KeyT, ValueT, OptChildHandlerT]"],
):
    __slots__ = ("_core", "memo_key")
    _core: core.TargetStateProvider
    memo_key: str

    def __init__(self, core_provider: core.TargetStateProvider):
        self._core = core_provider
        self.memo_key = core_provider.coco_memo_key()

    def target_state(
        self: TargetStateProvider[KeyT, ValueT, OptChildHandlerT],
        key: KeyT,
        value: ValueT,
    ) -> "TargetState[OptChildHandlerT]":
        return TargetState(self, key, value)

    def __coco_memo_key__(self) -> str:
        return self.memo_key


PendingTargetStateProvider: TypeAlias = TargetStateProvider[
    KeyT, ValueT, OptChildHandlerT, PendingS
]


class TargetState(Generic[OptChildHandlerT]):
    __slots__ = ("_provider", "_key", "_value")
    _provider: TargetStateProvider[Any, Any, OptChildHandlerT]
    _key: Any
    _value: Any

    def __init__(
        self,
        provider: TargetStateProvider[KeyT, ValueT, OptChildHandlerT],
        key: KeyT,
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
    target_state: TargetState[TargetHandler[KeyT, ValueT, Any, OptChildHandlerT]],
) -> PendingTargetStateProvider[KeyT, ValueT, OptChildHandlerT]:
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
    name: str, handler: TargetHandler[KeyT, ValueT, Any, OptChildHandlerT]
) -> TargetStateProvider[KeyT, ValueT, OptChildHandlerT]:
    provider = core.register_root_target_states_provider(name, handler)
    return TargetStateProvider(provider)
