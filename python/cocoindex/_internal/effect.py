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
from .scope import Scope
from .pending_marker import PendingS, MaybePendingS, ResolvesTo
from .typing import NonExistenceType


ActionT = TypeVar("ActionT")
ActionT_co = TypeVar("ActionT_co", covariant=True)
ActionT_contra = TypeVar("ActionT_contra", contravariant=True)
KeyT = TypeVar("KeyT", bound=Hashable)
KeyT_contra = TypeVar("KeyT_contra", contravariant=True, bound=Hashable)
ValueT = TypeVar("ValueT", default=Any)
ValueT_contra = TypeVar("ValueT_contra", contravariant=True, default=Any)
StateT = TypeVar("StateT", default=Any)
StateT_co = TypeVar("StateT_co", covariant=True, default=Any)
HandlerT_co = TypeVar(
    "HandlerT_co", covariant=True, bound="EffectHandler[Any, Any, Any, Any]"
)
OptChildHandlerT = TypeVar(
    "OptChildHandlerT",
    bound="EffectHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildHandlerT_co = TypeVar(
    "OptChildHandlerT_co",
    bound="EffectHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)


class ChildEffectDef(Generic[HandlerT_co], NamedTuple):
    handler: HandlerT_co


class EffectSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    def __call__(
        self: EffectSinkFn[ActionT_contra, None], actions: Sequence[ActionT_contra], /
    ) -> None: ...
    # Case 2: With child handler
    @overload
    def __call__(
        self: EffectSinkFn[ActionT_contra, HandlerT_co],
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildEffectDef[HandlerT_co] | None] | None: ...
    def __call__(
        self, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildEffectDef[Any] | None] | None: ...


class AsyncEffectSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    async def __call__(
        self: AsyncEffectSinkFn[ActionT_contra, None],
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    async def __call__(
        self: AsyncEffectSinkFn[ActionT_contra, HandlerT_co],
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildEffectDef[HandlerT_co] | None] | None: ...
    async def __call__(
        self, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildEffectDef[Any] | None] | None: ...


class EffectSink(Generic[ActionT_contra, OptChildHandlerT_co]):
    __slots__ = ("_core",)
    _core: core.EffectSink

    def __init__(self, core_effect_sink: core.EffectSink):
        self._core = core_effect_sink

    @staticmethod
    def from_fn(
        fn: EffectSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "EffectSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return EffectSink(core.EffectSink.new_sync(canonical))

    @staticmethod
    def from_async_fn(
        fn: AsyncEffectSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "EffectSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return EffectSink(core.EffectSink.new_async(canonical))


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


class EffectReconcileOutput(
    Generic[ActionT, StateT_co, OptChildHandlerT_co], NamedTuple
):
    action: ActionT
    sink: EffectSink[ActionT, OptChildHandlerT_co]
    state: StateT_co | NonExistenceType


class EffectHandler(Protocol[KeyT_contra, ValueT_contra, StateT, OptChildHandlerT_co]):
    def reconcile(
        self,
        key: KeyT_contra,
        desired_effect: ValueT_contra | NonExistenceType,
        prev_possible_states: Collection[StateT],
        prev_may_be_missing: bool,
        /,
    ) -> EffectReconcileOutput[Any, StateT, OptChildHandlerT_co] | None: ...


class EffectProvider(
    Generic[KeyT, ValueT, OptChildHandlerT, MaybePendingS],
    ResolvesTo["EffectProvider[KeyT, ValueT, OptChildHandlerT]"],
):
    __slots__ = ("_core", "memo_key")
    _core: core.EffectProvider
    memo_key: str

    def __init__(self, core_effect_provider: core.EffectProvider):
        self._core = core_effect_provider
        self.memo_key = core_effect_provider.coco_memo_key()

    def effect(
        self: EffectProvider[KeyT, ValueT, OptChildHandlerT],
        key: KeyT,
        value: ValueT,
    ) -> "Effect[OptChildHandlerT]":
        return Effect(self, key, value)

    def __coco_memo_key__(self) -> str:
        return self.memo_key


PendingEffectProvider: TypeAlias = EffectProvider[
    KeyT, ValueT, OptChildHandlerT, PendingS
]


class Effect(Generic[OptChildHandlerT]):
    __slots__ = ("_provider", "_key", "_value")
    _provider: EffectProvider[Any, Any, OptChildHandlerT]
    _key: Any
    _value: Any

    def __init__(
        self,
        provider: EffectProvider[KeyT, ValueT, OptChildHandlerT],
        key: KeyT,
        value: ValueT,
    ):
        self._provider = provider
        self._key = key
        self._value = value


def declare_effect(scope: Scope, effect: Effect[None]) -> None:
    """
    Declare an effect within the given scope.

    Args:
        scope: The scope for the effect declaration.
        effect: The effect to declare.
    """
    comp_ctx = scope._core_processor_ctx
    core.declare_effect(
        comp_ctx,
        scope._core_fn_call_ctx,
        effect._provider._core,
        effect._key,
        effect._value,
    )


def declare_effect_with_child(
    scope: Scope,
    effect: Effect[EffectHandler[KeyT, ValueT, Any, OptChildHandlerT]],
) -> PendingEffectProvider[KeyT, ValueT, OptChildHandlerT]:
    """
    Declare an effect with a child handler within the given scope.

    Args:
        scope: The scope for the effect declaration.
        effect: The effect to declare.

    Returns:
        An EffectProvider for the child effects.
    """
    comp_ctx = scope._core_processor_ctx
    provider = core.declare_effect_with_child(
        comp_ctx,
        scope._core_fn_call_ctx,
        effect._provider._core,
        effect._key,
        effect._value,
    )
    return EffectProvider(provider)


def register_root_effect_provider(
    name: str, handler: EffectHandler[KeyT, ValueT, Any, OptChildHandlerT]
) -> EffectProvider[KeyT, ValueT, OptChildHandlerT]:
    provider = core.register_root_effect_provider(name, handler)
    return EffectProvider(provider)
