from __future__ import annotations

from typing import (
    Collection,
    Generic,
    Hashable,
    NamedTuple,
    Protocol,
    Any,
    Sequence,
    overload,
)
import threading
import weakref
from typing_extensions import TypeIs, TypeVar

from . import core
from .scope import Scope
from .runtime import get_async_context


class NonExistenceType:
    __slots__ = ()
    _instance: "NonExistenceType | None" = None

    def __new__(cls) -> NonExistenceType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NON_EXISTENCE"


NON_EXISTENCE = NonExistenceType()


def is_non_existence(obj: Any) -> TypeIs[NonExistenceType]:
    return obj is NON_EXISTENCE


Action = TypeVar("Action")
Action_co = TypeVar("Action_co", covariant=True)
Action_contra = TypeVar("Action_contra", contravariant=True)
Key = TypeVar("Key", bound=Hashable)
Key_contra = TypeVar("Key_contra", contravariant=True, bound=Hashable)
Value = TypeVar("Value", default=Any)
Value_contra = TypeVar("Value_contra", contravariant=True, default=Any)
State = TypeVar("State", default=Any)
State_co = TypeVar("State_co", covariant=True, default=Any)
Handler_co = TypeVar(
    "Handler_co", covariant=True, bound="EffectHandler[Any, Any, Any, Any]"
)
OptChildHandler = TypeVar(
    "OptChildHandler",
    bound="EffectHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildHandler_co = TypeVar(
    "OptChildHandler_co",
    bound="EffectHandler[Any, Any, Any, Any] | None",
    default=None,
    covariant=True,
)


class ChildEffectDef(Generic[Handler_co], NamedTuple):
    handler: Handler_co


class EffectSinkFn(Protocol[Action_contra, OptChildHandler_co]):
    # Case 1: No child handler
    @overload
    def __call__(
        self: EffectSinkFn[Action_contra, None], actions: Sequence[Action_contra], /
    ) -> None: ...
    # Case 2: With child handler
    @overload
    def __call__(
        self: EffectSinkFn[Action_contra, Handler_co],
        actions: Sequence[Action_contra],
        /,
    ) -> Sequence[ChildEffectDef[Handler_co] | None] | None: ...
    def __call__(
        self, actions: Sequence[Action_contra], /
    ) -> Sequence[ChildEffectDef[Any] | None] | None: ...


class AsyncEffectSinkFn(Protocol[Action_contra, OptChildHandler_co]):
    # Case 1: No child handler
    @overload
    async def __call__(
        self: EffectSinkFn[Action_contra, None], actions: Sequence[Action_contra], /
    ) -> None: ...
    # Case 2: With child handler
    @overload
    async def __call__(
        self: EffectSinkFn[Action_contra, Handler_co],
        actions: Sequence[Action_contra],
        /,
    ) -> Sequence[ChildEffectDef[Handler_co] | None] | None: ...
    async def __call__(
        self, actions: Sequence[Action_contra], /
    ) -> Sequence[ChildEffectDef[Any] | None] | None: ...


class EffectSink(Generic[Action_contra, OptChildHandler_co]):
    __slots__ = ("_core",)
    _core: core.EffectSink

    def __init__(self, core_effect_sink: core.EffectSink):
        self._core = core_effect_sink

    @staticmethod
    def from_fn(
        fn: EffectSinkFn[Action_contra, OptChildHandler_co],
    ) -> "EffectSink[Action_contra, OptChildHandler_co]":
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return EffectSink(core.EffectSink.new_sync(canonical))

    @staticmethod
    def from_async_fn(
        fn: AsyncEffectSinkFn[Action_contra, OptChildHandler_co],
    ) -> "EffectSink[Action_contra, OptChildHandler_co]":
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return EffectSink(core.EffectSink.new_async(canonical, get_async_context()))


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


class EffectReconcileOutput(Generic[Action, State_co, OptChildHandler_co], NamedTuple):
    action: Action
    sink: EffectSink[Action, OptChildHandler_co]
    state: State_co | NonExistenceType


class EffectHandler(Protocol[Key_contra, Value_contra, State, OptChildHandler_co]):
    def reconcile(
        self,
        key: Key_contra,
        desired_effect: Value_contra | NonExistenceType,
        prev_possible_states: Collection[State],
        prev_may_be_missing: bool,
        /,
    ) -> EffectReconcileOutput[Any, State, OptChildHandler_co] | None: ...


class EffectProvider(Generic[Key, Value, OptChildHandler]):
    __slots__ = ("_core",)
    _core: core.EffectProvider

    def __init__(self, core_effect_provider: core.EffectProvider):
        self._core = core_effect_provider

    def effect(self, key: Key, value: Value) -> "Effect[OptChildHandler]":
        return Effect(self, key, value)


class Effect(Generic[OptChildHandler]):
    __slots__ = ("_provider", "_key", "_value")
    _provider: EffectProvider[Any, Any, OptChildHandler]
    _key: Any
    _value: Any

    def __init__(
        self,
        provider: EffectProvider[Key, Value, OptChildHandler],
        key: Key,
        value: Value,
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
    processor_ctx = scope._core_processor_ctx
    core.declare_effect(
        processor_ctx, effect._provider._core, effect._key, effect._value
    )


def declare_effect_with_child(
    scope: Scope,
    effect: Effect[EffectHandler[Key, Value, Any, OptChildHandler]],
) -> EffectProvider[Key, Value, OptChildHandler]:
    """
    Declare an effect with a child handler within the given scope.

    Args:
        scope: The scope for the effect declaration.
        effect: The effect to declare.

    Returns:
        An EffectProvider for the child effects.
    """
    processor_ctx = scope._core_processor_ctx
    provider = core.declare_effect_with_child(
        processor_ctx, effect._provider._core, effect._key, effect._value
    )
    return EffectProvider(provider)


def register_root_effect_provider(
    name: str, handler: EffectHandler[Key, Value, Any, OptChildHandler]
) -> EffectProvider[Key, Value, OptChildHandler]:
    provider = core.register_root_effect_provider(name, handler)
    return EffectProvider(provider)


core.init_effect_module(NON_EXISTENCE)
