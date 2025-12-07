from typing import (
    Collection,
    Generic,
    Hashable,
    NamedTuple,
    Protocol,
    Any,
    Sequence,
)
import threading
import weakref
from cocoindex._internal.context import component_ctx_var
from typing_extensions import TypeIs, TypeVar

from . import core
from .runtime import get_async_context


class NonExistenceType:
    __slots__ = ()
    _instance: "NonExistenceType | None" = None

    def __new__(cls) -> "NonExistenceType":
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
Decl = TypeVar("Decl", default=Any)
Decl_contra = TypeVar("Decl_contra", contravariant=True, default=Any)
State = TypeVar("State", default=Any)
State_co = TypeVar("State_co", covariant=True, default=Any)

OptChildRecon = TypeVar(
    "OptChildRecon",
    bound="EffectReconciler[Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildRecon_co = TypeVar(
    "OptChildRecon_co",
    bound="EffectReconciler[Any, Any] | None",
    default=None,
    covariant=True,
)


class EffectSinkFn(Protocol[Action_contra, OptChildRecon_co]):
    def __call__(
        self, actions: Collection[Action_contra]
    ) -> None | Sequence[OptChildRecon_co]: ...


class AsyncEffectSinkFn(Protocol[Action_contra, OptChildRecon_co]):
    async def __call__(
        self, actions: Collection[Action_contra]
    ) -> OptChildRecon_co: ...


class EffectSink(Generic[Action_contra, OptChildRecon_co]):
    __slots__ = ("_core",)
    _core: core.EffectSink

    def __init__(self, core_effect_sink: core.EffectSink):
        self._core = core_effect_sink

    @staticmethod
    def from_fn(
        fn: EffectSinkFn[Action_contra, OptChildRecon_co],
    ) -> "EffectSink[Action_contra, OptChildRecon_co]":
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return EffectSink(core.EffectSink.new_sync(canonical))

    @staticmethod
    def from_async_fn(
        fn: AsyncEffectSinkFn[Action_contra, OptChildRecon_co],
    ) -> "EffectSink[Action_contra, OptChildRecon_co]":
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


class EffectReconcileOutput(Generic[Action, State_co, OptChildRecon_co], NamedTuple):
    action: Action
    sink: EffectSink[Action, OptChildRecon_co]
    state: State_co | NonExistenceType


def _unwrap_reconcile_output(
    recon_output: EffectReconcileOutput[Action, State, OptChildRecon] | None,
) -> Any:
    if recon_output is None:
        return None
    return (recon_output.action, recon_output.sink._core, recon_output.state)


class EffectReconcilerFn(
    Protocol[Action, Key_contra, Decl_contra, State, OptChildRecon_co]
):
    def __call__(
        self,
        key: Key_contra,
        desired_effect: Decl_contra | NonExistenceType,
        prev_possible_states: Collection[State],
        prev_may_be_missing: bool,
    ) -> EffectReconcileOutput[Action, State, OptChildRecon_co] | None: ...


class EffectReconciler(Generic[Key_contra, Decl_contra, OptChildRecon_co]):
    __slots__ = ("_core",)
    _core: core.EffectReconciler

    def __init__(self, core_effect_reconciler: core.EffectReconciler):
        self._core = core_effect_reconciler

    @staticmethod
    def from_fn(
        fn: EffectReconcilerFn[
            Action, Key_contra, Decl_contra, State, OptChildRecon_co
        ],
    ) -> "EffectReconciler[Key_contra, Decl_contra, OptChildRecon_co]":
        return EffectReconciler(
            core.EffectReconciler.new_sync(
                lambda *args: _unwrap_reconcile_output(fn(*args))
            )
        )


class EffectProvider(Generic[Key, Decl, OptChildRecon]):
    __slots__ = ("_core",)
    _core: core.EffectProvider

    def __init__(self, core_effect_provider: core.EffectProvider):
        self._core = core_effect_provider

    def effect(self, key: Key, decl: Decl) -> "Effect[OptChildRecon]":
        return Effect(self, key, decl)


class Effect(Generic[OptChildRecon]):
    __slots__ = ("_provider", "_key", "_decl")
    _provider: EffectProvider[Any, Any, OptChildRecon]
    _key: Any
    _decl: Any

    def __init__(
        self,
        provider: EffectProvider[Key, Decl, OptChildRecon],
        key: Key,
        decl: Decl,
    ):
        self._provider = provider
        self._key = key
        self._decl = decl


def declare_effect(effect: Effect[None]) -> None:
    context = component_ctx_var.get()
    if context is None:
        raise RuntimeError("declare_effect* must be called within a component")
    core.declare_effect(context, effect._provider._core, effect._key, effect._decl)


def declare_effect_with_child(
    effect: Effect[EffectReconciler[Key, Decl]],
) -> EffectProvider[Key, Decl]:
    context = component_ctx_var.get()
    if context is None:
        raise RuntimeError("declare_effect* must be called within a component")
    provider = core.declare_effect(
        context, effect._provider._core, effect._key, effect._decl
    )
    return EffectProvider(provider)


def register_root_effect_provider(
    name: str, reconciler: EffectReconciler[Key, Decl, OptChildRecon]
) -> EffectProvider[Key, Decl, OptChildRecon]:
    provider = core.register_root_effect_provider(name, reconciler._core)
    return EffectProvider(provider)


core.init_effect_module(NON_EXISTENCE)
