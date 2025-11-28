from typing import (
    Generic,
    Iterable,
    NamedTuple,
    Protocol,
    Any,
)
from cocoindex._internal.context import component_ctx_var
from typing_extensions import TypeIs, TypeVar

from . import core, state
from .runtime import get_async_context, is_coroutine_fn


class NonExistence:
    __slots__ = ()

    def __repr__(self) -> str:
        return "NonExistence"


def is_non_existence(obj: Any) -> TypeIs[NonExistence]:
    return isinstance(obj, NonExistence)


Action = TypeVar("Action")
Action_contra = TypeVar("Action_contra", contravariant=True)
Key = TypeVar("Key", default=state.StateKey)
Key_contra = TypeVar("Key_contra", contravariant=True)
State = TypeVar("State", default=None)
Decl = TypeVar("Decl")
Decl_contra = TypeVar("Decl_contra", contravariant=True)


class EffectSinkFn(Protocol[Action_contra]):
    def __call__(self, actions: Iterable[Action_contra]) -> None: ...


class AsyncEffectSinkFn(Protocol[Action_contra]):
    async def __call__(self, actions: Iterable[Action_contra]) -> None: ...


class EffectSink(Generic[Action_contra]):
    __slots__ = ("_core",)
    _core: core.EffectSink

    def __init__(self, core_effect_sink: core.EffectSink):
        self._core = core_effect_sink

    @staticmethod
    def from_fn(fn: EffectSinkFn[Action_contra]) -> "EffectSink[Action_contra]":
        return EffectSink(core.EffectSink.new_sync(fn))

    @staticmethod
    def from_async_fn(
        fn: AsyncEffectSinkFn[Action_contra],
    ) -> "EffectSink[Action_contra]":
        return EffectSink(core.EffectSink.new_async(fn, get_async_context()))


class EffectReconcileOutput(Generic[Action, State], NamedTuple):
    state: State
    action: Action
    sink: EffectSink[Action]


class EffectReconcilerFn(Protocol[Decl_contra, Key_contra]):
    def __call__(
        self,
        key: Key_contra,
        desired_effect: Decl_contra | NonExistence,
        prev_possible_states: Iterable[State],
        prev_may_be_missing: bool,
    ) -> EffectReconcileOutput[Action, State]: ...


class EffectReconciler(Generic[Decl_contra, Key_contra]):
    __slots__ = ("_core",)
    _core: core.EffectReconciler

    def __init__(self, core_effect_reconciler: core.EffectReconciler):
        self._core = core_effect_reconciler

    @staticmethod
    def from_fn(
        fn: EffectReconcilerFn[Decl_contra, Key_contra],
    ) -> "EffectReconciler[Decl_contra, Key_contra]":
        return EffectReconciler(core.EffectReconciler.new_sync(fn))


class EffectProvider(Generic[Decl, Key]):
    __slots__ = ("_core",)
    _core: core.EffectProvider

    def __init__(self, core_effect_provider: core.EffectProvider):
        self._core = core_effect_provider


class Effect:
    __slots__ = ("_provider", "_decl", "_key")
    _provider: EffectProvider[Any, Any]
    _decl: Any
    _key: Any

    def __init__(
        self,
        provider: EffectProvider[Decl, Key],
        decl: Decl,
        key: Key,
    ):
        self._provider = provider
        self._decl = decl
        self._key = key


def _declare_effect(
    csp: state.StatePath,
    effect: Effect,
    child_reconciler: EffectReconciler[Decl, Key] | None,
) -> EffectProvider[Decl, Key] | None:
    context = component_ctx_var.get()
    if context is None:
        raise RuntimeError("declare_effect* must be called within a component")
    provider = core.declare_effect(
        csp._core,
        effect._provider._core,
        effect._decl,
        effect._key,
        child_reconciler._core if child_reconciler is not None else None,
    )
    return EffectProvider(provider) if provider is not None else None


def declare_effect(csp: state.StatePath, effect: Effect) -> None:
    _declare_effect(csp, effect, None)


def declare_effect_with_child(
    csp: state.StatePath,
    effect: Effect,
    child_reconciler: EffectReconciler[Decl, Key],
) -> EffectProvider[Decl, Key]:
    provider = _declare_effect(csp, effect, child_reconciler)
    if provider is None:
        raise RuntimeError("core.declare_effect is expected to return a provider")
    return provider


core.init_effect_module(NonExistence())
