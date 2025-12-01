from typing import (
    Collection,
    Generic,
    Hashable,
    NamedTuple,
    Protocol,
    Any,
)
from cocoindex._internal.context import component_ctx_var
from typing_extensions import TypeIs, TypeVar

from . import core, state
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
Action_contra = TypeVar("Action_contra", contravariant=True)
Key = TypeVar("Key", bound=Hashable)
Key_contra = TypeVar("Key_contra", contravariant=True, bound=Hashable)
Decl = TypeVar("Decl")
Decl_contra = TypeVar("Decl_contra", contravariant=True)
State = TypeVar("State", default=None)


class EffectSinkFn(Protocol[Action_contra]):
    def __call__(self, actions: Collection[Action_contra]) -> None: ...


class AsyncEffectSinkFn(Protocol[Action_contra]):
    async def __call__(self, actions: Collection[Action_contra]) -> None: ...


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
    action: Action
    sink: EffectSink[Action]
    state: State | NonExistenceType = NON_EXISTENCE


def _unwrap_reconcile_output(recon_output: EffectReconcileOutput[Action, State]) -> Any:
    return (recon_output.action, recon_output.sink._core, recon_output.state)


class EffectReconcilerFn(Protocol[Action, Key_contra, Decl_contra, State]):
    # TODO: Change output type to optional, to represent no-change.
    def __call__(
        self,
        key: Key_contra,
        desired_effect: Decl_contra | NonExistenceType,
        prev_possible_states: Collection[State],
        prev_may_be_missing: bool,
    ) -> EffectReconcileOutput[Action, State]: ...


class EffectReconciler(Generic[Action, Key_contra, Decl_contra, State]):
    __slots__ = ("_core",)
    _core: core.EffectReconciler

    def __init__(self, core_effect_reconciler: core.EffectReconciler):
        self._core = core_effect_reconciler

    @staticmethod
    def from_fn(
        fn: EffectReconcilerFn[Action, Key_contra, Decl_contra, State],
    ) -> "EffectReconciler[Action, Key_contra, Decl_contra, State]":
        return EffectReconciler(
            core.EffectReconciler.new_sync(
                lambda *args: _unwrap_reconcile_output(fn(*args))
            )
        )


class EffectProvider(Generic[Key, Decl]):
    __slots__ = ("_core",)
    _core: core.EffectProvider

    def __init__(self, core_effect_provider: core.EffectProvider):
        self._core = core_effect_provider

    def effect(self, key: Key, decl: Decl) -> "Effect":
        return Effect(self, key, decl)


class Effect:
    __slots__ = ("_provider", "_key", "_decl")
    _provider: EffectProvider[Any, Any]
    _key: Any
    _decl: Any

    def __init__(
        self,
        provider: EffectProvider[Key, Decl],
        key: Key,
        decl: Decl,
    ):
        self._provider = provider
        self._key = key
        self._decl = decl


def _declare_effect(
    csp: state.StatePath,
    effect: Effect,
    child_reconciler: EffectReconciler[Action, Key, Decl, State] | None,
) -> EffectProvider[Key, Decl] | None:
    context = component_ctx_var.get()
    if context is None:
        raise RuntimeError("declare_effect* must be called within a component")
    provider = core.declare_effect(
        csp._core,
        context,
        effect._provider._core,
        effect._key,
        effect._decl,
        child_reconciler._core if child_reconciler is not None else None,
    )
    return EffectProvider(provider) if provider is not None else None


def declare_effect(csp: state.StatePath, effect: Effect) -> None:
    _declare_effect(csp, effect, None)


def declare_effect_with_child(
    csp: state.StatePath,
    effect: Effect,
    child_reconciler: EffectReconciler[Action, Key, Decl, State],
) -> EffectProvider[Key, Decl]:
    provider = _declare_effect(csp, effect, child_reconciler)
    if provider is None:
        raise RuntimeError("core.declare_effect is expected to return a provider")
    return provider


def register_root_effect_provider(
    name: str, reconciler: EffectReconciler[Action, Key, Decl, State]
) -> EffectProvider[Key, Decl]:
    provider = core.register_root_effect_provider(name, reconciler._core)
    return EffectProvider(provider)


core.init_effect_module(NON_EXISTENCE)
