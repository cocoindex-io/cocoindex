from __future__ import annotations

from typing import (
    Collection,
    Generic,
    Literal,
    Mapping,
    NamedTuple,
    Protocol,
    Any,
    Sequence,
    TypeAlias,
    overload,
)
import threading
import warnings
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
HandlerT_contra = TypeVar(
    "HandlerT_contra", contravariant=True, bound="TargetHandler[Any, Any, Any]"
)
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
# Deprecated second type parameter of `TargetActionSink`. Defaults to `Any` so
# `TargetActionSink[A]` accepts a sink annotated the pre-slot way,
# `TargetActionSink[A, ChildHandler]`, and vice versa.
_DeprecatedChildHandlerT_co = TypeVar(
    "_DeprecatedChildHandlerT_co", default=Any, covariant=True
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


class ChildSlot(Generic[HandlerT_contra]):
    """Fulfillment handle for the child target states under one container action.

    A sink built with :meth:`TargetActionSink.from_fn_with_children` or
    :meth:`TargetActionSink.from_async_fn_with_children` receives one slot per
    action whose target state was declared with ``declare_target_state_with_child``
    (or ``mount_target``), keyed by the action's index in the batch. It must call
    :meth:`fulfill` exactly once per slot, before returning, with the handler for
    the child target states. A slot left unfulfilled fails the commit.
    """

    __slots__ = ("_core",)
    _core: core.ChildTargetSlot

    def __init__(self, core_slot: core.ChildTargetSlot) -> None:
        self._core = core_slot

    def fulfill(self, handler: HandlerT_contra, /) -> None:
        self._core.fulfill(_TypedTargetHandlerWrapper(handler))


class ChildTargetDef(Generic[HandlerT_co], NamedTuple):
    """Deprecated: a child handler returned from a sink under the pre-slot contract.

    Before child slots, a container sink built with
    :meth:`TargetActionSink.from_fn` / :meth:`TargetActionSink.from_async_fn`
    returned one ``ChildTargetDef`` (or ``None``) per action, index-aligned with
    ``actions``. Such sinks still work and emit a :class:`DeprecationWarning`;
    new code builds the sink with :meth:`TargetActionSink.from_fn_with_children`
    and fulfills each :class:`ChildSlot` instead. This class will be removed in
    a future release.
    """

    handler: HandlerT_co


class TargetActionSinkFn(Protocol[ActionT_contra]):
    """Sync callback of a sink built with :meth:`TargetActionSink.from_fn`."""

    def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> None: ...


class AsyncTargetActionSinkFn(Protocol[ActionT_contra]):
    """Async callback of a sink built with :meth:`TargetActionSink.from_async_fn`."""

    async def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> None: ...


class LegacyTargetActionSinkFn(Protocol[ActionT_contra]):
    """Deprecated: the pre-slot callback contract of :meth:`TargetActionSink.from_fn`.

    The callback returns one :class:`ChildTargetDef` (or ``None``) per action,
    index-aligned with ``actions``. Such sinks still run and emit a
    :class:`DeprecationWarning`; see :class:`ChildTargetDef`.
    """

    def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class LegacyAsyncTargetActionSinkFn(Protocol[ActionT_contra]):
    """Deprecated: async counterpart of :class:`LegacyTargetActionSinkFn`."""

    async def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class TargetActionSinkWithChildrenFn(Protocol[ActionT_contra]):
    def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        child_slots: Mapping[int, ChildSlot[Any]],
        /,
    ) -> None: ...


class AsyncTargetActionSinkWithChildrenFn(Protocol[ActionT_contra]):
    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        child_slots: Mapping[int, ChildSlot[Any]],
        /,
    ) -> None: ...


class TargetActionSink(Generic[ActionT_contra, _DeprecatedChildHandlerT_co]):
    """Applies batches of actions to the external system.

    Sinks share one identity — actions reconciled to them are batched and
    applied together — iff their callbacks are of the same type and compare
    equal. The same function or bound method always yields the same identity;
    a callable with value equality (e.g. a frozen dataclass implementing
    ``__call__``) may be constructed on the fly at each ``reconcile()`` call.
    The callback must support weak references, so an idle identity can be
    released; a tuple/NamedTuple callback is rejected with ``TypeError``.

    A sink built with :meth:`from_fn` / :meth:`from_async_fn` serves leaf
    target states (or, deprecated, a container whose callback returns
    ``ChildTargetDef`` entries). A sink whose actions may carry child target
    states (container targets, or a sink shared between a container and its
    leaves) is built with :meth:`from_fn_with_children` /
    :meth:`from_async_fn_with_children` and fulfills a :class:`ChildSlot` per
    child-bearing action.

    The second type parameter is deprecated and ignored. It remains so that
    ``TargetActionSink[Action, ChildHandler]`` annotations written for the
    pre-slot contract keep working; write ``TargetActionSink[Action]``.
    """

    __slots__ = ("_core",)
    _core: core.TargetActionSink

    def __init__(self, core_action_sink: core.TargetActionSink):
        self._core = core_action_sink

    def __class_getitem__(cls, params: Any) -> Any:
        if isinstance(params, tuple) and len(params) == 2:
            warnings.warn(
                "TargetActionSink takes one type argument; the second (child "
                "handler) argument is deprecated and ignored",
                DeprecationWarning,
                stacklevel=2,
            )
        return super().__class_getitem__(params)  # type: ignore[misc]

    @overload
    @staticmethod
    def from_fn(
        fn: TargetActionSinkFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]": ...
    @overload
    @staticmethod
    def from_fn(
        fn: LegacyTargetActionSinkFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]": ...
    @staticmethod
    def from_fn(
        fn: TargetActionSinkFn[Any] | LegacyTargetActionSinkFn[Any],
    ) -> "TargetActionSink[Any, Any]":
        """Create a leaf sink from a sync callback ``(context_provider, actions)``.

        A callback returning ``ChildTargetDef`` entries (the deprecated
        pre-slot contract, :class:`LegacyTargetActionSinkFn`) is still accepted.
        """
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_sync(canonical, with_children=False)
        )

    @overload
    @staticmethod
    def from_async_fn(
        fn: AsyncTargetActionSinkFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]": ...
    @overload
    @staticmethod
    def from_async_fn(
        fn: LegacyAsyncTargetActionSinkFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]": ...
    @staticmethod
    def from_async_fn(
        fn: AsyncTargetActionSinkFn[Any] | LegacyAsyncTargetActionSinkFn[Any],
    ) -> "TargetActionSink[Any, Any]":
        """Create a leaf sink from an async callback ``(context_provider, actions)``.

        A callback returning ``ChildTargetDef`` entries (the deprecated
        pre-slot contract, :class:`LegacyAsyncTargetActionSinkFn`) is still
        accepted.
        """
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_async(canonical, with_children=False)
        )

    @staticmethod
    def from_fn_with_children(
        fn: TargetActionSinkWithChildrenFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]":
        """Create a container sink from a sync callback
        ``(context_provider, actions, child_slots)``.

        ``child_slots`` maps the index of each action whose target state was
        declared with a child to the :class:`ChildSlot` the callback must
        fulfill; actions without a child (orphan deletes, leaf actions on a
        shared sink) have no entry.
        """
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_sync(canonical, with_children=True)
        )

    @staticmethod
    def from_async_fn_with_children(
        fn: AsyncTargetActionSinkWithChildrenFn[ActionT_contra],
    ) -> "TargetActionSink[ActionT_contra, _DeprecatedChildHandlerT_co]":
        """Async counterpart of :meth:`from_fn_with_children`."""
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_async(canonical, with_children=True)
        )


class _CanonicalKey:
    """Dict key for `_ObjectDeduper`: hashes by the referent's value without
    keeping the referent alive.

    Holds only a weak reference, so the referent must be weakref-able; one that
    is not (e.g. a tuple/NamedTuple instance) is rejected at construction rather
    than pinned for the process lifetime. A key whose referent died compares
    equal only to itself, so an expired entry can never satisfy a lookup by
    value.

    Equality also requires the exact same type. ``==`` alone is not type-safe
    (a NamedTuple equals any same-shaped tuple, and a user class may define a
    permissive ``__eq__``), and canonicalizing across types would route one
    type's actions to the other type's callback.
    """

    __slots__ = ("get", "_hash")
    get: weakref.ref[Any]
    _hash: int

    def __init__(self, obj: Any) -> None:
        try:
            self.get = weakref.ref(obj)
        except TypeError:
            raise TypeError(
                f"target action sink callback of type {qualified_name(type(obj))} "
                "must support weak references; define it as a frozen dataclass "
                "(with weakref_slot=True if it uses slots=True) rather than a "
                "tuple/NamedTuple"
            ) from None
        self._hash = hash(obj)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        obj = self.get()
        if obj is None:
            return False
        if isinstance(other, _CanonicalKey):
            other = other.get()
            if other is None:
                return False
        return type(other) is type(obj) and bool(obj == other)


class _ObjectDeduper:
    """Canonicalize equal objects of one type to one representative.

    `TargetActionSink.from_fn`/`from_async_fn` route callbacks through this so
    equal callbacks map to one canonical object, whose pointer the Rust side
    then uses to intern the sink keeper (see `SinkKeeperRegistry` in
    `rust/py/src/target_state.rs`) — together giving sinks value-based
    identity.

    Entries reference the canonical only weakly; the sink keeper holds its
    callback strongly, so a canonical stays pinned exactly while some sink
    still uses it. Once nothing does, the entry expires — a later equal object
    becomes the new canonical — and expired entries are swept once the map
    grows past a doubling threshold.
    """

    _MIN_PRUNE_AT = 64

    __slots__ = ("_lock", "_map", "_prune_at")
    _lock: threading.Lock
    _map: dict[_CanonicalKey, _CanonicalKey]
    _prune_at: int

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._map = {}
        self._prune_at = self._MIN_PRUNE_AT

    def get_canonical(self, obj: Any) -> Any:
        with self._lock:
            existing = self._map.get(obj)
            if existing is not None:
                value = existing.get()
                if value is not None:
                    return value

            key = _CanonicalKey(obj)
            if len(self._map) >= self._prune_at:
                self._map = {k: k for k in self._map if k.get() is not None}
                self._prune_at = max(self._MIN_PRUNE_AT, 2 * len(self._map))
            self._map[key] = key
            return obj


_SYNC_FN_DEDUPER = _ObjectDeduper()
_ASYNC_FN_DEDUPER = _ObjectDeduper()


class TargetReconcileOutput(
    Generic[ActionT, TrackingRecordT_co, OptChildHandlerT_co], NamedTuple
):
    """What ``reconcile()`` returns when an action is needed.

    The third type parameter declares the handler type of the child target
    states the action's sink fulfills (through :class:`ChildSlot`), or ``None``
    for a leaf target. Nothing in the tuple depends on it: it is how a
    :class:`TargetHandler` states its child handler type, which types the
    provider chain (``declare_target_state_with_child`` / ``mount_target``).
    """

    action: ActionT
    sink: TargetActionSink[ActionT]
    tracking_record: TrackingRecordT_co | NonExistenceType
    child_invalidation: Literal["destructive", "lossy"] | None = None


class TargetHandler(Protocol[ValueT_contra, TrackingRecordT, OptChildHandlerT_co]):
    """Reconciles one kind of target state.

    ``OptChildHandlerT_co`` is the handler type of the child target states this
    handler's sink fulfills, or ``None`` for a leaf target. A container handler
    declares it on the return type of ``reconcile``
    (``TargetReconcileOutput[Action, TrackingRecord, ChildHandler]``), or by
    subclassing ``TargetHandler[Spec, TrackingRecord, ChildHandler]``.
    """

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
