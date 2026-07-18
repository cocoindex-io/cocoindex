"""
Persistent memoization fingerprinting (implementation).

This module implements Python-side memo-key canonicalization; user-facing
behavior is documented in
`docs/src/content/docs/advanced_topics/memoization_keys.mdx`.
"""

from __future__ import annotations

import dataclasses
import functools
import math
import os
import pickle
import struct
import sys
import typing
import weakref

from . import core
from .serde import (
    get_param_annotation,
    make_deserialize_fn,
    qualified_name,
    strip_non_existence_type,
)
from .typing import Fingerprintable


_KeyFn = typing.Callable[[typing.Any], typing.Any]
_StateFn = typing.Callable[[typing.Any, typing.Any], typing.Any]


class _MemoTypeRegistry(typing.NamedTuple):
    key_fn: _KeyFn | None = None
    state_fn: _StateFn | None = None
    stable_type_id: str | None = None


_STABLE_TYPE_ID_MISSING = object()
_memo_type_registry: dict[
    int, tuple[weakref.ReferenceType[type], _MemoTypeRegistry]
] = {}


class StateFnEntry(typing.NamedTuple):
    """A state method paired with a deserializer for its ``prev_state`` parameter.

    ``deserialize_prev`` converts a ``StoredValue`` (or ``NON_EXISTENCE``) into the
    typed Python object expected by the state method.
    ``call`` is the original state method (bound to its instance).
    """

    deserialize_prev: typing.Callable[[typing.Any], typing.Any]
    call: typing.Callable[[typing.Any], typing.Any]


@dataclasses.dataclass(slots=True)
class _CanonicalizeState:
    seen: dict[int, int] = dataclasses.field(default_factory=dict)
    keepalive: list[object] = dataclasses.field(default_factory=list)

    def remember(self, obj: object) -> int | None:
        oid = id(obj)
        ordinal = self.seen.get(oid)
        if ordinal is not None:
            assert self.keepalive[ordinal] is obj
            return ordinal

        ordinal = len(self.keepalive)
        self.keepalive.append(obj)
        self.seen[oid] = ordinal
        return None


@functools.cache
def _make_state_deserialize_fn(
    raw_state_fn: typing.Callable[..., typing.Any],
) -> typing.Callable[[bytes | memoryview], typing.Any]:
    """Build a DeserializeFn from a state function's ``prev_state`` parameter type.

    Works for both ``__coco_memo_state__(self, prev_state)`` and registered
    ``state_fn(obj, prev_state)`` — in both cases the state type is at position 1.

    ``NonExistenceType`` is stripped from union types since it's a sentinel
    that's never serialized — only the actual state type is deserialized.
    """
    fn_label = qualified_name(raw_state_fn)
    try:
        ann = get_param_annotation(raw_state_fn, 1)
        ann = strip_non_existence_type(ann)
        return make_deserialize_fn(
            ann,
            source_label=f"prev_state param of {fn_label}()",
        )
    except Exception:
        return make_deserialize_fn(typing.Any)


def _make_state_fn_entry(
    state_fn: typing.Callable[..., typing.Any],
    raw_state_fn: typing.Callable[..., typing.Any],
) -> StateFnEntry:
    """Build a ``StateFnEntry`` pairing *state_fn* with a typed deserializer."""
    deser = _make_state_deserialize_fn(raw_state_fn)

    def _deserialize_prev(prev_state: typing.Any) -> typing.Any:
        if isinstance(prev_state, core.StoredValue):
            return prev_state.get(deser)
        return prev_state

    return StateFnEntry(deserialize_prev=_deserialize_prev, call=state_fn)


def canonical_module_name(obj: typing.Any) -> str:
    """Return ``obj.__module__``, mapping ``"__main__"`` to the entry script's basename.

    Why: ``fn.__module__`` is part of memoization keys, but the same script
    produces different module names depending on how it's invoked:

    * ``python main.py``           -> ``__main__``
    * ``cocoindex update main.py`` -> ``main`` (basename, set by the loader)
    * ``cocoindex update main``    -> ``main`` (importlib's module name)

    Canonicalizing ``__main__`` to the script's basename collapses all three to
    the same identity so memo caches are shared across them.

    Callers must pass an object that has ``__module__`` (classes, regular
    functions, lambdas, callable instances). If you're passing something looser
    like a bound method or ``functools.partial`` produced by user code, wrap
    with ``getattr(..., '__module__', None)`` at the call site instead.
    """
    mod: str = obj.__module__
    if mod == "__main__":
        main_mod = sys.modules.get("__main__")
        if main_mod is not None:
            main_file: str | None = getattr(main_mod, "__file__", None)
            if main_file:
                return os.path.splitext(os.path.basename(main_file))[0]
    return mod


def _memo_type_label(typ: type) -> str:
    """Return a user-facing label for stable type ID validation errors."""
    return f"{canonical_module_name(typ)}.{getattr(typ, '__qualname__', '<unknown>')}"


class _PreviousTypeId(str):
    """A prior automatic type identity carried through the stable-ID path."""

    __slots__ = ()

    def __new__(cls, module: str, qualname: str | None = None) -> _PreviousTypeId:
        payload = module if qualname is None else f"{len(module)}:{module}{qualname}"
        return super().__new__(cls, payload)

    def __getnewargs__(self) -> tuple[str]:
        return (str(self),)

    def _identity_parts(self) -> tuple[str, str]:
        module_length_str, separator, payload = self.partition(":")
        if separator == "":
            raise ValueError("invalid previous type identity payload")
        module_length = int(module_length_str)
        return payload[:module_length], payload[module_length:]

    @property
    def module(self) -> str:
        return self._identity_parts()[0]

    @property
    def qualname(self) -> str:
        return self._identity_parts()[1]


def _validate_stable_type_id(stable_type_id: object, *, source: str) -> str:
    """Validate a non-empty stable type ID."""
    if not isinstance(stable_type_id, str):
        raise TypeError(f"{source} must be a str, got {type(stable_type_id).__name__}")
    if stable_type_id.strip() == "":
        raise ValueError(
            f"{source} must be non-empty and contain non-whitespace characters"
        )
    return stable_type_id


def _validate_previous_type_id_part(value: object, *, source: str) -> str:
    """Validate and normalize one previous automatic identity part."""
    if isinstance(value, str):
        return _validate_stable_type_id(str.__str__(value), source=source)
    return _validate_stable_type_id(value, source=source)


def prev_type_id(module: str, qualname: str) -> str:
    """Return a marker that reuses a type's prior automatic identity."""
    module = _validate_previous_type_id_part(module, source="prev_type_id() module")
    qualname = _validate_previous_type_id_part(
        qualname, source="prev_type_id() qualname"
    )
    return _PreviousTypeId(module, qualname)


def _remove_memo_type_registry(
    type_id: int, dead_ref: weakref.ReferenceType[type]
) -> None:
    """Remove ``type_id`` only if ``dead_ref`` is still the stored weakref."""
    entry = _memo_type_registry.get(type_id)
    if entry is not None and entry[0] is dead_ref:
        _memo_type_registry.pop(type_id, None)


def _register_memo_type_registry(typ: type, registry: _MemoTypeRegistry) -> None:
    """Register memo configuration for one exact Python type."""
    type_id = id(typ)

    def _remove_stale_registry(dead_ref: weakref.ReferenceType[type]) -> None:
        _remove_memo_type_registry(type_id, dead_ref)

    _memo_type_registry[type_id] = (
        weakref.ref(typ, _remove_stale_registry),
        registry,
    )


def _unregister_memo_type_registry(typ: type) -> None:
    """Best-effort removal of an exact-type memo registration."""
    type_id = id(typ)
    entry = _memo_type_registry.get(type_id)
    if entry is not None and entry[0]() is typ:
        _memo_type_registry.pop(type_id, None)


def _registered_memo_type_registry(typ: type) -> _MemoTypeRegistry | None:
    """Return the exact type's registration from the identity-keyed table."""
    type_id = id(typ)
    entry = _memo_type_registry.get(type_id)
    if entry is None:
        return None
    ref, registry = entry
    if ref() is typ:
        return registry
    _memo_type_registry.pop(type_id, None)
    return None


def _lookup_stable_type_id(typ: type) -> str | None:
    """Resolve a registered or exact ``__coco_memo_type_id__`` stable type ID."""
    registry = _registered_memo_type_registry(typ)
    if registry is not None and registry.stable_type_id is not None:
        return registry.stable_type_id

    stable_type_id = typ.__dict__.get("__coco_memo_type_id__", _STABLE_TYPE_ID_MISSING)
    if stable_type_id is _STABLE_TYPE_ID_MISSING:
        return None
    return _validate_stable_type_id(
        stable_type_id,
        source=f"{_memo_type_label(typ)}.__coco_memo_type_id__",
    )


_MEMO_KEY_ATTR = "__coco_memo_key__"
_MEMO_STATE_ATTR = "__coco_memo_state__"
_CLASS_OBJECT_OWNER_IDENTITY: tuple[Fingerprintable, Fingerprintable] = (
    canonical_module_name(type),
    type.__qualname__,
)


def _type_identity_parts(typ: type) -> tuple[Fingerprintable, Fingerprintable]:
    """Return stable type ID or module+qualname type identity parts.

    The stable type ID case still returns two parts to preserve the existing
    module/qualname identity shape used by type-aware canonical forms. The
    tagged first slot keeps stable type IDs disjoint from ordinary module
    names; ``None`` fills the qualname slot.
    """
    stable_type_id = _lookup_stable_type_id(typ)
    if isinstance(stable_type_id, _PreviousTypeId):
        return stable_type_id._identity_parts()
    if stable_type_id is not None:
        return (("__coco_memo_type_id__", stable_type_id), None)
    return (canonical_module_name(typ), getattr(typ, "__qualname__", None))


def _canonicalize_key_fragment(
    obj: object,
    state: _CanonicalizeState,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    """Canonicalize a memo-key fragment within the current root traversal.

    Sharing traversal state preserves cycles through the parent object and keeps
    temporary fragment objects alive so their IDs cannot be reused during this
    traversal.
    """

    return _canonicalize(obj, state, state_methods)


def _canonicalize_registered_memo_key(
    obj: object,
    owner: type,
    registry: _MemoTypeRegistry,
    state: _CanonicalizeState,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    key_fn = registry.key_fn
    assert key_fn is not None
    key = key_fn(obj)
    tag = "hook"
    if registry.state_fn is not None:
        tag = "shook"
        bound = functools.partial(registry.state_fn, obj)
        state_methods.append(_make_state_fn_entry(bound, registry.state_fn))
    return (
        tag,
        *_type_identity_parts(owner),
        _canonicalize_key_fragment(key, state, state_methods),
    )


def _canonicalize_class_object(
    cls: type,
    state: _CanonicalizeState,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    """Canonicalize a class object without invoking memo attributes on it."""

    metaclass: type = type(cls)
    for owner in metaclass.__mro__:
        if owner is object:
            break
        registry = _registered_memo_type_registry(owner)
        if registry is not None and registry.key_fn is not None:
            return _canonicalize_registered_memo_key(
                cls, owner, registry, state, state_methods
            )

    return (
        "hook",
        *_CLASS_OBJECT_OWNER_IDENTITY,
        # This synthesized identity is already canonical; do not re-enter memo-key
        # dispatch, where a registration on ``object`` could intercept it.
        ("seq", _type_identity_parts(cls)),
    )


def _is_dataclass_instance(obj: object) -> bool:
    """Check if obj is a dataclass instance (not a class)."""
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


def _is_pydantic_model(obj: object) -> bool:
    """Check if obj is a Pydantic v2 model instance."""
    return hasattr(obj, "__pydantic_fields__") and not isinstance(obj, type)  # type: ignore[attr-defined]


def _canonicalize_dataclass(
    obj: object,
    state: _CanonicalizeState,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    """Canonicalize a dataclass instance.

    Preserves field definition order and includes all fields.
    Format: ("dataclass", module, qualname, ((field_name, value), ...))
    """
    typ = type(obj)
    fields = dataclasses.fields(obj)  # type: ignore[arg-type]
    return (
        "dataclass",
        *_type_identity_parts(typ),
        tuple(
            (field.name, _canonicalize(getattr(obj, field.name), state, state_methods))
            for field in fields
        ),
    )


def _canonicalize_pydantic(
    obj: object,
    state: _CanonicalizeState,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    """Canonicalize a Pydantic v2 model instance.

    Includes all fields (set and unset) to ensure determinism.
    Format: ("pydantic", module, qualname, ((field_name, value), ...))
    """
    typ = type(obj)
    field_names = obj.__pydantic_fields__.keys()  # type: ignore[attr-defined]
    return (
        "pydantic",
        *_type_identity_parts(typ),
        tuple(
            (name, _canonicalize(getattr(obj, name), state, state_methods))
            for name in field_names
        ),
    )


class NotMemoKeyable:
    """
    Base class for objects that must not be used as memoization keys.

    Inherit from this class when an object maintains internal state that would
    make memoization semantically incorrect (e.g., generators that track call counts).

    Attempting to use a `NotMemoKeyable` instance as a memo key will raise TypeError.
    """

    __slots__ = ()

    def __coco_memo_key__(self) -> typing.NoReturn:
        raise TypeError(
            f"{type(self).__name__} cannot be used as a memoization key. "
            "This type maintains internal state that is incompatible with memoization."
        )


@typing.overload
def register_memo_key_function(
    typ: type,
    key_fn: _KeyFn,
    *,
    state_fn: _StateFn | None = None,
    stable_type_id: str | None = None,
) -> None: ...


@typing.overload
def register_memo_key_function(
    typ: type,
    key_fn: None = None,
    *,
    stable_type_id: str,
) -> None: ...


def register_memo_key_function(
    typ: type,
    key_fn: _KeyFn | None = None,
    *,
    state_fn: _StateFn | None = None,
    stable_type_id: str | None = None,
) -> None:
    """Register a memo key function and/or stable type ID for a type.

    Key-function resolution is MRO-aware: the most specific registered base
    type wins. Stable type IDs registered without a key function apply to the
    exact type only; stable type IDs registered with a key function identify
    that selected owner type. Each call replaces the full registration for
    ``typ``: omitting ``stable_type_id`` clears any previous registered stable
    type ID, and omitting ``key_fn`` or passing ``None`` clears any previous
    key/state functions.

    When a registered stable type ID should affect a value used in ``deps=``,
    call this before the corresponding ``@coco.fn`` / ``@coco.fn.as_async``
    decorator is applied because ``deps`` fingerprints are computed at
    decoration time.
    """

    if not isinstance(typ, type):
        raise TypeError(
            "register_memo_key_function() expects typ to be a type, "
            f"got {type(typ).__name__}"
        )
    if stable_type_id is not None:
        stable_type_id = _validate_stable_type_id(
            stable_type_id,
            source="register_memo_key_function(..., stable_type_id)",
        )
    if key_fn is None:
        if state_fn is not None:
            raise TypeError(
                "register_memo_key_function() state_fn requires a memo key function"
            )
        if stable_type_id is None:
            raise TypeError(
                "register_memo_key_function() requires a key_fn or stable_type_id"
            )
    elif not callable(key_fn):
        raise TypeError(
            "register_memo_key_function() key_fn must be callable, "
            f"got {type(key_fn).__name__}"
        )
    if state_fn is not None and not callable(state_fn):
        raise TypeError(
            "register_memo_key_function() state_fn must be callable, "
            f"got {type(state_fn).__name__}"
        )

    _register_memo_type_registry(
        typ,
        _MemoTypeRegistry(
            key_fn=key_fn,
            state_fn=state_fn,
            stable_type_id=stable_type_id,
        ),
    )


def register_not_memo_keyable(typ: type) -> None:
    """Register a type as not memo-keyable.

    Internal helper for tests and internal registrations. It is intentionally
    not exported through the public ``cocoindex`` namespace until registered
    not-memo-keyable precedence is fixed for types that define
    ``__coco_memo_key__`` or otherwise supply memo-key behavior. Public code
    should inherit from ``coco.NotMemoKeyable`` when the type is user-owned.
    """

    if not isinstance(typ, type):
        raise TypeError(
            "register_not_memo_keyable() expects typ to be a type, "
            f"got {type(typ).__name__}"
        )

    def _raise_not_memo_keyable(obj: object) -> typing.NoReturn:
        raise TypeError(
            f"{type(obj).__name__} cannot be used as a memoization key. "
            "This type maintains internal state that is incompatible with memoization."
        )

    _register_memo_type_registry(typ, _MemoTypeRegistry(key_fn=_raise_not_memo_keyable))


def unregister_memo_key_function(typ: type) -> None:
    """Remove registered memo key function and stable type ID (best-effort)."""

    if isinstance(typ, type):
        _unregister_memo_type_registry(typ)


def _stable_sort_key(v: Fingerprintable) -> tuple[typing.Any, ...]:
    """Return a totally-ordered key for canonical values.

    This is used to deterministically sort dict/set canonical encodings without
    relying on Python comparing heterogeneous values directly.
    """

    # Important: bool is a subclass of int; check bool first.
    if v is None:
        return (0,)
    if isinstance(v, bool):
        return (1, 1 if v else 0)
    if isinstance(v, int):
        return (2, v)
    if isinstance(v, float):
        if math.isnan(v):
            return (3, "nan")
        # Use IEEE-754 bytes for a deterministic ordering (including -0.0 vs 0.0).
        return (3, struct.pack("!d", v))
    if isinstance(v, str):
        return (4, v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return (5, bytes(v))
    if isinstance(v, typing.Sequence):
        return (6, tuple(_stable_sort_key(e) for e in v))

    # For others, don't try to sort and just return a placeholder.
    return (99,)


def _canonicalize(
    obj: object,
    state: _CanonicalizeState | None,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    if state is None:
        state = _CanonicalizeState()

    # 1) Primitives
    if obj is None:
        return None
    if isinstance(obj, (bool, int, float, str, bytes, core.Fingerprint)):
        # bool is a subclass of int; returning as-is preserves bools correctly.
        return obj
    if isinstance(obj, (bytearray, memoryview)):
        return bytes(obj)

    # 2) Memo key dispatch. Raw class objects skip memo attributes but honor
    # explicit key registrations on their metaclass MRO.
    if isinstance(obj, type):
        return _canonicalize_class_object(obj, state, state_methods)

    hook = getattr(obj, _MEMO_KEY_ATTR, None)
    if hook is not None and callable(hook):
        k = hook()
        typ = type(obj)
        tag = "hook"
        state_hook = getattr(obj, _MEMO_STATE_ATTR, None)
        if state_hook is not None and callable(state_hook):
            tag = "shook"
            # raw function for type hint extraction (unbound method on class)
            raw_fn = getattr(typ, _MEMO_STATE_ATTR)
            state_methods.append(_make_state_fn_entry(state_hook, raw_fn))
        return (
            tag,
            *_type_identity_parts(typ),
            _canonicalize_key_fragment(k, state, state_methods),
        )

    for owner in type(obj).__mro__:
        registry = _registered_memo_type_registry(owner)
        if registry is not None and registry.key_fn is not None:
            return _canonicalize_registered_memo_key(
                obj, owner, registry, state, state_methods
            )

    # 3) Cycle / shared-reference tracking
    #
    # Note: we intentionally do this before branching on container types, so the
    # logic is shared and we support cyclic/self-referential structures.
    ordinal = state.remember(obj)
    if ordinal is not None:
        return ("ref", ordinal)

    # 4) Containers
    if isinstance(obj, typing.Sequence):
        return ("seq", tuple(_canonicalize(e, state, state_methods) for e in obj))

    if isinstance(obj, typing.Mapping):
        items: list[tuple[Fingerprintable, Fingerprintable]] = []
        for k, v in obj.items():
            items.append(
                (
                    _canonicalize(k, state, state_methods),
                    _canonicalize(v, state, state_methods),
                )
            )
        items.sort(key=lambda kv: (_stable_sort_key(kv[0]), _stable_sort_key(kv[1])))
        return ("map", tuple(items))

    if isinstance(obj, (set, frozenset)):
        elts = [_canonicalize(e, state, state_methods) for e in obj]
        elts.sort(key=_stable_sort_key)
        return ("set", tuple(elts))

    # 5) Dataclass instances
    if _is_dataclass_instance(obj):
        return _canonicalize_dataclass(obj, state, state_methods)

    # 6) Pydantic v2 models
    if _is_pydantic_model(obj):
        return _canonicalize_pydantic(obj, state, state_methods)

    # 7) Fallback
    try:
        payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        # Tag to avoid colliding with user-provided raw bytes.
        return ("pickle", payload)
    except Exception:
        raise TypeError(
            f"Unsupported type for memoization key: {type(obj)!r}. "
            "Provide __coco_memo_key__() or register a memo key function."
        ) from None


def _make_call_canonical(
    func: typing.Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    state_methods: list[StateFnEntry],
    *,
    version: str | int | None = None,
    prefix_args: tuple[object, ...] = (),
) -> Fingerprintable:
    function_identity = (
        canonical_module_name(func),
        getattr(func, "__qualname__", None),
    )
    canonical_args = tuple(
        _canonicalize(a, state=None, state_methods=state_methods) for a in prefix_args
    )
    canonical_args = canonical_args + tuple(
        _canonicalize(a, state=None, state_methods=state_methods) for a in args
    )
    canonical_kwargs = tuple(
        (k, _canonicalize(v, state=None, state_methods=state_methods))
        for k, v in sorted(kwargs.items())
    )
    return (
        "memo_call_v1",
        function_identity,
        version,
        canonical_args,
        canonical_kwargs,
    )


def memo_fingerprint(obj: object) -> core.Fingerprint:
    # State methods are meaningless for an object-only fingerprint; collect
    # into a throwaway list so the canonicalizer signature stays uniform.
    return core.fingerprint_simple_object(
        _canonicalize(obj, state=None, state_methods=[])
    )


def fingerprint_call(
    func: typing.Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    state_methods: list[StateFnEntry],
    *,
    version: str | int | None = None,
    prefix_args: tuple[object, ...] = (),
) -> core.Fingerprint:
    """Compute the deterministic fingerprint for a function call.

    Returns a `cocoindex._internal.core.Fingerprint` object (Python wrapper around a
    stable 16-byte digest). Use `bytes(fp)` or `fp.as_bytes()` to get raw bytes.

    Any state methods discovered during canonicalization are appended to
    *state_methods* (used by the execution layer for memo state validation).
    Pass an empty list when state methods aren't needed.
    """

    call_key_obj = _make_call_canonical(
        func,
        args,
        kwargs,
        state_methods,
        version=version,
        prefix_args=prefix_args,
    )
    # One Python -> Rust call.
    return core.fingerprint_simple_object(call_key_obj)


__all__ = [
    "NotMemoKeyable",
    "prev_type_id",
    "register_memo_key_function",
    "register_not_memo_keyable",
    "unregister_memo_key_function",
    "fingerprint_call",
    "memo_fingerprint",
]
