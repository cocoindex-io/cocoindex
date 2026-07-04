"""
Persistent memoization fingerprinting (implementation).

This module implements the Python-side canonicalization described in
`docs/docs/dev/memo_key.md`, and relies on a single Rust call to hash the final
canonical form into a fixed-size fingerprint.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
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
_BoundKeyFn = typing.Callable[[], typing.Any]
_StateFn = typing.Callable[[typing.Any, typing.Any], typing.Any]


class _MemoFns(typing.NamedTuple):
    key_fn: _KeyFn
    state_fn: _StateFn | None = None


_memo_fns: dict[type, _MemoFns] = {}
_memo_type_identifiers: dict[int, tuple[weakref.ReferenceType[type], str]] = {}


class StateFnEntry(typing.NamedTuple):
    """A state method paired with a deserializer for its ``prev_state`` parameter.

    ``deserialize_prev`` converts a ``StoredValue`` (or ``NON_EXISTENCE``) into the
    typed Python object expected by the state method.
    ``call`` is the original state method (bound to its instance).
    """

    deserialize_prev: typing.Callable[[typing.Any], typing.Any]
    call: typing.Callable[[typing.Any], typing.Any]


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
    """Return a user-facing label for type-identifier validation errors."""
    return f"{canonical_module_name(typ)}.{getattr(typ, '__qualname__', '<unknown>')}"


def _validate_memo_type_identifier(identifier: object, *, source: str) -> str:
    """Validate a non-empty stable memo type identifier."""
    if not isinstance(identifier, str):
        raise TypeError(f"{source} must be a str, got {type(identifier).__name__}")
    if identifier.strip() == "":
        raise ValueError(
            f"{source} must be non-empty and contain non-whitespace characters"
        )
    return identifier


def _remove_memo_type_identifier(
    type_id: int, dead_ref: weakref.ReferenceType[type]
) -> None:
    """Remove ``type_id`` only if ``dead_ref`` is still the stored weakref."""
    entry = _memo_type_identifiers.get(type_id)
    if entry is not None and entry[0] is dead_ref:
        _memo_type_identifiers.pop(type_id, None)


def register_memo_type_identifier(typ: type, identifier: str) -> None:
    """Register a stable memo type identity for one exact Python type.

    Type-aware fingerprints use it instead of module+qualname. Registration
    overrides ``typ.__coco_memo_type_id__``; explicit class-object hooks and
    pickle fallback must carry their own stable key.
    """
    if not isinstance(typ, type):
        raise TypeError(
            "register_memo_type_identifier() expects typ to be a type, "
            f"got {type(typ).__name__}"
        )
    identifier = _validate_memo_type_identifier(
        identifier, source="register_memo_type_identifier(..., identifier)"
    )
    type_id = id(typ)

    def _remove_stale_type_identifier(
        dead_ref: weakref.ReferenceType[type],
    ) -> None:
        _remove_memo_type_identifier(type_id, dead_ref)

    _memo_type_identifiers[type_id] = (
        weakref.ref(typ, _remove_stale_type_identifier),
        identifier,
    )


def _unregister_memo_type_identifier(typ: type) -> None:
    """Best-effort test helper for removing an exact-type registration."""
    type_id = id(typ)
    entry = _memo_type_identifiers.get(type_id)
    if entry is not None and entry[0]() is typ:
        _memo_type_identifiers.pop(type_id, None)


def _registered_memo_type_identifier(typ: type) -> str | None:
    """Return the registered identifier for ``typ`` from the id-keyed table."""
    type_id = id(typ)
    entry = _memo_type_identifiers.get(type_id)
    if entry is None:
        return None
    ref, identifier = entry
    if ref() is typ:
        return identifier
    _memo_type_identifiers.pop(type_id, None)
    return None


def _lookup_memo_type_identifier(typ: type) -> str | None:
    """Resolve a registered or exact ``__coco_memo_type_id__`` identifier."""
    identifier = _registered_memo_type_identifier(typ)
    if identifier is not None:
        return identifier
    if "__coco_memo_type_id__" in typ.__dict__:
        return _validate_memo_type_identifier(
            typ.__dict__["__coco_memo_type_id__"],
            source=f"{_memo_type_label(typ)}.__coco_memo_type_id__",
        )
    return None


_MEMO_KEY_ATTR = "__coco_memo_key__"
_MEMO_STATE_ATTR = "__coco_memo_state__"


def _callable_memo_hook(hook: object) -> _BoundKeyFn | None:
    """Return ``hook`` when it is callable."""
    if not callable(hook):
        return None
    return typing.cast(_BoundKeyFn, hook)


def _python_callable_accepts_no_args(hook: object) -> bool:
    """Return whether a Python function/method can bind zero arguments.

    This is deliberately limited to regular Python callables.  Some valid
    callable objects and C-extension functions cannot expose an inspectable
    signature; those are accepted and validated by the actual call.
    """
    if not (inspect.isfunction(hook) or inspect.ismethod(hook)):
        return True
    try:
        inspect.signature(hook).bind()
    except TypeError:
        return False
    return True


def _metaclass_object_memo_hook(cls: type) -> _BoundKeyFn | None:
    """Resolve a memo key hook from ``cls``'s metaclass."""
    for base in typing.cast(type, type(cls)).__mro__:
        raw = base.__dict__.get(_MEMO_KEY_ATTR)
        if raw is None:
            continue
        hook = (
            typing.cast(typing.Any, raw).__get__(cls, type(cls))
            if hasattr(raw, "__get__")
            else raw
        )
        return _callable_memo_hook(hook)
    return None


def _class_object_memo_hook(cls: type) -> _BoundKeyFn | None:
    """Resolve an explicit memo key hook for a class object.

    Plain class-body methods that require an instance are ignored because there
    is no ``self`` to bind.  Zero-argument functions assigned as class
    attributes, descriptor-based hooks, callable objects, and metaclass hooks
    remain valid class-object hooks.
    """
    for base in cls.__mro__:
        raw = base.__dict__.get(_MEMO_KEY_ATTR)
        if raw is None:
            continue
        hook = getattr(cls, _MEMO_KEY_ATTR, None)
        if isinstance(raw, (classmethod, staticmethod)):
            if not callable(hook) or not _python_callable_accepts_no_args(hook):
                hook_kind = (
                    "classmethod" if isinstance(raw, classmethod) else "staticmethod"
                )
                raise TypeError(
                    f"{_memo_type_label(base)}.{_MEMO_KEY_ATTR} is a {hook_kind} "
                    "that cannot be called with zero arguments; class-object hooks "
                    "must take no arguments after binding"
                )
            return typing.cast(_BoundKeyFn, hook)
        if callable(hook) and _python_callable_accepts_no_args(hook):
            return typing.cast(_BoundKeyFn, hook)
        break
    return _metaclass_object_memo_hook(cls)


def _class_object_state_fn_entry(cls: type) -> StateFnEntry | None:
    """Resolve a memo state hook bound to ``cls``'s metaclass."""
    typ = type(cls)
    for base in typing.cast(type, typ).__mro__:
        raw = base.__dict__.get(_MEMO_STATE_ATTR)
        if raw is None:
            continue
        state_hook = (
            typing.cast(typing.Any, raw).__get__(cls, typ)
            if hasattr(raw, "__get__")
            else raw
        )
        if not callable(state_hook):
            return None
        raw_fn = getattr(typ, _MEMO_STATE_ATTR)
        return _make_state_fn_entry(state_hook, raw_fn)
    return None


def _type_identity_parts(typ: type) -> tuple[Fingerprintable, Fingerprintable]:
    """Return stable-ID or module+qualname type identity parts."""
    identifier = _lookup_memo_type_identifier(typ)
    if identifier is not None:
        return (("__coco_memo_type_id__", identifier), None)
    return (canonical_module_name(typ), getattr(typ, "__qualname__", None))


def _is_dataclass_instance(obj: object) -> bool:
    """Check if obj is a dataclass instance (not a class)."""
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


def _is_pydantic_model(obj: object) -> bool:
    """Check if obj is a Pydantic v2 model instance."""
    return hasattr(obj, "__pydantic_fields__") and not isinstance(obj, type)  # type: ignore[attr-defined]


def _canonicalize_dataclass(
    obj: object,
    _seen: dict[int, int],
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
            (field.name, _canonicalize(getattr(obj, field.name), _seen, state_methods))
            for field in fields
        ),
    )


def _canonicalize_pydantic(
    obj: object,
    _seen: dict[int, int],
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
            (name, _canonicalize(getattr(obj, name), _seen, state_methods))
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


def register_memo_key_function(
    typ: type, key_fn: _KeyFn, *, state_fn: _StateFn | None = None
) -> None:
    """Register a memo key function for a type.

    Resolution is MRO-aware: the most specific registered base type wins.

    If *state_fn* is provided it is stored separately and used for memo state
    validation (see ``_canonicalize``).
    """

    _memo_fns[typ] = _MemoFns(key_fn, state_fn)


def register_not_memo_keyable(typ: type) -> None:
    """Register a type as not memo-keyable.

    Use this for third-party types that maintain internal state incompatible
    with memoization, but which you cannot modify to inherit from `NotMemoKeyable`.

    Example:
        import cocoindex as coco
        from some_library import StatefulGenerator

        coco.register_not_memo_keyable(StatefulGenerator)
    """

    def _raise_not_memo_keyable(obj: object) -> typing.NoReturn:
        raise TypeError(
            f"{type(obj).__name__} cannot be used as a memoization key. "
            "This type maintains internal state that is incompatible with memoization."
        )

    _memo_fns[typ] = _MemoFns(_raise_not_memo_keyable)


def unregister_memo_key_function(typ: type) -> None:
    """Remove a previously registered memo key function (best-effort)."""

    _memo_fns.pop(typ, None)


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
    _seen: dict[int, int] | None,
    state_methods: list[StateFnEntry],
) -> Fingerprintable:
    # 0) Cycle / shared-reference tracking for containers
    if _seen is None:
        _seen = {}

    # 1) Primitives
    if obj is None:
        return None
    if isinstance(obj, (bool, int, float, str, bytes, core.Fingerprint)):
        # bool is a subclass of int; returning as-is preserves bools correctly.
        return obj
    if isinstance(obj, (bytearray, memoryview)):
        return bytes(obj)

    # 2) Hook / registry (apply once, then recurse on returned key fragment)
    hook = (
        _class_object_memo_hook(obj)
        if isinstance(obj, type)
        else getattr(obj, _MEMO_KEY_ATTR, None)
    )
    if hook is not None and callable(hook):
        k = hook()
        typ = type(obj)
        tag = "hook"
        if isinstance(obj, type):
            state_entry = _class_object_state_fn_entry(obj)
            if state_entry is not None:
                tag = "shook"
                state_methods.append(state_entry)
        else:
            state_hook = getattr(obj, _MEMO_STATE_ATTR, None)
            if state_hook is not None and callable(state_hook):
                tag = "shook"
                # raw function for type hint extraction (unbound method on class)
                raw_fn = getattr(typ, _MEMO_STATE_ATTR)
                state_methods.append(_make_state_fn_entry(state_hook, raw_fn))
        return (
            tag,
            *_type_identity_parts(typ),
            _canonicalize(k, _seen, state_methods),
        )

    for base in type(obj).__mro__:
        memo = _memo_fns.get(base)
        if memo is not None:
            k = memo.key_fn(obj)
            tag = "hook"
            if memo.state_fn is not None:
                tag = "shook"
                bound = functools.partial(memo.state_fn, obj)
                state_methods.append(_make_state_fn_entry(bound, memo.state_fn))
            return (
                tag,
                *_type_identity_parts(base),
                _canonicalize(k, _seen, state_methods),
            )

    # 3) Cycle / shared-reference tracking
    #
    # Note: we intentionally do this before branching on container types, so the
    # logic is shared and we support cyclic/self-referential structures.
    oid = id(obj)
    ordinal = _seen.get(oid)
    if ordinal is not None:
        return ("ref", ordinal)
    _seen[oid] = len(_seen)

    # 4) Containers
    if isinstance(obj, typing.Sequence):
        return ("seq", tuple(_canonicalize(e, _seen, state_methods) for e in obj))

    if isinstance(obj, typing.Mapping):
        items: list[tuple[Fingerprintable, Fingerprintable]] = []
        for k, v in obj.items():
            items.append(
                (
                    _canonicalize(k, _seen, state_methods),
                    _canonicalize(v, _seen, state_methods),
                )
            )
        items.sort(key=lambda kv: (_stable_sort_key(kv[0]), _stable_sort_key(kv[1])))
        return ("map", tuple(items))

    if isinstance(obj, (set, frozenset)):
        elts = [_canonicalize(e, _seen, state_methods) for e in obj]
        elts.sort(key=_stable_sort_key)
        return ("set", tuple(elts))

    # 5) Dataclass instances
    if _is_dataclass_instance(obj):
        return _canonicalize_dataclass(obj, _seen, state_methods)

    # 6) Pydantic v2 models
    if _is_pydantic_model(obj):
        return _canonicalize_pydantic(obj, _seen, state_methods)

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
        _canonicalize(a, _seen=None, state_methods=state_methods) for a in prefix_args
    )
    canonical_args = canonical_args + tuple(
        _canonicalize(a, _seen=None, state_methods=state_methods) for a in args
    )
    canonical_kwargs = tuple(
        (k, _canonicalize(v, _seen=None, state_methods=state_methods))
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
        _canonicalize(obj, _seen=None, state_methods=[])
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


# Register memo key for class types.
register_memo_key_function(type, lambda cls: _type_identity_parts(cls))


__all__ = [
    "NotMemoKeyable",
    "register_memo_key_function",
    "register_memo_type_identifier",
    "register_not_memo_keyable",
    "unregister_memo_key_function",
    "fingerprint_call",
    "memo_fingerprint",
]
