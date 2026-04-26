"""
Persistent memoization fingerprinting (implementation).

This module implements the Python-side canonicalization described in
`docs/docs/dev/memo_key.md`, and relies on a single Rust call to hash the final
canonical form into a fixed-size fingerprint.
"""

from __future__ import annotations

import dataclasses
import functools
import math
import pickle
import struct
import typing

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


class _MemoFns(typing.NamedTuple):
    key_fn: _KeyFn
    state_fn: _StateFn | None = None


_memo_fns: dict[type, _MemoFns] = {}


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


def _is_dataclass_instance(obj: object) -> bool:
    """Check if obj is a dataclass instance (not a class)."""
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


def _is_pydantic_model(obj: object) -> bool:
    """Check if obj is a Pydantic v2 model instance."""
    return hasattr(obj, "__pydantic_fields__") and not isinstance(obj, type)  # type: ignore[attr-defined]


def _canonicalize_dataclass(obj: object, _seen: dict[int, int]) -> Fingerprintable:
    """Canonicalize a dataclass instance.

    Preserves field definition order and includes all fields.
    Format: ("dataclass", module, qualname, ((field_name, value), ...))
    """
    typ = type(obj)
    fields = dataclasses.fields(obj)  # type: ignore[arg-type]
    return (
        "dataclass",
        typ.__module__,
        typ.__qualname__,
        tuple(
            (field.name, _canonicalize(getattr(obj, field.name), _seen))
            for field in fields
        ),
    )


def _canonicalize_pydantic(obj: object, _seen: dict[int, int]) -> Fingerprintable:
    """Canonicalize a Pydantic v2 model instance.

    Includes all fields (set and unset) to ensure determinism.
    Format: ("pydantic", module, qualname, ((field_name, value), ...))
    """
    typ = type(obj)
    field_names = obj.__pydantic_fields__.keys()  # type: ignore[attr-defined]
    return (
        "pydantic",
        typ.__module__,
        typ.__qualname__,
        tuple((name, _canonicalize(getattr(obj, name), _seen)) for name in field_names),
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
    state_methods: list[StateFnEntry] | None = None,
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
    hook = getattr(obj, "__coco_memo_key__", None)
    if hook is not None and callable(hook):
        k = hook()
        typ = type(obj)
        tag = "hook"
        state_hook = getattr(obj, "__coco_memo_state__", None)
        if state_hook is not None and callable(state_hook):
            tag = "shook"
            if state_methods is not None:
                # raw function for type hint extraction (unbound method on class)
                raw_fn = getattr(typ, "__coco_memo_state__")
                state_methods.append(_make_state_fn_entry(state_hook, raw_fn))
        return (
            tag,
            typ.__module__,
            typ.__qualname__,
            _canonicalize(k, _seen, state_methods),
        )

    for base in type(obj).__mro__:
        memo = _memo_fns.get(base)
        if memo is not None:
            k = memo.key_fn(obj)
            tag = "hook"
            if memo.state_fn is not None:
                tag = "shook"
                if state_methods is not None:
                    bound = functools.partial(memo.state_fn, obj)
                    state_methods.append(_make_state_fn_entry(bound, memo.state_fn))
            return (
                tag,
                base.__module__,
                base.__qualname__,
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
        return _canonicalize_dataclass(obj, _seen)

    # 6) Pydantic v2 models
    if _is_pydantic_model(obj):
        return _canonicalize_pydantic(obj, _seen)

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
    *,
    version: str | int | None = None,
    state_methods: list[StateFnEntry] | None = None,
    prefix_args: tuple[object, ...] = (),
) -> Fingerprintable:
    function_identity = (
        getattr(func, "__module__", None),
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
    return core.fingerprint_simple_object(_canonicalize(obj, _seen=None))


def fingerprint_call(
    func: typing.Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    *,
    version: str | int | None = None,
    state_methods: list[StateFnEntry] | None = None,
    prefix_args: tuple[object, ...] = (),
) -> core.Fingerprint:
    """Compute the deterministic fingerprint for a function call.

    Returns a `cocoindex._internal.core.Fingerprint` object (Python wrapper around a
    stable 16-byte digest). Use `bytes(fp)` or `fp.as_bytes()` to get raw bytes.

    If *state_methods* is provided, any state methods discovered during
    canonicalization are appended to it (used by the execution layer for memo
    state validation).
    """

    call_key_obj = _make_call_canonical(
        func,
        args,
        kwargs,
        version=version,
        state_methods=state_methods,
        prefix_args=prefix_args,
    )
    # One Python -> Rust call.
    return core.fingerprint_simple_object(call_key_obj)


# Register memo key for class types.
register_memo_key_function(
    type,
    lambda cls: (getattr(cls, "__module__", None), getattr(cls, "__qualname__", None)),
)


__all__ = [
    "NotMemoKeyable",
    "register_memo_key_function",
    "register_not_memo_keyable",
    "unregister_memo_key_function",
    "fingerprint_call",
    "memo_fingerprint",
]
