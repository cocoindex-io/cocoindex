"""
Persistent memoization fingerprinting (implementation).

This module implements the Python-side canonicalization described in
`docs/docs/dev/memo_key.md`, and relies on a single Rust call to hash the final
canonical call key object into a fixed-size fingerprint.
"""

from __future__ import annotations

import math
import pickle
import struct
import typing

from . import core  # type: ignore


_KeyFn = typing.Callable[[typing.Any], typing.Any]


class Fingerprint(typing.Protocol):
    """Structural type for `cocoindex._internal.core.Fingerprint` (pyo3, untyped)."""

    def as_bytes(self) -> bytes: ...
    def to_base64(self) -> str: ...
    def __bytes__(self) -> bytes: ...


# Canonical values are composed of primitives and tuples of canonical values.
_CanonicalValue: typing.TypeAlias = typing.Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    tuple["_CanonicalValue", ...],
]

_memo_key_fns: dict[type, _KeyFn] = {}


def register_memo_key_function(typ: type, key_fn: _KeyFn) -> None:
    """Register a memo key function for a type.

    Resolution is MRO-aware: the most specific registered base type wins.
    """

    _memo_key_fns[typ] = key_fn


def unregister_memo_key_function(typ: type) -> None:
    """Remove a previously registered memo key function (best-effort)."""

    _memo_key_fns.pop(typ, None)


def _stable_sort_key(v: _CanonicalValue) -> tuple[typing.Any, ...]:
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
    if isinstance(v, tuple):
        return (6, tuple(_stable_sort_key(e) for e in v))
    raise TypeError(f"Unexpected non-canonical value encountered: {type(v)!r}")


_PRIMITIVE_TYPES: tuple[type, ...] = (bool, int, float, str, bytes)


def _canonicalize(obj: object, _seen: dict[int, int] | None) -> _CanonicalValue:
    # 0) Cycle / shared-reference tracking for containers
    if _seen is None:
        _seen = {}

    # 1) Primitives
    if obj is None:
        return None
    if isinstance(obj, _PRIMITIVE_TYPES):
        # bool is a subclass of int; returning as-is preserves bools correctly.
        return obj  # type: ignore[return-value]
    if isinstance(obj, (bytearray, memoryview)):
        return bytes(obj)

    # 2) Hook / registry (apply once, then recurse on returned key fragment)
    hook = getattr(obj, "__coco_memo_key__", None)
    if hook is not None and callable(hook):
        k = hook()
        typ = type(obj)
        return ("hook", typ.__module__, typ.__qualname__, _canonicalize(k, _seen))

    for base in type(obj).__mro__:
        fn = _memo_key_fns.get(base)
        if fn is not None:
            k = fn(obj)
            return ("hook", base.__module__, base.__qualname__, _canonicalize(k, _seen))

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
    if isinstance(obj, tuple):
        return ("tuple", tuple(_canonicalize(e, _seen) for e in obj))

    if isinstance(obj, list):
        return ("list", tuple(_canonicalize(e, _seen) for e in obj))

    if isinstance(obj, dict):
        items: list[tuple[_CanonicalValue, _CanonicalValue]] = []
        for k, v in obj.items():
            items.append((_canonicalize(k, _seen), _canonicalize(v, _seen)))
        items.sort(key=lambda kv: (_stable_sort_key(kv[0]), _stable_sort_key(kv[1])))
        return ("dict", tuple(items))

    if isinstance(obj, (set, frozenset)):
        elts = [_canonicalize(e, _seen) for e in obj]
        elts.sort(key=_stable_sort_key)
        return ("set", tuple(elts))

    # 5) Fallback
    try:
        payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        # Tag to avoid colliding with user-provided raw bytes.
        return ("pickle", payload)
    except Exception:
        raise TypeError(
            f"Unsupported type for memoization key: {type(obj)!r}. "
            "Provide __coco_memo_key__() or register a memo key function."
        ) from None


def _make_call_key_obj(
    func: typing.Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    *,
    version: str | int | None = None,
) -> _CanonicalValue:
    function_identity = (
        getattr(func, "__module__", None),
        getattr(func, "__qualname__", None),
    )
    canonical_args = tuple(_canonicalize(a, _seen=None) for a in args)
    canonical_kwargs = tuple(
        (k, _canonicalize(v, _seen=None)) for k, v in sorted(kwargs.items())
    )
    return (
        "memo_call_v1",
        function_identity,
        version,
        canonical_args,
        canonical_kwargs,
    )


def fingerprint_call(
    func: typing.Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    *,
    version: str | int | None = None,
) -> Fingerprint:
    """Compute the deterministic fingerprint for a function call.

    Returns a `cocoindex._internal.core.Fingerprint` object (Python wrapper around a
    stable 16-byte digest). Use `bytes(fp)` or `fp.as_bytes()` to get raw bytes.
    """

    call_key_obj = _make_call_key_obj(
        func,
        args,
        kwargs,
        version=version,
    )
    # One Python -> Rust call.
    return core.fingerprint_memo_key(call_key_obj)  # type: ignore


__all__ = [
    "Fingerprint",
    "register_memo_key_function",
    "unregister_memo_key_function",
    "fingerprint_call",
]
