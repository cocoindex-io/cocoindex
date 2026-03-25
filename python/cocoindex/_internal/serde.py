import datetime
import functools
import inspect
import io
import pathlib
import pickle
import threading
import types
import typing
import uuid
from collections.abc import Callable
from typing import Any, TypeAlias

import msgspec.msgpack


# ---------------------------------------------------------------------------
# Global registry: (module, qualname) -> Python object  (for restricted unpickle)
# ---------------------------------------------------------------------------

_UNPICKLE_SAFE_GLOBALS: dict[tuple[str, str], object] = {}

_BUILTIN_UNPICKLE_SAFE_TYPES: tuple[type, ...] = (
    bool,
    int,
    float,
    complex,
    str,
    bytes,
    bytearray,
    list,
    tuple,
    dict,
    set,
    frozenset,
    type(None),
)


def _all_subclasses(cls: type) -> list[type]:
    """Recursively collect all subclasses of a type."""
    result: list[type] = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_all_subclasses(sub))
    return result


_STDLIB_UNPICKLE_SAFE_TYPES: tuple[type, ...] = (
    pathlib.PurePath,
    *_all_subclasses(pathlib.PurePath),
    uuid.UUID,
    datetime.datetime,
    datetime.date,
    datetime.time,
    datetime.timedelta,
    datetime.timezone,
)


def _register_builtin_types() -> None:
    for t in _BUILTIN_UNPICKLE_SAFE_TYPES:
        _UNPICKLE_SAFE_GLOBALS[(t.__module__, t.__qualname__)] = t
    for t in _STDLIB_UNPICKLE_SAFE_TYPES:
        _UNPICKLE_SAFE_GLOBALS[(t.__module__, t.__qualname__)] = t

    # numpy (optional): register reconstruct globals needed for ndarray unpickling
    try:
        import numpy as np

        _UNPICKLE_SAFE_GLOBALS[("numpy", "dtype")] = np.dtype
        _UNPICKLE_SAFE_GLOBALS[("numpy", "ndarray")] = np.ndarray
        for _dtype_sub in _all_subclasses(np.dtype):
            _UNPICKLE_SAFE_GLOBALS[(_dtype_sub.__module__, _dtype_sub.__qualname__)] = (
                _dtype_sub
            )
        _core_numeric = getattr(np, "_core", None)
        if _core_numeric is not None:
            _frombuffer = getattr(_core_numeric.numeric, "_frombuffer", None)
            if _frombuffer is not None:
                _UNPICKLE_SAFE_GLOBALS[("numpy._core.numeric", "_frombuffer")] = (
                    _frombuffer
                )
    except ImportError:
        pass


_register_builtin_types()


# ---------------------------------------------------------------------------
# Pickle-serialize type set (types that MUST use pickle at top level)
# ---------------------------------------------------------------------------

_SERIALIZE_BY_PICKLE_TYPES: set[type] = set()


def _register_builtin_pickle_types() -> None:
    """Register types that must always use pickle serialization."""
    # complex is not natively supported by msgspec
    _SERIALIZE_BY_PICKLE_TYPES.add(complex)
    # pathlib types
    _SERIALIZE_BY_PICKLE_TYPES.add(pathlib.PurePath)
    for sub in _all_subclasses(pathlib.PurePath):
        _SERIALIZE_BY_PICKLE_TYPES.add(sub)
    # numpy (optional)
    try:
        import numpy as np

        _SERIALIZE_BY_PICKLE_TYPES.add(np.ndarray)
        _SERIALIZE_BY_PICKLE_TYPES.add(np.dtype)
    except ImportError:
        pass


_register_builtin_pickle_types()


# ---------------------------------------------------------------------------
# Public registration APIs
# ---------------------------------------------------------------------------


def unpickle_safe(cls: type) -> type:
    """Mark a class as safe to unpickle. Use as a decorator."""
    _UNPICKLE_SAFE_GLOBALS[(cls.__module__, cls.__qualname__)] = cls
    return cls


def _add_unpickle_safe_global(module: str, qualname: str, obj: object) -> None:
    """Register a non-type callable as safe to unpickle (internal use only)."""
    _UNPICKLE_SAFE_GLOBALS[(module, qualname)] = obj


def serialize_by_pickle(cls: type) -> type:
    """Decorator: serialize this type with pickle. Auto-registers as unpickle-safe."""
    _SERIALIZE_BY_PICKLE_TYPES.add(cls)
    unpickle_safe(cls)
    return cls


# ---------------------------------------------------------------------------
# Restricted unpickler
# ---------------------------------------------------------------------------


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> object:
        result = _UNPICKLE_SAFE_GLOBALS.get((module, name))
        if result is None:
            raise pickle.UnpicklingError(
                f"Forbidden global during unpickling: {module}.{name}"
            )
        return result


# ---------------------------------------------------------------------------
# Strict serialization (opt-in, for use in tests)
# ---------------------------------------------------------------------------

_strict_serialize: bool = False


def enable_strict_serialize() -> None:
    """Enable strict serialization type checking (call once in test setup)."""
    global _strict_serialize
    _strict_serialize = True


class _StrictPickler(pickle.Pickler):
    """Pickler that validates every object's type is in the unpickle allow-list."""

    def reducer_override(self, obj: object) -> object:
        # When obj is a class being pickled as a global reference, check it directly.
        if isinstance(obj, type):
            if obj.__module__ != "builtins":
                key = (obj.__module__, obj.__qualname__)
                if key not in _UNPICKLE_SAFE_GLOBALS:
                    raise pickle.PicklingError(
                        f"Type not registered for safe unpickling: {obj.__module__}.{obj.__qualname__}"
                    )
            return NotImplemented
        # For instances, check their type.
        t = type(obj)
        if t.__module__ == "builtins":
            return NotImplemented
        key = (t.__module__, t.__qualname__)
        if key not in _UNPICKLE_SAFE_GLOBALS:
            raise pickle.PicklingError(
                f"Type not registered for safe unpickling: {t.__module__}.{t.__qualname__}"
            )
        return NotImplemented


# ---------------------------------------------------------------------------
# Pydantic detection (conditional — pydantic is optional)
# ---------------------------------------------------------------------------


def _is_pydantic_instance(obj: Any) -> bool:
    return hasattr(obj, "__pydantic_fields__")


def _is_pydantic_model_type(tp: Any) -> bool:
    try:
        import pydantic

        return isinstance(tp, type) and issubclass(tp, pydantic.BaseModel)
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Strict pickle dumps (validates all nested types)
# ---------------------------------------------------------------------------


def _strict_pickle_dumps(value: Any) -> bytes:
    """Pickle with strict type validation. Always uses protocol 5."""
    if _strict_serialize:
        buf = io.BytesIO()
        _StrictPickler(buf, 5).dump(value)
        return buf.getvalue()
    return pickle.dumps(value, 5)


# ---------------------------------------------------------------------------
# Serialization hooks (cross-pollination bridge)
# ---------------------------------------------------------------------------


def _enc_hook(obj: Any) -> Any:
    """Msgspec enc_hook: handles types msgspec can't encode natively."""
    # C: Quarantine pickle types (check first — explicit opt-in wins)
    if type(obj) in _SERIALIZE_BY_PICKLE_TYPES:
        return msgspec.msgpack.Ext(100, _strict_pickle_dumps(obj))
    key = (type(obj).__module__, type(obj).__qualname__)
    if key in _UNPICKLE_SAFE_GLOBALS:
        return msgspec.msgpack.Ext(100, _strict_pickle_dumps(obj))
    # B: Bridge Pydantic into msgspec
    if _is_pydantic_instance(obj):
        return obj.model_dump(mode="json")
    raise NotImplementedError(f"Cannot serialize {type(obj).__name__}")


_msgspec_encoder = msgspec.msgpack.Encoder(enc_hook=_enc_hook)


# ---------------------------------------------------------------------------
# Deserialization hooks
# ---------------------------------------------------------------------------


def _ext_hook(code: int, data: memoryview) -> Any:  # type: ignore[type-arg]
    """Un-quarantine pickle inside msgspec payloads."""
    if code == 100:
        return _RestrictedUnpickler(io.BytesIO(bytes(data))).load()
    raise ValueError(f"Unknown extension code: {code}")


def _dec_hook(type_hint: Any, obj: Any) -> Any:
    """Reconstruct Pydantic models nested inside msgspec payloads."""
    if _is_pydantic_model_type(type_hint):
        return type_hint.model_validate(obj)
    raise TypeError(f"Cannot decode type: {type_hint}")


# ---------------------------------------------------------------------------
# DeserializeFn: type alias and factory
# ---------------------------------------------------------------------------


class DeserializationError(Exception):
    """Raised when deserialization fails, wrapping the original exception with context."""


DeserializeFn: TypeAlias = Callable[[bytes | memoryview], Any]
"""A callable that deserializes bytes into a Python object."""


def qualified_name(fn: Any) -> str:
    """Return ``module.qualname`` for a function or method, or ``repr(fn)`` as fallback."""
    module: str | None = getattr(fn, "__module__", None)
    qualname: str | None = getattr(fn, "__qualname__", None) or getattr(
        fn, "__name__", None
    )
    if qualname is None:
        return str(repr(fn))
    if module is not None:
        return f"{module}.{qualname}"
    return qualname


def make_deserialize_fn(
    type_hint: Any,
    source_label: str | None = None,
) -> DeserializeFn:
    """Create a ``DeserializeFn`` for the given type hint.

    Returns a closure that handles routing-byte dispatch
    (``0x01`` msgspec, ``0x02`` pydantic, ``0x80`` pickle).

    The msgspec Decoder is constructed eagerly; callers are responsible for
    resolving forward-reference type hints before calling this function.
    The Pydantic TypeAdapter is still lazy (pydantic is optional).

    *source_label* is included in error messages to identify where the type
    hint came from (e.g. ``"return type of process_file()"``).
    """
    decoder = msgspec.msgpack.Decoder(
        type=type_hint, ext_hook=_ext_hook, dec_hook=_dec_hook
    )
    pydantic_adapter: Any = None
    pydantic_lock = threading.Lock()

    def _error_context() -> str:
        parts = [f"type_hint={type_hint!r}"]
        if source_label is not None:
            parts.append(f"source={source_label}")
        return ", ".join(parts)

    def _deserialize(data: bytes | memoryview) -> Any:
        nonlocal pydantic_adapter
        mv = memoryview(data) if not isinstance(data, memoryview) else data
        routing_byte = mv[0]

        # A: Msgspec (most common)
        if routing_byte == 0x01:
            try:
                return decoder.decode(mv[1:])
            except Exception as e:
                raise DeserializationError(
                    f"Failed to deserialize msgspec payload ({_error_context()})"
                ) from e

        # B: Pydantic
        if routing_byte == 0x02:
            try:
                if pydantic_adapter is None:
                    with pydantic_lock:
                        if pydantic_adapter is None:
                            import pydantic

                            pydantic_adapter = pydantic.TypeAdapter(type_hint)
                raw = msgspec.msgpack.decode(mv[1:], ext_hook=_ext_hook)
                if type_hint is Any:
                    return raw
                return pydantic_adapter.validate_python(raw)
            except Exception as e:
                raise DeserializationError(
                    f"Failed to deserialize pydantic payload ({_error_context()})"
                ) from e

        # C: Pickle (legacy and @serialize_by_pickle)
        if routing_byte == 0x80:
            try:
                return _RestrictedUnpickler(io.BytesIO(bytes(mv))).load()
            except Exception as e:
                raise DeserializationError(
                    f"Failed to deserialize pickle payload ({_error_context()})"
                ) from e

        raise DeserializationError(
            f"Unknown routing byte: {routing_byte:#x} ({_error_context()})"
        )

    return _deserialize


# ---------------------------------------------------------------------------
# Top-level serialize / deserialize
# ---------------------------------------------------------------------------


def serialize(value: Any) -> bytes:
    """Serialize a value using the routing-byte protocol (C → B → A priority)."""
    # C: Explicit pickle (user opted in — highest priority)
    if type(value) in _SERIALIZE_BY_PICKLE_TYPES:
        return _strict_pickle_dumps(value)

    # B: Pydantic BaseModel
    if _is_pydantic_instance(value):
        payload = msgspec.msgpack.encode(
            value.model_dump(mode="json"),
            enc_hook=_enc_hook,
        )
        return b"\x02" + payload

    # A: Msgspec (default for dataclasses, NamedTuples, primitives, collections)
    return b"\x01" + _msgspec_encoder.encode(value)


@functools.cache
def _get_deserialize_fn(type_hint: Any) -> DeserializeFn:
    return make_deserialize_fn(type_hint)


def deserialize(data: bytes, type_hint: Any = Any) -> Any:
    """Deserialize data using the routing-byte protocol."""
    return _get_deserialize_fn(type_hint)(data)


def fn_ret_deserializer(fn: Any) -> DeserializeFn:
    """Return a ``DeserializeFn`` that deserializes *fn*'s return type.

    Zero upfront cost — all work is deferred to the first call.
    For ``@coco.fn``-decorated functions the pre-built ``DeserializeFn`` is reused.
    For plain functions the return-type annotation is inspected at call time.
    """
    fn_label = qualified_name(fn)

    def _deserialize(data: bytes | memoryview) -> Any:
        cached: DeserializeFn | None = getattr(
            fn, "_resolved_return_deserializer", None
        )
        if cached is not None:
            return cached(data)
        try:
            hint = typing.get_type_hints(fn).get("return", Any)
        except Exception:
            hint = Any
        return make_deserialize_fn(hint, source_label=f"return type of {fn_label}()")(
            data
        )

    return _deserialize


# ---------------------------------------------------------------------------
# Type hint extraction helpers
# ---------------------------------------------------------------------------


def get_param_annotation(func: Any, position: int) -> Any:
    """Resolve the type annotation for the parameter at *position* in *func*.

    Uses ``inspect.get_annotations(eval_str=True)`` so that both
    ``from __future__ import annotations`` and explicit string annotations
    are resolved automatically.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if position >= len(params):
        return Any
    param_name = params[position].name
    raw = inspect.get_annotations(func, eval_str=True)
    return raw.get(param_name, Any)


def strip_non_existence_type(hint: Any) -> Any:
    """Remove ``NonExistenceType`` from a union type hint.

    ``tuple[int, str] | NonExistenceType`` → ``tuple[int, str]``
    Non-union hints are returned unchanged.
    """
    from .typing import NonExistenceType

    origin = typing.get_origin(hint)
    if origin is not types.UnionType and origin is not typing.Union:
        return hint
    args = [a for a in typing.get_args(hint) if a is not NonExistenceType]
    if len(args) == 1:
        return args[0]
    if not args:
        return Any
    return typing.Union[tuple(args)]


def unwrap_element_type(hint: Any) -> Any:
    """Extract the element type ``T`` from a generic container type.

    Accepts ``Collection[T]``, ``Sequence[T]``, ``list[T]``, etc.
    Returns ``T`` if *hint* is a single-argument generic whose origin is a
    collection-like type; returns ``Any`` otherwise.
    """
    origin = typing.get_origin(hint)
    if origin is None:
        return Any
    # Check that the origin is a known collection/sequence type.
    import collections.abc

    if not (
        issubclass(origin, (list, tuple, set, frozenset))
        or issubclass(origin, (collections.abc.Collection, collections.abc.Sequence))
    ):
        return Any
    args = typing.get_args(hint)
    return args[0] if args else Any
