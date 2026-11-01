"""
Compact in-memory form of declared target-state values and their actions.

The engine holds each declared target state's value from its declaration until
its component commits, and each action `reconcile()` builds until the sink has
applied it. For row-shaped values the Python objects are most of that cost: a
dict of fifteen short strings and ints takes about 1 KB, its encoding here
about 150 B. This module gives the engine an exact, compact encoding for both,
so the engine holds bytes and hands an equivalent object back where one is
needed: to `reconcile()`, and to the sink.

Only plain data is encoded. Pickle restores exact builtin containers and
scalars, and a fixed set of value types (date/time types, `Decimal`, `UUID`,
`Fraction`, `ipaddress` and `pathlib` objects, numpy arrays, scalars and
dtypes, enum members, NamedTuples), exactly: a tuple stays a tuple, a
`Decimal` a `Decimal`, and two references to one object within a value still
share it. A value holding anything else (a dataclass, a pydantic model, a
msgspec Struct, a subclass of a builtin, a handle, a lambda) is left to be held
as the object itself, as are scalars, already compact, and values whose
encoding exceeds `_MAX_ENCODED_SIZE`.

A dict whose keys are exact strings, and whose values cannot refer back to it,
is encoded as a row: its values only, plus the id of its key tuple, interned
once per process (`_KeySchemas`). A row's column names then cost nothing per
row, and decoded rows share their key objects, as rows built by a connector do.

What a handler or sink observes: an equal copy of the declared value, taken at
declaration, instead of the declared object, and an equal copy of the action
it returned. Each value is encoded on its own, so an object that many declared
values share is copied into each of their encodings.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import fractions
import io
import ipaddress
import operator
import pathlib
import pickle
import threading
import types
import uuid
import zoneinfo
from typing import Any

import numpy as np

_PROTOCOL = 5

# An encoding larger than this is not held: such a value is mostly payload
# (text, vectors) that is compact as an object already, and an object shared
# by many declared values would be copied into each of their encodings.
_MAX_ENCODED_SIZE = 64 * 1024

# First byte of a value's encoding.
_PICKLED = 0  # a pickle of the value follows
_ROW = 1  # a 2-byte key schema id, then a pickle of the row's values tuple
_ROW_SCHEMA_ID_SIZE = 2

# Held as the object itself even at the top of a value: already compact.
_SCALAR_TYPES: frozenset[type] = frozenset({type(None), bool, int, float, str, bytes})


def _all_subclasses(cls: type) -> list[type]:
    result: list[type] = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_all_subclasses(sub))
    return result


# Value types whose instances pickle exactly. Exact builtin containers and
# scalars never reach the pickler's hook, so they are not listed.
_VALUE_TYPES: frozenset[type] = frozenset(
    {
        complex,
        datetime.date,
        datetime.time,
        datetime.datetime,
        datetime.timedelta,
        datetime.timezone,
        zoneinfo.ZoneInfo,
        decimal.Decimal,
        uuid.UUID,
        fractions.Fraction,
        ipaddress.IPv4Address,
        ipaddress.IPv6Address,
        ipaddress.IPv4Network,
        ipaddress.IPv6Network,
        ipaddress.IPv4Interface,
        ipaddress.IPv6Interface,
        np.ndarray,
        pathlib.PurePath,
        *_all_subclasses(pathlib.PurePath),
    }
)

# Values a row's values may hold without being able to refer back to the row.
_ROW_ATOM_TYPES: frozenset[type] = (_SCALAR_TYPES | _VALUE_TYPES) - {np.ndarray}


class _Unencodable(Exception):
    """Raised while pickling a value that holds something other than plain data."""


def _is_plain_data(obj: Any) -> bool:
    """Whether `obj`, which the pickler is about to reduce, pickles exactly."""
    cls = type(obj)
    if cls in _VALUE_TYPES:
        return True
    if isinstance(obj, (enum.Enum, np.generic, np.dtype)):
        return True
    if isinstance(obj, tuple):
        # Exact tuples are pickled without the hook; a NamedTuple gets here.
        return hasattr(cls, "_fields")
    # Classes and functions pickle by reference: as the reconstructors in the
    # reductions of the types above, or as plain data within a value.
    if isinstance(obj, type) or cls is types.FunctionType:
        return True
    # `isinstance`: a C method using its defining class (`ZoneInfo._unpickle`
    # on 3.14) is a subtype of the builtin function type.
    if isinstance(obj, (types.BuiltinFunctionType, types.MethodType)):
        # A module-level builtin, or one bound to a class; one bound to an
        # instance would pickle that instance.
        return isinstance(obj.__self__, (types.ModuleType, type))
    return False


class _Pickler(pickle.Pickler):
    def reducer_override(self, obj: Any) -> Any:
        if _is_plain_data(obj):
            return NotImplemented
        raise _Unencodable


def _dump(header: bytes, obj: Any) -> bytes:
    buf = io.BytesIO()
    buf.write(header)
    _Pickler(buf, _PROTOCOL).dump(obj)
    return buf.getvalue()


def _all_exact_str(keys: tuple[Any, ...]) -> bool:
    return all(type(key) is str for key in keys)


class _KeySchemas:
    """Key tuples of the dicts encoded as rows, interned for the process.

    Bounded: once full, dicts with an unseen key tuple are pickled whole.
    """

    _MAX_SCHEMAS = 4096

    __slots__ = ("_ids", "_keys", "_lock")
    _ids: dict[tuple[str, ...], int]
    _keys: list[tuple[str, ...]]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._ids = {}
        self._keys = []
        self._lock = threading.Lock()

    def id_of(self, keys: tuple[Any, ...]) -> int | None:
        schema_id = self._ids.get(keys)
        if schema_id is not None:
            # Keys equal to a schema's can still be str subclasses, which a
            # decoded row would turn into plain strings.
            if all(map(operator.is_, keys, self._keys[schema_id])) or _all_exact_str(
                keys
            ):
                return schema_id
            return None
        if not _all_exact_str(keys):
            return None
        with self._lock:
            schema_id = self._ids.get(keys)
            if schema_id is None:
                if len(self._keys) >= self._MAX_SCHEMAS:
                    return None
                schema_id = len(self._keys)
                self._keys.append(keys)
                self._ids[keys] = schema_id
            return schema_id

    def keys(self, schema_id: int) -> tuple[str, ...]:
        return self._keys[schema_id]


_KEY_SCHEMAS = _KeySchemas()


def _is_row(values: tuple[Any, ...]) -> bool:
    """Whether no value can refer back to the dict holding `values`, so the
    dict can be rebuilt from them."""
    for value in values:
        cls = type(value)
        if cls in _ROW_ATOM_TYPES:
            continue
        if cls is list or cls is tuple:
            for item in value:
                if type(item) not in _ROW_ATOM_TYPES:
                    return False
            continue
        if cls is np.ndarray and not value.dtype.hasobject:
            continue
        if isinstance(value, np.generic):
            continue
        return False
    return True


def _encode_dict(value: dict[Any, Any]) -> bytes:
    keys = tuple(value)
    values = tuple(value.values())
    schema_id = _KEY_SCHEMAS.id_of(keys) if _is_row(values) else None
    if schema_id is None:
        return _dump(bytes((_PICKLED,)), value)
    header = bytes((_ROW,)) + schema_id.to_bytes(_ROW_SCHEMA_ID_SIZE, "little")
    return _dump(header, values)


def encode_value(value: Any) -> bytes | None:
    """The encoding the engine holds for a declared value, or `None` to hold the
    object itself."""
    cls = type(value)
    if cls in _SCALAR_TYPES:
        return None
    try:
        if cls is dict:
            data = _encode_dict(value)
        else:
            data = _dump(bytes((_PICKLED,)), value)
    except Exception:
        # Not plain data (or not picklable at all): hold the object.
        return None
    if len(data) > _MAX_ENCODED_SIZE:
        return None
    return data


def decode_value(data: bytes) -> Any:
    """A new object equal to the value `data` encodes."""
    view = memoryview(data)
    if view[0] == _ROW:
        body_start = 1 + _ROW_SCHEMA_ID_SIZE
        keys = _KEY_SCHEMAS.keys(int.from_bytes(view[1:body_start], "little"))
        return dict(zip(keys, pickle.loads(view[body_start:])))
    return pickle.loads(view[1:])


class _ActionPickler(_Pickler):
    """Pickles an action, writing the declared value it holds as a reference to
    that value's encoding."""

    def __init__(self, file: io.BytesIO, desired: Any) -> None:
        super().__init__(file, _PROTOCOL)
        self._desired = desired
        self.refers_to_value = False

    def persistent_id(self, obj: Any) -> Any:
        if obj is self._desired:
            self.refers_to_value = True
            return 0
        return None


def encode_action(action: Any, desired: Any) -> tuple[bytes, bool] | None:
    """The encoding the engine holds for an action `reconcile()` built for the
    declared value `desired` (held encoded), or `None` to hold the action object
    itself.

    The flag says whether the encoding refers to the value's encoding, which
    the engine then keeps alive with it and passes to `decode_action`.
    """
    buf = io.BytesIO()
    pickler = _ActionPickler(buf, desired)
    try:
        pickler.dump(action)
    except Exception:
        return None
    data = buf.getvalue()
    if len(data) > _MAX_ENCODED_SIZE:
        return None
    return data, pickler.refers_to_value


_NOT_DECODED = object()


class _ActionUnpickler(pickle.Unpickler):
    def __init__(self, file: io.BytesIO, value_data: bytes) -> None:
        super().__init__(file)
        self._value_data = value_data
        self._value: Any = _NOT_DECODED

    def persistent_load(self, pid: Any) -> Any:
        if pid != 0:
            raise pickle.UnpicklingError(f"unexpected persistent id {pid!r}")
        # Decoded once: every reference in the action shares the one object,
        # as they shared the declared one.
        if self._value is _NOT_DECODED:
            self._value = decode_value(self._value_data)
        return self._value


def decode_action(data: bytes, value_data: bytes | None) -> Any:
    """A new object equal to the action `data` encodes; `value_data` is the
    encoding of the declared value it refers to, if it does."""
    if value_data is None:
        return pickle.loads(data)
    return _ActionUnpickler(io.BytesIO(data), value_data).load()
