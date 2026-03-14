import datetime
import io
import pathlib
import pickle
import uuid
from typing import Any


# ---------------------------------------------------------------------------
# Global registry: (module, qualname) -> Python object
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
# Public registration APIs
# ---------------------------------------------------------------------------


def unpickle_safe(cls: type) -> type:
    """Mark a class as safe to unpickle. Use as a decorator."""
    _UNPICKLE_SAFE_GLOBALS[(cls.__module__, cls.__qualname__)] = cls
    return cls


def add_unpickle_safe_global(module: str, qualname: str, obj: object) -> None:
    """Register a non-type callable as safe to unpickle."""
    _UNPICKLE_SAFE_GLOBALS[(module, qualname)] = obj


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
# Serialize / Deserialize
# ---------------------------------------------------------------------------


def serialize(value: Any) -> bytes:
    return pickle.dumps(value, 5)


def deserialize(data: bytes) -> Any:
    return _RestrictedUnpickler(io.BytesIO(data)).load()
