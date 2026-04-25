"""
Utilities to dump/load objects (for configs, specs).
"""

from __future__ import annotations

import datetime
import base64
from enum import Enum
from typing import Any


def _is_namedtuple_type(t: type) -> bool:
    return isinstance(t, type) and issubclass(t, tuple) and hasattr(t, "_fields")


def dump_engine_object(v: Any, *, bytes_to_base64: bool = False) -> Any:
    """Recursively dump an object for engine. Engine side uses `Pythonized` to catch."""
    if v is None:
        return None
    elif isinstance(v, (str, int, float, bool)):
        return v
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, datetime.timedelta):
        total_secs = v.total_seconds()
        secs = int(total_secs)
        nanos = int((total_secs - secs) * 1e9)
        return {"secs": secs, "nanos": nanos}
    elif _is_namedtuple_type(type(v)):
        # Handle NamedTuple objects specifically to use dict format
        field_names = list(getattr(type(v), "_fields", ()))
        result = {}
        for name in field_names:
            val = getattr(v, name)
            result[name] = dump_engine_object(
                val, bytes_to_base64=bytes_to_base64
            )  # Include all values, including None
        if hasattr(v, "kind") and "kind" not in result:
            result["kind"] = v.kind
        return result
    elif hasattr(v, "__dict__"):  # for dataclass-like objects
        s = {}
        for k, val in v.__dict__.items():
            if val is None:
                # Skip None values
                continue
            s[k] = dump_engine_object(val, bytes_to_base64=bytes_to_base64)
        if hasattr(v, "kind") and "kind" not in s:
            s["kind"] = v.kind
        return s
    elif isinstance(v, (list, tuple)):
        return [dump_engine_object(item) for item in v]
    elif isinstance(v, dict):
        return {k: dump_engine_object(v) for k, v in v.items()}
    return str(v)
