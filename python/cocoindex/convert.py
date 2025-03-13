"""
Utilities to convert between Python and engine values.
"""
import dataclasses
from typing import Any

def to_engine_value(value: Any) -> Any:
    """Convert a Python value to an engine value."""
    if dataclasses.is_dataclass(value):
        return [to_engine_value(getattr(value, f.name)) for f in dataclasses.fields(value)]
    if isinstance(value, (list, tuple)):
        return [to_engine_value(v) for v in value]
    return value
