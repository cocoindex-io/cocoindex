"""
Utilities to dump/load objects (for configs, specs).
"""

from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, Mapping, TypeVar, overload, get_origin

import numpy as np

from ._internal import datatype
from . import engine_type


T = TypeVar("T")


def get_auto_default_for_type(
    type_info: datatype.DataTypeInfo,
) -> tuple[Any, bool]:
    """
    Get an auto-default value for a type annotation if it's safe to do so.

    Returns:
        A tuple of (default_value, is_supported) where:
        - default_value: The default value if auto-defaulting is supported
        - is_supported: True if auto-defaulting is supported for this type
    """
    # Case 1: Nullable types (Optional[T] or T | None)
    if type_info.nullable:
        return None, True

    # Case 2: Table types (KTable or LTable) - check if it's a list or dict type
    if isinstance(type_info.variant, datatype.SequenceType):
        return [], True
    elif isinstance(type_info.variant, datatype.MappingType):
        return {}, True

    return None, False


def dump_engine_object(v: Any) -> Any:
    """Recursively dump an object for engine. Engine side uses `Pythonized` to catch."""
    if v is None:
        return None
    elif isinstance(v, engine_type.EnrichedValueType):
        return v.encode()
    elif isinstance(v, engine_type.FieldSchema):
        return v.encode()
    elif isinstance(v, type) or get_origin(v) is not None:
        return engine_type.encode_enriched_type(v)
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, datetime.timedelta):
        total_secs = v.total_seconds()
        secs = int(total_secs)
        nanos = int((total_secs - secs) * 1e9)
        return {"secs": secs, "nanos": nanos}
    elif datatype.is_namedtuple_type(type(v)):
        # Handle NamedTuple objects specifically to use dict format
        field_names = list(getattr(type(v), "_fields", ()))
        result = {}
        for name in field_names:
            val = getattr(v, name)
            result[name] = dump_engine_object(val)  # Include all values, including None
        if hasattr(v, "kind") and "kind" not in result:
            result["kind"] = v.kind
        return result
    elif hasattr(v, "__dict__"):  # for dataclass-like objects
        s = {}
        for k, val in v.__dict__.items():
            if val is None:
                # Skip None values
                continue
            s[k] = dump_engine_object(val)
        if hasattr(v, "kind") and "kind" not in s:
            s["kind"] = v.kind
        return s
    elif isinstance(v, (list, tuple)):
        return [dump_engine_object(item) for item in v]
    elif isinstance(v, np.ndarray):
        return v.tolist()
    elif isinstance(v, dict):
        return {k: dump_engine_object(v) for k, v in v.items()}
    return v


@overload
def load_engine_object(expected_type: type[T], v: Any) -> T: ...
@overload
def load_engine_object(expected_type: Any, v: Any) -> Any: ...
def load_engine_object(expected_type: Any, v: Any) -> Any:
    """Recursively load an object that was produced by dump_engine_object().

    Args:
        expected_type: The Python type annotation to reconstruct to.
        v: The engine-facing Pythonized object (e.g., dict/list/primitive) to convert.

    Returns:
        A Python object matching the expected_type where possible.
    """
    # Fast path
    if v is None:
        return None

    type_info = datatype.analyze_type_info(expected_type)
    variant = type_info.variant

    if type_info.core_type is engine_type.EnrichedValueType:
        return engine_type.EnrichedValueType.decode(v)
    if type_info.core_type is engine_type.FieldSchema:
        return engine_type.FieldSchema.decode(v)

    # Any or unknown → return as-is
    if isinstance(variant, datatype.AnyType) or type_info.base_type is Any:
        return v

    # Enum handling
    if isinstance(expected_type, type) and issubclass(expected_type, Enum):
        return expected_type(v)

    # TimeDelta special form {secs, nanos}
    if isinstance(variant, datatype.BasicType) and variant.kind == "TimeDelta":
        if isinstance(v, Mapping) and "secs" in v and "nanos" in v:
            secs = int(v["secs"])  # type: ignore[index]
            nanos = int(v["nanos"])  # type: ignore[index]
            return datetime.timedelta(seconds=secs, microseconds=nanos / 1_000)
        return v

    # List, NDArray (Vector-ish), or general sequences
    if isinstance(variant, datatype.SequenceType):
        elem_type = variant.elem_type if variant.elem_type else Any
        if type_info.base_type is np.ndarray:
            # Reconstruct NDArray with appropriate dtype if available
            try:
                dtype = datatype.extract_ndarray_elem_dtype(type_info.core_type)
            except (TypeError, ValueError, AttributeError):
                dtype = None
            return np.array(v, dtype=dtype)
        # Regular Python list
        return [load_engine_object(elem_type, item) for item in v]

    # Dict / Mapping
    if isinstance(variant, datatype.MappingType):
        key_t = variant.key_type
        val_t = variant.value_type
        return {
            load_engine_object(key_t, k): load_engine_object(val_t, val)
            for k, val in v.items()
        }

    # Structs (dataclass, NamedTuple, or Pydantic)
    if isinstance(variant, datatype.StructType):
        struct_type = variant.struct_type
        init_kwargs: dict[str, Any] = {}
        for field_info in variant.fields:
            if field_info.name in v:
                init_kwargs[field_info.name] = load_engine_object(
                    field_info.type_hint, v[field_info.name]
                )
            else:
                type_info = datatype.analyze_type_info(field_info.type_hint)
                auto_default, is_supported = get_auto_default_for_type(type_info)
                if is_supported:
                    init_kwargs[field_info.name] = auto_default
        return struct_type(**init_kwargs)

    # Union with discriminator support via "kind"
    if isinstance(variant, datatype.UnionType):
        if isinstance(v, Mapping) and "kind" in v:
            discriminator = v["kind"]
            for typ in variant.variant_types:
                t_info = datatype.analyze_type_info(typ)
                if isinstance(t_info.variant, datatype.StructType):
                    t_struct = t_info.variant.struct_type
                    candidate_kind = getattr(t_struct, "kind", None)
                    if candidate_kind == discriminator:
                        # Remove discriminator for constructor
                        v_wo_kind = dict(v)
                        v_wo_kind.pop("kind", None)
                        return load_engine_object(t_struct, v_wo_kind)
        # Fallback: try each variant until one succeeds
        for typ in variant.variant_types:
            try:
                return load_engine_object(typ, v)
            except (TypeError, ValueError):
                continue
        return v

    # Basic types and everything else: handle numpy scalars and passthrough
    if isinstance(v, np.ndarray) and type_info.base_type is list:
        return v.tolist()
    if isinstance(v, (list, tuple)) and type_info.base_type not in (list, tuple):
        # If a non-sequence basic type expected, attempt direct cast
        try:
            return type_info.core_type(v)
        except (TypeError, ValueError):
            return v
    return v
