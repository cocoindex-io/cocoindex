"""
Utilities to convert between Python and engine values.
"""

import dataclasses
import datetime
import inspect
import uuid
from enum import Enum
from typing import Any, Callable, Mapping, get_origin

import numpy as np

from .typing import (
    KEY_FIELD_NAME,
    TABLE_TYPES,
    AnalyzedTypeInfo,
    DtypeRegistry,
    analyze_type_info,
    encode_enriched_type,
    extract_ndarray_scalar_dtype,
    is_namedtuple_type,
)


def encode_engine_value(value: Any) -> Any:
    """Encode a Python value to an engine value."""
    if dataclasses.is_dataclass(value):
        return [
            encode_engine_value(getattr(value, f.name))
            for f in dataclasses.fields(value)
        ]
    if is_namedtuple_type(type(value)):
        return [encode_engine_value(getattr(value, name)) for name in value._fields]
    if isinstance(value, np.number):
        return value.item()
    if isinstance(value, np.ndarray):
        return value
    if isinstance(value, (list, tuple)):
        return [encode_engine_value(v) for v in value]
    if isinstance(value, dict):
        return [
            [encode_engine_value(k)] + encode_engine_value(v) for k, v in value.items()
        ]
    if isinstance(value, uuid.UUID):
        return value.bytes
    return value


_CONVERTIBLE_KINDS = {
    ("Float32", "Float64"),
    ("LocalDateTime", "OffsetDateTime"),
}


def _is_type_kind_convertible_to(src_type_kind: str, dst_type_kind: str) -> bool:
    return (
        src_type_kind == dst_type_kind
        or (src_type_kind, dst_type_kind) in _CONVERTIBLE_KINDS
    )


def make_engine_value_decoder(
    field_path: list[str],
    src_type: dict[str, Any],
    dst_annotation: Any,
) -> Callable[[Any], Any]:
    """
    Make a decoder from an engine value to a Python value.

    Args:
        field_path: The path to the field in the engine value. For error messages.
        src_type: The type of the engine value, mapped from a `cocoindex::base::schema::ValueType`.
        dst_annotation: The type annotation of the Python value.

    Returns:
        A decoder from an engine value to a Python value.
    """

    src_type_kind = src_type["kind"]

    dst_type_info: AnalyzedTypeInfo | None = None
    if (
        dst_annotation is not None
        and dst_annotation is not inspect.Parameter.empty
        and dst_annotation is not Any
    ):
        dst_type_info = analyze_type_info(dst_annotation)
        if not _is_type_kind_convertible_to(src_type_kind, dst_type_info.kind):
            raise ValueError(
                f"Type mismatch for `{''.join(field_path)}`: "
                f"passed in {src_type_kind}, declared {dst_annotation} ({dst_type_info.kind})"
            )

    if src_type_kind == "Uuid":
        return lambda value: uuid.UUID(bytes=value)

    if dst_type_info is None:
        if src_type_kind == "Struct" or src_type_kind in TABLE_TYPES:
            raise ValueError(
                f"Missing type annotation for `{''.join(field_path)}`."
                f"It's required for {src_type_kind} type."
            )
        return lambda value: value

    if dst_type_info.kind in ("Float32", "Float64", "Int64"):
        dst_core_type = dst_type_info.core_type

        def decode_scalar(value: Any) -> Any | None:
            if value is None:
                if dst_type_info.nullable:
                    return None
                raise ValueError(
                    f"Received null for non-nullable scalar `{''.join(field_path)}`"
                )
            return dst_core_type(value)

        return decode_scalar

    if src_type_kind == "Vector":
        field_path_str = "".join(field_path)
        expected_dim = (
            dst_type_info.vector_info.dim if dst_type_info.vector_info else None
        )

        elem_decoder = None
        scalar_dtype = None
        if dst_type_info.np_number_type is None:  # for Non-NDArray vector
            elem_decoder = make_engine_value_decoder(
                field_path + ["[*]"],
                src_type["element_type"],
                dst_type_info.elem_type,
            )
        else:  # for NDArray vector
            scalar_dtype = extract_ndarray_scalar_dtype(dst_type_info.np_number_type)
            _ = DtypeRegistry.validate_dtype_and_get_kind(scalar_dtype)

        def decode_vector(value: Any) -> Any | None:
            if value is None:
                if dst_type_info.nullable:
                    return None
                raise ValueError(
                    f"Received null for non-nullable vector `{field_path_str}`"
                )
            if not isinstance(value, (np.ndarray, list)):
                raise TypeError(
                    f"Expected NDArray or list for vector `{field_path_str}`, got {type(value)}"
                )
            if expected_dim is not None and len(value) != expected_dim:
                raise ValueError(
                    f"Vector dimension mismatch for `{field_path_str}`: "
                    f"expected {expected_dim}, got {len(value)}"
                )

            if elem_decoder is not None:  # for Non-NDArray vector
                return [elem_decoder(v) for v in value]
            else:  # for NDArray vector
                return np.array(value, dtype=scalar_dtype)

        return decode_vector

    if dst_type_info.struct_type is not None:
        return _make_engine_struct_value_decoder(
            field_path, src_type["fields"], dst_type_info.struct_type
        )

    if src_type_kind in TABLE_TYPES:
        field_path.append("[*]")
        elem_type_info = analyze_type_info(dst_type_info.elem_type)
        if elem_type_info.struct_type is None:
            raise ValueError(
                f"Type mismatch for `{''.join(field_path)}`: "
                f"declared `{dst_type_info.kind}`, a dataclass or NamedTuple type expected"
            )
        engine_fields_schema = src_type["row"]["fields"]
        if elem_type_info.key_type is not None:
            key_field_schema = engine_fields_schema[0]
            field_path.append(f".{key_field_schema.get('name', KEY_FIELD_NAME)}")
            key_decoder = make_engine_value_decoder(
                field_path, key_field_schema["type"], elem_type_info.key_type
            )
            field_path.pop()
            value_decoder = _make_engine_struct_value_decoder(
                field_path, engine_fields_schema[1:], elem_type_info.struct_type
            )

            def decode(value: Any) -> Any | None:
                if value is None:
                    return None
                return {key_decoder(v[0]): value_decoder(v[1:]) for v in value}
        else:
            elem_decoder = _make_engine_struct_value_decoder(
                field_path, engine_fields_schema, elem_type_info.struct_type
            )

            def decode(value: Any) -> Any | None:
                if value is None:
                    return None
                return [elem_decoder(v) for v in value]

        field_path.pop()
        return decode

    if src_type_kind == "Union":
        return lambda value: value[1]

    return lambda value: value


def _make_engine_struct_value_decoder(
    field_path: list[str],
    src_fields: list[dict[str, Any]],
    dst_struct_type: type,
) -> Callable[[list[Any]], Any]:
    """Make a decoder from an engine field values to a Python value."""

    src_name_to_idx = {f["name"]: i for i, f in enumerate(src_fields)}

    parameters: Mapping[str, inspect.Parameter]
    if dataclasses.is_dataclass(dst_struct_type):
        parameters = inspect.signature(dst_struct_type).parameters
    elif is_namedtuple_type(dst_struct_type):
        defaults = getattr(dst_struct_type, "_field_defaults", {})
        fields = getattr(dst_struct_type, "_fields", ())
        parameters = {
            name: inspect.Parameter(
                name=name,
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=defaults.get(name, inspect.Parameter.empty),
                annotation=dst_struct_type.__annotations__.get(
                    name, inspect.Parameter.empty
                ),
            )
            for name in fields
        }
    else:
        raise ValueError(f"Unsupported struct type: {dst_struct_type}")

    def make_closure_for_value(
        name: str, param: inspect.Parameter
    ) -> Callable[[list[Any]], Any]:
        src_idx = src_name_to_idx.get(name)
        if src_idx is not None:
            field_path.append(f".{name}")
            field_decoder = make_engine_value_decoder(
                field_path, src_fields[src_idx]["type"], param.annotation
            )
            field_path.pop()
            return (
                lambda values: field_decoder(values[src_idx])
                if len(values) > src_idx
                else param.default
            )

        default_value = param.default
        if default_value is inspect.Parameter.empty:
            raise ValueError(
                f"Field without default value is missing in input: {''.join(field_path)}"
            )

        return lambda _: default_value

    field_value_decoder = [
        make_closure_for_value(name, param) for (name, param) in parameters.items()
    ]

    return lambda values: dst_struct_type(
        *(decoder(values) for decoder in field_value_decoder)
    )


def dump_engine_object(v: Any) -> Any:
    """Recursively dump an object for engine. Engine side uses `Pythonized` to catch."""
    if v is None:
        return None
    elif isinstance(v, type) or get_origin(v) is not None:
        return encode_enriched_type(v)
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, datetime.timedelta):
        total_secs = v.total_seconds()
        secs = int(total_secs)
        nanos = int((total_secs - secs) * 1e9)
        return {"secs": secs, "nanos": nanos}
    elif hasattr(v, "__dict__"):
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
    elif isinstance(v, dict):
        return {k: dump_engine_object(v) for k, v in v.items()}
    return v
