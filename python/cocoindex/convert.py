"""
Utilities to convert between Python and engine values.
"""

import dataclasses
import datetime
import inspect
from enum import Enum
from typing import Any, Callable, Mapping, get_origin

import numpy as np

from .typing import (
    KEY_FIELD_NAME,
    TABLE_TYPES,
    analyze_type_info,
    encode_enriched_type,
    is_namedtuple_type,
    is_struct_type,
    AnalyzedTypeInfo,
    AnalyzedAnyType,
    AnalyzedDictType,
    AnalyzedListType,
    AnalyzedBasicType,
    AnalyzedUnionType,
    AnalyzedUnknownType,
    AnalyzedStructType,
    is_numpy_number_type,
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
        if not value:
            return {}

        first_val = next(iter(value.values()))
        if is_struct_type(type(first_val)):  # KTable
            return [
                [encode_engine_value(k)] + encode_engine_value(v)
                for k, v in value.items()
            ]
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

    dst_type_info = analyze_type_info(dst_annotation)
    dst_type_variant = dst_type_info.variant

    if isinstance(dst_type_variant, AnalyzedUnknownType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, an unsupported type"
        )

    if src_type_kind == "Struct":
        return _make_engine_struct_value_decoder(
            field_path,
            src_type["fields"],
            dst_type_info,
        )

    if src_type_kind in TABLE_TYPES:
        field_path.append("[*]")
        engine_fields_schema = src_type["row"]["fields"]

        if src_type_kind == "LTable":
            if isinstance(dst_type_variant, AnalyzedAnyType):
                return _make_engine_ltable_to_list_dict_decoder(
                    field_path, engine_fields_schema
                )
            if not isinstance(dst_type_variant, AnalyzedListType):
                raise ValueError(
                    f"Type mismatch for `{''.join(field_path)}`: "
                    f"declared `{dst_type_info.core_type}`, a list type expected"
                )
            row_decoder = _make_engine_struct_value_decoder(
                field_path,
                engine_fields_schema,
                analyze_type_info(dst_type_variant.elem_type),
            )

            def decode(value: Any) -> Any | None:
                if value is None:
                    return None
                return [row_decoder(v) for v in value]

        elif src_type_kind == "KTable":
            if isinstance(dst_type_variant, AnalyzedAnyType):
                return _make_engine_ktable_to_dict_dict_decoder(
                    field_path, engine_fields_schema
                )
            if not isinstance(dst_type_variant, AnalyzedDictType):
                raise ValueError(
                    f"Type mismatch for `{''.join(field_path)}`: "
                    f"declared `{dst_type_info.core_type}`, a dict type expected"
                )

            key_field_schema = engine_fields_schema[0]
            field_path.append(f".{key_field_schema.get('name', KEY_FIELD_NAME)}")
            key_decoder = make_engine_value_decoder(
                field_path, key_field_schema["type"], dst_type_variant.key_type
            )
            field_path.pop()
            value_decoder = _make_engine_struct_value_decoder(
                field_path,
                engine_fields_schema[1:],
                analyze_type_info(dst_type_variant.value_type),
            )

            def decode(value: Any) -> Any | None:
                if value is None:
                    return None
                return {key_decoder(v[0]): value_decoder(v[1:]) for v in value}

        field_path.pop()
        return decode

    if src_type_kind == "Union":
        if isinstance(dst_type_variant, AnalyzedAnyType):
            return lambda value: value[1]

        dst_type_variants = (
            dst_type_variant.variant_types
            if isinstance(dst_type_variant, AnalyzedUnionType)
            else [dst_annotation]
        )
        src_type_variants = src_type["types"]
        decoders = []
        for i, src_type_variant in enumerate(src_type_variants):
            src_field_path = field_path + [f"[{i}]"]
            decoder = None
            for dst_type_variant in dst_type_variants:
                try:
                    decoder = make_engine_value_decoder(
                        src_field_path, src_type_variant, dst_type_variant
                    )
                    break
                except ValueError:
                    pass
            if decoder is None:
                raise ValueError(
                    f"Type mismatch for `{''.join(field_path)}`: "
                    f"cannot find matched target type for source type variant {src_type_variant}"
                )
            decoders.append(decoder)
        return lambda value: decoders[value[0]](value[1])

    if isinstance(dst_type_variant, AnalyzedAnyType):
        return lambda value: value

    if src_type_kind == "Vector":
        field_path_str = "".join(field_path)
        if not isinstance(dst_type_variant, AnalyzedListType):
            raise ValueError(
                f"Type mismatch for `{''.join(field_path)}`: "
                f"declared `{dst_type_info.core_type}`, a list type expected"
            )
        expected_dim = (
            dst_type_variant.vector_info.dim
            if dst_type_variant and dst_type_variant.vector_info
            else None
        )

        vec_elem_decoder = None
        scalar_dtype = None
        if dst_type_variant and dst_type_info.base_type is np.ndarray:
            if is_numpy_number_type(dst_type_variant.elem_type):
                scalar_dtype = dst_type_variant.elem_type
        else:
            vec_elem_decoder = make_engine_value_decoder(
                field_path + ["[*]"],
                src_type["element_type"],
                dst_type_variant and dst_type_variant.elem_type,
            )

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

            if vec_elem_decoder is not None:  # for Non-NDArray vector
                return [vec_elem_decoder(v) for v in value]
            else:  # for NDArray vector
                return np.array(value, dtype=scalar_dtype)

        return decode_vector

    if isinstance(dst_type_variant, AnalyzedBasicType):
        if not _is_type_kind_convertible_to(src_type_kind, dst_type_variant.kind):
            raise ValueError(
                f"Type mismatch for `{''.join(field_path)}`: "
                f"passed in {src_type_kind}, declared {dst_annotation} ({dst_type_variant.kind})"
            )

        if dst_type_variant.kind in ("Float32", "Float64", "Int64"):
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

    return lambda value: value


def _make_engine_struct_value_decoder(
    field_path: list[str],
    src_fields: list[dict[str, Any]],
    dst_type_info: AnalyzedTypeInfo,
) -> Callable[[list[Any]], Any]:
    """Make a decoder from an engine field values to a Python value."""

    dst_type_variant = dst_type_info.variant

    use_dict = False
    if isinstance(dst_type_variant, AnalyzedAnyType):
        use_dict = True
    elif isinstance(dst_type_variant, AnalyzedDictType):
        analyzed_key_type = analyze_type_info(dst_type_variant.key_type)
        analyzed_value_type = analyze_type_info(dst_type_variant.value_type)
        use_dict = (
            isinstance(analyzed_key_type.variant, AnalyzedAnyType)
            or (
                isinstance(analyzed_key_type.variant, AnalyzedBasicType)
                and analyzed_key_type.variant.kind == "Str"
            )
        ) and isinstance(analyzed_value_type.variant, AnalyzedAnyType)
    if use_dict:
        return _make_engine_struct_to_dict_decoder(field_path, src_fields)

    if not isinstance(dst_type_variant, AnalyzedStructType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, a dataclass, NamedTuple or dict[str, Any] expected"
        )

    src_name_to_idx = {f["name"]: i for i, f in enumerate(src_fields)}
    dst_struct_type = dst_type_variant.struct_type

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


def _make_engine_struct_to_dict_decoder(
    field_path: list[str],
    src_fields: list[dict[str, Any]],
) -> Callable[[list[Any] | None], dict[str, Any] | None]:
    """Make a decoder from engine field values to a Python dict."""

    field_decoders = []
    for i, field_schema in enumerate(src_fields):
        field_name = field_schema["name"]
        field_path.append(f".{field_name}")
        field_decoder = make_engine_value_decoder(
            field_path,
            field_schema["type"],
            Any,  # Use Any for recursive decoding
        )
        field_path.pop()
        field_decoders.append((field_name, field_decoder))

    def decode_to_dict(values: list[Any] | None) -> dict[str, Any] | None:
        if values is None:
            return None
        if len(field_decoders) != len(values):
            raise ValueError(
                f"Field count mismatch: expected {len(field_decoders)}, got {len(values)}"
            )
        return {
            field_name: field_decoder(value)
            for value, (field_name, field_decoder) in zip(values, field_decoders)
        }

    return decode_to_dict


def _make_engine_ltable_to_list_dict_decoder(
    field_path: list[str],
    src_fields: list[dict[str, Any]],
) -> Callable[[list[Any] | None], list[dict[str, Any]] | None]:
    """Make a decoder from engine LTable values to a list of dicts."""

    # Create a decoder for each row (struct) to dict
    row_decoder = _make_engine_struct_to_dict_decoder(field_path, src_fields)

    def decode_to_list_dict(values: list[Any] | None) -> list[dict[str, Any]] | None:
        if values is None:
            return None
        result = []
        for i, row_values in enumerate(values):
            decoded_row = row_decoder(row_values)
            if decoded_row is None:
                raise ValueError(
                    f"LTable row at index {i} decoded to None, which is not allowed."
                )
            result.append(decoded_row)
        return result

    return decode_to_list_dict


def _make_engine_ktable_to_dict_dict_decoder(
    field_path: list[str],
    src_fields: list[dict[str, Any]],
) -> Callable[[list[Any] | None], dict[Any, dict[str, Any]] | None]:
    """Make a decoder from engine KTable values to a dict of dicts."""

    if not src_fields:
        raise ValueError("KTable must have at least one field for the key")

    # First field is the key, remaining fields are the value
    key_field_schema = src_fields[0]
    value_fields_schema = src_fields[1:]

    # Create decoders
    field_path.append(f".{key_field_schema.get('name', KEY_FIELD_NAME)}")
    key_decoder = make_engine_value_decoder(field_path, key_field_schema["type"], Any)
    field_path.pop()

    value_decoder = _make_engine_struct_to_dict_decoder(field_path, value_fields_schema)

    def decode_to_dict_dict(
        values: list[Any] | None,
    ) -> dict[Any, dict[str, Any]] | None:
        if values is None:
            return None
        result = {}
        for row_values in values:
            if not row_values:
                raise ValueError("KTable row must have at least 1 value (the key)")
            key = key_decoder(row_values[0])
            if len(row_values) == 1:
                value: dict[str, Any] = {}
            else:
                tmp = value_decoder(row_values[1:])
                if tmp is None:
                    value = {}
                else:
                    value = tmp
            if isinstance(key, dict):
                key = tuple(key.values())
            result[key] = value
        return result

    return decode_to_dict_dict


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
