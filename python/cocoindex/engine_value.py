"""
Utilities to encode/decode values in cocoindex (for data).
"""

from __future__ import annotations

import inspect
import warnings
from typing import Any, Callable, TypeVar

import numpy as np
from ._internal import datatype
from . import engine_type
from .engine_object import get_auto_default_for_type


T = TypeVar("T")


class ChildFieldPath:
    """Context manager to append a field to field_path on enter and pop it on exit."""

    _field_path: list[str]
    _field_name: str

    def __init__(self, field_path: list[str], field_name: str):
        self._field_path: list[str] = field_path
        self._field_name = field_name

    def __enter__(self) -> ChildFieldPath:
        self._field_path.append(self._field_name)
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self._field_path.pop()


_CONVERTIBLE_KINDS = {
    ("Float32", "Float64"),
    ("LocalDateTime", "OffsetDateTime"),
}


def _is_type_kind_convertible_to(src_type_kind: str, dst_type_kind: str) -> bool:
    return (
        src_type_kind == dst_type_kind
        or (src_type_kind, dst_type_kind) in _CONVERTIBLE_KINDS
    )


# Pre-computed type info for missing/Any type annotations
ANY_TYPE_INFO = datatype.analyze_type_info(inspect.Parameter.empty)


def make_engine_key_encoder(type_info: datatype.DataTypeInfo) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a key type.
    """
    value_encoder = make_engine_value_encoder(type_info)
    if isinstance(type_info.variant, datatype.BasicType):
        return lambda value: [value_encoder(value)]
    else:
        return value_encoder


def make_engine_value_encoder(type_info: datatype.DataTypeInfo) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a specific type.
    """
    variant = type_info.variant

    if isinstance(variant, datatype.OtherType):
        raise ValueError(f"Type annotation `{type_info.core_type}` is unsupported")

    if isinstance(variant, datatype.SequenceType):
        elem_type_info = (
            datatype.analyze_type_info(variant.elem_type)
            if variant.elem_type
            else ANY_TYPE_INFO
        )
        if isinstance(elem_type_info.variant, datatype.StructType):
            elem_encoder = make_engine_value_encoder(elem_type_info)

            def encode_struct_list(value: Any) -> Any:
                return None if value is None else [elem_encoder(v) for v in value]

            return encode_struct_list

        # Otherwise it's a vector, falling into basic type in the engine.

    if isinstance(variant, datatype.MappingType):
        key_type_info = datatype.analyze_type_info(variant.key_type)
        key_encoder = make_engine_key_encoder(key_type_info)

        value_type_info = datatype.analyze_type_info(variant.value_type)
        if not isinstance(value_type_info.variant, datatype.StructType):
            raise ValueError(
                f"Value type for dict is required to be a struct (e.g. dataclass or NamedTuple), got {variant.value_type}. "
                f"If you want a free-formed dict, use `cocoindex.Json` instead."
            )
        value_encoder = make_engine_value_encoder(value_type_info)

        def encode_struct_dict(value: Any) -> Any:
            if not value:
                return []
            return [key_encoder(k) + value_encoder(v) for k, v in value.items()]

        return encode_struct_dict

    if isinstance(variant, datatype.StructType):
        field_encoders = [
            (
                field_info.name,
                make_engine_value_encoder(
                    datatype.analyze_type_info(field_info.type_hint)
                ),
            )
            for field_info in variant.fields
        ]

        def encode_struct(value: Any) -> Any:
            if value is None:
                return None
            return [encoder(getattr(value, name)) for name, encoder in field_encoders]

        return encode_struct

    def encode_basic_value(value: Any) -> Any:
        if isinstance(value, np.number):
            return value.item()
        if isinstance(value, np.ndarray):
            return value
        if isinstance(value, (list, tuple)):
            return [encode_basic_value(v) for v in value]
        return value

    return encode_basic_value


def make_engine_key_decoder(
    field_path: list[str],
    key_fields_schema: list[engine_type.FieldSchema],
    dst_type_info: datatype.DataTypeInfo,
) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a key type.
    """
    if len(key_fields_schema) == 1 and isinstance(
        dst_type_info.variant, (datatype.BasicType, datatype.AnyType)
    ):
        single_key_decoder = make_engine_value_decoder(
            field_path,
            key_fields_schema[0].value_type.type,
            dst_type_info,
            for_key=True,
        )

        def key_decoder(value: list[Any]) -> Any:
            return single_key_decoder(value[0])

        return key_decoder

    return make_engine_struct_decoder(
        field_path,
        key_fields_schema,
        dst_type_info,
        for_key=True,
    )


def make_engine_value_decoder(
    field_path: list[str],
    src_type: engine_type.ValueType,
    dst_type_info: datatype.DataTypeInfo,
    for_key: bool = False,
) -> Callable[[Any], Any]:
    """
    Make a decoder from an engine value to a Python value.

    Args:
        field_path: The path to the field in the engine value. For error messages.
        src_type: The type of the engine value, mapped from a `cocoindex::base::schema::engine_type.ValueType`.
        dst_annotation: The type annotation of the Python value.

    Returns:
        A decoder from an engine value to a Python value.
    """

    src_type_kind = src_type.kind

    dst_type_variant = dst_type_info.variant

    if isinstance(dst_type_variant, datatype.OtherType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, an unsupported type"
        )

    if isinstance(src_type, engine_type.StructType):  # type: ignore[redundant-cast]
        return make_engine_struct_decoder(
            field_path,
            src_type.fields,
            dst_type_info,
            for_key=for_key,
        )

    if isinstance(src_type, engine_type.TableType):  # type: ignore[redundant-cast]
        with ChildFieldPath(field_path, "[*]"):
            engine_fields_schema = src_type.row.fields

            if src_type.kind == "LTable":
                if isinstance(dst_type_variant, datatype.AnyType):
                    dst_elem_type = Any
                elif isinstance(dst_type_variant, datatype.SequenceType):
                    dst_elem_type = dst_type_variant.elem_type
                else:
                    raise ValueError(
                        f"Type mismatch for `{''.join(field_path)}`: "
                        f"declared `{dst_type_info.core_type}`, a list type expected"
                    )
                row_decoder = make_engine_struct_decoder(
                    field_path,
                    engine_fields_schema,
                    datatype.analyze_type_info(dst_elem_type),
                )

                def decode(value: Any) -> Any | None:
                    if value is None:
                        return None
                    return [row_decoder(v) for v in value]

            elif src_type.kind == "KTable":
                if isinstance(dst_type_variant, datatype.AnyType):
                    key_type, value_type = Any, Any
                elif isinstance(dst_type_variant, datatype.MappingType):
                    key_type = dst_type_variant.key_type
                    value_type = dst_type_variant.value_type
                else:
                    raise ValueError(
                        f"Type mismatch for `{''.join(field_path)}`: "
                        f"declared `{dst_type_info.core_type}`, a dict type expected"
                    )

                num_key_parts = src_type.num_key_parts or 1
                key_decoder = make_engine_key_decoder(
                    field_path,
                    engine_fields_schema[0:num_key_parts],
                    datatype.analyze_type_info(key_type),
                )
                value_decoder = make_engine_struct_decoder(
                    field_path,
                    engine_fields_schema[num_key_parts:],
                    datatype.analyze_type_info(value_type),
                )

                def decode(value: Any) -> Any | None:
                    if value is None:
                        return None
                    return {
                        key_decoder(v[0:num_key_parts]): value_decoder(
                            v[num_key_parts:]
                        )
                        for v in value
                    }

        return decode

    if isinstance(src_type, engine_type.BasicValueType) and src_type.kind == "Union":
        if isinstance(dst_type_variant, datatype.AnyType):
            return lambda value: value[1]

        dst_type_info_variants = (
            [datatype.analyze_type_info(t) for t in dst_type_variant.variant_types]
            if isinstance(dst_type_variant, datatype.UnionType)
            else [dst_type_info]
        )
        # mypy: union info exists for Union kind
        assert src_type.union is not None  # type: ignore[unreachable]
        src_type_variants_basic: list[engine_type.BasicValueType] = (
            src_type.union.variants
        )
        src_type_variants = src_type_variants_basic
        decoders = []
        for i, src_type_variant in enumerate(src_type_variants):
            with ChildFieldPath(field_path, f"[{i}]"):
                decoder = None
                for dst_type_info_variant in dst_type_info_variants:
                    try:
                        decoder = make_engine_value_decoder(
                            field_path, src_type_variant, dst_type_info_variant
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

    if isinstance(dst_type_variant, datatype.AnyType):
        return lambda value: value

    if isinstance(src_type, engine_type.BasicValueType) and src_type.kind == "Vector":
        field_path_str = "".join(field_path)
        if not isinstance(dst_type_variant, datatype.SequenceType):
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
            if datatype.is_numpy_number_type(dst_type_variant.elem_type):
                scalar_dtype = dst_type_variant.elem_type
        else:
            # mypy: vector info exists for Vector kind
            assert src_type.vector is not None  # type: ignore[unreachable]
            vec_elem_decoder = make_engine_value_decoder(
                field_path + ["[*]"],
                src_type.vector.element_type,
                datatype.analyze_type_info(
                    dst_type_variant.elem_type if dst_type_variant else Any
                ),
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

    if isinstance(dst_type_variant, datatype.BasicType):
        if not _is_type_kind_convertible_to(src_type_kind, dst_type_variant.kind):
            raise ValueError(
                f"Type mismatch for `{''.join(field_path)}`: "
                f"passed in {src_type_kind}, declared {dst_type_info.core_type} ({dst_type_variant.kind})"
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


def make_engine_struct_decoder(
    field_path: list[str],
    src_fields: list[engine_type.FieldSchema],
    dst_type_info: datatype.DataTypeInfo,
    for_key: bool = False,
) -> Callable[[list[Any]], Any]:
    """Make a decoder from an engine field values to a Python value."""

    dst_type_variant = dst_type_info.variant

    if isinstance(dst_type_variant, datatype.AnyType):
        if for_key:
            return _make_engine_struct_to_tuple_decoder(field_path, src_fields)
        else:
            return _make_engine_struct_to_dict_decoder(field_path, src_fields, Any)
    elif isinstance(dst_type_variant, datatype.MappingType):
        analyzed_key_type = datatype.analyze_type_info(dst_type_variant.key_type)
        if (
            isinstance(analyzed_key_type.variant, datatype.AnyType)
            or analyzed_key_type.core_type is str
        ):
            return _make_engine_struct_to_dict_decoder(
                field_path, src_fields, dst_type_variant.value_type
            )

    if not isinstance(dst_type_variant, datatype.StructType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, a dataclass, NamedTuple, Pydantic model or dict[str, Any] expected"
        )

    src_name_to_idx = {f.name: i for i, f in enumerate(src_fields)}
    dst_struct_type = dst_type_variant.struct_type

    def make_closure_for_field(
        field_info: datatype.StructFieldInfo,
    ) -> Callable[[list[Any]], Any]:
        name = field_info.name
        src_idx = src_name_to_idx.get(name)
        type_info = datatype.analyze_type_info(field_info.type_hint)

        with ChildFieldPath(field_path, f".{name}"):
            if src_idx is not None:
                field_decoder = make_engine_value_decoder(
                    field_path,
                    src_fields[src_idx].value_type.type,
                    type_info,
                    for_key=for_key,
                )
                return lambda values: field_decoder(values[src_idx])

            default_value = field_info.default_value
            if default_value is not inspect.Parameter.empty:
                return lambda _: default_value

            auto_default, is_supported = get_auto_default_for_type(type_info)
            if is_supported:
                warnings.warn(
                    f"Field '{name}' (type {field_info.type_hint}) without default value is missing in input: "
                    f"{''.join(field_path)}. Auto-assigning default value: {auto_default}",
                    UserWarning,
                    stacklevel=4,
                )
                return lambda _: auto_default

            raise ValueError(
                f"Field '{name}' (type {field_info.type_hint}) without default value is missing in input: {''.join(field_path)}"
            )

    # Different construction for different struct types
    if datatype.is_pydantic_model(dst_struct_type):
        # Pydantic models prefer keyword arguments
        pydantic_fields_decoder = [
            (field_info.name, make_closure_for_field(field_info))
            for field_info in dst_type_variant.fields
        ]
        return lambda values: dst_struct_type(
            **{
                field_name: decoder(values)
                for field_name, decoder in pydantic_fields_decoder
            }
        )
    else:
        struct_fields_decoder = [
            make_closure_for_field(field_info) for field_info in dst_type_variant.fields
        ]
        # Dataclasses and NamedTuples can use positional arguments
        return lambda values: dst_struct_type(
            *(decoder(values) for decoder in struct_fields_decoder)
        )


def _make_engine_struct_to_dict_decoder(
    field_path: list[str],
    src_fields: list[engine_type.FieldSchema],
    value_type_annotation: Any,
) -> Callable[[list[Any] | None], dict[str, Any] | None]:
    """Make a decoder from engine field values to a Python dict."""

    field_decoders = []
    value_type_info = datatype.analyze_type_info(value_type_annotation)
    for field_schema in src_fields:
        field_name = field_schema.name
        with ChildFieldPath(field_path, f".{field_name}"):
            field_decoder = make_engine_value_decoder(
                field_path,
                field_schema.value_type.type,
                value_type_info,
            )
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


def _make_engine_struct_to_tuple_decoder(
    field_path: list[str],
    src_fields: list[engine_type.FieldSchema],
) -> Callable[[list[Any] | None], tuple[Any, ...] | None]:
    """Make a decoder from engine field values to a Python tuple."""

    field_decoders = []
    value_type_info = datatype.analyze_type_info(Any)
    for field_schema in src_fields:
        field_name = field_schema.name
        with ChildFieldPath(field_path, f".{field_name}"):
            field_decoders.append(
                make_engine_value_decoder(
                    field_path,
                    field_schema.value_type.type,
                    value_type_info,
                )
            )

    def decode_to_tuple(values: list[Any] | None) -> tuple[Any, ...] | None:
        if values is None:
            return None
        if len(field_decoders) != len(values):
            raise ValueError(
                f"Field count mismatch: expected {len(field_decoders)}, got {len(values)}"
            )
        return tuple(
            field_decoder(value) for value, field_decoder in zip(values, field_decoders)
        )

    return decode_to_tuple
