"""
Utilities to encode/decode values in cocoindex (for data).
"""

from __future__ import annotations

import dataclasses
import inspect
import warnings
from typing import Any, Callable, Mapping, TypeVar

import numpy as np
from .typing import (
    AnalyzedAnyType,
    AnalyzedBasicType,
    AnalyzedDictType,
    AnalyzedListType,
    AnalyzedStructType,
    AnalyzedTypeInfo,
    AnalyzedUnionType,
    AnalyzedUnknownType,
    analyze_type_info,
    is_namedtuple_type,
    is_pydantic_model,
    is_numpy_number_type,
    ValueType,
    FieldSchema,
    BasicValueType,
    StructType,
    TableType,
)
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
ANY_TYPE_INFO = analyze_type_info(inspect.Parameter.empty)


def make_engine_key_encoder(type_info: AnalyzedTypeInfo) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a key type.
    """
    value_encoder = make_engine_value_encoder(type_info)
    if isinstance(type_info.variant, AnalyzedBasicType):
        return lambda value: [value_encoder(value)]
    else:
        return value_encoder


def make_engine_value_encoder(type_info: AnalyzedTypeInfo) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a specific type.
    """
    variant = type_info.variant

    if isinstance(variant, AnalyzedUnknownType):
        raise ValueError(f"Type annotation `{type_info.core_type}` is unsupported")

    if isinstance(variant, AnalyzedListType):
        elem_type_info = (
            analyze_type_info(variant.elem_type) if variant.elem_type else ANY_TYPE_INFO
        )
        if isinstance(elem_type_info.variant, AnalyzedStructType):
            elem_encoder = make_engine_value_encoder(elem_type_info)

            def encode_struct_list(value: Any) -> Any:
                return None if value is None else [elem_encoder(v) for v in value]

            return encode_struct_list

        # Otherwise it's a vector, falling into basic type in the engine.

    if isinstance(variant, AnalyzedDictType):
        key_type_info = analyze_type_info(variant.key_type)
        key_encoder = make_engine_key_encoder(key_type_info)

        value_type_info = analyze_type_info(variant.value_type)
        if not isinstance(value_type_info.variant, AnalyzedStructType):
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

    if isinstance(variant, AnalyzedStructType):
        struct_type = variant.struct_type

        if dataclasses.is_dataclass(struct_type):
            fields = dataclasses.fields(struct_type)
            field_encoders = [
                make_engine_value_encoder(analyze_type_info(f.type)) for f in fields
            ]
            field_names = [f.name for f in fields]

            def encode_dataclass(value: Any) -> Any:
                if value is None:
                    return None
                return [
                    encoder(getattr(value, name))
                    for encoder, name in zip(field_encoders, field_names)
                ]

            return encode_dataclass

        elif is_namedtuple_type(struct_type):
            annotations = struct_type.__annotations__
            field_names = list(getattr(struct_type, "_fields", ()))
            field_encoders = [
                make_engine_value_encoder(
                    analyze_type_info(annotations[name])
                    if name in annotations
                    else ANY_TYPE_INFO
                )
                for name in field_names
            ]

            def encode_namedtuple(value: Any) -> Any:
                if value is None:
                    return None
                return [
                    encoder(getattr(value, name))
                    for encoder, name in zip(field_encoders, field_names)
                ]

            return encode_namedtuple

        elif is_pydantic_model(struct_type):
            # Type guard: ensure we have model_fields attribute
            if hasattr(struct_type, "model_fields"):
                field_names = list(struct_type.model_fields.keys())  # type: ignore[attr-defined]
                field_encoders = [
                    make_engine_value_encoder(
                        analyze_type_info(struct_type.model_fields[name].annotation)  # type: ignore[attr-defined]
                    )
                    for name in field_names
                ]
            else:
                raise ValueError(f"Invalid Pydantic model: {struct_type}")

            def encode_pydantic(value: Any) -> Any:
                if value is None:
                    return None
                return [
                    encoder(getattr(value, name))
                    for encoder, name in zip(field_encoders, field_names)
                ]

            return encode_pydantic

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
    key_fields_schema: list[FieldSchema],
    dst_type_info: AnalyzedTypeInfo,
) -> Callable[[Any], Any]:
    """
    Create an encoder closure for a key type.
    """
    if len(key_fields_schema) == 1 and isinstance(
        dst_type_info.variant, (AnalyzedBasicType, AnalyzedAnyType)
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
    src_type: ValueType,
    dst_type_info: AnalyzedTypeInfo,
    for_key: bool = False,
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

    src_type_kind = src_type.kind

    dst_type_variant = dst_type_info.variant

    if isinstance(dst_type_variant, AnalyzedUnknownType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, an unsupported type"
        )

    if isinstance(src_type, StructType):  # type: ignore[redundant-cast]
        return make_engine_struct_decoder(
            field_path,
            src_type.fields,
            dst_type_info,
            for_key=for_key,
        )

    if isinstance(src_type, TableType):  # type: ignore[redundant-cast]
        with ChildFieldPath(field_path, "[*]"):
            engine_fields_schema = src_type.row.fields

            if src_type.kind == "LTable":
                if isinstance(dst_type_variant, AnalyzedAnyType):
                    dst_elem_type = Any
                elif isinstance(dst_type_variant, AnalyzedListType):
                    dst_elem_type = dst_type_variant.elem_type
                else:
                    raise ValueError(
                        f"Type mismatch for `{''.join(field_path)}`: "
                        f"declared `{dst_type_info.core_type}`, a list type expected"
                    )
                row_decoder = make_engine_struct_decoder(
                    field_path,
                    engine_fields_schema,
                    analyze_type_info(dst_elem_type),
                )

                def decode(value: Any) -> Any | None:
                    if value is None:
                        return None
                    return [row_decoder(v) for v in value]

            elif src_type.kind == "KTable":
                if isinstance(dst_type_variant, AnalyzedAnyType):
                    key_type, value_type = Any, Any
                elif isinstance(dst_type_variant, AnalyzedDictType):
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
                    analyze_type_info(key_type),
                )
                value_decoder = make_engine_struct_decoder(
                    field_path,
                    engine_fields_schema[num_key_parts:],
                    analyze_type_info(value_type),
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

    if isinstance(src_type, BasicValueType) and src_type.kind == "Union":
        if isinstance(dst_type_variant, AnalyzedAnyType):
            return lambda value: value[1]

        dst_type_info_variants = (
            [analyze_type_info(t) for t in dst_type_variant.variant_types]
            if isinstance(dst_type_variant, AnalyzedUnionType)
            else [dst_type_info]
        )
        # mypy: union info exists for Union kind
        assert src_type.union is not None  # type: ignore[unreachable]
        src_type_variants_basic: list[BasicValueType] = src_type.union.variants
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

    if isinstance(dst_type_variant, AnalyzedAnyType):
        return lambda value: value

    if isinstance(src_type, BasicValueType) and src_type.kind == "Vector":

    # force numeric vectors to be converted directly to NumPy arrays
    scalar_dtype = (
        dst_type_variant.elem_type
        if dst_type_variant and is_numpy_number_type(dst_type_variant.elem_type)
        else None
    )
    expected_dim = (
        dst_type_variant.vector_info.dim
        if dst_type_variant and dst_type_variant.vector_info
        else None
    )

    def decode_vector(value: Any) -> Any | None:
        if value is None:
            return None if dst_type_info.nullable else np.zeros(expected_dim, dtype=scalar_dtype)

        if not isinstance(value, (list, np.ndarray)):
            raise TypeError(f"Expected NDArray or list for vector `{''.join(field_path)}`, got {type(value)}")


        arr = np.array(value, dtype=scalar_dtype)

        if expected_dim is not None and arr.shape[0] != expected_dim:
            raise ValueError(
                f"Vector dimension mismatch for `{''.join(field_path)}`: "
                f"expected {expected_dim}, got {arr.shape[0]}"
            )
        return arr

    return decode_vector


    if isinstance(dst_type_variant, AnalyzedBasicType):
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
    src_fields: list[FieldSchema],
    dst_type_info: AnalyzedTypeInfo,
    for_key: bool = False,
) -> Callable[[list[Any]], Any]:
    """Make a decoder from an engine field values to a Python value."""

    dst_type_variant = dst_type_info.variant

    if isinstance(dst_type_variant, AnalyzedAnyType):
        if for_key:
            return _make_engine_struct_to_tuple_decoder(field_path, src_fields)
        else:
            return _make_engine_struct_to_dict_decoder(field_path, src_fields, Any)
    elif isinstance(dst_type_variant, AnalyzedDictType):
        analyzed_key_type = analyze_type_info(dst_type_variant.key_type)
        if (
            isinstance(analyzed_key_type.variant, AnalyzedAnyType)
            or analyzed_key_type.core_type is str
        ):
            return _make_engine_struct_to_dict_decoder(
                field_path, src_fields, dst_type_variant.value_type
            )

    if not isinstance(dst_type_variant, AnalyzedStructType):
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"declared `{dst_type_info.core_type}`, a dataclass, NamedTuple, Pydantic model or dict[str, Any] expected"
        )

    src_name_to_idx = {f.name: i for i, f in enumerate(src_fields)}
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
    elif is_pydantic_model(dst_struct_type):
        # For Pydantic models, we can use model_fields to get field information
        parameters = {}
        # Type guard: ensure we have model_fields attribute
        if hasattr(dst_struct_type, "model_fields"):
            model_fields = dst_struct_type.model_fields  # type: ignore[attr-defined]
        else:
            model_fields = {}
        for name, field_info in model_fields.items():
            default_value = (
                field_info.default
                if field_info.default is not ...
                else inspect.Parameter.empty
            )
            parameters[name] = inspect.Parameter(
                name=name,
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=default_value,
                annotation=field_info.annotation,
            )
    else:
        raise ValueError(f"Unsupported struct type: {dst_struct_type}")

    def make_closure_for_field(
        name: str, param: inspect.Parameter
    ) -> Callable[[list[Any]], Any]:
        src_idx = src_name_to_idx.get(name)
        type_info = analyze_type_info(param.annotation)

        with ChildFieldPath(field_path, f".{name}"):
            if src_idx is not None:
                field_decoder = make_engine_value_decoder(
                    field_path,
                    src_fields[src_idx].value_type.type,
                    type_info,
                    for_key=for_key,
                )
                return lambda values: field_decoder(values[src_idx])

            default_value = param.default
            if default_value is not inspect.Parameter.empty:
                return lambda _: default_value

            auto_default, is_supported = get_auto_default_for_type(type_info)
            if is_supported:
                warnings.warn(
                    f"Field '{name}' (type {param.annotation}) without default value is missing in input: "
                    f"{''.join(field_path)}. Auto-assigning default value: {auto_default}",
                    UserWarning,
                    stacklevel=4,
                )
                return lambda _: auto_default

            raise ValueError(
                f"Field '{name}' (type {param.annotation}) without default value is missing in input: {''.join(field_path)}"
            )

    field_value_decoder = [
        make_closure_for_field(name, param) for (name, param) in parameters.items()
    ]

    # Different construction for different struct types
    if is_pydantic_model(dst_struct_type):
        # Pydantic models prefer keyword arguments
        field_names = list(parameters.keys())
        return lambda values: dst_struct_type(
            **{
                field_names[i]: decoder(values)
                for i, decoder in enumerate(field_value_decoder)
            }
        )
    else:
        # Dataclasses and NamedTuples can use positional arguments
        return lambda values: dst_struct_type(
            *(decoder(values) for decoder in field_value_decoder)
        )


def _make_engine_struct_to_dict_decoder(
    field_path: list[str],
    src_fields: list[FieldSchema],
    value_type_annotation: Any,
) -> Callable[[list[Any] | None], dict[str, Any] | None]:
    """Make a decoder from engine field values to a Python dict."""

    field_decoders = []
    value_type_info = analyze_type_info(value_type_annotation)
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
    src_fields: list[FieldSchema],
) -> Callable[[list[Any] | None], tuple[Any, ...] | None]:
    """Make a decoder from engine field values to a Python tuple."""

    field_decoders = []
    value_type_info = analyze_type_info(Any)
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
