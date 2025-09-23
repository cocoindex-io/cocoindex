"""
Utilities to convert between Python and engine values.
"""

from __future__ import annotations

import dataclasses
import datetime
import inspect
import warnings
from enum import Enum
from typing import Any, Callable, Mapping, get_origin, TypeVar, overload

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
    EnrichedValueType,
    analyze_type_info,
    encode_enriched_type,
    is_namedtuple_type,
    is_numpy_number_type,
    extract_ndarray_elem_dtype,
    ValueType,
    FieldSchema,
    BasicValueType,
    StructType,
    TableType,
)


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
        value_type_info = analyze_type_info(variant.value_type)
        if not isinstance(value_type_info.variant, AnalyzedStructType):
            raise ValueError(
                f"Value type for dict is required to be a struct (e.g. dataclass or NamedTuple), got {variant.value_type}. "
                f"If you want a free-formed dict, use `cocoindex.Json` instead."
            )
        value_encoder = make_engine_value_encoder(value_type_info)

        key_type_info = analyze_type_info(variant.key_type)
        key_encoder = make_engine_value_encoder(key_type_info)
        if isinstance(key_type_info.variant, AnalyzedBasicType):

            def encode_row(k: Any, v: Any) -> Any:
                return [key_encoder(k)] + value_encoder(v)

        else:

            def encode_row(k: Any, v: Any) -> Any:
                return key_encoder(k) + value_encoder(v)

        def encode_struct_dict(value: Any) -> Any:
            if not value:
                return []
            return [encode_row(k, v) for k, v in value.items()]

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
            # mypy: vector info exists for Vector kind
            assert src_type.vector is not None  # type: ignore[unreachable]
            vec_elem_decoder = make_engine_value_decoder(
                field_path + ["[*]"],
                src_type.vector.element_type,
                analyze_type_info(
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


def _get_auto_default_for_type(
    type_info: AnalyzedTypeInfo,
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
    if isinstance(type_info.variant, AnalyzedListType):
        return [], True
    elif isinstance(type_info.variant, AnalyzedDictType):
        return {}, True

    return None, False


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
            f"declared `{dst_type_info.core_type}`, a dataclass, NamedTuple or dict[str, Any] expected"
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

            auto_default, is_supported = _get_auto_default_for_type(type_info)
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


def dump_engine_object(v: Any) -> Any:
    """Recursively dump an object for engine. Engine side uses `Pythonized` to catch."""
    if v is None:
        return None
    elif isinstance(v, EnrichedValueType):
        return v.encode()
    elif isinstance(v, FieldSchema):
        return v.encode()
    elif isinstance(v, type) or get_origin(v) is not None:
        return encode_enriched_type(v)
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, datetime.timedelta):
        total_secs = v.total_seconds()
        secs = int(total_secs)
        nanos = int((total_secs - secs) * 1e9)
        return {"secs": secs, "nanos": nanos}
    elif is_namedtuple_type(type(v)):
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

    type_info = analyze_type_info(expected_type)
    variant = type_info.variant

    if type_info.core_type is EnrichedValueType:
        return EnrichedValueType.decode(v)
    if type_info.core_type is FieldSchema:
        return FieldSchema.decode(v)

    # Any or unknown → return as-is
    if isinstance(variant, AnalyzedAnyType) or type_info.base_type is Any:
        return v

    # Enum handling
    if isinstance(expected_type, type) and issubclass(expected_type, Enum):
        return expected_type(v)

    # TimeDelta special form {secs, nanos}
    if isinstance(variant, AnalyzedBasicType) and variant.kind == "TimeDelta":
        if isinstance(v, Mapping) and "secs" in v and "nanos" in v:
            secs = int(v["secs"])  # type: ignore[index]
            nanos = int(v["nanos"])  # type: ignore[index]
            return datetime.timedelta(seconds=secs, microseconds=nanos / 1_000)
        return v

    # List, NDArray (Vector-ish), or general sequences
    if isinstance(variant, AnalyzedListType):
        elem_type = variant.elem_type if variant.elem_type else Any
        if type_info.base_type is np.ndarray:
            # Reconstruct NDArray with appropriate dtype if available
            try:
                dtype = extract_ndarray_elem_dtype(type_info.core_type)
            except (TypeError, ValueError, AttributeError):
                dtype = None
            return np.array(v, dtype=dtype)
        # Regular Python list
        return [load_engine_object(elem_type, item) for item in v]

    # Dict / Mapping
    if isinstance(variant, AnalyzedDictType):
        key_t = variant.key_type
        val_t = variant.value_type
        return {
            load_engine_object(key_t, k): load_engine_object(val_t, val)
            for k, val in v.items()
        }

    # Structs (dataclass or NamedTuple)
    if isinstance(variant, AnalyzedStructType):
        struct_type = variant.struct_type
        if dataclasses.is_dataclass(struct_type):
            if not isinstance(v, Mapping):
                raise ValueError(f"Expected dict for dataclass, got {type(v)}")
            # Drop auxiliary discriminator "kind" if present
            dc_init_kwargs: dict[str, Any] = {}
            field_types = {f.name: f.type for f in dataclasses.fields(struct_type)}
            for name, f_type in field_types.items():
                if name in v:
                    dc_init_kwargs[name] = load_engine_object(f_type, v[name])
            return struct_type(**dc_init_kwargs)
        elif is_namedtuple_type(struct_type):
            if not isinstance(v, Mapping):
                raise ValueError(f"Expected dict for NamedTuple, got {type(v)}")
            # Dict format (from dump/load functions)
            annotations = getattr(struct_type, "__annotations__", {})
            field_names = list(getattr(struct_type, "_fields", ()))
            nt_init_kwargs: dict[str, Any] = {}
            for name in field_names:
                f_type = annotations.get(name, Any)
                if name in v:
                    nt_init_kwargs[name] = load_engine_object(f_type, v[name])
            return struct_type(**nt_init_kwargs)
        return v

    # Union with discriminator support via "kind"
    if isinstance(variant, AnalyzedUnionType):
        if isinstance(v, Mapping) and "kind" in v:
            discriminator = v["kind"]
            for typ in variant.variant_types:
                t_info = analyze_type_info(typ)
                if isinstance(t_info.variant, AnalyzedStructType):
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
