"""
Utilities to convert between Python and engine values.
"""
import dataclasses
import inspect
import uuid

from typing import Any, Callable
from .typing import analyze_type_info, COLLECTION_TYPES

def to_engine_value(value: Any) -> Any:
    """Convert a Python value to an engine value."""
    if dataclasses.is_dataclass(value):
        return [to_engine_value(getattr(value, f.name)) for f in dataclasses.fields(value)]
    if isinstance(value, (list, tuple)):
        return [to_engine_value(v) for v in value]
    if isinstance(value, uuid.UUID):
        return value.bytes
    return value

def make_engine_value_converter(
        field_path: list[str],
        src_type: dict[str, Any],
        dst_annotation,
    ) -> Callable[[Any], Any]:
    """
    Make a converter from an engine value to a Python value.

    Args:
        field_path: The path to the field in the engine value. For error messages.
        src_type: The type of the engine value, mapped from a `cocoindex::base::schema::ValueType`.
        dst_annotation: The type annotation of the Python value.

    Returns:
        A converter from an engine value to a Python value.
    """

    src_type_kind = src_type['kind']

    if dst_annotation is inspect.Parameter.empty:
        if src_type_kind == 'Struct' or src_type_kind in COLLECTION_TYPES:
            raise ValueError(f"Missing type annotation for `{''.join(field_path)}`."
                             f"It's required for {src_type_kind} type.")
        return lambda value: value

    dst_type_info = analyze_type_info(dst_annotation)

    if src_type_kind != dst_type_info.kind:
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"passed in {src_type_kind}, declared {dst_annotation} ({dst_type_info.kind})")

    if dst_type_info.dataclass_type is not None:
        return _make_engine_struct_value_converter(
            field_path, src_type['fields'], dst_type_info.dataclass_type)

    if src_type_kind in COLLECTION_TYPES:
        field_path.append('[*]')
        elem_type_info = analyze_type_info(dst_type_info.elem_type)
        if elem_type_info.dataclass_type is None:
            raise ValueError(f"Type mismatch for `{''.join(field_path)}`: "
                             f"declared `{dst_type_info.kind}`, a dataclass type expected")
        elem_converter = _make_engine_struct_value_converter(
            field_path, src_type['row']['fields'], elem_type_info.dataclass_type)
        field_path.pop()
        return lambda value: [elem_converter(v) for v in value] if value is not None else None

    if src_type_kind == 'Uuid':
        return lambda value: uuid.UUID(bytes=value)

    return lambda value: value

def _make_engine_struct_value_converter(
        field_path: list[str],
        src_fields: list[dict[str, Any]],
        dst_dataclass_type: type,
    ) -> Callable[[list], Any]:
    """Make a converter from an engine field values to a Python value."""

    src_name_to_idx = {f['name']: i for i, f in enumerate(src_fields)}
    def make_closure_for_value(name: str, param: inspect.Parameter) -> Callable[[list], Any]:
        src_idx = src_name_to_idx.get(name)
        if src_idx is not None:
            field_path.append(f'.{name}')
            field_converter = make_engine_value_converter(
                field_path, src_fields[src_idx]['type'], param.annotation)
            field_path.pop()
            return lambda values: field_converter(values[src_idx])

        default_value = param.default
        if default_value is inspect.Parameter.empty:
            raise ValueError(
                f"Field without default value is missing in input: {''.join(field_path)}")

        return lambda _: default_value

    field_value_converters = [
        make_closure_for_value(name, param)
        for (name, param) in inspect.signature(dst_dataclass_type).parameters.items()]

    return lambda values: dst_dataclass_type(
        *(converter(values) for converter in field_value_converters))
