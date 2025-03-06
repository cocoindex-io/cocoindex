import typing
import collections
import dataclasses
from typing import Annotated, NamedTuple, Any

class Vector(NamedTuple):
    dim: int | None

class TypeKind(NamedTuple):
    kind: str
class TypeAttr:
    key: str
    value: Any

    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

Float32 = Annotated[float, TypeKind('Float32')]
Float64 = Annotated[float, TypeKind('Float64')]
Range = Annotated[tuple[int, int], TypeKind('Range')]
Json = Annotated[Any, TypeKind('Json')]

def _find_annotation(metadata, cls):
    for m in iter(metadata):
        if isinstance(m, cls):
            return m
    return None

def _get_origin_type_and_metadata(t):
    if typing.get_origin(t) is Annotated:
        return (t.__origin__, t.__metadata__)
    return (t, ())

def _dump_fields_schema(cls: type) -> list[dict[str, Any]]:
    return [
                {
                    'name': field.name,
                    'value_type': _dump_enriched_type(field.type),
                }
                for field in dataclasses.fields(cls)
            ]

def _dump_type(t, metadata):
    origin_type = typing.get_origin(t)
    if origin_type is collections.abc.Sequence or origin_type is list:
        args = typing.get_args(t)
        elem_type, elem_type_metadata = _get_origin_type_and_metadata(args[0])
        vector_annot = _find_annotation(metadata, Vector)
        if vector_annot is not None:
            encoded_type = {
                'kind': 'Vector',
                'element_type': _dump_type(elem_type, elem_type_metadata),
                'dimension': vector_annot.dim,
            }
        elif dataclasses.is_dataclass(elem_type):
            encoded_type = {
                'kind': 'Table',
                'row': _dump_fields_schema(elem_type),
            }
        else:
            raise ValueError(f"Unsupported type: {t}")
    elif dataclasses.is_dataclass(t):
        encoded_type = {
            'kind': 'Struct',
            'fields': _dump_fields_schema(t),
        }
    else:
        type_kind = _find_annotation(metadata, TypeKind)
        if type_kind is not None:
            kind = type_kind.kind
        else:
            if t is bytes:
                kind = 'Bytes'
            elif t is str:
                kind = 'Str'
            elif t is bool:
                kind = 'Bool'
            elif t is int:
                kind = 'Int64'
            elif t is float:
                kind = 'Float64'
            else:
                raise ValueError(f"type unsupported yet: {t}")
        encoded_type = { 'kind': kind }
    
    return encoded_type

def _dump_enriched_type(t) -> dict[str, Any] | None:
    if t is None:
        return None
    t, metadata = _get_origin_type_and_metadata(t)
    enriched_type_json = {'type': _dump_type(t, metadata)}
    attrs = None
    for attr in metadata:
        if isinstance(attr, TypeAttr):
            if attrs is None:
                attrs = dict()
            attrs[attr.key] = attr.value
    if attrs is not None:
        enriched_type_json['attrs'] = attrs
    return enriched_type_json


def dump_type(t) -> dict[str, Any] | None:
    """
    Convert a Python type to a CocoIndex's type in JSON.
    """
    return _dump_enriched_type(t)
