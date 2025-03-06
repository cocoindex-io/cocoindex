import typing
import collections
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

def _type_to_json_value(t, metadata):
    origin_type = typing.get_origin(t)
    if origin_type is collections.abc.Sequence or origin_type is list:
        dim = _find_annotation(metadata, Vector)
        if dim is None:
            raise ValueError(f"Vector dimension not found for {t}")
        args = typing.get_args(t)
        origin_type, metadata = _get_origin_type_and_metadata(args[0])
        type_json = {
            'kind': 'Vector',
            'element_type': _type_to_json_value(origin_type, metadata),
            'dimension': dim.dim,
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
        type_json = { 'kind': kind }
    
    return type_json

def _enriched_type_to_json_value(t) -> dict[str, Any] | None:
    if t is None:
        return None
    t, metadata = _get_origin_type_and_metadata(t)
    enriched_type_json = {'type': _type_to_json_value(t, metadata)}
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
    return _enriched_type_to_json_value(t)
