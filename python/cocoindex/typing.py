import typing
import collections
from typing import Annotated, NamedTuple, Any

class Vector(NamedTuple):
    dim: int | None

class NumBits(NamedTuple):
    bits: int

class TypeAttr:
    key: str
    value: Any

    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

Float32 = Annotated[float, NumBits(32)]
Float64 = Annotated[float, NumBits(64)]
Range = Annotated[tuple[int, int], 'range']
Json = Annotated[Any, 'json']

def _find_annotation(metadata, cls):
    for m in iter(metadata):
        if isinstance(m, cls):
            return m
    return None

def _get_origin_type_and_metadata(t):
    if typing.get_origin(t) is Annotated:
        return (t.__origin__, t.__metadata__)
    return (t, ())

def _basic_type_to_json_value(t, metadata):
    origin_type = typing.get_origin(t)
    if origin_type is collections.abc.Sequence or origin_type is list:
        dim = _find_annotation(metadata, Vector)
        if dim is None:
            raise ValueError(f"Vector dimension not found for {t}")
        args = typing.get_args(t)
        type_json = {
            'kind': 'Vector',
            'element_type': _basic_type_to_json_value(*_get_origin_type_and_metadata(args[0])),
            'dimension': dim.dim,
        }
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
            num_bits = _find_annotation(metadata, NumBits)
            kind = 'Float32' if num_bits is not None and num_bits.bits <= 32 else 'Float64'
        elif t is Range:
            kind = 'Range'
        elif t is Json:
            kind = 'Json'
        else:
            raise ValueError(f"type unsupported yet: {t}")
        type_json = { 'kind': kind }
    
    return type_json

def _enriched_type_to_json_value(t) -> dict[str, Any] | None:
    if t is None:
        return None
    t, metadata = _get_origin_type_and_metadata(t)
    enriched_type_json = {'type': _basic_type_to_json_value(t, metadata)}
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
