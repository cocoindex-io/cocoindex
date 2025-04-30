import typing
import collections
import dataclasses
import datetime
import types
import inspect
import uuid
from typing import Annotated, NamedTuple, Any, TypeVar, TYPE_CHECKING, overload, Sequence, Protocol, Generic, Literal

class VectorInfo(NamedTuple):
    dim: int | None

class TypeKind(NamedTuple):
    kind: str

class TypeAttr:
    key: str
    value: Any

    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

Annotation = TypeKind | TypeAttr | VectorInfo

Float32 = Annotated[float, TypeKind('Float32')]
Float64 = Annotated[float, TypeKind('Float64')]
Range = Annotated[tuple[int, int], TypeKind('Range')]
Json = Annotated[Any, TypeKind('Json')]
LocalDateTime = Annotated[datetime.datetime, TypeKind('LocalDateTime')]
OffsetDateTime = Annotated[datetime.datetime, TypeKind('OffsetDateTime')]

if TYPE_CHECKING:
    T_co = TypeVar('T_co', covariant=True)
    Dim_co = TypeVar('Dim_co', bound=int, covariant=True)

    class Vector(Sequence[T_co], Generic[T_co, Dim_co], Protocol):
        """Vector[T, Dim] is a special typing alias for a list[T] with optional dimension info"""
else:
    class Vector:  # type: ignore[unreachable]
        """ A special typing alias for a list[T] with optional dimension info """
        def __class_getitem__(self, params):
            if not isinstance(params, tuple):
                # Only element type provided
                elem_type = params
                return Annotated[list[elem_type], VectorInfo(dim=None)]
            else:
                # Element type and dimension provided
                elem_type, dim = params
                if typing.get_origin(dim) is Literal:
                    dim = typing.get_args(dim)[0]  # Extract the literal value
                return Annotated[list[elem_type], VectorInfo(dim=dim)]

TABLE_TYPES = ('KTable', 'LTable')
KEY_FIELD_NAME = '_key'

ElementType = type | tuple[type, type]

def _is_struct_type(t) -> bool:
    return isinstance(t, type) and dataclasses.is_dataclass(t)

@dataclasses.dataclass
class AnalyzedTypeInfo:
    """
    Analyzed info of a Python type.
    """
    kind: str
    vector_info: VectorInfo | None  # For Vector
    elem_type: ElementType | None   # For Vector and Table

    key_type: type | None           # For element of KTable
    dataclass_type: type | None     # For Struct

    attrs: dict[str, Any] | None
    nullable: bool = False

def analyze_type_info(t) -> AnalyzedTypeInfo:
    """
    Analyze a Python type and return the analyzed info.
    """
    if isinstance(t, tuple) and len(t) == 2:
        key_type, value_type = t
        result = analyze_type_info(value_type)
        result.key_type = key_type
        return result

    annotations: tuple[Annotation, ...] = ()
    base_type = None
    nullable = False
    while True:
        base_type = typing.get_origin(t)
        if base_type is Annotated:
            annotations = t.__metadata__
            t = t.__origin__
        elif base_type is types.UnionType:
            possible_types = typing.get_args(t)
            non_none_types = [arg for arg in possible_types if arg not in (None, types.NoneType)]
            if len(non_none_types) != 1:
                raise ValueError(
                    f"Expect exactly one non-None choice for Union type, but got {len(non_none_types)}: {t}")
            t = non_none_types[0]
            if len(possible_types) > 1:
                nullable = True
        else:
            break

    attrs = None
    vector_info = None
    kind = None
    for attr in annotations:
        if isinstance(attr, TypeAttr):
            if attrs is None:
                attrs = dict()
            attrs[attr.key] = attr.value
        elif isinstance(attr, VectorInfo):
            vector_info = attr
        elif isinstance(attr, TypeKind):
            kind = attr.kind

    dataclass_type = None
    elem_type = None
    key_type = None
    if _is_struct_type(t):
        if kind is None:
            kind = 'Struct'
        elif kind != 'Struct':
            raise ValueError(f"Unexpected type kind for struct: {kind}")
        dataclass_type = t
    elif base_type is collections.abc.Sequence or base_type is list:
        args = typing.get_args(t)
        elem_type = args[0]

        if kind is None:
            if _is_struct_type(elem_type):
                kind = 'LTable'
                if vector_info is not None:
                    raise ValueError("Vector element must be a simple type, not a struct")
            else:
                kind = 'Vector'
                if vector_info is None:
                    vector_info = VectorInfo(dim=None)
        elif not (kind == 'Vector' or kind in TABLE_TYPES):
            raise ValueError(f"Unexpected type kind for list: {kind}")
    elif base_type is collections.abc.Mapping or base_type is dict:
        args = typing.get_args(t)
        elem_type = (args[0], args[1])
        kind = 'KTable'
    elif kind is None:
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
        elif t is uuid.UUID:
            kind = 'Uuid'
        elif t is datetime.date:
            kind = 'Date'
        elif t is datetime.time:
            kind = 'Time'
        elif t is datetime.datetime:
            kind = 'OffsetDateTime'
        else:
            raise ValueError(f"type unsupported yet: {t}")

    return AnalyzedTypeInfo(kind=kind, vector_info=vector_info,
                            elem_type=elem_type, key_type=key_type, dataclass_type=dataclass_type,
                            attrs=attrs, nullable=nullable)

def _encode_fields_schema(dataclass_type: type, key_type: type | None = None) -> list[dict[str, Any]]:
    result = []
    def add_field(name: str, t) -> None:
        try:
            type_info = encode_enriched_type_info(analyze_type_info(t))
        except ValueError as e:
            e.add_note(f"Failed to encode annotation for field - "
                       f"{dataclass_type.__name__}.{name}: {t}")
            raise
        type_info['name'] = name
        result.append(type_info)

    if key_type is not None:
        add_field(KEY_FIELD_NAME, key_type)
    for field in dataclasses.fields(dataclass_type):
        add_field(field.name, field.type)
    return result

def _encode_type(type_info: AnalyzedTypeInfo) -> dict[str, Any]:
    encoded_type: dict[str, Any] = { 'kind': type_info.kind }

    if type_info.kind == 'Struct':
        if type_info.dataclass_type is None:
            raise ValueError("Struct type must have a dataclass type")
        encoded_type['fields'] = _encode_fields_schema(type_info.dataclass_type, type_info.key_type)
        if doc := inspect.getdoc(type_info.dataclass_type):
            encoded_type['description'] = doc

    elif type_info.kind == 'Vector':
        if type_info.vector_info is None:
            raise ValueError("Vector type must have a vector info")
        if type_info.elem_type is None:
            raise ValueError("Vector type must have an element type")
        encoded_type['element_type'] = _encode_type(analyze_type_info(type_info.elem_type))
        encoded_type['dimension'] = type_info.vector_info.dim

    elif type_info.kind in TABLE_TYPES:
        if type_info.elem_type is None:
            raise ValueError(f"{type_info.kind} type must have an element type")
        row_type_info = analyze_type_info(type_info.elem_type)
        encoded_type['row'] = _encode_type(row_type_info)

    return encoded_type

def encode_enriched_type_info(enriched_type_info: AnalyzedTypeInfo) -> dict[str, Any]:
    """
    Encode an enriched type info to a CocoIndex engine's type representation
    """
    encoded: dict[str, Any] = {'type': _encode_type(enriched_type_info)}

    if enriched_type_info.attrs is not None:
        encoded['attrs'] = enriched_type_info.attrs

    if enriched_type_info.nullable:
        encoded['nullable'] = True

    return encoded

@overload
def encode_enriched_type(t: None) -> None:
    ...

@overload
def encode_enriched_type(t: Any) -> dict[str, Any]:
    ...

def encode_enriched_type(t) -> dict[str, Any] | None:
    """
    Convert a Python type to a CocoIndex engine's type representation
    """
    if t is None:
        return None

    return encode_enriched_type_info(analyze_type_info(t))

def resolve_forward_ref(t):
    if t is str:
        return eval(t) # pylint: disable=eval-used
    return t
