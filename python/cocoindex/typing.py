import typing
import collections
import dataclasses
import types
from typing import Annotated, NamedTuple, Any, TypeVar, TYPE_CHECKING, overload

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

Annotation = Vector | TypeKind | TypeAttr

Float32 = Annotated[float, TypeKind('Float32')]
Float64 = Annotated[float, TypeKind('Float64')]
Range = Annotated[tuple[int, int], TypeKind('Range')]
Json = Annotated[Any, TypeKind('Json')]

COLLECTION_TYPES = ('Table', 'List')

R = TypeVar("R")

if TYPE_CHECKING:
    Table = Annotated[list[R], TypeKind('Table')]
    List = Annotated[list[R], TypeKind('List')]
else:
    # pylint: disable=too-few-public-methods
    class Table:  # type: ignore[unreachable]
        """
        A Table type, which has a list of rows. The first field of each row is the key.
        """
        def __class_getitem__(cls, item: type[R]):
            return Annotated[list[item], TypeKind('Table')]

    # pylint: disable=too-few-public-methods
    class List:  # type: ignore[unreachable]
        """
        A List type, which has a list of ordered rows.
        """
        def __class_getitem__(cls, item: type[R]):
            return Annotated[list[item], TypeKind('List')]

@dataclasses.dataclass
class AnalyzedTypeInfo:
    """
    Analyzed info of a Python type.
    """
    kind: str
    vector_info: Vector | None
    elem_type: type | None
    dataclass_type: type | None
    attrs: dict[str, Any] | None
    nullable: bool = False

def analyze_type_info(t) -> AnalyzedTypeInfo:
    """
    Analyze a Python type and return the analyzed info.
    """
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
        elif isinstance(attr, Vector):
            vector_info = attr
        elif isinstance(attr, TypeKind):
            kind = attr.kind

    dataclass_type = None
    elem_type = None
    if isinstance(t, type) and dataclasses.is_dataclass(t):
        if kind is None:
            kind = 'Struct'
        elif kind != 'Struct':
            raise ValueError(f"Unexpected type kind for struct: {kind}")
        dataclass_type = t
    elif base_type is collections.abc.Sequence or base_type is list:
        if kind is None:
            kind = 'Vector' if vector_info is not None else 'List'
        elif not (kind == 'Vector' or kind in COLLECTION_TYPES):
            raise ValueError(f"Unexpected type kind for list: {kind}")

        args = typing.get_args(t)
        if len(args) != 1:
            raise ValueError(f"{kind} must have exactly one type argument")
        elem_type = args[0]
    elif kind is None:
        if base_type is collections.abc.Sequence or base_type is list:
            kind = 'Vector' if vector_info is not None else 'List'
        elif t is bytes:
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
            raise ValueError(f"type unsupported yet: {base_type}")

    return AnalyzedTypeInfo(kind=kind, vector_info=vector_info, elem_type=elem_type,
                            dataclass_type=dataclass_type, attrs=attrs, nullable=nullable)

def _encode_fields_schema(dataclass_type: type) -> list[dict[str, Any]]:
    return [{ 'name': field.name,
              **encode_enriched_type_info(analyze_type_info(field.type))
            } for field in dataclasses.fields(dataclass_type)]

def _encode_type(type_info: AnalyzedTypeInfo) -> dict[str, Any]:
    encoded_type: dict[str, Any] = { 'kind': type_info.kind }

    if type_info.kind == 'Struct':
        if type_info.dataclass_type is None:
            raise ValueError("Struct type must have a dataclass type")
        encoded_type['fields'] = _encode_fields_schema(type_info.dataclass_type)

    elif type_info.kind == 'Vector':
        if type_info.vector_info is None:
            raise ValueError("Vector type must have a vector info")
        if type_info.elem_type is None:
            raise ValueError("Vector type must have an element type")
        encoded_type['element_type'] = _encode_type(analyze_type_info(type_info.elem_type))
        encoded_type['dimension'] = type_info.vector_info.dim

    elif type_info.kind in COLLECTION_TYPES:
        if type_info.elem_type is None:
            raise ValueError(f"{type_info.kind} type must have an element type")
        row_type_info = analyze_type_info(type_info.elem_type)
        if row_type_info.dataclass_type is None:
            raise ValueError(f"{type_info.kind} type must have a dataclass type")
        encoded_type['row'] = {
            'fields': _encode_fields_schema(row_type_info.dataclass_type),
        }

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
