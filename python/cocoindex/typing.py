import collections
import dataclasses
import datetime
import inspect
import types
import typing
import uuid
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    NamedTuple,
    Protocol,
    TypeVar,
    overload,
)

import numpy as np
from numpy.typing import NDArray


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

Float32 = Annotated[float, TypeKind("Float32")]
Float64 = Annotated[float, TypeKind("Float64")]
Range = Annotated[tuple[int, int], TypeKind("Range")]
Json = Annotated[Any, TypeKind("Json")]
LocalDateTime = Annotated[datetime.datetime, TypeKind("LocalDateTime")]
OffsetDateTime = Annotated[datetime.datetime, TypeKind("OffsetDateTime")]

if TYPE_CHECKING:
    T_co = TypeVar("T_co", covariant=True)
    Dim_co = TypeVar("Dim_co", bound=int | None, covariant=True, default=None)

    class Vector(Protocol, Generic[T_co, Dim_co]):
        """Vector[T, Dim] is a special typing alias for an NDArray[T] with optional dimension info"""

        def __getitem__(self, index: int) -> T_co: ...
        def __len__(self) -> int: ...

else:

    class Vector:  # type: ignore[unreachable]
        """A special typing alias for an NDArray[T] with optional dimension info"""

        def __class_getitem__(self, params):
            if not isinstance(params, tuple):
                # No dimension provided, e.g., Vector[np.float32]
                dtype = params
                # Use NDArray for supported numeric dtypes, else list
                if dtype in DtypeRegistry._DTYPE_TO_KIND:
                    return Annotated[NDArray[dtype], VectorInfo(dim=None)]
                return Annotated[list[dtype], VectorInfo(dim=None)]
            else:
                # Element type and dimension provided, e.g., Vector[np.float32, Literal[3]]
                dtype, dim_literal = params
                # Extract the literal value
                dim_val = (
                    typing.get_args(dim_literal)[0]
                    if typing.get_origin(dim_literal) is Literal
                    else None
                )
                if dtype in DtypeRegistry._DTYPE_TO_KIND:
                    return Annotated[NDArray[dtype], VectorInfo(dim=dim_val)]
                return Annotated[list[dtype], VectorInfo(dim=dim_val)]


TABLE_TYPES: tuple[str, str] = ("KTable", "LTable")
KEY_FIELD_NAME: str = "_key"

ElementType = type | tuple[type, type] | Annotated[Any, TypeKind]


def extract_ndarray_scalar_dtype(ndarray_type: Any) -> Any:
    args = typing.get_args(ndarray_type)
    _, dtype_spec = args
    dtype_args = typing.get_args(dtype_spec)
    if not dtype_args:
        raise ValueError(f"Invalid dtype specification: {dtype_spec}")
    return dtype_args[0]


def is_numpy_number_type(t: type) -> bool:
    return isinstance(t, type) and issubclass(t, np.number)


def is_namedtuple_type(t: type) -> bool:
    return isinstance(t, type) and issubclass(t, tuple) and hasattr(t, "_fields")


def _is_struct_type(t: ElementType | None) -> bool:
    return isinstance(t, type) and (
        dataclasses.is_dataclass(t) or is_namedtuple_type(t)
    )


class DtypeRegistry:
    """
    Registry for NumPy dtypes used in CocoIndex.
    Maps NumPy dtypes to their CocoIndex type kind.
    """

    _DTYPE_TO_KIND: dict[ElementType, str] = {
        np.float32: "Float32",
        np.float64: "Float64",
        np.int64: "Int64",
    }

    @classmethod
    def validate_dtype_and_get_kind(cls, dtype: ElementType) -> str:
        """
        Validate that the given dtype is supported, and get its CocoIndex kind by dtype.
        """
        if dtype is Any:
            raise TypeError(
                "NDArray for Vector must use a concrete numpy dtype, got `Any`."
            )
        kind = cls._DTYPE_TO_KIND.get(dtype)
        if kind is None:
            raise ValueError(
                f"Unsupported NumPy dtype in NDArray: {dtype}. "
                f"Supported dtypes: {cls._DTYPE_TO_KIND.keys()}"
            )
        return kind


@dataclasses.dataclass
class AnalyzedTypeInfo:
    """
    Analyzed info of a Python type.
    """

    kind: str
    core_type: Any
    vector_info: VectorInfo | None  # For Vector
    elem_type: ElementType | None  # For Vector and Table

    key_type: type | None  # For element of KTable
    struct_type: type | None  # For Struct, a dataclass or namedtuple
    np_number_type: (
        type | None
    )  # NumPy dtype for the element type, if represented by numpy.ndarray or a NumPy scalar

    attrs: dict[str, Any] | None
    nullable: bool = False
    union_variant_types: typing.List[ElementType] | None = None  # For Union


def analyze_type_info(t: Any) -> AnalyzedTypeInfo:
    """
    Analyze a Python type and return the analyzed info.
    """
    if isinstance(t, tuple) and len(t) == 2:
        kt, vt = t
        result = analyze_type_info(vt)
        result.key_type = kt
        return result

    annotations: tuple[Annotation, ...] = ()
    base_type = None
    nullable = False
    while True:
        base_type = typing.get_origin(t)
        if base_type is Annotated:
            annotations = t.__metadata__
            t = t.__origin__
        else:
            break

    attrs: dict[str, Any] | None = None
    vector_info: VectorInfo | None = None
    kind: str | None = None
    for attr in annotations:
        if isinstance(attr, TypeAttr):
            if attrs is None:
                attrs = dict()
            attrs[attr.key] = attr.value
        elif isinstance(attr, VectorInfo):
            vector_info = attr
        elif isinstance(attr, TypeKind):
            kind = attr.kind

    struct_type: type | None = None
    elem_type: ElementType | None = None
    union_variant_types: typing.List[ElementType] | None = None
    key_type: type | None = None
    np_number_type: type | None = None
    if _is_struct_type(t):
        struct_type = t

        if kind is None:
            kind = "Struct"
        elif kind != "Struct":
            raise ValueError(f"Unexpected type kind for struct: {kind}")
    elif is_numpy_number_type(t):
        np_number_type = t
        kind = DtypeRegistry.validate_dtype_and_get_kind(t)
    elif base_type is collections.abc.Sequence or base_type is list:
        args = typing.get_args(t)
        elem_type = args[0]

        if kind is None:
            if _is_struct_type(elem_type):
                kind = "LTable"
                if vector_info is not None:
                    raise ValueError(
                        "Vector element must be a simple type, not a struct"
                    )
            else:
                kind = "Vector"
                if vector_info is None:
                    vector_info = VectorInfo(dim=None)
        elif not (kind == "Vector" or kind in TABLE_TYPES):
            raise ValueError(f"Unexpected type kind for list: {kind}")
    elif base_type is np.ndarray:
        kind = "Vector"
        np_number_type = t
        elem_type = extract_ndarray_scalar_dtype(np_number_type)
        _ = DtypeRegistry.validate_dtype_and_get_kind(elem_type)
        vector_info = VectorInfo(dim=None) if vector_info is None else vector_info

    elif base_type is collections.abc.Mapping or base_type is dict:
        args = typing.get_args(t)
        elem_type = (args[0], args[1])
        kind = "KTable"
    elif base_type is types.UnionType:
        possible_types = typing.get_args(t)
        non_none_types = [
            arg for arg in possible_types if arg not in (None, types.NoneType)
        ]

        if len(non_none_types) == 0:
            return analyze_type_info(None)

        nullable = len(non_none_types) < len(possible_types)

        if len(non_none_types) == 1:
            result = analyze_type_info(non_none_types[0])
            result.nullable = nullable
            return result

        kind = "Union"
        union_variant_types = non_none_types
    elif kind is None:
        if t is bytes:
            kind = "Bytes"
        elif t is str:
            kind = "Str"
        elif t is bool:
            kind = "Bool"
        elif t is int:
            kind = "Int64"
        elif t is float:
            kind = "Float64"
        elif t is uuid.UUID:
            kind = "Uuid"
        elif t is datetime.date:
            kind = "Date"
        elif t is datetime.time:
            kind = "Time"
        elif t is datetime.datetime:
            kind = "OffsetDateTime"
        elif t is datetime.timedelta:
            kind = "TimeDelta"
        else:
            raise ValueError(f"type unsupported yet: {t}")

    return AnalyzedTypeInfo(
        kind=kind,
        core_type=t,
        vector_info=vector_info,
        elem_type=elem_type,
        union_variant_types=union_variant_types,
        key_type=key_type,
        struct_type=struct_type,
        np_number_type=np_number_type,
        attrs=attrs,
        nullable=nullable,
    )


def _encode_fields_schema(
    struct_type: type, key_type: type | None = None
) -> list[dict[str, Any]]:
    result = []

    def add_field(name: str, t: Any) -> None:
        try:
            type_info = encode_enriched_type_info(analyze_type_info(t))
        except ValueError as e:
            e.add_note(
                f"Failed to encode annotation for field - "
                f"{struct_type.__name__}.{name}: {t}"
            )
            raise
        type_info["name"] = name
        result.append(type_info)

    if key_type is not None:
        add_field(KEY_FIELD_NAME, key_type)

    if dataclasses.is_dataclass(struct_type):
        for field in dataclasses.fields(struct_type):
            add_field(field.name, field.type)
    elif is_namedtuple_type(struct_type):
        for name, field_type in struct_type.__annotations__.items():
            add_field(name, field_type)

    return result


def _encode_type(type_info: AnalyzedTypeInfo) -> dict[str, Any]:
    encoded_type: dict[str, Any] = {"kind": type_info.kind}

    if type_info.kind == "Struct":
        if type_info.struct_type is None:
            raise ValueError("Struct type must have a dataclass or namedtuple type")
        encoded_type["fields"] = _encode_fields_schema(
            type_info.struct_type, type_info.key_type
        )
        if doc := inspect.getdoc(type_info.struct_type):
            encoded_type["description"] = doc

    elif type_info.kind == "Vector":
        if type_info.vector_info is None:
            raise ValueError("Vector type must have a vector info")
        if type_info.elem_type is None:
            raise ValueError("Vector type must have an element type")
        elem_type_info = analyze_type_info(type_info.elem_type)
        encoded_type["element_type"] = _encode_type(elem_type_info)
        encoded_type["dimension"] = type_info.vector_info.dim

    elif type_info.kind == "Union":
        if type_info.union_variant_types is None:
            raise ValueError("Union type must have a variant type list")
        encoded_type["types"] = [
            _encode_type(analyze_type_info(typ))
            for typ in type_info.union_variant_types
        ]

    elif type_info.kind in TABLE_TYPES:
        if type_info.elem_type is None:
            raise ValueError(f"{type_info.kind} type must have an element type")
        row_type_info = analyze_type_info(type_info.elem_type)
        encoded_type["row"] = _encode_type(row_type_info)

    return encoded_type


def encode_enriched_type_info(enriched_type_info: AnalyzedTypeInfo) -> dict[str, Any]:
    """
    Encode an enriched type info to a CocoIndex engine's type representation
    """
    encoded: dict[str, Any] = {"type": _encode_type(enriched_type_info)}

    if enriched_type_info.attrs is not None:
        encoded["attrs"] = enriched_type_info.attrs

    if enriched_type_info.nullable:
        encoded["nullable"] = True

    return encoded


@overload
def encode_enriched_type(t: None) -> None: ...


@overload
def encode_enriched_type(t: Any) -> dict[str, Any]: ...


def encode_enriched_type(t: Any) -> dict[str, Any] | None:
    """
    Convert a Python type to a CocoIndex engine's type representation
    """
    if t is None:
        return None

    return encode_enriched_type_info(analyze_type_info(t))


def resolve_forward_ref(t: Any) -> Any:
    if isinstance(t, str):
        return eval(t)  # pylint: disable=eval-used
    return t
