import dataclasses
import datetime
import uuid
from collections.abc import Mapping, Sequence
from typing import Annotated, NamedTuple

import numpy as np
from numpy.typing import NDArray

from cocoindex.typing import (
    TypeAttr,
    TypeKind,
    VectorInfo,
)
from cocoindex._internal.datatype import (
    BasicType,
    MappingType,
    SequenceType,
    StructType,
    OtherType,
    DataTypeInfo,
    analyze_type_info,
)


@dataclasses.dataclass
class SimpleDataclass:
    name: str
    value: int


class SimpleNamedTuple(NamedTuple):
    name: str
    value: int


def test_ndarray_float32_no_dim() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.float32]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info is None
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_ndarray_float64_with_dim() -> None:
    from typing import get_args, get_origin

    typ = Annotated[NDArray[np.float64], VectorInfo(dim=128)]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info == VectorInfo(dim=128)
    assert result.variant.elem_type == np.float64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float64]


def test_ndarray_int64_no_dim() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.int64]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info is None
    assert result.variant.elem_type == np.int64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.int64]


def test_nullable_ndarray() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.float32] | None
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info is None
    assert result.variant.elem_type == np.float32
    assert result.nullable is True
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_scalar_numpy_types() -> None:
    for np_type, expected_kind in [
        (np.int64, "Int64"),
        (np.float32, "Float32"),
        (np.float64, "Float64"),
    ]:
        type_info = analyze_type_info(np_type)
        assert isinstance(type_info.variant, BasicType)
        assert type_info.variant.kind == expected_kind, (
            f"Expected {expected_kind} for {np_type}, got {type_info.variant.kind}"
        )
        assert type_info.core_type == np_type, (
            f"Expected {np_type}, got {type_info.core_type}"
        )


def test_list_of_primitives() -> None:
    typ = list[str]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=list[str],
        base_type=list,
        variant=SequenceType(elem_type=str, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_list_of_structs() -> None:
    typ = list[SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=list[SimpleDataclass],
        base_type=list,
        variant=SequenceType(elem_type=SimpleDataclass, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_sequence_of_int() -> None:
    typ = Sequence[int]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=Sequence[int],
        base_type=Sequence,
        variant=SequenceType(elem_type=int, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_list_with_vector_info() -> None:
    typ = Annotated[list[int], VectorInfo(dim=5)]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=list[int],
        base_type=list,
        variant=SequenceType(elem_type=int, vector_info=VectorInfo(dim=5)),
        attrs=None,
        nullable=False,
    )


def test_dict_str_int() -> None:
    typ = dict[str, int]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=dict[str, int],
        base_type=dict,
        variant=MappingType(key_type=str, value_type=int),
        attrs=None,
        nullable=False,
    )


def test_mapping_str_dataclass() -> None:
    typ = Mapping[str, SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=Mapping[str, SimpleDataclass],
        base_type=Mapping,
        variant=MappingType(key_type=str, value_type=SimpleDataclass),
        attrs=None,
        nullable=False,
    )


def test_dataclass() -> None:
    typ = SimpleDataclass
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=SimpleDataclass,
        base_type=SimpleDataclass,
        variant=StructType(struct_type=SimpleDataclass),
        attrs=None,
        nullable=False,
    )


def test_named_tuple() -> None:
    typ = SimpleNamedTuple
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=SimpleNamedTuple,
        base_type=SimpleNamedTuple,
        variant=StructType(struct_type=SimpleNamedTuple),
        attrs=None,
        nullable=False,
    )


def test_str() -> None:
    typ = str
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=str,
        base_type=str,
        variant=BasicType(kind="Str"),
        attrs=None,
        nullable=False,
    )


def test_bool() -> None:
    typ = bool
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=bool,
        base_type=bool,
        variant=BasicType(kind="Bool"),
        attrs=None,
        nullable=False,
    )


def test_bytes() -> None:
    typ = bytes
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=bytes,
        base_type=bytes,
        variant=BasicType(kind="Bytes"),
        attrs=None,
        nullable=False,
    )


def test_uuid() -> None:
    typ = uuid.UUID
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=uuid.UUID,
        base_type=uuid.UUID,
        variant=BasicType(kind="Uuid"),
        attrs=None,
        nullable=False,
    )


def test_date() -> None:
    typ = datetime.date
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=datetime.date,
        base_type=datetime.date,
        variant=BasicType(kind="Date"),
        attrs=None,
        nullable=False,
    )


def test_time() -> None:
    typ = datetime.time
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=datetime.time,
        base_type=datetime.time,
        variant=BasicType(kind="Time"),
        attrs=None,
        nullable=False,
    )


def test_timedelta() -> None:
    typ = datetime.timedelta
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=datetime.timedelta,
        base_type=datetime.timedelta,
        variant=BasicType(kind="TimeDelta"),
        attrs=None,
        nullable=False,
    )


def test_float() -> None:
    typ = float
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=float,
        base_type=float,
        variant=BasicType(kind="Float64"),
        attrs=None,
        nullable=False,
    )


def test_int() -> None:
    typ = int
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=int,
        base_type=int,
        variant=BasicType(kind="Int64"),
        attrs=None,
        nullable=False,
    )


def test_type_with_attributes() -> None:
    typ = Annotated[str, TypeAttr("key", "value")]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=str,
        base_type=str,
        variant=BasicType(kind="Str"),
        attrs={"key": "value"},
        nullable=False,
    )


def test_annotated_struct_with_type_kind() -> None:
    typ = Annotated[SimpleDataclass, TypeKind("Vector")]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, BasicType)
    assert result.variant.kind == "Vector"


def test_annotated_list_with_type_kind() -> None:
    typ = Annotated[list[int], TypeKind("Struct")]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, BasicType)
    assert result.variant.kind == "Struct"


def test_unknown_type() -> None:
    typ = set
    result = analyze_type_info(typ)
    assert isinstance(result.variant, OtherType)
