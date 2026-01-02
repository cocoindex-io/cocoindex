import dataclasses
from collections.abc import Mapping, Sequence
from typing import Annotated, NamedTuple

import numpy as np
from numpy.typing import NDArray

from cocoindex._internal.datatype import (
    MappingType,
    SequenceType,
    StructType,
    LeafType,
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
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_ndarray_float64_with_dim() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.float64]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.elem_type == np.float64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float64]


def test_ndarray_int64_no_dim() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.int64]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.elem_type == np.int64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.int64]


def test_nullable_ndarray() -> None:
    from typing import get_args, get_origin

    typ = NDArray[np.float32] | None
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
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
        assert isinstance(type_info.variant, LeafType)
        assert type_info.core_type == np_type, (
            f"Expected {np_type}, got {type_info.core_type}"
        )


def test_list_of_primitives() -> None:
    typ = list[str]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=list[str],
        base_type=list,
        variant=SequenceType(elem_type=str),
        annotations=(),
        nullable=False,
    )


def test_list_of_structs() -> None:
    typ = list[SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=list[SimpleDataclass],
        base_type=list,
        variant=SequenceType(elem_type=SimpleDataclass),
        annotations=(),
        nullable=False,
    )


def test_sequence_of_int() -> None:
    typ = Sequence[int]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=Sequence[int],
        base_type=Sequence,
        variant=SequenceType(elem_type=int),
        annotations=(),
        nullable=False,
    )


def test_dict_str_int() -> None:
    typ = dict[str, int]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=dict[str, int],
        base_type=dict,
        variant=MappingType(key_type=str, value_type=int),
        annotations=(),
        nullable=False,
    )


def test_mapping_str_dataclass() -> None:
    typ = Mapping[str, SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=Mapping[str, SimpleDataclass],
        base_type=Mapping,
        variant=MappingType(key_type=str, value_type=SimpleDataclass),
        annotations=(),
        nullable=False,
    )


def test_dataclass() -> None:
    typ = SimpleDataclass
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=SimpleDataclass,
        base_type=SimpleDataclass,
        variant=StructType(struct_type=SimpleDataclass),
        annotations=(),
        nullable=False,
    )


def test_named_tuple() -> None:
    typ = SimpleNamedTuple
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=SimpleNamedTuple,
        base_type=SimpleNamedTuple,
        variant=StructType(struct_type=SimpleNamedTuple),
        annotations=(),
        nullable=False,
    )


def test_str() -> None:
    typ = str
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=str,
        base_type=str,
        variant=LeafType(),
        annotations=(),
        nullable=False,
    )


def test_bool() -> None:
    typ = bool
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=bool,
        base_type=bool,
        variant=LeafType(),
        annotations=(),
        nullable=False,
    )


def test_bytes() -> None:
    typ = bytes
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=bytes,
        base_type=bytes,
        variant=LeafType(),
        annotations=(),
        nullable=False,
    )


def test_float() -> None:
    typ = float
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=float,
        base_type=float,
        variant=LeafType(),
        annotations=(),
        nullable=False,
    )


def test_int() -> None:
    typ = int
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=int,
        base_type=int,
        variant=LeafType(),
        annotations=(),
        nullable=False,
    )


def test_type_with_attributes() -> None:
    typ = Annotated[Annotated[str, "Annotation1"], "Annotation2"]
    result = analyze_type_info(typ)
    assert result == DataTypeInfo(
        core_type=str,
        base_type=str,
        variant=LeafType(),
        annotations=("Annotation1", "Annotation2"),
        nullable=False,
    )
