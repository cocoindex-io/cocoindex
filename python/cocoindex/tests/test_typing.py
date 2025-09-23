import dataclasses
import datetime
import uuid
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal, NamedTuple, get_args, get_origin

import numpy as np
from numpy.typing import NDArray

from cocoindex.typing import (
    AnalyzedBasicType,
    AnalyzedDictType,
    AnalyzedListType,
    AnalyzedStructType,
    AnalyzedUnknownType,
    AnalyzedTypeInfo,
    TypeAttr,
    TypeKind,
    Vector,
    VectorInfo,
    analyze_type_info,
    decode_engine_value_type,
    encode_enriched_type,
    encode_enriched_type_info,
    encode_engine_value_type,
)


@dataclasses.dataclass
class SimpleDataclass:
    name: str
    value: int


class SimpleNamedTuple(NamedTuple):
    name: str
    value: int


def test_ndarray_float32_no_dim() -> None:
    typ = NDArray[np.float32]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.vector_info is None
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_vector_float32_no_dim() -> None:
    typ = Vector[np.float32]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.vector_info == VectorInfo(dim=None)
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_ndarray_float64_with_dim() -> None:
    typ = Annotated[NDArray[np.float64], VectorInfo(dim=128)]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.vector_info == VectorInfo(dim=128)
    assert result.variant.elem_type == np.float64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float64]


def test_vector_float32_with_dim() -> None:
    typ = Vector[np.float32, Literal[384]]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.vector_info == VectorInfo(dim=384)
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_ndarray_int64_no_dim() -> None:
    typ = NDArray[np.int64]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.vector_info is None
    assert result.variant.elem_type == np.int64
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.int64]


def test_nullable_ndarray() -> None:
    typ = NDArray[np.float32] | None
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
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
        assert isinstance(type_info.variant, AnalyzedBasicType)
        assert type_info.variant.kind == expected_kind, (
            f"Expected {expected_kind} for {np_type}, got {type_info.variant.kind}"
        )
        assert type_info.core_type == np_type, (
            f"Expected {np_type}, got {type_info.core_type}"
        )


def test_vector_str() -> None:
    typ = Vector[str]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.elem_type is str
    assert result.variant.vector_info == VectorInfo(dim=None)


def test_non_numpy_vector() -> None:
    typ = Vector[float, Literal[3]]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedListType)
    assert result.variant.elem_type is float
    assert result.variant.vector_info == VectorInfo(dim=3)


def test_list_of_primitives() -> None:
    typ = list[str]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=list[str],
        base_type=list,
        variant=AnalyzedListType(elem_type=str, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_list_of_structs() -> None:
    typ = list[SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=list[SimpleDataclass],
        base_type=list,
        variant=AnalyzedListType(elem_type=SimpleDataclass, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_sequence_of_int() -> None:
    typ = Sequence[int]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=Sequence[int],
        base_type=Sequence,
        variant=AnalyzedListType(elem_type=int, vector_info=None),
        attrs=None,
        nullable=False,
    )


def test_list_with_vector_info() -> None:
    typ = Annotated[list[int], VectorInfo(dim=5)]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=list[int],
        base_type=list,
        variant=AnalyzedListType(elem_type=int, vector_info=VectorInfo(dim=5)),
        attrs=None,
        nullable=False,
    )


def test_dict_str_int() -> None:
    typ = dict[str, int]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=dict[str, int],
        base_type=dict,
        variant=AnalyzedDictType(key_type=str, value_type=int),
        attrs=None,
        nullable=False,
    )


def test_mapping_str_dataclass() -> None:
    typ = Mapping[str, SimpleDataclass]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=Mapping[str, SimpleDataclass],
        base_type=Mapping,
        variant=AnalyzedDictType(key_type=str, value_type=SimpleDataclass),
        attrs=None,
        nullable=False,
    )


def test_dataclass() -> None:
    typ = SimpleDataclass
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=SimpleDataclass,
        base_type=SimpleDataclass,
        variant=AnalyzedStructType(struct_type=SimpleDataclass),
        attrs=None,
        nullable=False,
    )


def test_named_tuple() -> None:
    typ = SimpleNamedTuple
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=SimpleNamedTuple,
        base_type=SimpleNamedTuple,
        variant=AnalyzedStructType(struct_type=SimpleNamedTuple),
        attrs=None,
        nullable=False,
    )


def test_str() -> None:
    typ = str
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=str,
        base_type=str,
        variant=AnalyzedBasicType(kind="Str"),
        attrs=None,
        nullable=False,
    )


def test_bool() -> None:
    typ = bool
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=bool,
        base_type=bool,
        variant=AnalyzedBasicType(kind="Bool"),
        attrs=None,
        nullable=False,
    )


def test_bytes() -> None:
    typ = bytes
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=bytes,
        base_type=bytes,
        variant=AnalyzedBasicType(kind="Bytes"),
        attrs=None,
        nullable=False,
    )


def test_uuid() -> None:
    typ = uuid.UUID
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=uuid.UUID,
        base_type=uuid.UUID,
        variant=AnalyzedBasicType(kind="Uuid"),
        attrs=None,
        nullable=False,
    )


def test_date() -> None:
    typ = datetime.date
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=datetime.date,
        base_type=datetime.date,
        variant=AnalyzedBasicType(kind="Date"),
        attrs=None,
        nullable=False,
    )


def test_time() -> None:
    typ = datetime.time
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=datetime.time,
        base_type=datetime.time,
        variant=AnalyzedBasicType(kind="Time"),
        attrs=None,
        nullable=False,
    )


def test_timedelta() -> None:
    typ = datetime.timedelta
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=datetime.timedelta,
        base_type=datetime.timedelta,
        variant=AnalyzedBasicType(kind="TimeDelta"),
        attrs=None,
        nullable=False,
    )


def test_float() -> None:
    typ = float
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=float,
        base_type=float,
        variant=AnalyzedBasicType(kind="Float64"),
        attrs=None,
        nullable=False,
    )


def test_int() -> None:
    typ = int
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=int,
        base_type=int,
        variant=AnalyzedBasicType(kind="Int64"),
        attrs=None,
        nullable=False,
    )


def test_type_with_attributes() -> None:
    typ = Annotated[str, TypeAttr("key", "value")]
    result = analyze_type_info(typ)
    assert result == AnalyzedTypeInfo(
        core_type=str,
        base_type=str,
        variant=AnalyzedBasicType(kind="Str"),
        attrs={"key": "value"},
        nullable=False,
    )


def test_encode_enriched_type_none() -> None:
    typ = None
    result = encode_enriched_type(typ)
    assert result is None


def test_encode_enriched_type_struct() -> None:
    typ = SimpleDataclass
    result = encode_enriched_type(typ)
    assert result["type"]["kind"] == "Struct"
    assert len(result["type"]["fields"]) == 2
    assert result["type"]["fields"][0]["name"] == "name"
    assert result["type"]["fields"][0]["type"]["kind"] == "Str"
    assert result["type"]["fields"][1]["name"] == "value"
    assert result["type"]["fields"][1]["type"]["kind"] == "Int64"


def test_encode_enriched_type_vector() -> None:
    typ = NDArray[np.float32]
    result = encode_enriched_type(typ)
    assert result["type"]["kind"] == "Vector"
    assert result["type"]["element_type"]["kind"] == "Float32"
    assert result["type"]["dimension"] is None


def test_encode_enriched_type_ltable() -> None:
    typ = list[SimpleDataclass]
    result = encode_enriched_type(typ)
    assert result["type"]["kind"] == "LTable"
    assert "fields" in result["type"]["row"]
    assert len(result["type"]["row"]["fields"]) == 2


def test_encode_enriched_type_with_attrs() -> None:
    typ = Annotated[str, TypeAttr("key", "value")]
    result = encode_enriched_type(typ)
    assert result["type"]["kind"] == "Str"
    assert result["attrs"] == {"key": "value"}


def test_encode_enriched_type_nullable() -> None:
    typ = str | None
    result = encode_enriched_type(typ)
    assert result["type"]["kind"] == "Str"
    assert result["nullable"] is True


def test_encode_scalar_numpy_types_schema() -> None:
    for np_type, expected_kind in [
        (np.int64, "Int64"),
        (np.float32, "Float32"),
        (np.float64, "Float64"),
    ]:
        schema = encode_enriched_type(np_type)
        assert schema["type"]["kind"] == expected_kind, (
            f"Expected {expected_kind} for {np_type}, got {schema['type']['kind']}"
        )
        assert not schema.get("nullable", False)


def test_annotated_struct_with_type_kind() -> None:
    typ = Annotated[SimpleDataclass, TypeKind("Vector")]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedBasicType)
    assert result.variant.kind == "Vector"


def test_annotated_list_with_type_kind() -> None:
    typ = Annotated[list[int], TypeKind("Struct")]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedBasicType)
    assert result.variant.kind == "Struct"


def test_unknown_type() -> None:
    typ = set
    result = analyze_type_info(typ)
    assert isinstance(result.variant, AnalyzedUnknownType)


# ========================= Encode/Decode Tests =========================


def encode_type_from_annotation(t: Any) -> dict[str, Any]:
    """Helper function to encode a Python type annotation to its dictionary representation."""
    return encode_enriched_type_info(analyze_type_info(t))


def test_basic_types_encode_decode() -> None:
    """Test encode/decode roundtrip for basic Python types."""
    test_cases = [
        str,
        int,
        float,
        bool,
        bytes,
        uuid.UUID,
        datetime.date,
        datetime.time,
        datetime.datetime,
        datetime.timedelta,
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_vector_types_encode_decode() -> None:
    """Test encode/decode roundtrip for vector types."""
    test_cases = [
        NDArray[np.float32],
        NDArray[np.float64],
        NDArray[np.int64],
        Vector[np.float32],
        Vector[np.float32, Literal[128]],
        Vector[str],
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_struct_types_encode_decode() -> None:
    """Test encode/decode roundtrip for struct types."""
    test_cases = [
        SimpleDataclass,
        SimpleNamedTuple,
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_table_types_encode_decode() -> None:
    """Test encode/decode roundtrip for table types."""
    test_cases = [
        list[SimpleDataclass],  # LTable
        dict[str, SimpleDataclass],  # KTable
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_nullable_types_encode_decode() -> None:
    """Test encode/decode roundtrip for nullable types."""
    test_cases = [
        str | None,
        int | None,
        NDArray[np.float32] | None,
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_annotated_types_encode_decode() -> None:
    """Test encode/decode roundtrip for annotated types."""
    test_cases = [
        Annotated[str, TypeAttr("key", "value")],
        Annotated[NDArray[np.float32], VectorInfo(dim=256)],
        Annotated[list[int], VectorInfo(dim=10)],
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_engine_value_type(encoded["type"])
        reencoded = encode_engine_value_type(decoded)
        assert reencoded == encoded["type"]


def test_complex_nested_encode_decode() -> None:
    """Test complex nested structure encode/decode roundtrip."""

    # Create a complex nested structure using Python type annotations
    @dataclasses.dataclass
    class ComplexStruct:
        embedding: NDArray[np.float32]
        metadata: str | None
        score: Annotated[float, TypeAttr("indexed", True)]

    encoded = encode_type_from_annotation(ComplexStruct)
    decoded = decode_engine_value_type(encoded["type"])
    reencoded = encode_engine_value_type(decoded)
    assert reencoded == encoded["type"]
