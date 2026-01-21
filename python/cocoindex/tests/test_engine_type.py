import dataclasses
import datetime
import uuid
from typing import Annotated, Any, Literal, NamedTuple

import numpy as np
from numpy.typing import NDArray

from cocoindex.typing import (
    TypeAttr,
    Vector,
    VectorInfo,
)
from cocoindex._internal.datatype import analyze_type_info
from cocoindex.engine_type import (
    decode_value_type,
    encode_enriched_type,
    encode_enriched_type_info,
    encode_value_type,
)


@dataclasses.dataclass
class SimpleDataclass:
    name: str
    value: int


@dataclasses.dataclass
class SimpleDataclassWithDescription:
    """This is a simple dataclass with a description."""

    name: str
    value: int


class SimpleNamedTuple(NamedTuple):
    name: str
    value: int


def test_encode_enriched_type_none() -> None:
    typ = None
    result = encode_enriched_type(typ)
    assert result is None


def test_encode_enriched_dataclass() -> None:
    typ = SimpleDataclass
    result = encode_enriched_type(typ)
    assert result == {
        "type": {
            "kind": "Struct",
            "description": "SimpleDataclass(name: str, value: int)",
            "fields": [
                {"name": "name", "type": {"kind": "Str"}},
                {"name": "value", "type": {"kind": "Int64"}},
            ],
        },
    }


def test_encode_enriched_dataclass_with_description() -> None:
    typ = SimpleDataclassWithDescription
    result = encode_enriched_type(typ)
    assert result == {
        "type": {
            "kind": "Struct",
            "description": "This is a simple dataclass with a description.",
            "fields": [
                {"name": "name", "type": {"kind": "Str"}},
                {"name": "value", "type": {"kind": "Int64"}},
            ],
        },
    }


def test_encode_named_tuple() -> None:
    typ = SimpleNamedTuple
    result = encode_enriched_type(typ)
    assert result == {
        "type": {
            "kind": "Struct",
            "description": "SimpleNamedTuple(name, value)",
            "fields": [
                {"name": "name", "type": {"kind": "Str"}},
                {"name": "value", "type": {"kind": "Int64"}},
            ],
        },
    }


def test_encode_enriched_type_vector() -> None:
    typ = NDArray[np.float32]
    result = encode_enriched_type(typ)
    assert result == {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float32"},
            "dimension": None,
        },
    }


def test_encode_enriched_type_ltable() -> None:
    typ = list[SimpleDataclass]
    result = encode_enriched_type(typ)
    assert result == {
        "type": {
            "kind": "LTable",
            "row": {
                "description": "SimpleDataclass(name: str, value: int)",
                "fields": [
                    {"name": "name", "type": {"kind": "Str"}},
                    {"name": "value", "type": {"kind": "Int64"}},
                ],
            },
        },
    }


def test_encode_enriched_type_with_attrs() -> None:
    typ = Annotated[str, TypeAttr("key", "value")]
    result = encode_enriched_type(typ)
    assert result == {
        "type": {"kind": "Str"},
        "attrs": {"key": "value"},
    }


def test_encode_enriched_type_nullable() -> None:
    typ = str | None
    result = encode_enriched_type(typ)
    assert result == {
        "type": {"kind": "Str"},
        "nullable": True,
    }


def test_encode_scalar_numpy_types_schema() -> None:
    for np_type, expected_kind in [
        (np.int64, "Int64"),
        (np.float32, "Float32"),
        (np.float64, "Float64"),
    ]:
        schema = encode_enriched_type(np_type)
        assert schema == {
            "type": {"kind": expected_kind},
        }, f"Expected kind {expected_kind} for {np_type}, got {schema}"


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
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
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
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
        assert reencoded == encoded["type"]


def test_struct_types_encode_decode() -> None:
    """Test encode/decode roundtrip for struct types."""
    test_cases = [
        SimpleDataclass,
        SimpleNamedTuple,
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
        assert reencoded == encoded["type"]


def test_table_types_encode_decode() -> None:
    """Test encode/decode roundtrip for table types."""
    test_cases = [
        list[SimpleDataclass],  # LTable
        dict[str, SimpleDataclass],  # KTable
    ]

    for typ in test_cases:
        encoded = encode_type_from_annotation(typ)
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
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
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
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
        decoded = decode_value_type(encoded["type"])
        reencoded = encode_value_type(decoded)
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
    decoded = decode_value_type(encoded["type"])
    reencoded = encode_value_type(decoded)
    assert reencoded == encoded["type"]
