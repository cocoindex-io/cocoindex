"""Tests for cocoindex.typing module (Vector type alias, VectorInfo, TypeKind, TypeAttr)."""

from typing import Literal, get_args, get_origin

import numpy as np

from cocoindex.typing import (
    Vector,
    VectorInfo,
)
from cocoindex._internal.datatype import (
    SequenceType,
    analyze_type_info,
)


def test_vector_float32_no_dim() -> None:
    typ = Vector[np.float32]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info == VectorInfo(dim=None)
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_vector_float32_with_dim() -> None:
    typ = Vector[np.float32, Literal[384]]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.vector_info == VectorInfo(dim=384)
    assert result.variant.elem_type == np.float32
    assert result.nullable is False
    assert get_origin(result.core_type) == np.ndarray
    assert get_args(result.core_type)[1] == np.dtype[np.float32]


def test_vector_str() -> None:
    typ = Vector[str]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.elem_type is str
    assert result.variant.vector_info == VectorInfo(dim=None)


def test_non_numpy_vector() -> None:
    typ = Vector[float, Literal[3]]
    result = analyze_type_info(typ)
    assert isinstance(result.variant, SequenceType)
    assert result.variant.elem_type is float
    assert result.variant.vector_info == VectorInfo(dim=3)
