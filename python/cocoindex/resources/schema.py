"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import typing as _typing
import dataclasses as _dataclasses

if _typing.TYPE_CHECKING:
    import numpy as _np


@_typing.runtime_checkable
class VectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_vector_schema__(self) -> VectorSchema: ...


@_dataclasses.dataclass(slots=True, frozen=True)
class VectorSchema:
    """Additional information for a vector column."""

    dtype: _np.dtype
    size: int

    def __coco_vector_schema__(self) -> VectorSchema:
        return self


@_typing.runtime_checkable
class MultiVectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_multi_vector_schema__(self) -> MultiVectorSchema: ...


@_dataclasses.dataclass(slots=True, frozen=True)
class MultiVectorSchema:
    """Additional information for a vector column."""

    vector_schema: VectorSchema

    def __coco_multi_vector_schema__(self) -> MultiVectorSchema:
        return self


__all__ = [
    "MultiVectorSchema",
    "MultiVectorSchemaProvider",
    "VectorSchema",
    "VectorSchemaProvider",
]
