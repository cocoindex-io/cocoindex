"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import typing as _typing
import dataclasses as _dataclasses
import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey

if _typing.TYPE_CHECKING:
    import numpy as _np


@_typing.runtime_checkable
class VectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_vector_schema__(self) -> _typing.Awaitable[VectorSchema]: ...


@_dataclasses.dataclass(slots=True, frozen=True)
class VectorSchema:
    """Additional information for a vector column."""

    dtype: _np.dtype
    size: int

    async def __coco_vector_schema__(self) -> VectorSchema:
        return self


async def get_vector_schema(obj: object) -> VectorSchema | None:
    """Helper function to get the vector schema from an object, if it provides one."""
    if isinstance(obj, coco.ContextKey):
        obj = coco.use_context(obj)
    if isinstance(obj, VectorSchemaProvider):
        return await obj.__coco_vector_schema__()
    return None


@_typing.runtime_checkable
class MultiVectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_multi_vector_schema__(self) -> _typing.Awaitable[MultiVectorSchema]: ...


@_dataclasses.dataclass(slots=True, frozen=True)
class MultiVectorSchema:
    """Additional information for a vector column."""

    vector_schema: VectorSchema

    async def __coco_multi_vector_schema__(self) -> MultiVectorSchema:
        return self


async def get_multi_vector_schema(obj: object) -> MultiVectorSchema | None:
    """Helper function to get the multi-vector schema from an object, if it provides one."""
    if isinstance(obj, coco.ContextKey):
        obj = coco.use_context(obj)
    if isinstance(obj, MultiVectorSchemaProvider):
        return await obj.__coco_multi_vector_schema__()
    return None


__all__ = [
    "MultiVectorSchema",
    "MultiVectorSchemaProvider",
    "VectorSchema",
    "VectorSchemaProvider",
    "get_multi_vector_schema",
    "get_vector_schema",
]
