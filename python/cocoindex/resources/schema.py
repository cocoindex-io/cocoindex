"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import typing as _typing
import cocoindex as coco
import msgspec as _msgspec
import numpy as _np


@_typing.runtime_checkable
class VectorSchemaProvider(_typing.Protocol):
    """Additional information for a vector column."""

    def __coco_vector_schema__(self) -> _typing.Awaitable[VectorSchema]: ...


class VectorSchema(_msgspec.Struct, frozen=True, tag=True):
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


class MultiVectorSchema(_msgspec.Struct, frozen=True, tag=True):
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
