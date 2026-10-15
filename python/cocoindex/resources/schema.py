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


class VectorSchema(_msgspec.Struct, tag=True):
    """Additional information for a vector column."""

    dtype: _np.dtype
    size: int

    def __post_init__(self) -> None:
        # Normalize the dtype (e.g. the `np.float32` scalar-type class, or a
        # `"float32"` string) to a `np.dtype` instance. The class form pickles
        # by reference to the `numpy.float32` global, which the restricted
        # unpickler used for persisted records refuses — breaking the second
        # run (issue #2535). Normalizing here also keeps the runtime value
        # aligned with the declared `np.dtype` field type.
        # Note: intentionally not `frozen=True` — msgspec forbids mutating a
        # frozen struct inside `__post_init__`, so normalization needs a
        # mutable struct; `__hash__` is defined explicitly to preserve
        # hashability with value semantics.
        self.dtype = _np.dtype(self.dtype)

    def __hash__(self) -> int:
        return hash((self.dtype.str, self.size))

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
