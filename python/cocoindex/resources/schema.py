"""
Schema-related helper types.

Currently this module contains helpers for connector schemas that need extra
out-of-band information beyond Python type annotations.
"""

from __future__ import annotations

import collections.abc as _collections_abc
import math as _math
import numbers as _numbers
import operator as _operator
import typing as _typing
import cocoindex as _coco
import msgspec as _msgspec
import numpy as _np


class SparseVector(_msgspec.Struct, frozen=True):
    """Canonical sparse vector value.

    Indices are 0-based, sorted ascending, and unique. Values are stored in a
    parallel tuple so the representation is deterministic and serializable.
    """

    indices: tuple[int, ...]
    values: tuple[float, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.indices, tuple):
            raise TypeError("indices must be a tuple of integers")
        if not isinstance(self.values, tuple):
            raise TypeError("values must be a tuple of floats")
        if len(self.indices) != len(self.values):
            raise ValueError("indices and values must have the same length")
        previous_index = -1
        for index, value in zip(self.indices, self.values, strict=True):
            if type(index) is not int:
                raise TypeError("indices must contain only integers")
            if type(value) is not float:
                raise TypeError("values must contain only floats")
            if index < 0:
                raise ValueError("indices must be non-negative")
            if index <= previous_index:
                raise ValueError("indices must be sorted ascending and unique")
            if not _math.isfinite(value):
                raise ValueError("values must be finite")
            previous_index = index

    @classmethod
    def from_arrays(
        cls,
        indices: _typing.Iterable[_typing.SupportsIndex],
        values: _typing.Iterable[_typing.SupportsFloat],
    ) -> SparseVector:
        """Coerce parallel numeric arrays and sort them by ascending index."""
        normalized_indices = _normalize_sparse_indices(indices)
        normalized_values = _normalize_sparse_values(values)
        if len(normalized_indices) != len(normalized_values):
            raise ValueError("indices and values must have the same length")
        items = sorted(zip(normalized_indices, normalized_values, strict=True))
        return cls(
            indices=tuple(index for index, _ in items),
            values=tuple(value for _, value in items),
        )

    @classmethod
    def from_mapping(cls, m: _typing.Mapping[int, float]) -> SparseVector:
        """Coerce and sort an index-to-value mapping."""
        if not isinstance(m, _collections_abc.Mapping):
            raise TypeError("expected a Mapping[int, float]")
        return cls.from_arrays(m.keys(), m.values())


def _normalize_sparse_indices(indices: object) -> tuple[int, ...]:
    try:
        iterator = iter(indices)  # type: ignore[call-overload]
    except TypeError as e:
        raise TypeError("indices must be an iterable of integers") from e

    normalized: list[int] = []
    for index in iterator:
        if isinstance(index, (bool, _np.bool_)):
            raise TypeError("sparse vector indices must be integers, not bool")
        try:
            normalized.append(_operator.index(index))
        except TypeError as e:
            raise TypeError(
                f"sparse vector indices must be integers, got {index!r}"
            ) from e
    return tuple(normalized)


def _normalize_sparse_values(values: object) -> tuple[float, ...]:
    try:
        iterator = iter(values)  # type: ignore[call-overload]
    except TypeError as e:
        raise TypeError("values must be an iterable of real numbers") from e

    normalized: list[float] = []
    for value in iterator:
        value_type = type(value)
        if value_type is float:
            normalized.append(value)
            continue
        if value_type is int:
            normalized.append(float(value))
            continue
        if isinstance(value, (bool, _np.bool_)) or not isinstance(value, _numbers.Real):
            raise TypeError(f"sparse vector values must be real numbers, got {value!r}")
        normalized.append(float(value))
    return tuple(normalized)


def as_sparse_vector(
    v: SparseVector | _typing.Mapping[int, float],
) -> SparseVector:
    """Normalize a sparse vector value to the canonical representation."""
    if isinstance(v, SparseVector):
        return v
    if not isinstance(v, _collections_abc.Mapping):
        raise TypeError(
            f"expected SparseVector or Mapping[int, float], got {type(v).__name__}"
        )
    return SparseVector.from_mapping(v)


@_typing.runtime_checkable
class SparseVectorSchemaProvider(_typing.Protocol):
    """Provider of additional information for a sparse vector column."""

    def __coco_sparse_vector_schema__(
        self,
    ) -> _typing.Awaitable[SparseVectorSchema]: ...


class SparseVectorSchema(_msgspec.Struct, frozen=True, tag=True):
    """Additional information for a sparse vector column."""

    size: int | None = None

    def __post_init__(self) -> None:
        if self.size is not None:
            if isinstance(self.size, (bool, _np.bool_)):
                raise TypeError("sparse vector size must be an integer or None")
            try:
                size = _operator.index(self.size)
            except TypeError as e:
                raise TypeError("sparse vector size must be an integer or None") from e
            if size <= 0:
                raise ValueError("sparse vector size must be positive")
            _msgspec.structs.force_setattr(self, "size", size)

    async def __coco_sparse_vector_schema__(self) -> SparseVectorSchema:
        return self


async def get_sparse_vector_schema(obj: object) -> SparseVectorSchema | None:
    """Get the sparse vector schema from an object, if it provides one."""
    if isinstance(obj, _coco.ContextKey):
        obj = _coco.use_context(obj)
    if isinstance(obj, SparseVectorSchemaProvider):
        return await obj.__coco_sparse_vector_schema__()
    return None


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
    if isinstance(obj, _coco.ContextKey):
        obj = _coco.use_context(obj)
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
    if isinstance(obj, _coco.ContextKey):
        obj = _coco.use_context(obj)
    if isinstance(obj, MultiVectorSchemaProvider):
        return await obj.__coco_multi_vector_schema__()
    return None


__all__ = [
    "MultiVectorSchema",
    "MultiVectorSchemaProvider",
    "SparseVector",
    "SparseVectorSchema",
    "SparseVectorSchemaProvider",
    "VectorSchema",
    "VectorSchemaProvider",
    "as_sparse_vector",
    "get_multi_vector_schema",
    "get_sparse_vector_schema",
    "get_vector_schema",
]
