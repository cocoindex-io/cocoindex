from __future__ import annotations

import numpy as np
import pytest

import cocoindex as coco
from cocoindex.resources.schema import (
    SparseVector,
    SparseVectorSchema,
    as_sparse_vector,
    get_sparse_vector_schema,
)

from tests import common


class _SparseSchemaProvider:
    def __init__(self, schema: SparseVectorSchema) -> None:
        self._schema = schema

    async def __coco_sparse_vector_schema__(self) -> SparseVectorSchema:
        return self._schema


_SPARSE_SCHEMA_KEY = coco.ContextKey[_SparseSchemaProvider](
    "test_sparse_vector/schema_provider"
)


def test_sparse_vector_from_arrays_normalizes_and_sorts() -> None:
    sparse_vector = SparseVector.from_arrays(
        np.array([7, 1], dtype=np.int64),
        np.array([0.5, 1], dtype=np.float32),
    )

    assert sparse_vector == SparseVector(indices=(1, 7), values=(1.0, 0.5))


def test_sparse_vector_from_mapping_normalizes_order_and_values() -> None:
    sparse_vector = SparseVector.from_mapping({7: 0.9, 1: 0.5})

    assert sparse_vector == SparseVector(indices=(1, 7), values=(0.5, 0.9))
    assert as_sparse_vector(sparse_vector) is sparse_vector


def test_sparse_vector_from_mapping_requires_integral_keys() -> None:
    with pytest.raises(TypeError, match="indices must be integers"):
        SparseVector.from_mapping({1.5: 0.5})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="not bool"):
        SparseVector.from_mapping({True: 0.5})

    for value in (True, "0.5"):
        with pytest.raises(TypeError, match="values must be real numbers"):
            SparseVector.from_mapping({1: value})  # type: ignore[dict-item]


@pytest.mark.parametrize(
    ("indices", "values", "message"),
    [
        ((1,), (), "same length"),
        ((2, 1), (0.2, 0.1), "sorted ascending and unique"),
        ((1, 1), (0.1, 0.2), "sorted ascending and unique"),
        ((-1,), (0.1,), "non-negative"),
    ],
)
def test_sparse_vector_rejects_invalid_indices_and_values(
    indices: tuple[int, ...], values: tuple[float, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        SparseVector(indices=indices, values=values)


@pytest.mark.parametrize(
    ("indices", "values", "message"),
    [
        ([1], (0.5,), "indices must be a tuple"),
        (np.array([1]), (0.5,), "indices must be a tuple"),
        ((1,), [0.5], "values must be a tuple"),
        ((1,), np.array([0.5]), "values must be a tuple"),
        ((1.0,), (0.5,), "indices must contain only integers"),
        ((1,), (1,), "values must contain only floats"),
        ((True,), (0.5,), "indices must contain only integers"),
    ],
)
def test_sparse_vector_rejects_invalid_containers_and_elements(
    indices: object, values: object, message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        SparseVector(indices=indices, values=values)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_sparse_vector_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="values must be finite"):
        SparseVector(indices=(1,), values=(value,))
    with pytest.raises(ValueError, match="values must be finite"):
        SparseVector.from_mapping({1: value})


@pytest.mark.parametrize("value", [[(1, 0.5)], np.array([0.5]), "1:0.5"])
def test_as_sparse_vector_rejects_non_mapping_values(value: object) -> None:
    with pytest.raises(TypeError, match="SparseVector or Mapping"):
        as_sparse_vector(value)  # type: ignore[arg-type]


def test_sparse_vector_mapping_normalization_is_canonical() -> None:
    first = {1: 0.5, 7: 0.9}
    second = {7: 0.9, 1: 0.5}

    assert as_sparse_vector(first) == as_sparse_vector(second)


def test_sparse_vector_schema_defaults_and_validates_size() -> None:
    assert SparseVectorSchema(size=100).size == 100
    numpy_size_schema = SparseVectorSchema(size=np.int64(100))  # type: ignore[arg-type]
    assert numpy_size_schema.size == 100
    assert type(numpy_size_schema.size) is int

    for size in (0, -1):
        with pytest.raises(ValueError, match="size must be positive"):
            SparseVectorSchema(size=size)

    with pytest.raises(TypeError, match="size must be an integer"):
        SparseVectorSchema(size=1.5)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_sparse_vector_schema_resolves_schema_provider_and_context_key() -> (
    None
):
    schema = SparseVectorSchema(size=100)
    provider = _SparseSchemaProvider(schema)

    assert await get_sparse_vector_schema(schema) is schema
    assert await get_sparse_vector_schema(provider) is schema

    env = common.create_test_env(__file__)
    env.context_provider.provide(_SPARSE_SCHEMA_KEY, provider)

    async def resolve_from_context() -> SparseVectorSchema | None:
        return await get_sparse_vector_schema(_SPARSE_SCHEMA_KEY)

    app = coco.App(
        coco.AppConfig(name="test_sparse_schema_context", environment=env),
        resolve_from_context,
    )
    assert await app.update() is schema
