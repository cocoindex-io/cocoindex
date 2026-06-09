"""Tests for VectorSchema and MultiVectorSchema concrete implementations."""

import asyncio

import numpy as np

from cocoindex.resources.schema import (
    MultiVectorSchema,
    MultiVectorSchemaProvider,
    VectorSchema,
    VectorSchemaProvider,
    get_multi_vector_schema,
    get_vector_schema,
)


class TestVectorSchema:
    def test_construction(self) -> None:
        schema = VectorSchema(dtype=np.dtype("float32"), size=128)
        assert schema.dtype == np.dtype("float32")
        assert schema.size == 128

    def test_different_dtypes(self) -> None:
        schema_f32 = VectorSchema(dtype=np.dtype("float32"), size=64)
        schema_f64 = VectorSchema(dtype=np.dtype("float64"), size=64)
        assert schema_f32.dtype == np.dtype("float32")
        assert schema_f64.dtype == np.dtype("float64")

    def test_equality(self) -> None:
        schema1 = VectorSchema(dtype=np.dtype("float32"), size=256)
        schema2 = VectorSchema(dtype=np.dtype("float32"), size=256)
        assert schema1 == schema2

    def test_inequality_by_size(self) -> None:
        schema1 = VectorSchema(dtype=np.dtype("float32"), size=128)
        schema2 = VectorSchema(dtype=np.dtype("float32"), size=256)
        assert schema1 != schema2

    def test_coco_vector_schema_returns_self(self) -> None:
        schema = VectorSchema(dtype=np.dtype("float32"), size=128)
        result = asyncio.run(schema.__coco_vector_schema__())
        assert result is schema

    def test_is_vector_schema_provider(self) -> None:
        schema = VectorSchema(dtype=np.dtype("float32"), size=128)
        assert isinstance(schema, VectorSchemaProvider)


class TestMultiVectorSchema:
    def test_construction(self) -> None:
        vs = VectorSchema(dtype=np.dtype("float32"), size=128)
        mvs = MultiVectorSchema(vector_schema=vs)
        assert mvs.vector_schema == vs

    def test_coco_multi_vector_schema_returns_self(self) -> None:
        vs = VectorSchema(dtype=np.dtype("float32"), size=64)
        mvs = MultiVectorSchema(vector_schema=vs)
        result = asyncio.run(mvs.__coco_multi_vector_schema__())
        assert result is mvs

    def test_is_multi_vector_schema_provider(self) -> None:
        vs = VectorSchema(dtype=np.dtype("float32"), size=64)
        mvs = MultiVectorSchema(vector_schema=vs)
        assert isinstance(mvs, MultiVectorSchemaProvider)

    def test_equality(self) -> None:
        vs = VectorSchema(dtype=np.dtype("float32"), size=32)
        mvs1 = MultiVectorSchema(vector_schema=vs)
        mvs2 = MultiVectorSchema(vector_schema=vs)
        assert mvs1 == mvs2


class TestGetVectorSchema:
    def test_returns_schema_for_provider(self) -> None:
        schema = VectorSchema(dtype=np.dtype("float32"), size=128)
        result = asyncio.run(get_vector_schema(schema))
        assert result == schema

    def test_returns_none_for_non_provider(self) -> None:
        result = asyncio.run(get_vector_schema("not a provider"))
        assert result is None

    def test_returns_none_for_none(self) -> None:
        result = asyncio.run(get_vector_schema(None))
        assert result is None

    def test_returns_none_for_plain_object(self) -> None:
        result = asyncio.run(get_vector_schema(object()))
        assert result is None


class TestGetMultiVectorSchema:
    def test_returns_schema_for_provider(self) -> None:
        vs = VectorSchema(dtype=np.dtype("float32"), size=64)
        mvs = MultiVectorSchema(vector_schema=vs)
        result = asyncio.run(get_multi_vector_schema(mvs))
        assert result == mvs

    def test_returns_none_for_non_provider(self) -> None:
        result = asyncio.run(get_multi_vector_schema("not a provider"))
        assert result is None

    def test_returns_none_for_plain_vector_schema(self) -> None:
        """VectorSchema is not a MultiVectorSchemaProvider."""
        vs = VectorSchema(dtype=np.dtype("float32"), size=128)
        result = asyncio.run(get_multi_vector_schema(vs))
        assert result is None
