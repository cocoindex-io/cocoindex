from __future__ import annotations

import collections.abc
from typing import Annotated

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex._internal.datatype import analyze_type_info
from cocoindex.connectorkits import reject_sparse_vectors, resolve_vector_schemas
from cocoindex.resources.schema import (
    SparseVector,
    SparseVectorSchema,
    VectorSchema,
)

from tests import common

_DENSE_SCHEMA = VectorSchema(dtype=np.dtype(np.float32), size=4)
_DENSE_SCHEMA_KEY = coco.ContextKey[VectorSchema](
    "test_connectorkits_vector_schema/dense"
)
_UNRELATED_CONTEXT = coco.ContextKey[str]("test_connectorkits_vector_schema/unrelated")


@pytest.mark.asyncio
@pytest.mark.parametrize("context_first", [False, True])
async def test_direct_schema_wins_without_resolving_context(
    context_first: bool,
) -> None:
    annotations: tuple[object, ...] = (
        (_UNRELATED_CONTEXT, _DENSE_SCHEMA)
        if context_first
        else (_DENSE_SCHEMA, _UNRELATED_CONTEXT)
    )

    schemas = await resolve_vector_schemas(np.ndarray, annotations)

    assert schemas.vector is _DENSE_SCHEMA
    assert schemas.sparse is None


@pytest.mark.asyncio
async def test_context_resolution_stops_after_first_schema() -> None:
    env = common.create_test_env(__file__)
    env.context_provider.provide(_DENSE_SCHEMA_KEY, _DENSE_SCHEMA)

    async def resolve() -> None:
        schemas = await resolve_vector_schemas(
            np.ndarray, [_DENSE_SCHEMA_KEY, _UNRELATED_CONTEXT]
        )
        assert schemas.vector is _DENSE_SCHEMA
        assert schemas.sparse is None

    app = coco.App(
        coco.AppConfig(name="test_connectorkits_context_schema", environment=env),
        resolve,
    )
    await app.update()


@pytest.mark.asyncio
async def test_resolver_rejects_dense_sparse_conflict() -> None:
    with pytest.raises(ValueError, match="both VectorSchema and SparseVectorSchema"):
        await resolve_vector_schemas(
            SparseVector,
            [_DENSE_SCHEMA, SparseVectorSchema()],
        )


@pytest.mark.asyncio
async def test_resolver_validates_sparse_schema_base_type() -> None:
    with pytest.raises(TypeError, match="requires a SparseVector field"):
        await resolve_vector_schemas(str, [SparseVectorSchema()])


@pytest.mark.asyncio
@pytest.mark.parametrize("base_type", [SparseVector, dict, collections.abc.Mapping])
async def test_resolver_rejects_dense_schema_on_sparse_shapes(
    base_type: object,
) -> None:
    with pytest.raises(TypeError, match="VectorSchema requires a dense vector field"):
        await resolve_vector_schemas(base_type, [_DENSE_SCHEMA])


@pytest.mark.asyncio
async def test_nullable_schema_resolution_preserves_inner_first_precedence() -> None:
    inner_schema = VectorSchema(dtype=np.dtype(np.float32), size=384)
    outer_schema = VectorSchema(dtype=np.dtype(np.float32), size=768)
    field_type = Annotated[
        Annotated[NDArray[np.float32], inner_schema] | None,
        outer_schema,
    ]

    type_info = analyze_type_info(field_type)
    schemas = await resolve_vector_schemas(type_info.base_type, type_info.annotations)

    assert type_info.nullable
    assert schemas.vector is inner_schema


@pytest.mark.parametrize(
    ("base_type", "annotations"),
    [
        (SparseVector, ()),
        (SparseVector, (SparseVectorSchema(),)),
    ],
)
def test_non_resolving_guard_rejects_sparse_columns(
    base_type: object, annotations: tuple[object, ...]
) -> None:
    with pytest.raises(ValueError, match="Example does not support sparse vector"):
        reject_sparse_vectors(
            base_type,
            annotations,
            connector_name="Example",
        )


def test_non_resolving_guard_ignores_context_keys() -> None:
    reject_sparse_vectors(
        np.ndarray,
        [_UNRELATED_CONTEXT],
        connector_name="Example",
    )


@pytest.mark.asyncio
async def test_resolver_can_reject_sparse_for_dense_only_connector() -> None:
    with pytest.raises(ValueError, match="Example does not support sparse vector"):
        await resolve_vector_schemas(
            SparseVector,
            [],
            reject_sparse_vectors_for="Example",
        )
