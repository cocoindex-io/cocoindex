from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, cast

import pytest

from cocoindex.resources.schema import SparseVector, SparseVectorSchema

try:
    from cocoindex.connectors import postgres
    from cocoindex.connectors.postgres._target import _make_sparsevec_encoder

    _HAS_POSTGRES = True
except ImportError:
    _HAS_POSTGRES = False

pytestmark = pytest.mark.skipif(
    not _HAS_POSTGRES, reason="postgres dependencies not installed"
)


@dataclass
class _SparseVectorRow:
    id: str
    embedding: Annotated[
        SparseVector,
        SparseVectorSchema(size=100),
    ]


@pytest.mark.asyncio
async def test_postgres_sparse_vector_schema_and_encoder() -> None:
    schema = await postgres.TableSchema.from_class(_SparseVectorRow, primary_key=["id"])
    sparse_column = schema.columns["embedding"]
    assert sparse_column.type == "sparsevec(100)"
    assert sparse_column.encoder is not None
    assert sparse_column.encoder({7: 0.9, 1: 0.5}) == "{2:0.5,8:0.9}/100"

    encoder = _make_sparsevec_encoder(100)
    assert encoder(SparseVector(indices=(1, 7), values=(0.0, 0.9))) == "{8:0.9}/100"
    assert encoder(SparseVector(indices=(), values=())) == "{}/100"
    with pytest.raises(ValueError, match="index 100 out of range"):
        encoder(SparseVector(indices=(100,), values=(0.5,)))
    with pytest.raises(ValueError, match="values must be finite"):
        encoder({1: float("nan")})

    @dataclass
    class OverrideSparseRow:
        id: str
        embedding: SparseVector

    override_schema = await postgres.TableSchema.from_class(
        OverrideSparseRow,
        primary_key=["id"],
        column_overrides={"embedding": SparseVectorSchema(size=50)},
    )
    assert override_schema.columns["embedding"].type == "sparsevec(50)"


@pytest.mark.asyncio
async def test_postgres_nullable_sparse_vector_preserves_schema() -> None:
    @dataclass
    class NullableSparseRow:
        id: str
        embedding: Annotated[
            SparseVector | None,
            SparseVectorSchema(size=100),
        ]

    schema = await postgres.TableSchema.from_class(
        NullableSparseRow, primary_key=["id"]
    )

    assert schema.columns["embedding"].type == "sparsevec(100)"
    assert schema.columns["embedding"].nullable


@pytest.mark.asyncio
async def test_postgres_rejects_native_override_with_sparse_metadata() -> None:
    @dataclass
    class NativeAndSparseRow:
        id: str
        embedding: Annotated[
            SparseVector,
            SparseVectorSchema(size=100),
        ]

    with pytest.raises(ValueError, match="cannot combine PgType"):
        await postgres.TableSchema.from_class(
            NativeAndSparseRow,
            primary_key=["id"],
            column_overrides={"embedding": postgres.PgType("sparsevec(100)")},
        )


@pytest.mark.asyncio
async def test_postgres_sparse_vector_schema_requires_dimension() -> None:
    @dataclass
    class MissingDimensionRow:
        id: str
        embedding: Annotated[
            SparseVector,
            SparseVectorSchema(),
        ]

    @dataclass
    class MissingAnnotationRow:
        id: str
        embedding: SparseVector

    for row_type in (MissingDimensionRow, MissingAnnotationRow):
        with pytest.raises(ValueError, match="sparsevec requires a dimension.*size"):
            await postgres.TableSchema.from_class(row_type, primary_key=["id"])


@pytest.mark.asyncio
async def test_postgres_sparse_vector_index_requires_hnsw() -> None:
    schema = await postgres.TableSchema.from_class(_SparseVectorRow, primary_key=["id"])
    table = postgres.TableTarget(cast(Any, None), schema)

    with pytest.raises(ValueError, match="only supports HNSW"):
        table.declare_vector_index(column="embedding", method="ivfflat")
