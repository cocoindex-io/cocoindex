"""Tests for the zvec target connector."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Iterator

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex._internal.context_keys import ContextProvider
from cocoindex.connectorkits import target
from cocoindex.resources.schema import VectorSchema

from tests import common

try:
    import zvec

    from cocoindex.connectors import zvec as zc

    HAS_ZVEC = True
except ImportError:
    HAS_ZVEC = False

requires_zvec = pytest.mark.skipif(not HAS_ZVEC, reason="zvec is not installed")

pytestmark = requires_zvec


ZVEC_DB = coco.ContextKey["zc.ManagedConnection"]("zvec_test_db")


# =============================================================================
# Fixtures and helpers
# =============================================================================


@pytest.fixture
def conn() -> Iterator[Any]:
    base = Path(tempfile.mkdtemp(prefix="zvec_test_"))
    connection = zc.connect(base)
    yield connection
    connection.close()


_counter = {"n": 0}


def make_test_env(connection: Any, env_name: str) -> coco.Environment:
    ctx = ContextProvider()
    ctx.provide(ZVEC_DB, connection)
    _counter["n"] += 1
    settings = coco.Settings.from_env(
        db_path=common.get_env_db_path(
            f"connectors__test_zvec_target__{env_name}__{_counter['n']}"
        )
    )
    return coco.Environment(settings, context_provider=ctx)


def fetch_doc(connection: Any, collection_name: str, doc_id: str) -> Any:
    col = connection.open_existing(collection_name)
    result = col.fetch(ids=doc_id)
    return result.get(doc_id)


# =============================================================================
# Row types
# =============================================================================


# zvec collections require at least one vector field, so every row type carries one.
_Embedding = Annotated[
    NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=4)
]

_EMB = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)


@dataclass
class SimpleDoc:
    id: str
    title: str
    year: int
    embedding: _Embedding


@dataclass
class TypedDoc:
    id: str
    name: str
    score: float
    active: bool
    tags: list[str]
    embedding: _Embedding


@dataclass
class VectorDoc:
    id: str
    title: str
    embedding: _Embedding


# `from __future__ import annotations` keeps these as strings, so the zc
# reference is only resolved (via from_class) in tests, which are skipped when
# zvec is absent.
@dataclass
class SparseDoc:
    id: str
    title: str
    sparse: Annotated[dict[int, float], zc.ZvecVectorDef(sparse=True)]


@dataclass
class MultiVectorDoc:
    id: str
    dense: _Embedding
    sparse: Annotated[dict[int, float], zc.ZvecVectorDef(sparse=True)]


# =============================================================================
# Mutable per-test source state
# =============================================================================

_rows: list[Any] = []
_row_type: type = SimpleDoc
_collection: str = "test_collection"
_managed_by: target.ManagedBy = target.ManagedBy.SYSTEM
_declare_enabled: bool = True


async def _declare() -> None:
    if not _declare_enabled:
        return
    table = await coco.use_mount(
        coco.component_subpath("setup", "col"),
        zc.declare_collection_target,
        ZVEC_DB,
        _collection,
        await zc.CollectionSchema.from_class(_row_type, primary_key=["id"]),
        managed_by=_managed_by,
    )
    for row in _rows:
        table.declare_row(row=row)


def _make_app(connection: Any, env_name: str) -> coco.App[[], None]:
    env = make_test_env(connection, env_name)
    return coco.App(coco.AppConfig(name=env_name, environment=env), _declare)


def _reset(row_type: type, collection: str) -> None:
    global _row_type, _collection, _managed_by, _declare_enabled
    _row_type = row_type
    _collection = collection
    _managed_by = target.ManagedBy.SYSTEM
    _declare_enabled = True
    _rows.clear()


# =============================================================================
# Tests
# =============================================================================


def test_create_and_insert(conn: Any) -> None:
    _reset(SimpleDoc, "test_create")
    app = _make_app(conn, "test_create_and_insert")

    _rows.extend(
        [
            SimpleDoc(id="1", title="Alice", year=2020, embedding=_EMB),
            SimpleDoc(id="2", title="Bob", year=2021, embedding=_EMB),
        ]
    )
    app.update_blocking()

    assert conn.collection_path("test_create").exists()
    doc = fetch_doc(conn, "test_create", "1")
    assert doc is not None
    assert doc.fields == {"title": "Alice", "year": 2020}
    assert fetch_doc(conn, "test_create", "2").fields == {"title": "Bob", "year": 2021}


def test_update_row(conn: Any) -> None:
    _reset(SimpleDoc, "test_update")
    app = _make_app(conn, "test_update_row")

    _rows.append(SimpleDoc(id="1", title="Alice", year=2020, embedding=_EMB))
    app.update_blocking()
    assert fetch_doc(conn, "test_update", "1").fields["title"] == "Alice"

    _rows[0] = SimpleDoc(id="1", title="Alice v2", year=2099, embedding=_EMB)
    app.update_blocking()
    doc = fetch_doc(conn, "test_update", "1")
    assert doc.fields == {"title": "Alice v2", "year": 2099}


def test_delete_row(conn: Any) -> None:
    _reset(SimpleDoc, "test_delete")
    app = _make_app(conn, "test_delete_row")

    _rows.extend(
        [
            SimpleDoc(id="1", title="A", year=1, embedding=_EMB),
            SimpleDoc(id="2", title="B", year=2, embedding=_EMB),
        ]
    )
    app.update_blocking()
    assert fetch_doc(conn, "test_delete", "2") is not None

    _rows[:] = [_rows[0]]
    app.update_blocking()
    assert fetch_doc(conn, "test_delete", "1") is not None
    assert fetch_doc(conn, "test_delete", "2") is None


def test_multiple_scalar_types(conn: Any) -> None:
    _reset(TypedDoc, "test_types")
    app = _make_app(conn, "test_multiple_scalar_types")

    _rows.append(
        TypedDoc(
            id="1", name="x", score=3.5, active=True, tags=["a", "b"], embedding=_EMB
        )
    )
    app.update_blocking()

    doc = fetch_doc(conn, "test_types", "1")
    assert doc.fields["name"] == "x"
    assert doc.fields["score"] == pytest.approx(3.5)
    assert doc.fields["active"] is True
    assert list(doc.fields["tags"]) == ["a", "b"]


def test_drop_collection(conn: Any) -> None:
    global _declare_enabled
    _reset(SimpleDoc, "test_drop")
    # Reuse one app/env so CocoIndex retains tracking state across runs.
    app = _make_app(conn, "test_drop_collection")

    _rows.append(SimpleDoc(id="1", title="A", year=1, embedding=_EMB))
    app.update_blocking()
    assert conn.collection_path("test_drop").exists()

    # Stop declaring the collection: it should be destroyed on the next run.
    _declare_enabled = False
    app.update_blocking()
    assert not conn.collection_path("test_drop").exists()


def test_no_op_when_unchanged(conn: Any) -> None:
    _reset(SimpleDoc, "test_noop")
    app = _make_app(conn, "test_no_op_when_unchanged")

    _rows.append(SimpleDoc(id="1", title="A", year=1, embedding=_EMB))
    app.update_blocking()
    # Running again with identical data should be a no-op and not error.
    app.update_blocking()
    assert fetch_doc(conn, "test_noop", "1").fields == {"title": "A", "year": 1}


def test_dense_vector(conn: Any) -> None:
    _reset(VectorDoc, "test_dense")
    app = _make_app(conn, "test_dense_vector")

    _rows.append(
        VectorDoc(
            id="1",
            title="hello",
            embedding=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
        )
    )
    app.update_blocking()

    col = conn.open_existing("test_dense")
    results = col.query(
        zvec.VectorQuery(field_name="embedding", vector=[0.1, 0.2, 0.3, 0.4]),
        topk=5,
    )
    assert [d.id for d in results] == ["1"]


def test_sparse_vector(conn: Any) -> None:
    _reset(SparseDoc, "test_sparse")
    app = _make_app(conn, "test_sparse_vector")

    _rows.append(SparseDoc(id="1", title="s", sparse={1: 0.5, 7: 0.9}))
    app.update_blocking()

    doc = fetch_doc(conn, "test_sparse", "1")
    assert doc is not None
    assert doc.fields["title"] == "s"


def test_multiple_vector_fields(conn: Any) -> None:
    _reset(MultiVectorDoc, "test_multivec")
    app = _make_app(conn, "test_multiple_vector_fields")

    _rows.append(
        MultiVectorDoc(
            id="1",
            dense=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
            sparse={2: 0.3},
        )
    )
    app.update_blocking()

    col = conn.open_existing("test_multivec")
    results = col.query(
        zvec.VectorQuery(field_name="dense", vector=[0.1, 0.2, 0.3, 0.4]), topk=5
    )
    assert [d.id for d in results] == ["1"]


def test_multiple_collections(conn: Any) -> None:
    async def _declare_two() -> None:
        schema = await zc.CollectionSchema.from_class(SimpleDoc, primary_key=["id"])
        t1 = await coco.use_mount(
            coco.component_subpath("setup", "c1"),
            zc.declare_collection_target,
            ZVEC_DB,
            "collection_one",
            schema,
        )
        t2 = await coco.use_mount(
            coco.component_subpath("setup", "c2"),
            zc.declare_collection_target,
            ZVEC_DB,
            "collection_two",
            schema,
        )
        t1.declare_row(row=SimpleDoc(id="1", title="one", year=1, embedding=_EMB))
        t2.declare_row(row=SimpleDoc(id="1", title="two", year=2, embedding=_EMB))

    env = make_test_env(conn, "test_multiple_collections")
    app = coco.App(
        coco.AppConfig(name="test_multiple_collections", environment=env),
        _declare_two,
    )
    app.update_blocking()

    assert fetch_doc(conn, "collection_one", "1").fields["title"] == "one"
    assert fetch_doc(conn, "collection_two", "1").fields["title"] == "two"


def test_user_managed_collection(conn: Any) -> None:
    # Pre-create the collection outside CocoIndex.
    schema = zvec.CollectionSchema(
        name="user_col",
        fields=[
            zvec.FieldSchema(
                name="title", data_type=zvec.DataType.STRING, nullable=True
            ),
            zvec.FieldSchema(name="year", data_type=zvec.DataType.INT64, nullable=True),
        ],
        vectors=[
            zvec.VectorSchema(
                name="embedding",
                data_type=zvec.DataType.VECTOR_FP32,
                dimension=4,
                index_param=zvec.HnswIndexParam(metric_type=zvec.MetricType.COSINE),
            )
        ],
    )
    conn.open_or_create("user_col", schema)

    global _row_type, _collection, _managed_by
    _row_type = SimpleDoc
    _collection = "user_col"
    _managed_by = target.ManagedBy.USER
    _rows.clear()
    _rows.append(SimpleDoc(id="1", title="A", year=1, embedding=_EMB))

    global _declare_enabled
    app = _make_app(conn, "test_user_managed")
    app.update_blocking()
    assert fetch_doc(conn, "user_col", "1").fields["title"] == "A"

    # Stop declaring: a user-managed collection must NOT be destroyed.
    _declare_enabled = False
    app.update_blocking()
    assert conn.collection_path("user_col").exists()


@pytest.mark.asyncio
async def test_schema_validation(conn: Any) -> None:
    schema = await zc.CollectionSchema.from_class(SimpleDoc, primary_key=["id"])

    # zvec requires collection names of at least 3 characters.
    with pytest.raises(ValueError, match="at least 3 characters"):
        zc.collection_target(ZVEC_DB, "ab", schema)

    # Composite primary keys are unsupported (single string id only).
    with pytest.raises(ValueError, match="exactly one primary key"):
        await zc.CollectionSchema.from_class(SimpleDoc, primary_key=["id", "title"])

    # A collection must declare at least one vector field.
    @dataclass
    class NoVectorDoc:
        id: str
        title: str

    no_vec_schema = await zc.CollectionSchema.from_class(
        NoVectorDoc, primary_key=["id"]
    )
    with pytest.raises(ValueError, match="at least one vector field"):
        zc.collection_target(ZVEC_DB, "no_vec_collection", no_vec_schema)
