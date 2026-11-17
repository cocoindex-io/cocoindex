"""Tests for Turbopuffer target connector.

Helper-level tests run without a turbopuffer service.

Live tests are gated on the ``TURBOPUFFER_API_KEY`` env var; they are skipped
when it isn't set. Set ``TURBOPUFFER_REGION`` to override the default region
(``gcp-us-central1``). Each test uses a unique namespace name and tears it down
afterwards.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import numpy as np
import pytest
import pytest_asyncio

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)


# =============================================================================
# Optional dependency check
# =============================================================================

try:
    from turbopuffer import AsyncTurbopuffer  # type: ignore

    HAS_TURBOPUFFER = True
except ImportError:
    HAS_TURBOPUFFER = False

requires_turbopuffer = pytest.mark.skipif(
    not HAS_TURBOPUFFER, reason="turbopuffer is not installed"
)

if HAS_TURBOPUFFER:
    from cocoindex.connectors import turbopuffer
    from cocoindex.connectors.turbopuffer._target import (
        _build_write_schema,
        _row_to_upsert,
        _vector_type_str,
    )


# =============================================================================
# Live test config
# =============================================================================

_TPUF_API_KEY = os.environ.get("TURBOPUFFER_API_KEY", "")
_TPUF_REGION = os.environ.get("TURBOPUFFER_REGION", "gcp-us-central1")
_HAS_LIVE = bool(_TPUF_API_KEY)

requires_live = pytest.mark.skipif(
    not (HAS_TURBOPUFFER and _HAS_LIVE),
    reason="TURBOPUFFER_API_KEY not set; skipping live tests",
)

if HAS_TURBOPUFFER:
    TPUF: coco.ContextKey[Any] = coco.ContextKey("test_turbopuffer_db")


# =============================================================================
# Unit tests — helpers (no service needed)
# =============================================================================


@requires_turbopuffer
class TestVectorTypeStr:
    def test_f32(self) -> None:
        vs = VectorSchema(dtype=np.dtype(np.float32), size=384)
        assert _vector_type_str(vs) == "[384]f32"

    def test_f16(self) -> None:
        vs = VectorSchema(dtype=np.dtype(np.float16), size=128)
        assert _vector_type_str(vs) == "[128]f16"

    def test_unsupported_dtype(self) -> None:
        vs = VectorSchema(dtype=np.dtype(np.float64), size=64)
        with pytest.raises(ValueError, match="float32 or float16"):
            _vector_type_str(vs)


@requires_turbopuffer
class TestRowToUpsert:
    @pytest.fixture
    def single_schema(self) -> Any:
        return turbopuffer.NamespaceSchema(
            vectors=turbopuffer._target._ResolvedVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
            ),
            distance="cosine_distance",
        )

    @pytest.fixture
    def named_schema(self) -> Any:
        return turbopuffer.NamespaceSchema(
            vectors=turbopuffer._target._ResolvedNamedVectorsDef(
                vectors={
                    "text": turbopuffer._target._ResolvedVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
                    ),
                    "image": turbopuffer._target._ResolvedVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=2),
                    ),
                }
            ),
            distance="cosine_distance",
        )

    def test_single_vector_list(self, single_schema: Any) -> None:
        row = turbopuffer.Row(
            id="a", vector=[1.0, 2.0, 3.0], attributes={"text": "hello"}
        )
        out = _row_to_upsert(row, single_schema)
        assert out == {"id": "a", "vector": [1.0, 2.0, 3.0], "text": "hello"}

    def test_single_vector_ndarray(self, single_schema: Any) -> None:
        row = turbopuffer.Row(id=42, vector=np.array([1.0, 2.0, 3.0], dtype=np.float32))
        out = _row_to_upsert(row, single_schema)
        assert out["id"] == 42
        assert out["vector"] == [1.0, 2.0, 3.0]

    def test_single_vector_dict_rejected(self, single_schema: Any) -> None:
        row = turbopuffer.Row(id="a", vector={"x": [1.0, 2.0, 3.0]})
        with pytest.raises(ValueError, match="single unnamed vector"):
            _row_to_upsert(row, single_schema)

    def test_named_vectors(self, named_schema: Any) -> None:
        row = turbopuffer.Row(
            id="a",
            vector={"text": [1.0, 2.0, 3.0], "image": [0.5, 0.5]},
            attributes={"title": "T"},
        )
        out = _row_to_upsert(row, named_schema)
        assert out == {
            "id": "a",
            "text": [1.0, 2.0, 3.0],
            "image": [0.5, 0.5],
            "title": "T",
        }

    def test_named_vectors_missing_field(self, named_schema: Any) -> None:
        row = turbopuffer.Row(id="a", vector={"text": [1.0, 2.0, 3.0]})
        with pytest.raises(ValueError, match="missing vector fields"):
            _row_to_upsert(row, named_schema)

    def test_named_vectors_non_dict_rejected(self, named_schema: Any) -> None:
        row = turbopuffer.Row(id="a", vector=[1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="named vectors"):
            _row_to_upsert(row, named_schema)

    def test_reserved_attribute_id(self, single_schema: Any) -> None:
        row = turbopuffer.Row(
            id="a", vector=[1.0, 2.0, 3.0], attributes={"id": "shadow"}
        )
        with pytest.raises(ValueError, match="reserved"):
            _row_to_upsert(row, single_schema)

    def test_reserved_attribute_vector(self, single_schema: Any) -> None:
        row = turbopuffer.Row(
            id="a", vector=[1.0, 2.0, 3.0], attributes={"vector": [9.0]}
        )
        with pytest.raises(ValueError, match="reserved"):
            _row_to_upsert(row, single_schema)

    def test_named_vector_attribute_collision(self, named_schema: Any) -> None:
        # An attribute name that collides with a named vector field must raise,
        # not silently overwrite the vector.
        row = turbopuffer.Row(
            id="a",
            vector={"text": [1.0, 2.0, 3.0], "image": [0.5, 0.5]},
            attributes={"text": "would collide"},
        )
        with pytest.raises(ValueError, match="reserved"):
            _row_to_upsert(row, named_schema)


@requires_turbopuffer
class TestNamespaceSchemaCreate:
    @pytest.mark.asyncio
    async def test_named_vector_id_rejected(self) -> None:
        # "id" as a named vector field name would silently overwrite row.id
        # at the wire level — must raise.
        with pytest.raises(ValueError, match="reserved"):
            await turbopuffer.NamespaceSchema.create(
                vectors={
                    "id": turbopuffer.VectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
                    ),
                },
            )

    @pytest.mark.asyncio
    async def test_empty_named_vectors_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            await turbopuffer.NamespaceSchema.create(vectors={})

    @pytest.mark.asyncio
    async def test_unsupported_dtype_rejected(self) -> None:
        # float64 isn't supported by turbopuffer; must fail at schema construction
        # rather than waiting for the first write.
        with pytest.raises(ValueError, match="float32 or float16"):
            await turbopuffer.NamespaceSchema.create(
                vectors=turbopuffer.VectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float64), size=3),
                ),
            )


@requires_turbopuffer
class TestBuildWriteSchema:
    def test_single(self) -> None:
        schema = turbopuffer.NamespaceSchema(
            vectors=turbopuffer._target._ResolvedVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=8),
            ),
            distance="cosine_distance",
        )
        assert _build_write_schema(schema) == {
            "vector": {"type": "[8]f32", "ann": True}
        }

    def test_named(self) -> None:
        schema = turbopuffer.NamespaceSchema(
            vectors=turbopuffer._target._ResolvedNamedVectorsDef(
                vectors={
                    "a": turbopuffer._target._ResolvedVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=4),
                    ),
                    "b": turbopuffer._target._ResolvedVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float16), size=16),
                    ),
                }
            ),
            distance="euclidean_squared",
        )
        out = _build_write_schema(schema)
        assert out == {
            "a": {"type": "[4]f32", "ann": True},
            "b": {"type": "[16]f16", "ann": True},
        }


class _RecordingNamespace:
    def __init__(self, writes: list[dict[str, Any]]) -> None:
        self._writes = writes

    async def write(self, **kwargs: Any) -> None:
        self._writes.append(kwargs)


class _RecordingClient:
    """Stands in for ``AsyncTurbopuffer``: records every write."""

    def __init__(self) -> None:
        self.writes: list[dict[str, Any]] = []

    def namespace(self, name: str) -> _RecordingNamespace:
        return _RecordingNamespace(self.writes)


@requires_turbopuffer
class TestWriteRequestSize:
    """A batch of about 40 MiB goes out in several writes, each under the
    32 MiB the connector allows a request (Turbopuffer refuses one over
    512 MB), every row and id once."""

    @pytest.mark.asyncio
    async def test_large_batch_is_split(self) -> None:
        import json

        from cocoindex.connectors.turbopuffer._target import _RowAction, _RowHandler

        limit = 32 * 1024 * 1024
        schema = turbopuffer.NamespaceSchema(
            vectors=turbopuffer._target._ResolvedVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
            ),
            distance="cosine_distance",
        )
        client = _RecordingClient()
        handler = _RowHandler(client, "ns", schema)  # type: ignore[arg-type]
        text = "x" * (1024 * 1024)
        actions = [
            _RowAction(row_id=i, upsert={"id": i, "vector": [1.0, 0.0, 0.0], "t": text})
            for i in range(40)
        ] + [_RowAction(row_id=f"gone-{i}", upsert=None) for i in range(1000)]
        await handler._apply_actions(None, actions)  # type: ignore[arg-type]

        assert len(client.writes) > 1
        for write in client.writes:
            assert len(json.dumps(write)) <= limit
            assert write["distance_metric"] == "cosine_distance"
            assert write["schema"] == {"vector": {"type": "[3]f32", "ann": True}}
        upserted = [r["id"] for w in client.writes for r in w.get("upsert_rows", [])]
        deleted = [i for w in client.writes for i in w.get("deletes", [])]
        assert upserted == list(range(40))
        assert deleted == [f"gone-{i}" for i in range(1000)]


# =============================================================================
# Live test fixtures
# =============================================================================


@pytest_asyncio.fixture
async def tpuf_namespace() -> AsyncIterator[tuple[Any, str]]:
    """Provide an AsyncTurbopuffer client and a unique test namespace name.

    Cleans up the namespace after the test.
    """
    client = AsyncTurbopuffer(region=_TPUF_REGION, api_key=_TPUF_API_KEY)
    ns_name = f"cocoindex_test_{uuid.uuid4().hex[:12]}"
    coco_env.context_provider.provide(TPUF, client)
    try:
        yield client, ns_name
    finally:
        try:
            await client.namespace(ns_name).delete_all()
        except Exception:
            pass


# =============================================================================
# Live test app helpers (read state via globals — same pattern as other tests)
# =============================================================================

_live_rows: list[Any] = []
_live_namespace_name: str = ""
_live_schema: Any = None


async def _declare_live_target() -> None:
    target = await turbopuffer.mount_namespace_target(
        TPUF, _live_namespace_name, _live_schema
    )
    for row in _live_rows:
        target.declare_row(row=row)


async def _query_all(client: Any, namespace_name: str) -> list[dict[str, Any]]:
    """Fetch all rows from the namespace, sorted by id."""
    ns = client.namespace(namespace_name)
    result = await ns.query(top_k=1000, include_attributes=True)
    rows = list(getattr(result, "rows", []) or [])
    return sorted(
        ({"id": r.id, **(getattr(r, "attributes", None) or {})} for r in rows),
        key=lambda r: str(r["id"]),
    )


# =============================================================================
# Live tests
# =============================================================================


@requires_live
@pytest.mark.asyncio
async def test_insert_update_delete(tpuf_namespace: tuple[Any, str]) -> None:
    """Insert rows, update one, delete one. Verify reconciliation."""
    client, ns_name = tpuf_namespace
    global _live_rows, _live_namespace_name, _live_schema

    _live_namespace_name = ns_name
    _live_schema = await turbopuffer.NamespaceSchema.create(
        vectors=turbopuffer.VectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
        )
    )

    app = coco.App(
        coco.AppConfig(name="test_tpuf_insert_update", environment=coco_env),
        _declare_live_target,
    )

    # Insert
    _live_rows = [
        turbopuffer.Row(id="a", vector=[1.0, 0.0, 0.0], attributes={"label": "alpha"}),
        turbopuffer.Row(id="b", vector=[0.0, 1.0, 0.0], attributes={"label": "beta"}),
    ]
    await app.update()

    rows = await _query_all(client, ns_name)
    assert len(rows) == 2
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["label"] == "alpha"
    assert by_id["b"]["label"] == "beta"

    # Update one row's attribute
    _live_rows[0] = turbopuffer.Row(
        id="a", vector=[1.0, 0.0, 0.0], attributes={"label": "alpha2"}
    )
    await app.update()

    rows = await _query_all(client, ns_name)
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["label"] == "alpha2"

    # Drop one row
    _live_rows = [_live_rows[1]]
    await app.update()

    rows = await _query_all(client, ns_name)
    assert {r["id"] for r in rows} == {"b"}


@requires_live
@pytest.mark.asyncio
async def test_named_vectors(tpuf_namespace: tuple[Any, str]) -> None:
    """Named vectors are written under their respective field names."""
    client, ns_name = tpuf_namespace
    global _live_rows, _live_namespace_name, _live_schema

    _live_namespace_name = ns_name
    _live_schema = await turbopuffer.NamespaceSchema.create(
        vectors={
            "v1": turbopuffer.VectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=3),
            ),
            "v2": turbopuffer.VectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=2),
            ),
        }
    )

    app = coco.App(
        coco.AppConfig(name="test_tpuf_named", environment=coco_env),
        _declare_live_target,
    )

    _live_rows = [
        turbopuffer.Row(
            id="x",
            vector={"v1": [1.0, 0.0, 0.0], "v2": [0.5, 0.5]},
            attributes={"title": "doc"},
        ),
    ]
    await app.update()

    rows = await _query_all(client, ns_name)
    assert len(rows) == 1
    assert rows[0]["id"] == "x"
    assert rows[0]["title"] == "doc"


# =============================================================================
# End-to-end on an in-memory fake — a namespace deleted outside CocoIndex
# =============================================================================


class _FakeNamespace:
    def __init__(self, store: dict[str, dict[Any, dict[str, Any]]], name: str):
        self._store = store
        self._name = name

    async def write(self, **kwargs: Any) -> None:
        rows = self._store.setdefault(self._name, {})
        for row in kwargs.get("upsert_rows", []):
            rows[row["id"]] = row
        for row_id in kwargs.get("deletes", []):
            rows.pop(row_id, None)
        # Turbopuffer keeps a namespace only while it holds a document.
        if not rows:
            del self._store[self._name]

    async def delete_all(self) -> None:
        self._store.pop(self._name, None)

    async def exists(self) -> bool:
        return self._name in self._store


_deleted_ns_component_runs = 0


if HAS_TURBOPUFFER:
    # AsyncTurbopuffer is Any when turbopuffer is not installed (the CI type
    # check).
    class _FakeTurbopuffer(AsyncTurbopuffer):  # type: ignore[misc, unused-ignore]
        """Stands in for ``AsyncTurbopuffer``: namespaces in memory."""

        def __init__(self) -> None:
            self.stored: dict[str, dict[Any, dict[str, Any]]] = {}

        def namespace(self, name: str) -> Any:  # type: ignore[override, unused-ignore]
            return _FakeNamespace(self.stored, name)

    @coco.fn(memo=True)
    async def _declare_row_in_component(
        row_id: str, target: "turbopuffer.NamespaceTarget[coco.ResolvedS]"
    ) -> None:
        global _deleted_ns_component_runs
        _deleted_ns_component_runs += 1
        target.declare_row(turbopuffer.Row(id=row_id, vector=[1.0, 0.0, 0.0]))


@requires_turbopuffer
@pytest.mark.asyncio
async def test_namespace_deleted_outside_cocoindex_is_filled_again() -> None:
    """A namespace deleted outside CocoIndex is written again on the next run,
    with all its rows: those of child components that would otherwise be
    skipped as unchanged too."""
    global _deleted_ns_component_runs
    _deleted_ns_component_runs = 0
    client = _FakeTurbopuffer()
    db_key: coco.ContextKey[Any] = coco.ContextKey(
        f"test_tpuf_deleted_{uuid.uuid4().hex}"
    )
    env = common.create_test_env(__file__, suffix="deleted_namespace")
    env.context_provider.provide(db_key, client)
    schema = await turbopuffer.NamespaceSchema.create(
        vectors=turbopuffer.VectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=3)
        )
    )

    async def declare_fn() -> None:
        target = await turbopuffer.mount_namespace_target(db_key, "ns", schema)
        await coco.mount_each(
            _declare_row_in_component, [(i, i) for i in ("a", "b")], target
        )

    app = coco.App(
        coco.AppConfig(name="test_tpuf_deleted_namespace", environment=env),
        declare_fn,
    )
    await app.update()
    assert set(client.stored["ns"]) == {"a", "b"}
    assert _deleted_ns_component_runs == 2
    await app.update()
    assert _deleted_ns_component_runs == 2, "unchanged children are skipped"

    await client.namespace("ns").delete_all()
    await app.update()
    assert set(client.stored["ns"]) == {"a", "b"}
    assert _deleted_ns_component_runs == 4, "the children run again to write their rows"

    await app.update()
    assert _deleted_ns_component_runs == 4
