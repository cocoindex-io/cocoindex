"""Tests for Valkey target connector (vector search index).

Uses testcontainers to spin up a real Valkey instance with valkey-search module,
or connects to a running instance via VALKEY_TEST_SERVER=1 env var.

Run with:
    VALKEY_TEST_SERVER=1 uv run pytest python/tests/connectors/test_valkey_target.py -v
"""

from __future__ import annotations

import asyncio
import os
import struct
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any, NamedTuple, cast

import numpy as np
import pytest
import pytest_asyncio

import cocoindex as coco
from cocoindex.resources.schema import SparseVector, VectorSchema

from tests import common

# =============================================================================
# Check dependencies
# =============================================================================

try:
    from glide import GlideClient, GlideClientConfiguration, NodeAddress
    from glide.async_commands import ft as glide_ft

    HAS_GLIDE = True
except ImportError:
    HAS_GLIDE = False

if HAS_GLIDE:
    from cocoindex.connectors import valkey
    from cocoindex.connectors.valkey._target import _vector_to_bytes

requires_glide = pytest.mark.skipif(not HAS_GLIDE, reason="valkey-glide not installed")

_VALKEY_HOST = os.environ.get("VALKEY_HOST", "localhost")
_VALKEY_PORT = int(os.environ.get("VALKEY_PORT", "6379"))
_HAS_SERVER = bool(os.environ.get("VALKEY_TEST_SERVER"))

requires_server = pytest.mark.skipif(
    not (HAS_GLIDE and _HAS_SERVER),
    reason="VALKEY_TEST_SERVER not set; skipping integration tests",
)

_VALKEY_DB_KEY: coco.ContextKey[Any] = coco.ContextKey("test_valkey_target_db")

# =============================================================================
# Test utilities
# =============================================================================

_DIM = 4
_VECTOR_SCHEMA = VectorSchema(dtype=np.dtype(np.float32), size=_DIM)


@requires_glide
@pytest.mark.parametrize(
    "vector",
    [
        {1: 0.5, 7: 0.9},
        SparseVector(indices=(1, 7), values=(0.5, 0.9)),
    ],
)
def test_vector_to_bytes_rejects_sparse_shapes(vector: object) -> None:
    with pytest.raises(
        ValueError, match="Valkey does not support sparse vector values"
    ):
        _vector_to_bytes(cast(Any, vector))


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _make_vector(dim: int, value: float = 1.0) -> list[float]:
    """Create a test vector filled with a value."""
    return [value] * dim


def _decode_vector(blob: bytes, dim: int) -> list[float]:
    """Decode a float32 binary blob to a list of floats."""
    return list(struct.unpack(f"<{dim}f", blob))


async def _wait_for_index_count(
    client: Any,
    index_name: str,
    expected_count: int,
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    """Poll FT.INFO until the index has the expected document count, or timeout."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = await glide_ft.info(client, index_name)
            # FT.INFO returns a flat list of key-value pairs.
            # Look for num_docs or equivalent field.
            if isinstance(info, list):
                for i, item in enumerate(info):
                    key_val = item.decode() if isinstance(item, bytes) else str(item)
                    if key_val == "num_docs" and i + 1 < len(info):
                        count_val = info[i + 1]
                        count = int(
                            count_val.decode()
                            if isinstance(count_val, bytes)
                            else count_val
                        )
                        if count >= expected_count:
                            return
                        break
        except Exception:
            pass
        await asyncio.sleep(interval)
    # Fall through — test assertions will catch any real problem


# =============================================================================
# Fixtures
# =============================================================================


class _ValkeyEnv(NamedTuple):
    """Bundle of client + coco environment for valkey target tests."""

    client: Any
    coco_env: coco.Environment


@pytest.fixture(scope="module")
def valkey_server() -> Iterator[tuple[str, int]]:
    """Provide a Valkey server address.

    Uses VALKEY_HOST/VALKEY_PORT env vars if set (default localhost:6379).
    If testcontainers is available and no explicit host is configured,
    attempts to spin up a container; falls back to defaults on failure.
    """
    if not (HAS_GLIDE and _HAS_SERVER):
        pytest.skip("VALKEY_TEST_SERVER not set or valkey-glide not installed")

    # Always default to env-configured (or localhost) connection
    yield _VALKEY_HOST, _VALKEY_PORT


@pytest_asyncio.fixture
async def valkey_env(
    request: pytest.FixtureRequest,
    valkey_server: tuple[str, int],
) -> AsyncIterator[_ValkeyEnv]:
    """Create a GlideClient and coco environment bound to the current event loop."""
    host, port = valkey_server
    config = GlideClientConfiguration([NodeAddress(host=host, port=port)])
    client = await GlideClient.create(config)

    coco_env = common.create_test_env(__file__, suffix=request.node.name)
    coco_env.context_provider.provide(_VALKEY_DB_KEY, client)

    yield _ValkeyEnv(client, coco_env)
    await client.close()


# =============================================================================
# Unit tests — no server needed
# =============================================================================


@requires_glide
class TestCreateClientConfig:
    """Tests for create_client_config helper."""

    def test_default_config(self) -> None:
        config = valkey.create_client_config()
        assert config is not None

    def test_custom_host_port(self) -> None:
        config = valkey.create_client_config("myhost", 7777)
        assert config is not None


@requires_glide
class TestVectorDef:
    """Tests for VectorDef construction."""

    def test_defaults(self) -> None:
        vd = valkey.VectorDef(schema=_VECTOR_SCHEMA)
        assert vd.distance == "cosine"
        assert vd.algorithm == "hnsw"

    def test_custom_values(self) -> None:
        vd = valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="l2", algorithm="flat")
        assert vd.distance == "l2"
        assert vd.algorithm == "flat"

    def test_ip_distance(self) -> None:
        vd = valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="ip")
        assert vd.distance == "ip"


@requires_glide
class TestFieldDef:
    """Tests for FieldDef construction."""

    def test_text_field(self) -> None:
        fd = valkey.FieldDef("content", "text")
        assert fd.name == "content"
        assert fd.type == "text"
        assert fd.sortable is False

    def test_tag_field(self) -> None:
        fd = valkey.FieldDef("category", "tag")
        assert fd.type == "tag"

    def test_numeric_sortable(self) -> None:
        fd = valkey.FieldDef("price", "numeric", sortable=True)
        assert fd.type == "numeric"
        assert fd.sortable is True


@requires_glide
class TestDocument:
    """Tests for Document construction."""

    def test_with_payload(self) -> None:
        doc = valkey.Document(id="d1", vector=[1.0, 2.0], payload={"k": "v"})
        assert doc.id == "d1"
        assert doc.vector == [1.0, 2.0]
        assert doc.payload == {"k": "v"}

    def test_without_payload(self) -> None:
        doc = valkey.Document(id="d2", vector=[1.0])
        assert doc.payload is None

    def test_numpy_vector(self) -> None:
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        doc = valkey.Document(id="d3", vector=arr)
        assert isinstance(doc.vector, np.ndarray)


@requires_glide
class TestInputValidation:
    """Tests for input validation on index_name and doc_id."""

    def test_valid_index_name(self) -> None:
        from cocoindex.connectors.valkey._target import _validate_name

        assert _validate_name("my-index_123", "index_name") == "my-index_123"

    def test_invalid_index_name_with_spaces(self) -> None:
        from cocoindex.connectors.valkey._target import _validate_name

        with pytest.raises(ValueError, match="index_name must contain only"):
            _validate_name("my index", "index_name")

    def test_invalid_index_name_with_colon(self) -> None:
        from cocoindex.connectors.valkey._target import _validate_name

        with pytest.raises(ValueError, match="index_name must contain only"):
            _validate_name("my:index", "index_name")

    def test_invalid_index_name_with_braces(self) -> None:
        from cocoindex.connectors.valkey._target import _validate_name

        with pytest.raises(ValueError, match="index_name must contain only"):
            _validate_name("my{index}", "index_name")

    def test_empty_name_rejected(self) -> None:
        from cocoindex.connectors.valkey._target import _validate_name

        with pytest.raises(ValueError, match="index_name must contain only"):
            _validate_name("", "index_name")


# =============================================================================
# Integration tests — require running Valkey with search module
# =============================================================================


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_create_index_and_insert_documents(valkey_env: _ValkeyEnv) -> None:
    """Test creating an index and inserting multiple documents."""
    index_name = _unique_name("test_create")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(
                    schema=_VECTOR_SCHEMA, distance="cosine", algorithm="hnsw"
                ),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_create_insert", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "hello"}
            ),
            valkey.Document(
                id="doc2", vector=_make_vector(_DIM, 2.0), payload={"text": "world"}
            ),
        ]
    )
    await app.update()

    client = valkey_env.client
    prefix = f"{index_name}:"
    assert await client.hget(f"{prefix}doc1", "text") in (b"hello", "hello")
    assert await client.hget(f"{prefix}doc2", "text") in (b"world", "world")

    # Verify vector stored correctly
    vec_blob = await client.hget(f"{prefix}doc1", "vector")
    assert vec_blob is not None
    assert _decode_vector(vec_blob, _DIM) == pytest.approx([1.0] * _DIM)  # type: ignore[arg-type]


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_numpy_vector(valkey_env: _ValkeyEnv) -> None:
    """Test inserting a document with a numpy array vector."""
    index_name = _unique_name("test_numpy")
    vec_arr = np.array([3.0, 4.0, 5.0, 6.0], dtype=np.float32)
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_numpy", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(valkey.Document(id="np1", vector=vec_arr, payload={"x": "y"}))
    await app.update()

    blob = await valkey_env.client.hget(f"{index_name}:np1", "vector")
    assert blob is not None
    assert _decode_vector(blob, _DIM) == pytest.approx([3.0, 4.0, 5.0, 6.0])  # type: ignore[arg-type]


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_document_without_payload(valkey_env: _ValkeyEnv) -> None:
    """Test inserting a document with no payload (vector only)."""
    index_name = _unique_name("test_nopayload")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_nopayload", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(valkey.Document(id="bare", vector=_make_vector(_DIM, 7.0)))
    await app.update()

    blob = await valkey_env.client.hget(f"{index_name}:bare", "vector")
    assert blob is not None
    # No payload fields stored
    text_val = await valkey_env.client.hget(f"{index_name}:bare", "text")
    assert text_val is None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_update_document(valkey_env: _ValkeyEnv) -> None:
    """Test updating a document's payload and vector."""
    index_name = _unique_name("test_update")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_update_doc", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(
        valkey.Document(
            id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "original"}
        )
    )
    await app.update()

    source_docs.clear()
    source_docs.append(
        valkey.Document(
            id="doc1", vector=_make_vector(_DIM, 9.0), payload={"text": "updated"}
        )
    )
    await app.update()

    client = valkey_env.client
    assert await client.hget(f"{index_name}:doc1", "text") in (b"updated", "updated")
    blob = await client.hget(f"{index_name}:doc1", "vector")
    assert _decode_vector(blob, _DIM) == pytest.approx([9.0] * _DIM)  # type: ignore[arg-type]


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_update_document_removes_stale_fields(valkey_env: _ValkeyEnv) -> None:
    """Test that updating a document removes payload fields no longer present."""
    index_name = _unique_name("test_stale")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_stale_fields", environment=valkey_env.coco_env),
        declare_fn,
    )

    # First version has two payload fields
    source_docs.append(
        valkey.Document(
            id="doc1",
            vector=_make_vector(_DIM, 1.0),
            payload={"text": "hello", "category": "greeting"},
        )
    )
    await app.update()

    client = valkey_env.client
    assert await client.hget(f"{index_name}:doc1", "category") in (
        b"greeting",
        "greeting",
    )

    # Update: remove the "category" field from payload
    source_docs.clear()
    source_docs.append(
        valkey.Document(
            id="doc1",
            vector=_make_vector(_DIM, 2.0),
            payload={"text": "updated"},
        )
    )
    await app.update()

    # "category" should no longer exist in the hash
    assert await client.hget(f"{index_name}:doc1", "text") in (b"updated", "updated")
    assert await client.hget(f"{index_name}:doc1", "category") is None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_delete_document(valkey_env: _ValkeyEnv) -> None:
    """Test deleting a document when no longer declared."""
    index_name = _unique_name("test_delete")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_delete_doc", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "keep"}
            ),
            valkey.Document(
                id="doc2", vector=_make_vector(_DIM, 2.0), payload={"text": "remove"}
            ),
        ]
    )
    await app.update()

    source_docs.clear()
    source_docs.append(
        valkey.Document(
            id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "keep"}
        )
    )
    await app.update()

    client = valkey_env.client
    assert await client.hget(f"{index_name}:doc1", "text") is not None
    assert await client.hget(f"{index_name}:doc2", "text") is None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_no_change_optimization(valkey_env: _ValkeyEnv) -> None:
    """Test that unchanged data doesn't trigger writes."""
    index_name = _unique_name("test_nochange")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_no_change", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(
        valkey.Document(
            id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "stable"}
        )
    )
    await app.update()
    await app.update()  # Second run — no-op

    assert await valkey_env.client.hget(f"{index_name}:doc1", "text") in (
        b"stable",
        "stable",
    )


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_flat_algorithm(valkey_env: _ValkeyEnv) -> None:
    """Test creating an index with FLAT algorithm."""
    index_name = _unique_name("test_flat")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(
                    schema=_VECTOR_SCHEMA, distance="l2", algorithm="flat"
                ),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_flat", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(
        valkey.Document(
            id="doc1", vector=_make_vector(_DIM, 1.0), payload={"text": "flat"}
        )
    )
    await app.update()

    assert await valkey_env.client.hget(f"{index_name}:doc1", "text") is not None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_ip_distance_metric(valkey_env: _ValkeyEnv) -> None:
    """Test creating an index with inner product distance metric."""
    index_name = _unique_name("test_ip")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="ip"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_ip", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(valkey.Document(id="d1", vector=_make_vector(_DIM, 1.0)))
    await app.update()
    assert await valkey_env.client.hget(f"{index_name}:d1", "vector") is not None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_drop_index_when_not_declared(valkey_env: _ValkeyEnv) -> None:
    """Test that index is dropped when no longer declared."""
    index_name = _unique_name("test_drop")
    source_docs: list[valkey.Document] = []
    declare_index = True

    async def declare_fn() -> None:
        if not declare_index:
            return
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_drop_index", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(valkey.Document(id="d1", vector=_make_vector(_DIM, 1.0)))
    await app.update()

    info = await glide_ft.info(valkey_env.client, index_name)
    assert info is not None

    declare_index = False
    source_docs.clear()
    await app.update()

    try:
        await glide_ft.info(valkey_env.client, index_name)
        pytest.fail("Index should have been dropped")
    except Exception:
        pass


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_schema_change_triggers_index_recreation(valkey_env: _ValkeyEnv) -> None:
    """Test that changing schema (e.g. distance metric) recreates the index."""
    index_name = _unique_name("test_schema_change")
    source_docs: list[valkey.Document] = []
    use_l2 = False

    async def declare_fn() -> None:
        dist: Any = "l2" if use_l2 else "cosine"
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance=dist),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_schema_change", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.append(
        valkey.Document(id="d1", vector=_make_vector(_DIM, 1.0), payload={"v": "1"})
    )
    await app.update()
    assert await valkey_env.client.hget(f"{index_name}:d1", "v") is not None

    # Change distance metric — should recreate index and re-insert
    use_l2 = True
    await app.update()
    # Document should still be accessible after recreation
    assert await valkey_env.client.hget(f"{index_name}:d1", "v") is not None


# =============================================================================
# Field indexing and search/filter tests
# =============================================================================


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_text_field_search(valkey_env: _ValkeyEnv) -> None:
    """Test full-text search on a TEXT indexed field."""
    index_name = _unique_name("test_text_search")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
                fields=[valkey.FieldDef("content", "text")],
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_text_search", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="d1",
                vector=_make_vector(_DIM, 1.0),
                payload={"content": "machine learning algorithms"},
            ),
            valkey.Document(
                id="d2",
                vector=_make_vector(_DIM, 2.0),
                payload={"content": "deep learning neural networks"},
            ),
            valkey.Document(
                id="d3",
                vector=_make_vector(_DIM, 3.0),
                payload={"content": "database management systems"},
            ),
        ]
    )
    await app.update()
    await _wait_for_index_count(valkey_env.client, index_name, 3)

    # Search for "learning" — should match d1 and d2
    result = await glide_ft.search(valkey_env.client, index_name, "@content:learning")
    assert isinstance(result, list)
    assert result[0] == 2


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_tag_field_filter(valkey_env: _ValkeyEnv) -> None:
    """Test exact-match filtering on a TAG indexed field."""
    index_name = _unique_name("test_tag_filter")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
                fields=[valkey.FieldDef("category", "tag")],
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_tag_filter", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="d1", vector=_make_vector(_DIM, 1.0), payload={"category": "sports"}
            ),
            valkey.Document(
                id="d2", vector=_make_vector(_DIM, 2.0), payload={"category": "tech"}
            ),
            valkey.Document(
                id="d3", vector=_make_vector(_DIM, 3.0), payload={"category": "sports"}
            ),
        ]
    )
    await app.update()
    await _wait_for_index_count(valkey_env.client, index_name, 3)

    # Filter by tag — should match d1 and d3
    result = await glide_ft.search(valkey_env.client, index_name, "@category:{sports}")
    assert isinstance(result, list)
    assert result[0] == 2

    # Filter for tech — should match only d2
    result2 = await glide_ft.search(valkey_env.client, index_name, "@category:{tech}")
    assert isinstance(result2, list)
    assert result2[0] == 1


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_numeric_field_range_filter(valkey_env: _ValkeyEnv) -> None:
    """Test numeric range filtering on a NUMERIC indexed field."""
    index_name = _unique_name("test_numeric")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
                fields=[valkey.FieldDef("price", "numeric", sortable=True)],
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_numeric", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="cheap", vector=_make_vector(_DIM, 1.0), payload={"price": "5"}
            ),
            valkey.Document(
                id="mid", vector=_make_vector(_DIM, 2.0), payload={"price": "50"}
            ),
            valkey.Document(
                id="expensive", vector=_make_vector(_DIM, 3.0), payload={"price": "500"}
            ),
        ]
    )
    await app.update()
    await _wait_for_index_count(valkey_env.client, index_name, 3)

    # Range: price >= 10 AND price <= 100
    result = await glide_ft.search(valkey_env.client, index_name, "@price:[10 100]")
    assert isinstance(result, list)
    assert result[0] == 1  # Only "mid" (50)

    # Range: price >= 50
    result2 = await glide_ft.search(valkey_env.client, index_name, "@price:[50 +inf]")
    assert isinstance(result2, list)
    assert result2[0] == 2  # "mid" and "expensive"


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_combined_field_filters(valkey_env: _ValkeyEnv) -> None:
    """Test combining multiple field filters in one query."""
    index_name = _unique_name("test_combined")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
                fields=[
                    valkey.FieldDef("category", "tag"),
                    valkey.FieldDef("price", "numeric"),
                ],
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_combined", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(
                id="d1",
                vector=_make_vector(_DIM, 1.0),
                payload={"category": "electronics", "price": "100"},
            ),
            valkey.Document(
                id="d2",
                vector=_make_vector(_DIM, 2.0),
                payload={"category": "electronics", "price": "500"},
            ),
            valkey.Document(
                id="d3",
                vector=_make_vector(_DIM, 3.0),
                payload={"category": "clothing", "price": "50"},
            ),
        ]
    )
    await app.update()
    await _wait_for_index_count(valkey_env.client, index_name, 3)

    # electronics AND price >= 200
    result = await glide_ft.search(
        valkey_env.client, index_name, "@category:{electronics} @price:[200 +inf]"
    )
    assert isinstance(result, list)
    assert result[0] == 1  # Only d2


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_vector_knn_search(valkey_env: _ValkeyEnv) -> None:
    """Test KNN vector similarity search via FT.SEARCH."""
    index_name = _unique_name("test_knn")
    source_docs: list[valkey.Document] = []

    async def declare_fn() -> None:
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_knn", environment=valkey_env.coco_env),
        declare_fn,
    )

    source_docs.extend(
        [
            valkey.Document(id="a", vector=[1.0, 0.0, 0.0, 0.0]),
            valkey.Document(id="b", vector=[0.0, 1.0, 0.0, 0.0]),
            valkey.Document(id="c", vector=[0.9, 0.1, 0.0, 0.0]),  # Closest to query
        ]
    )
    await app.update()
    await _wait_for_index_count(valkey_env.client, index_name, 3)

    # KNN search for vector closest to [1, 0, 0, 0]
    from glide import FtSearchOptions

    query_vec = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)
    result = await glide_ft.search(
        valkey_env.client,
        index_name,
        "*=>[KNN 2 @vector $query_vec]",
        options=FtSearchOptions(params={b"query_vec": query_vec}),
    )
    assert isinstance(result, list)
    assert result[0] == 2  # Returns top 2


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_multiple_indexes(valkey_env: _ValkeyEnv) -> None:
    """Test managing multiple indexes in the same Valkey instance."""
    idx1 = _unique_name("test_multi1")
    idx2 = _unique_name("test_multi2")
    docs1: list[valkey.Document] = []
    docs2: list[valkey.Document] = []

    async def declare_fn() -> None:
        index1 = await coco.use_mount(
            coco.component_subpath("setup", "index1"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            idx1,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        index2 = await coco.use_mount(
            coco.component_subpath("setup", "index2"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            idx2,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="l2"),
            ),
        )
        for doc in docs1:
            index1.declare_document(doc)
        for doc in docs2:
            index2.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_multi", environment=valkey_env.coco_env),
        declare_fn,
    )

    docs1.append(
        valkey.Document(id="a", vector=_make_vector(_DIM, 1.0), payload={"src": "idx1"})
    )
    docs2.append(
        valkey.Document(id="b", vector=_make_vector(_DIM, 2.0), payload={"src": "idx2"})
    )
    await app.update()

    client = valkey_env.client
    assert await client.hget(f"{idx1}:a", "src") in (b"idx1", "idx1")
    assert await client.hget(f"{idx2}:b", "src") in (b"idx2", "idx2")
    # Keys are isolated by prefix
    assert await client.hget(f"{idx1}:b", "src") is None
    assert await client.hget(f"{idx2}:a", "src") is None


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_field_schema_change_recreates_index(valkey_env: _ValkeyEnv) -> None:
    """Test that adding/removing indexed fields triggers index recreation."""
    index_name = _unique_name("test_field_change")
    source_docs: list[valkey.Document] = []
    use_fields = False

    async def declare_fn() -> None:
        fields = [valkey.FieldDef("category", "tag")] if use_fields else None
        index = await coco.use_mount(
            coco.component_subpath("setup", "index"),
            valkey.declare_index_target,
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
                fields=fields,
            ),
        )
        for doc in source_docs:
            index.declare_document(doc)

    app = coco.App(
        coco.AppConfig(name="test_field_change", environment=valkey_env.coco_env),
        declare_fn,
    )

    # First: create index without fields
    source_docs.append(
        valkey.Document(
            id="d1", vector=_make_vector(_DIM, 1.0), payload={"category": "x"}
        )
    )
    await app.update()

    # Now add a field — should trigger recreation
    use_fields = True
    await app.update()

    # Document should still exist after recreation
    assert await valkey_env.client.hget(f"{index_name}:d1", "category") is not None

    # And the field should now be searchable
    await _wait_for_index_count(valkey_env.client, index_name, 1)
    result = await glide_ft.search(valkey_env.client, index_name, "@category:{x}")
    assert isinstance(result, list)
    assert result[0] == 1


@requires_glide
@requires_server
@pytest.mark.asyncio
async def test_mount_index_target(valkey_env: _ValkeyEnv) -> None:
    """Test the mount_index_target convenience wrapper."""
    index_name = _unique_name("test_mount")

    async def declare_fn() -> None:
        index = await valkey.mount_index_target(
            _VALKEY_DB_KEY,
            index_name,
            await valkey.IndexSchema.create(
                vectors=valkey.VectorDef(schema=_VECTOR_SCHEMA, distance="cosine"),
            ),
        )
        index.declare_document(
            valkey.Document(id="m1", vector=_make_vector(_DIM, 1.0), payload={"k": "v"})
        )

    app = coco.App(
        coco.AppConfig(name="test_mount", environment=valkey_env.coco_env),
        declare_fn,
    )
    await app.update()

    assert await valkey_env.client.hget(f"{index_name}:m1", "k") in (b"v", "v")
