"""Tests for Qdrant target connector.

Helper-level tests run without a Qdrant service.

Live tests are gated on the ``QDRANT_URL`` env var; they are skipped when it
isn't set.
"""

from __future__ import annotations

import os
import uuid
from typing import cast

import pytest

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models

    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

requires_qdrant = pytest.mark.skipif(
    not HAS_QDRANT, reason="qdrant-client is not installed"
)

if HAS_QDRANT:
    import numpy as np

    import cocoindex as coco
    from cocoindex.connectors import qdrant
    import msgspec

    from cocoindex._internal import serde
    from cocoindex.connectors.qdrant._target import (
        _CollectionHandler,
        _CollectionTrackingRecordCore,
        _PointHandler,
        _ResolvedQdrantNamedVectorsDef,
        _ResolvedQdrantVectorDef,
        _sparse_vector_params_from_def,
        _distance_from_spec,
        _multivector_comparator,
        _validate_point_id,
        _vector_params_from_def,
    )
    from cocoindex.resources.schema import MultiVectorSchema, VectorSchema
    from tests import common

requires_qdrant_url = pytest.mark.skipif(
    not os.environ.get("QDRANT_URL"), reason="QDRANT_URL is not set"
)

# A fixed valid point ID for tests (Qdrant only accepts u64 ints and UUIDs).
_POINT_UUID = "550e8400-e29b-41d4-a716-446655440000"


# =============================================================================
# Unit tests — _distance_from_spec (no service needed)
# =============================================================================


@requires_qdrant
class TestDistanceFromSpec:
    def test_cosine(self) -> None:
        assert _distance_from_spec("cosine") == qdrant_models.Distance.COSINE

    def test_dot(self) -> None:
        assert _distance_from_spec("dot") == qdrant_models.Distance.DOT

    def test_dotproduct_alias(self) -> None:
        assert _distance_from_spec("dotproduct") == qdrant_models.Distance.DOT

    def test_euclid(self) -> None:
        assert _distance_from_spec("euclid") == qdrant_models.Distance.EUCLID

    def test_euclidean_alias(self) -> None:
        assert _distance_from_spec("euclidean") == qdrant_models.Distance.EUCLID

    def test_l2_alias(self) -> None:
        assert _distance_from_spec("l2") == qdrant_models.Distance.EUCLID

    def test_case_insensitive(self) -> None:
        assert _distance_from_spec("COSINE") == qdrant_models.Distance.COSINE
        assert _distance_from_spec("DOT") == qdrant_models.Distance.DOT
        assert _distance_from_spec("EUCLID") == qdrant_models.Distance.EUCLID

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported Qdrant distance metric"):
            _distance_from_spec("manhattan")


# =============================================================================
# Unit tests — _multivector_comparator (no service needed)
# =============================================================================


@requires_qdrant
class TestMultivectorComparator:
    def test_max_sim(self) -> None:
        result = _multivector_comparator("max_sim")
        assert result == qdrant_models.MultiVectorComparator.MAX_SIM

    def test_case_insensitive(self) -> None:
        result = _multivector_comparator("MAX_SIM")
        assert result == qdrant_models.MultiVectorComparator.MAX_SIM

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported multivector comparator"):
            _multivector_comparator("min_sim")


# =============================================================================
# Unit tests — _vector_params_from_def (no service needed)
# =============================================================================


@requires_qdrant
class TestVectorParamsFromDef:
    def test_vector_schema_cosine(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=128),
            distance="cosine",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 128
        assert params.distance == qdrant_models.Distance.COSINE
        assert params.multivector_config is None

    def test_vector_schema_dot(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=64),
            distance="dot",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 64
        assert params.distance == qdrant_models.Distance.DOT
        assert params.multivector_config is None

    def test_vector_schema_euclid(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=32),
            distance="euclid",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 32
        assert params.distance == qdrant_models.Distance.EUCLID
        assert params.multivector_config is None

    def test_multivector_schema(self) -> None:
        inner = VectorSchema(dtype=np.dtype(np.float32), size=256)
        multi_schema = MultiVectorSchema(vector_schema=inner)
        vector_def = _ResolvedQdrantVectorDef(
            schema=multi_schema,
            distance="cosine",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 256
        assert params.distance == qdrant_models.Distance.COSINE
        assert params.multivector_config is not None
        assert (
            params.multivector_config.comparator
            == qdrant_models.MultiVectorComparator.MAX_SIM
        )


# =============================================================================
# Unit tests — sparse vectors
# =============================================================================


@requires_qdrant
class TestSparseVectorSupport:
    @pytest.mark.asyncio
    async def test_collection_schema_create_resolves_sparse_vector_params(self) -> None:
        schema = await qdrant.CollectionSchema.create(
            vectors={
                "dense": qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                )
            },
            sparse_vectors={"sparse": qdrant.QdrantSparseVectorDef(modifier="idf")},
        )

        assert schema.sparse_vectors is not None
        sparse_def = schema.sparse_vectors.sparse_vectors["sparse"]
        params = _sparse_vector_params_from_def(sparse_def)
        assert isinstance(params, qdrant_models.SparseVectorParams)
        assert params.modifier == qdrant_models.Modifier.IDF

    @pytest.mark.asyncio
    async def test_create_collection_forwards_sparse_vectors_config(self) -> None:
        class FakeQdrantClient:
            def __init__(self) -> None:
                self.create_kwargs: dict[str, object] | None = None

            def create_collection(self, **kwargs: object) -> bool:
                self.create_kwargs = kwargs
                return True

        schema = await qdrant.CollectionSchema.create(
            vectors={
                "dense": qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                )
            },
            sparse_vectors={"sparse": qdrant.QdrantSparseVectorDef(modifier="idf")},
        )
        client = FakeQdrantClient()

        await _CollectionHandler()._create_collection(
            client,  # type: ignore[arg-type]
            "test_sparse_config",
            schema,
            if_not_exists=False,
        )

        assert client.create_kwargs is not None
        sparse_config = client.create_kwargs["sparse_vectors_config"]
        assert isinstance(sparse_config, dict)
        assert set(sparse_config) == {"sparse"}
        assert isinstance(sparse_config["sparse"], qdrant_models.SparseVectorParams)
        assert sparse_config["sparse"].modifier == qdrant_models.Modifier.IDF

    def test_point_fingerprint_changes_when_sparse_vector_changes(self) -> None:
        handler = _PointHandler(
            client=cast(QdrantClient, object()),
            collection_name="test_sparse_fingerprint",
        )
        point_v1 = qdrant.PointStruct(
            id=_POINT_UUID,
            vector={
                "dense": [0.1, 0.2, 0.3, 0.4],
                "sparse": qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 0.9]),
            },
            payload={"text": "hello"},
        )
        point_v2 = qdrant.PointStruct(
            id=_POINT_UUID,
            vector={
                "dense": [0.1, 0.2, 0.3, 0.4],
                "sparse": qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 1.1]),
            },
            payload={"text": "hello"},
        )

        out_v1 = handler.reconcile(_POINT_UUID, point_v1, [], True)
        out_v2 = handler.reconcile(_POINT_UUID, point_v2, [], True)

        assert out_v1 is not None
        assert out_v2 is not None
        assert out_v1.tracking_record != out_v2.tracking_record, (
            "sparse vector indices/values must participate in point change detection"
        )


# =============================================================================
# Unit tests — point ID validation (mirrors Qdrant's server-side rules)
# =============================================================================


@requires_qdrant
class TestPointIdValidation:
    """Matrix verified against a live Qdrant 1.18 server over REST and gRPC."""

    def test_valid_ids_pass_through(self) -> None:
        assert _validate_point_id(0) == 0
        assert _validate_point_id(2**64 - 1) == 2**64 - 1
        assert _validate_point_id(_POINT_UUID) == _POINT_UUID
        hex_form = uuid.UUID(_POINT_UUID).hex
        assert _validate_point_id(hex_form) == hex_form
        urn_form = f"urn:uuid:{_POINT_UUID}"
        assert _validate_point_id(urn_form) == urn_form

    def test_uuid_instance_converted_to_string(self) -> None:
        assert _validate_point_id(uuid.UUID(_POINT_UUID)) == _POINT_UUID

    def test_arbitrary_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="strings must be UUIDs"):
            _validate_point_id("chunk-1")

    def test_out_of_range_ints_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsigned 64-bit range"):
            _validate_point_id(-1)
        with pytest.raises(ValueError, match="unsigned 64-bit range"):
            _validate_point_id(2**64)

    def test_other_types_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid Qdrant point ID of type"):
            _validate_point_id(1.5)


# =============================================================================
# Unit tests — tracking record upgrade compatibility
# =============================================================================


@requires_qdrant
class TestTrackingRecordUpgradeCompat:
    def test_pre_sparse_tracking_record_decodes_equal_to_dense_only(self) -> None:
        """Records written before sparse-vector support must decode equal to a
        new dense-only record — otherwise every existing collection would be
        destructively replaced (and its data lost) on upgrade."""

        class PreSparseCore(msgspec.Struct, frozen=True, array_like=True):
            vectors: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef

        dense = _ResolvedQdrantNamedVectorsDef(
            vectors={
                "dense": _ResolvedQdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=4),
                    distance="cosine",
                    multivector_comparator="max_sim",
                )
            }
        )
        old_bytes = serde._msgspec_encoder.encode(PreSparseCore(vectors=dense))
        decoded = msgspec.msgpack.Decoder(
            type=_CollectionTrackingRecordCore,
            ext_hook=serde._ext_hook,
            dec_hook=serde._dec_hook,
        ).decode(old_bytes)
        assert decoded == _CollectionTrackingRecordCore(
            vectors=dense, sparse_vectors=None
        )


# =============================================================================
# Live test — Qdrant service required
# =============================================================================


@requires_qdrant
@requires_qdrant_url
def test_live_dense_sparse_vectors_and_hybrid_query() -> None:
    qdrant_url = os.environ["QDRANT_URL"]
    client = qdrant.create_client(qdrant_url, prefer_grpc=True)
    collection_name = f"coco_sparse_{uuid.uuid4().hex}"
    db_key = coco.ContextKey[QdrantClient](f"test_qdrant_sparse_{uuid.uuid4().hex}")
    env = common.create_test_env(__file__, suffix=collection_name)
    env.context_provider.provide(db_key, client)

    @coco.fn
    async def app_main() -> None:
        target = await qdrant.mount_collection_target(
            db_key,
            collection_name,
            await qdrant.CollectionSchema.create(
                vectors={
                    "dense": qdrant.QdrantVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                    )
                },
                sparse_vectors={"sparse": qdrant.QdrantSparseVectorDef(modifier="idf")},
            ),
        )
        target.declare_point(
            qdrant.PointStruct(
                id=1,
                vector={
                    "dense": [0.1, 0.2, 0.3, 0.4],
                    "sparse": qdrant_models.SparseVector(
                        indices=[1, 7], values=[0.5, 0.9]
                    ),
                },
                payload={"text": "hybrid sparse dense"},
            )
        )

    app = coco.App(
        coco.AppConfig(name="test_qdrant_sparse_hybrid", environment=env),
        app_main,
    )

    try:
        app.update_blocking()

        dense_result = client.query_points(
            collection_name=collection_name,
            query=[0.1, 0.2, 0.3, 0.4],
            using="dense",
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in dense_result.points] == [1]

        sparse_query = qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 0.9])
        sparse_result = client.query_points(
            collection_name=collection_name,
            query=sparse_query,
            using="sparse",
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in sparse_result.points] == [1]

        hybrid_result = client.query_points(
            collection_name=collection_name,
            prefetch=[
                qdrant_models.Prefetch(
                    query=[0.1, 0.2, 0.3, 0.4], using="dense", limit=10
                ),
                qdrant_models.Prefetch(query=sparse_query, using="sparse", limit=10),
            ],
            query=qdrant_models.FusionQuery(fusion=qdrant_models.Fusion.RRF),
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in hybrid_result.points] == [1]
    finally:
        try:
            client.delete_collection(collection_name=collection_name)
        except Exception:
            pass
