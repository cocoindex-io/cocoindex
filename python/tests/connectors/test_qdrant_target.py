"""Tests for Qdrant target connector.

Helper-level tests run without a Qdrant service.

Live tests are gated on the ``QDRANT_URL`` env var; they are skipped when it
isn't set.
"""

from __future__ import annotations

import os

import httpx
import uuid
from typing import Any, NamedTuple, cast

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
    from cocoindex.connectors.qdrant import _target as qdrant_target
    from cocoindex.connectorkits import statediff
    from cocoindex.connectorkits.target import ManagedBy
    from cocoindex.connectors.qdrant._target import (
        _CollectionHandler,
        _CollectionSpec,
        _CollectionTrackingRecordCore,
        _PointHandler,
        _ResolvedQdrantNamedVectorsDef,
        _ResolvedQdrantSparseVectorDef,
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
                ),
                "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
            },
        )

        assert isinstance(schema.vectors, _ResolvedQdrantNamedVectorsDef)
        sparse_def = schema.vectors.vectors["sparse"]
        assert isinstance(sparse_def, _ResolvedQdrantSparseVectorDef)
        params = _sparse_vector_params_from_def(sparse_def)
        assert isinstance(params, qdrant_models.SparseVectorParams)
        assert params.modifier == qdrant_models.Modifier.IDF

    @pytest.mark.asyncio
    async def test_sparse_only_schema_and_bare_sparse_rejected(self) -> None:
        schema = await qdrant.CollectionSchema.create(
            vectors={"sparse": qdrant.QdrantSparseVectorDef()},
        )
        assert isinstance(schema.vectors, _ResolvedQdrantNamedVectorsDef)

        with pytest.raises(ValueError, match="always named"):
            await qdrant.CollectionSchema.create(
                vectors=qdrant.QdrantSparseVectorDef(),  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError, match="at least one vector"):
            await qdrant.CollectionSchema.create()

    @pytest.mark.asyncio
    async def test_create_collection_splits_dense_and_sparse_configs(self) -> None:
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
                ),
                "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
            },
        )
        client = FakeQdrantClient()

        await _CollectionHandler()._create_collection(
            client,  # type: ignore[arg-type]
            "test_sparse_config",
            schema,
            if_not_exists=False,
        )

        assert client.create_kwargs is not None
        dense_config = client.create_kwargs["vectors_config"]
        assert isinstance(dense_config, dict)
        assert set(dense_config) == {"dense"}
        sparse_config = client.create_kwargs["sparse_vectors_config"]
        assert isinstance(sparse_config, dict)
        assert set(sparse_config) == {"sparse"}
        assert isinstance(sparse_config["sparse"], qdrant_models.SparseVectorParams)
        assert sparse_config["sparse"].modifier == qdrant_models.Modifier.IDF

    @pytest.mark.asyncio
    async def test_create_collection_sparse_only_passes_no_dense_config(self) -> None:
        class FakeQdrantClient:
            def __init__(self) -> None:
                self.create_kwargs: dict[str, object] | None = None

            def create_collection(self, **kwargs: object) -> bool:
                self.create_kwargs = kwargs
                return True

        schema = await qdrant.CollectionSchema.create(
            vectors={"sparse": qdrant.QdrantSparseVectorDef()},
        )
        client = FakeQdrantClient()
        await _CollectionHandler()._create_collection(
            client,  # type: ignore[arg-type]
            "test_sparse_only",
            schema,
            if_not_exists=False,
        )
        assert client.create_kwargs is not None
        assert client.create_kwargs["vectors_config"] is None
        sparse_config = client.create_kwargs["sparse_vectors_config"]
        assert isinstance(sparse_config, dict)
        assert set(sparse_config) == {"sparse"}

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

    def test_uuid_text_forms_canonicalized(self) -> None:
        # Qdrant stores all of these as the same point.
        for form in (
            _POINT_UUID.upper(),
            uuid.UUID(_POINT_UUID).hex,
            f"urn:uuid:{_POINT_UUID}",
        ):
            assert _validate_point_id(form) == _POINT_UUID

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
# Unit tests — vectors checked when a point is declared (no service needed)
# =============================================================================


class _Sparse(NamedTuple):
    """A sparse vector as a test parameter, built into a ``SparseVector`` in
    the test (``qdrant_models`` exists only when qdrant-client is installed)."""

    indices: list[int]
    values: list[float]


class _UnusedProvider:
    """A provider a test never reaches: the declaration fails before it."""

    def target_state(self, *args: Any) -> Any:
        raise AssertionError("the declaration should have been refused")


def _dense_def(size: int = 3) -> "_ResolvedQdrantVectorDef":
    return _ResolvedQdrantVectorDef(
        schema=VectorSchema(dtype=np.dtype(np.float32), size=size),
        distance="cosine",
        multivector_comparator="max_sim",
    )


def _multi_def(size: int = 3) -> "_ResolvedQdrantVectorDef":
    return _ResolvedQdrantVectorDef(
        schema=MultiVectorSchema(
            vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=size)
        ),
        distance="cosine",
        multivector_comparator="max_sim",
    )


@requires_qdrant
class TestPointVectorCheck:
    """A vector that does not fit the collection fails its own declaration,
    rather than the whole batch it would be written in (or, for a NaN, every
    score it is in)."""

    @pytest.mark.parametrize(
        ("vector", "message"),
        [
            ([1.0, 2.0], "vector has 2 elements but the collection expects 3"),
            (
                [1.0, 2.0, 3.0, 4.0],
                "vector has 4 elements but the collection expects 3",
            ),
            ([], "vector has 0 elements but the collection expects 3"),
            ([1.0, float("nan"), 0.0], r"vector contains a non-finite \(NaN/Inf\)"),
            ([float("-inf"), 0.0, 0.0], r"vector contains a non-finite \(NaN/Inf\)"),
            ([[1.0, 2.0, 3.0]], "vector is a multivector but the collection expects"),
        ],
    )
    def test_single_vector(self, vector: Any, message: str) -> None:
        target = qdrant.CollectionTarget(
            _UnusedProvider(),  # type: ignore[arg-type]
            qdrant.CollectionSchema(_dense_def()),
        )
        with pytest.raises(ValueError, match=f"Qdrant point 7: {message}"):
            target.declare_point(qdrant_models.PointStruct(id=7, vector=vector))

    @pytest.mark.parametrize(
        ("vector", "message"),
        [
            ([], "vector is an empty multivector"),
            ([1.0, 2.0, 3.0], "vector is a single vector but the collection"),
            ([[1.0, 2.0, 3.0], [1.0, 2.0]], "vector has 2 elements but"),
            ([[1.0, 2.0, float("nan")]], "vector contains a non-finite"),
        ],
    )
    def test_multivector(self, vector: Any, message: str) -> None:
        target = qdrant.CollectionTarget(
            _UnusedProvider(),  # type: ignore[arg-type]
            qdrant.CollectionSchema(_multi_def()),
        )
        with pytest.raises(ValueError, match=f"Qdrant point 7: {message}"):
            target.declare_point(qdrant_models.PointStruct(id=7, vector=vector))

    @pytest.mark.parametrize(
        ("vector", "message"),
        [
            ({"dense": [1.0, 2.0]}, "vector field 'dense' has 2 elements"),
            ({"other": [1.0, 2.0, 3.0]}, "vector name 'other' is not declared"),
            (
                {"sparse": _Sparse(indices=[1, 2], values=[1.0])},
                "vector field 'sparse' has 2 indices but 1 values",
            ),
            (
                {"sparse": _Sparse(indices=[1, 1], values=[1.0, 2.0])},
                "vector field 'sparse' repeats an index",
            ),
            (
                {"sparse": _Sparse(indices=[1], values=[float("nan")])},
                "vector field 'sparse' contains a non-finite",
            ),
            ({"sparse": [1.0, 2.0, 3.0]}, "vector field 'sparse' is dense but"),
            (
                {"dense": _Sparse(indices=[1], values=[1.0])},
                "vector field 'dense' is sparse but",
            ),
            ([1.0, 2.0, 3.0], "vector is unnamed but the collection declares named"),
        ],
    )
    def test_named_vectors(self, vector: Any, message: str) -> None:
        target = qdrant.CollectionTarget(
            _UnusedProvider(),  # type: ignore[arg-type]
            qdrant.CollectionSchema(
                _ResolvedQdrantNamedVectorsDef(
                    vectors={
                        "dense": _dense_def(),
                        "sparse": _ResolvedQdrantSparseVectorDef(),
                    }
                )
            ),
        )
        if isinstance(vector, dict):
            vector = {
                name: qdrant_models.SparseVector(indices=v.indices, values=v.values)
                if isinstance(v, _Sparse)
                else v
                for name, v in vector.items()
            }
        with pytest.raises(ValueError, match=f"Qdrant point 7: {message}"):
            target.declare_point(qdrant_models.PointStruct(id=7, vector=vector))

    def test_vectors_that_fit_pass(self) -> None:
        check = qdrant_target._check_point_vectors
        check(1, [1.0, 2.0, 3.0], _dense_def())
        check(1, [[1.0, 2.0, 3.0], [0.0, 0.0, 1.0]], _multi_def())
        named = _ResolvedQdrantNamedVectorsDef(
            vectors={"dense": _dense_def(), "sparse": _ResolvedQdrantSparseVectorDef()}
        )
        sparse = qdrant_models.SparseVector(indices=[3, 1], values=[0.5, 1.0])
        check(1, {"dense": [1.0, 2.0, 3.0], "sparse": sparse}, named)
        # A point may leave out a named vector, and Qdrant computes the
        # vectors of an inference object itself.
        check(1, {"sparse": sparse}, named)
        check(1, {"dense": qdrant_models.Document(text="hi", model="m")}, named)


class _RecordingQdrantClient:
    """Stands in for ``QdrantClient``: records every write."""

    def __init__(self) -> None:
        self.upserts: list[list[Any]] = []
        self.deletes: list[list[Any]] = []

    def upsert(self, *, collection_name: str, points: list[Any]) -> None:
        self.upserts.append(points)

    def delete(self, *, collection_name: str, points_selector: Any) -> None:
        self.deletes.append(points_selector.points)


@requires_qdrant
class TestWriteRequestSize:
    @pytest.mark.asyncio
    async def test_large_batch_is_split_under_the_request_limit(self) -> None:
        """A batch of about 40 MiB, over Qdrant's default 32 MiB request limit
        (enforced on REST), goes out in several requests under it, every point
        and id once, in order."""
        from cocoindex.connectors.qdrant._target import _PointAction

        limit = 32 * 1024 * 1024
        client = _RecordingQdrantClient()
        handler = _PointHandler(client, "docs")  # type: ignore[arg-type]
        text = "x" * (1024 * 1024)
        upserts = [
            _PointAction(
                point_id=i,
                point=qdrant_models.PointStruct(
                    id=i, vector=[1.0, 0.0, 0.0], payload={"text": text}
                ),
            )
            for i in range(40)
        ]
        await handler._apply_actions(None, upserts)  # type: ignore[arg-type]
        assert len(client.upserts) > 1
        for points in client.upserts:
            assert sum(len(p.model_dump_json()) for p in points) <= limit
        assert [p.id for points in client.upserts for p in points] == list(range(40))

        # Half a million UUID deletes: about 20 MiB of ids, over the 16 MiB a
        # request carries.
        ids = [str(uuid.UUID(int=i)) for i in range(500_000)]
        await handler._apply_actions(
            None,  # type: ignore[arg-type]
            [_PointAction(point_id=i, point=None) for i in ids],
        )
        assert len(client.deletes) > 1
        for batch in client.deletes:
            assert sum(len(str(i)) + 3 for i in batch) <= limit
        assert [i for batch in client.deletes for i in batch] == ids

    @pytest.mark.asyncio
    async def test_points_are_sized_without_encoding_their_vectors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A point is sized from its vectors' length, never by encoding it, and
        the estimate is above its real size: requests stay under the limit."""
        from cocoindex.connectors.qdrant._target import _PointAction

        limit = 16 * 1024 * 1024
        dim = 100_000
        vectors: list[Any] = [
            [-0.12345678901234567] * dim,
            {"dense": [1e-300] * dim, "multi": [[0.5] * (dim // 4)] * 4},
            {"sparse": qdrant_models.SparseVector(indices=[1, 2], values=[0.1, 0.2])},
        ]
        points = [
            qdrant_models.PointStruct(
                id=i, vector=vectors[i % len(vectors)], payload={"n": i}
            )
            for i in range(12)
        ]
        real_sizes = [len(p.model_dump_json()) for p in points]

        def no_encoding(self: Any, **kwargs: Any) -> str:
            raise AssertionError("a point was encoded to size it")

        monkeypatch.setattr(qdrant_models.PointStruct, "model_dump_json", no_encoding)
        client = _RecordingQdrantClient()
        handler = _PointHandler(client, "docs")  # type: ignore[arg-type]
        await handler._apply_actions(
            None,  # type: ignore[arg-type]
            [_PointAction(point_id=i, point=p) for i, p in enumerate(points)],
        )
        assert len(client.upserts) > 1
        sizes = dict(zip(range(12), real_sizes))
        for request in client.upserts:
            assert sum(sizes[p.id] for p in request) <= limit
        assert [p.id for request in client.upserts for p in request] == list(range(12))


# =============================================================================
# Unit tests — tracking record upgrade compatibility
# =============================================================================


@requires_qdrant
class TestTrackingRecordUpgradeCompat:
    def test_pre_sparse_tracking_record_compat(self) -> None:
        """Records written before sparse-vector support must decode equal to a
        new dense-only record, and a new dense-only record must encode to the
        same bytes as before — otherwise existing collections would be
        destructively replaced (and their data lost) on upgrade."""

        # Pre-sparse on-disk shape: inner dict values were the bare dense
        # struct, not the dense|sparse tagged union. The explicit tag pins
        # the historical class name so the encoding matches old records.
        class PreSparseNamed(
            msgspec.Struct, frozen=True, tag="_ResolvedQdrantNamedVectorsDef"
        ):
            vectors: dict[str, _ResolvedQdrantVectorDef]

        class PreSparseCore(msgspec.Struct, frozen=True, array_like=True):
            vectors: _ResolvedQdrantVectorDef | PreSparseNamed

        dense_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=4),
            distance="cosine",
            multivector_comparator="max_sim",
        )
        old_bytes = serde._msgspec_encoder.encode(
            PreSparseCore(vectors=PreSparseNamed(vectors={"dense": dense_def}))
        )

        new_record = _CollectionTrackingRecordCore(
            vectors=_ResolvedQdrantNamedVectorsDef(vectors={"dense": dense_def})
        )
        decoded = msgspec.msgpack.Decoder(
            type=_CollectionTrackingRecordCore,
            ext_hook=serde._ext_hook,
            dec_hook=serde._dec_hook,
        ).decode(old_bytes)
        assert decoded == new_record, "old records must decode equal to new"

        new_bytes = serde._msgspec_encoder.encode(new_record)
        assert new_bytes == old_bytes, (
            "dense-only records must encode byte-identically to pre-sparse "
            "records; a byte change would flip the statediff and destroy "
            "existing collections on upgrade"
        )


# =============================================================================
# End-to-end — point ID canonicalization upgrade
# =============================================================================


def _server_point_id(point_id: Any) -> Any:
    return str(uuid.UUID(point_id)) if isinstance(point_id, str) else point_id


if HAS_QDRANT:
    # QdrantClient is Any when qdrant-client is not installed (the CI type check).
    class _ServerLikeMemoryClient(QdrantClient):  # type: ignore[misc, unused-ignore]
        """In-memory Qdrant that stores point IDs the way the server does:
        every UUID text form names one point. Local mode keeps raw strings."""

        def __init__(self) -> None:
            super().__init__(":memory:")

        def upsert(self, *args: Any, **kwargs: Any) -> Any:
            kwargs["points"] = [
                p.model_copy(update={"id": _server_point_id(p.id)})
                for p in kwargs["points"]
            ]
            return super().upsert(*args, **kwargs)

        def delete(self, *args: Any, **kwargs: Any) -> Any:
            selector = kwargs["points_selector"]
            kwargs["points_selector"] = qdrant_models.PointIdsList(
                points=[_server_point_id(p) for p in selector.points]
            )
            return super().delete(*args, **kwargs)


@pytest.fixture(params=["server_like_memory", "live"])
def point_id_client(request: pytest.FixtureRequest) -> "QdrantClient":
    if request.param == "live":
        if not os.environ.get("QDRANT_URL"):
            pytest.skip("QDRANT_URL is not set")
        return qdrant.create_client(os.environ["QDRANT_URL"], prefer_grpc=True)
    return _ServerLikeMemoryClient()


@requires_qdrant
def test_non_canonical_point_ids_survive_upgrade(
    point_id_client: "QdrantClient", monkeypatch: pytest.MonkeyPatch
) -> None:
    """Points declared with non-canonical UUID strings before point IDs were
    canonicalized still exist after the upgrade. The first run after it
    upserts their canonical keys and skips deleting their old keys, which name
    the same points. A point no longer declared is still deleted."""
    client = point_id_client
    upper_form = _POINT_UUID.upper()
    other_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    hex_form = uuid.UUID(other_uuid).hex
    collection_name = f"coco_point_ids_{uuid.uuid4().hex}"
    db_key = coco.ContextKey[QdrantClient](f"test_qdrant_point_ids_{uuid.uuid4().hex}")
    env = common.create_test_env(__file__, suffix=collection_name)
    env.context_provider.provide(db_key, client)
    declared: list[str] = []

    @coco.fn
    async def app_main() -> None:
        target = await qdrant.mount_collection_target(
            db_key,
            collection_name,
            await qdrant.CollectionSchema.create(
                vectors=qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=2)
                )
            ),
        )
        for point_id in declared:
            target.declare_point(
                qdrant.PointStruct(id=point_id, vector=[0.1, 0.2], payload={})
            )

    app = coco.App(
        coco.AppConfig(name="test_qdrant_point_ids", environment=env), app_main
    )

    def stored_ids() -> set[Any]:
        points, _ = client.scroll(collection_name=collection_name, limit=10)
        return {p.id for p in points}

    try:
        # Run 1, before the fix: the raw strings are the keys.
        monkeypatch.setattr(qdrant_target, "_validate_point_id", lambda raw: raw)
        declared[:] = [upper_form, hex_form]
        app.update_blocking()
        assert stored_ids() == {_POINT_UUID, other_uuid}

        # Run 2, after the fix: the first point moves to its canonical key and
        # must survive the delete of its old key; the second is undeclared.
        monkeypatch.undo()
        declared[:] = [upper_form]
        app.update_blocking()
        assert stored_ids() == {_POINT_UUID}

        # Two forms of one UUID are one point, so they can't both be declared.
        declared[:] = [upper_form, _POINT_UUID]
        with pytest.raises(Exception, match="already declared"):
            app.update_blocking()
    finally:
        client.delete_collection(collection_name=collection_name)


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
                    ),
                    "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
                },
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


# =============================================================================
# Unit tests — the vector dtype Qdrant stores
# =============================================================================


def _typed_dense_def(dtype: Any, *, multi: bool = False) -> "_ResolvedQdrantVectorDef":
    schema = VectorSchema(dtype=np.dtype(dtype), size=4)
    return _ResolvedQdrantVectorDef(
        schema=MultiVectorSchema(vector_schema=schema) if multi else schema,
        distance="cosine",
        multivector_comparator="max_sim",
    )


@requires_qdrant
class TestVectorDtype:
    def test_float16_reaches_qdrant(self) -> None:
        """A float16 schema creates a float16 collection; float32 and float64
        (Qdrant has no 64-bit vectors) its default, float32."""
        for multi in (False, True):
            params = _vector_params_from_def(_typed_dense_def(np.float16, multi=multi))
            assert params.datatype == qdrant_models.Datatype.FLOAT16
            for dtype in (np.float32, np.float64):
                params = _vector_params_from_def(_typed_dense_def(dtype, multi=multi))
                assert params.datatype is None

    def test_uint8_reaches_qdrant_and_other_integers_are_refused(self) -> None:
        """A uint8 schema creates a uint8 collection; a dtype Qdrant stores no
        vectors of is refused when the schema is built, not stored as float32."""
        for multi in (False, True):
            params = _vector_params_from_def(_typed_dense_def(np.uint8, multi=multi))
            assert params.datatype == qdrant_models.Datatype.UINT8
            for dtype in (np.int8, np.int32, np.uint16, np.complex64, np.bool_):
                with pytest.raises(ValueError, match="stores no vectors of dtype"):
                    qdrant.CollectionSchema(_typed_dense_def(dtype, multi=multi))
                with pytest.raises(ValueError, match="stores no vectors of dtype"):
                    qdrant.CollectionSchema(
                        _ResolvedQdrantNamedVectorsDef(
                            vectors={"v": _typed_dense_def(dtype, multi=multi)}
                        )
                    )
        for stored in (np.float16, np.float32, np.float64, np.uint8):
            qdrant.CollectionSchema(_typed_dense_def(stored))

    @pytest.mark.asyncio
    async def test_schema_create_refuses_a_dtype_qdrant_has_no_vectors_of(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="stores no vectors of dtype int32"):
            await qdrant.CollectionSchema.create(
                qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.int32), size=4)
                )
            )

    def test_float32_float64_change_alone_keeps_the_collection(self) -> None:
        """Qdrant stores a float64 schema's vectors as float32, so a change
        between the two alone recreates nothing, also against a record stored
        with float64; a change to or from float16 recreates the collection."""

        def vectors(dtype: Any, *, multi: bool, named: bool) -> Any:
            dense = _typed_dense_def(dtype, multi=multi)
            if named:
                return _ResolvedQdrantNamedVectorsDef(
                    vectors={"dense": dense, "sparse": _ResolvedQdrantSparseVectorDef()}
                )
            return dense

        def plan(prev_dtype: Any, dtype: Any) -> list[tuple[Any, Any]]:
            out = []
            for multi in (False, True):
                for named in (False, True):
                    # A record as stored before the fix: the dtype as declared.
                    prev = statediff.MutualTrackingRecord(
                        tracking_record=_CollectionTrackingRecordCore(
                            vectors=vectors(prev_dtype, multi=multi, named=named)
                        ),
                        managed_by=ManagedBy.SYSTEM,
                    )
                    spec = _CollectionSpec(
                        schema=qdrant.CollectionSchema(
                            vectors(dtype, multi=multi, named=named)
                        ),
                        missing=False,
                    )
                    output = _CollectionHandler().reconcile(
                        ("db", "docs"), spec, [prev], False
                    )
                    assert output is not None
                    out.append((output.action.main_action, output.child_invalidation))
            return out

        assert plan(np.float32, np.float64) == [(None, None)] * 4
        assert plan(np.float64, np.float32) == [(None, None)] * 4
        assert plan(np.float64, np.float64) == [(None, None)] * 4
        assert plan(np.float32, np.float16) == [("replace", "destructive")] * 4
        assert plan(np.float16, np.float64) == [("replace", "destructive")] * 4


# =============================================================================
# End-to-end — a collection deleted outside CocoIndex
# =============================================================================


@pytest.fixture(params=["memory", "live"])
def any_qdrant_client(request: pytest.FixtureRequest) -> "QdrantClient":
    if request.param == "live":
        if not os.environ.get("QDRANT_URL"):
            pytest.skip("QDRANT_URL is not set")
        return qdrant.create_client(os.environ["QDRANT_URL"], prefer_grpc=True)
    return QdrantClient(":memory:")


_dropped_point_component_runs = 0


if HAS_QDRANT:

    @coco.fn(memo=True)
    async def _declare_point_in_component(
        point_id: int, target: "qdrant.CollectionTarget[coco.ResolvedS]"
    ) -> None:
        global _dropped_point_component_runs
        _dropped_point_component_runs += 1
        target.declare_point(
            qdrant.PointStruct(
                id=point_id, vector=[1.0, 0.0, float(point_id)], payload={"n": point_id}
            )
        )


@requires_qdrant
def test_collection_deleted_outside_cocoindex_comes_back(
    any_qdrant_client: "QdrantClient",
) -> None:
    """A collection deleted outside CocoIndex is created again on the next run,
    with all its points: those of child components that would otherwise be
    skipped as unchanged too."""
    global _dropped_point_component_runs
    _dropped_point_component_runs = 0
    client = any_qdrant_client
    collection_name = f"coco_dropped_{uuid.uuid4().hex}"
    db_key = coco.ContextKey[QdrantClient](f"test_qdrant_dropped_{uuid.uuid4().hex}")
    env = common.create_test_env(__file__, suffix=collection_name)
    env.context_provider.provide(db_key, client)

    @coco.fn
    async def app_main() -> None:
        target = await qdrant.mount_collection_target(
            db_key,
            collection_name,
            await qdrant.CollectionSchema.create(
                vectors=qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=3)
                )
            ),
        )
        await coco.mount_each(
            _declare_point_in_component, [(str(i), i) for i in (1, 2)], target
        )

    app = coco.App(
        coco.AppConfig(name=f"test_qdrant_dropped_{collection_name}", environment=env),
        app_main,
    )

    def stored_ids() -> list[Any]:
        points, _ = client.scroll(collection_name=collection_name, limit=10)
        return sorted(p.id for p in points)

    try:
        app.update_blocking()
        assert stored_ids() == [1, 2]
        assert _dropped_point_component_runs == 2
        app.update_blocking()
        assert _dropped_point_component_runs == 2, "unchanged children are skipped"

        client.delete_collection(collection_name=collection_name)
        app.update_blocking()
        assert stored_ids() == [1, 2]
        assert _dropped_point_component_runs == 4, (
            "the children run again to write their points"
        )

        app.update_blocking()
        assert stored_ids() == [1, 2]
        assert _dropped_point_component_runs == 4
    finally:
        try:
            client.delete_collection(collection_name=collection_name)
        except Exception:
            pass


@requires_qdrant
def test_old_client_reads_only_not_found_as_a_missing_collection() -> None:
    """A qdrant-client older than 1.8 has no `collection_exists`, so the check
    falls back to `get_collection`. Only "not found" means missing there: a
    connection or auth error raises, instead of reading as a collection
    deleted outside CocoIndex, which would rewrite every point."""
    from qdrant_client.http.exceptions import UnexpectedResponse

    class _OldClient:
        def __init__(self, error: Exception | None) -> None:
            self._error = error

        def get_collection(self, collection_name: str) -> object:
            if self._error is not None:
                raise self._error
            return object()

    def exists(error: Exception | None) -> bool:
        return qdrant_target._collection_exists(cast(Any, _OldClient(error)), "c")

    assert exists(None) is True
    assert exists(ValueError("Collection c not found")) is False
    not_found = UnexpectedResponse(404, "Not Found", b"", httpx.Headers())
    assert exists(not_found) is False
    with pytest.raises(UnexpectedResponse):
        exists(UnexpectedResponse(401, "Unauthorized", b"", httpx.Headers()))
    with pytest.raises(ConnectionError):
        exists(ConnectionError("connection refused"))
