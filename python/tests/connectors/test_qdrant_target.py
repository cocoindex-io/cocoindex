"""Tests for Qdrant target connector.

Helper-level tests run without a Qdrant service.

Live tests are gated on the ``QDRANT_URL`` env var; they are skipped when it
isn't set.
"""

from __future__ import annotations

import pytest

try:
    from qdrant_client.http import models as qdrant_models

    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

requires_qdrant = pytest.mark.skipif(
    not HAS_QDRANT, reason="qdrant-client is not installed"
)

if HAS_QDRANT:
    import numpy as np

    from cocoindex.connectors.qdrant._target import (
        _ResolvedQdrantVectorDef,
        _distance_from_spec,
        _multivector_comparator,
        _vector_params_from_def,
    )
    from cocoindex.resources.schema import MultiVectorSchema, VectorSchema


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
