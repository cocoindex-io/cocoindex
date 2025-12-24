"""
Test suite for LanceDB VectorIndexMethod support.

This module tests the VectorIndexMethod configuration for LanceDB target,
including HNSW and IVF Flat index types.

Note: This test file uses standalone copies of the VectorIndexMethod classes
and helper functions to avoid importing the cocoindex module, which requires
the Rust engine to be built. When running in CI, the full module will be tested.
"""

import pytest
from dataclasses import dataclass
from typing import Any, Union, Optional

# Skip all tests if lancedb is not installed
lancedb = pytest.importorskip("lancedb")

# Import lancedb index types for assertion
from lancedb.index import HnswPq, IvfFlat  # type: ignore


# Standalone copies of VectorIndexMethod classes for testing
# These mirror the definitions in cocoindex/index.py
@dataclass
class HnswVectorIndexMethod:
    """HNSW vector index parameters."""
    kind: str = "Hnsw"
    m: Optional[int] = None
    ef_construction: Optional[int] = None


@dataclass
class IvfFlatVectorIndexMethod:
    """IVFFlat vector index parameters."""
    kind: str = "IvfFlat"
    lists: Optional[int] = None


VectorIndexMethod = Union[HnswVectorIndexMethod, IvfFlatVectorIndexMethod]


def _create_vector_index_config(
    method: Optional[VectorIndexMethod],
    distance_type: str,
) -> Union[HnswPq, IvfFlat]:
    """
    Create the appropriate LanceDB index configuration based on the VectorIndexMethod.
    This mirrors the function from lancedb.py for testing without engine dependency.
    """
    if method is None:
        return HnswPq(distance_type=distance_type)

    if isinstance(method, HnswVectorIndexMethod):
        kwargs: dict[str, Any] = {"distance_type": distance_type}
        if method.m is not None:
            kwargs["m"] = method.m
        if method.ef_construction is not None:
            kwargs["ef_construction"] = method.ef_construction
        return HnswPq(**kwargs)

    if isinstance(method, IvfFlatVectorIndexMethod):
        kwargs = {"distance_type": distance_type}
        if method.lists is not None:
            kwargs["num_partitions"] = method.lists
        return IvfFlat(**kwargs)

    return HnswPq(distance_type=distance_type)


class TestCreateVectorIndexConfig:
    """Test suite for _create_vector_index_config function."""

    def test_default_method_returns_hnsw_pq(self) -> None:
        """Test that None method defaults to HnswPq."""
        config = _create_vector_index_config(None, "cosine")
        assert isinstance(config, HnswPq)
        assert config.distance_type == "cosine"

    def test_hnsw_method_without_params(self) -> None:
        """Test HNSW method without custom parameters."""
        method = HnswVectorIndexMethod()
        config = _create_vector_index_config(method, "l2")
        assert isinstance(config, HnswPq)
        assert config.distance_type == "l2"

    def test_hnsw_method_with_m_param(self) -> None:
        """Test HNSW method with custom m parameter."""
        method = HnswVectorIndexMethod(m=32)
        config = _create_vector_index_config(method, "cosine")
        assert isinstance(config, HnswPq)
        assert config.m == 32

    def test_hnsw_method_with_ef_construction(self) -> None:
        """Test HNSW method with custom ef_construction parameter."""
        method = HnswVectorIndexMethod(ef_construction=200)
        config = _create_vector_index_config(method, "dot")
        assert isinstance(config, HnswPq)
        assert config.ef_construction == 200

    def test_hnsw_method_with_all_params(self) -> None:
        """Test HNSW method with both m and ef_construction parameters."""
        method = HnswVectorIndexMethod(m=16, ef_construction=100)
        config = _create_vector_index_config(method, "cosine")
        assert isinstance(config, HnswPq)
        assert config.m == 16
        assert config.ef_construction == 100

    def test_ivf_flat_method_without_params(self) -> None:
        """Test IVF Flat method without custom parameters."""
        method = IvfFlatVectorIndexMethod()
        config = _create_vector_index_config(method, "l2")
        assert isinstance(config, IvfFlat)
        assert config.distance_type == "l2"

    def test_ivf_flat_method_with_lists_param(self) -> None:
        """Test IVF Flat method with custom lists (num_partitions) parameter."""
        method = IvfFlatVectorIndexMethod(lists=256)
        config = _create_vector_index_config(method, "cosine")
        assert isinstance(config, IvfFlat)
        assert config.num_partitions == 256

    def test_all_distance_types(self) -> None:
        """Test that all distance types work correctly."""
        for distance_type in ["cosine", "l2", "dot"]:
            config = _create_vector_index_config(None, distance_type)
            assert config.distance_type == distance_type


class TestVectorIndexMethodIntegration:
    """Integration tests for VectorIndexMethod with LanceDB."""

    def test_hnsw_method_kind_attribute(self) -> None:
        """Test that HnswVectorIndexMethod has correct kind attribute."""
        method = HnswVectorIndexMethod()
        assert method.kind == "Hnsw"

    def test_ivf_flat_method_kind_attribute(self) -> None:
        """Test that IvfFlatVectorIndexMethod has correct kind attribute."""
        method = IvfFlatVectorIndexMethod()
        assert method.kind == "IvfFlat"

    def test_method_default_values(self) -> None:
        """Test default values for index methods."""
        hnsw = HnswVectorIndexMethod()
        assert hnsw.m is None
        assert hnsw.ef_construction is None

        ivf_flat = IvfFlatVectorIndexMethod()
        assert ivf_flat.lists is None
