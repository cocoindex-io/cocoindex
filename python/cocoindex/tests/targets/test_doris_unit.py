"""
Unit tests for Doris connector (no database connection required).
"""
# mypy: disable-error-code="no-untyped-def"

import uuid
import math
from typing import Literal
import pytest

from cocoindex.targets.doris import (
    DorisTarget,
    _TableKey,
    _State,
    _VectorIndex,
    _Connector,
    _convert_value_type_to_doris_type,
    _convert_value_for_doris,
    _validate_identifier,
    _generate_create_table_ddl,
    _get_vector_dimension,
    _is_vector_indexable,
    _is_retryable_mysql_error,
    DorisSchemaError,
    DorisConnectionError,
    RetryConfig,
    with_retry,
)
from cocoindex.engine_type import (
    FieldSchema,
    EnrichedValueType,
    BasicValueType,
    VectorTypeSchema,
)
from cocoindex import op
from cocoindex.index import (
    IndexOptions,
    VectorIndexDef,
    VectorSimilarityMetric,
)

_BasicKind = Literal[
    "Bytes",
    "Str",
    "Bool",
    "Int64",
    "Float32",
    "Float64",
    "Range",
    "Uuid",
    "Date",
    "Time",
    "LocalDateTime",
    "OffsetDateTime",
    "TimeDelta",
    "Json",
    "Vector",
    "Union",
]


def _mock_field(
    name: str, kind: _BasicKind, nullable: bool = False, dim: int | None = None
) -> FieldSchema:
    """Create mock FieldSchema for testing."""
    if kind == "Vector":
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"),
            dimension=dim,
        )
        basic_type = BasicValueType(kind=kind, vector=vec_schema)
    else:
        basic_type = BasicValueType(kind=kind)
    return FieldSchema(
        name=name,
        value_type=EnrichedValueType(type=basic_type, nullable=nullable),
    )


# ============================================================
# TYPE MAPPING AND JSON FALLBACK TESTS
# ============================================================


class TestTypeMapping:
    """Test CocoIndex type -> Doris SQL type conversion."""

    @pytest.mark.parametrize(
        "kind,expected_doris",
        [
            ("Str", "TEXT"),
            ("Bool", "BOOLEAN"),
            ("Int64", "BIGINT"),
            ("Float32", "FLOAT"),
            ("Float64", "DOUBLE"),
            ("Uuid", "VARCHAR(36)"),
            ("Json", "JSON"),
        ],
    )
    def test_basic_type_mapping(self, kind: _BasicKind, expected_doris: str) -> None:
        basic_type = BasicValueType(kind=kind)
        enriched = EnrichedValueType(type=basic_type)
        assert _convert_value_type_to_doris_type(enriched) == expected_doris

    def test_vector_with_dimension_maps_to_array(self) -> None:
        """Vector with dimension should map to ARRAY<FLOAT>."""
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=384
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        enriched = EnrichedValueType(type=basic_type)
        assert _convert_value_type_to_doris_type(enriched) == "ARRAY<FLOAT>"

    def test_vector_without_dimension_falls_back_to_json(self) -> None:
        """Vector without dimension should fall back to JSON (like Postgres/Qdrant)."""
        # No dimension
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=None
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        enriched = EnrichedValueType(type=basic_type)
        assert _convert_value_type_to_doris_type(enriched) == "JSON"

        # No vector schema at all
        basic_type = BasicValueType(kind="Vector", vector=None)
        enriched = EnrichedValueType(type=basic_type)
        assert _convert_value_type_to_doris_type(enriched) == "JSON"

    def test_unsupported_type_falls_back_to_json(self) -> None:
        """Unsupported types should fall back to JSON."""
        basic_type = BasicValueType(kind="Union")
        enriched = EnrichedValueType(type=basic_type)
        assert _convert_value_type_to_doris_type(enriched) == "JSON"


class TestVectorIndexability:
    """Test vector indexability and dimension extraction."""

    def test_vector_indexability(self) -> None:
        """Only vectors with fixed dimension are indexable."""
        # With dimension - indexable
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=384
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        enriched = EnrichedValueType(type=basic_type)
        assert _is_vector_indexable(enriched) is True

        # Without dimension - not indexable
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=None
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        enriched = EnrichedValueType(type=basic_type)
        assert _is_vector_indexable(enriched) is False

    def test_get_vector_dimension(self) -> None:
        """Test dimension extraction returns None for non-indexable vectors."""
        fields = [_mock_field("embedding", "Vector", dim=384)]
        assert _get_vector_dimension(fields, "embedding") == 384

        # No dimension
        fields = [_mock_field("embedding", "Vector", dim=None)]
        assert _get_vector_dimension(fields, "embedding") is None

        # Field not found
        fields = [_mock_field("other", "Str")]
        assert _get_vector_dimension(fields, "embedding") is None


# ============================================================
# VALUE CONVERSION TESTS
# ============================================================


class TestValueConversion:
    """Test Python value -> Doris-compatible format conversion."""

    def test_special_value_handling(self) -> None:
        """Test handling of special values (UUID, NaN, None)."""
        test_uuid = uuid.uuid4()
        assert _convert_value_for_doris(test_uuid) == str(test_uuid)
        assert _convert_value_for_doris(math.nan) is None
        assert _convert_value_for_doris(None) is None

    def test_collection_conversion(self) -> None:
        """Test list and dict conversion."""
        assert _convert_value_for_doris([1.0, 2.0, 3.0]) == [1.0, 2.0, 3.0]
        assert _convert_value_for_doris({"key": "value"}) == {"key": "value"}


# ============================================================
# DDL AND SCHEMA TESTS
# ============================================================


class TestDDLGeneration:
    """Test DDL generation for Doris."""

    def test_create_table_structure(self) -> None:
        """Test basic table DDL generation."""
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert "DUPLICATE KEY" in ddl  # Required for vector index support
        assert "id BIGINT NOT NULL" in ddl
        assert "content TEXT" in ddl

    def test_vector_column_ddl(self) -> None:
        """Test vector column DDL with and without dimension."""
        # With dimension - ARRAY<FLOAT>
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("embedding", "Vector", dim=768)],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)
        assert "embedding ARRAY<FLOAT> NOT NULL" in ddl

        # Without dimension - JSON
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=None
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[
                FieldSchema(
                    name="embedding", value_type=EnrichedValueType(type=basic_type)
                )
            ],
        )
        ddl = _generate_create_table_ddl(key, state)
        assert "embedding JSON" in ddl

    def test_vector_index_ddl(self) -> None:
        """Test vector index DDL generation."""
        state = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("embedding", "Vector", dim=768)],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=768,
                )
            ],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert "INDEX idx_embedding_ann (embedding) USING ANN" in ddl
        assert '"index_type" = "hnsw"' in ddl


class TestIdentifierValidation:
    """Test SQL identifier validation."""

    def test_valid_identifiers(self) -> None:
        _validate_identifier("valid_table_name")
        _validate_identifier("MyTable123")

    def test_invalid_identifiers(self) -> None:
        with pytest.raises(DorisSchemaError):
            _validate_identifier("invalid-name")
        with pytest.raises(DorisSchemaError):
            _validate_identifier("'; DROP TABLE users; --")


# ============================================================
# CONNECTOR LOGIC TESTS
# ============================================================


class TestConnectorLogic:
    """Test connector business logic."""

    def test_vector_index_skipped_for_no_dimension(self) -> None:
        """Test that vector index is skipped when dimension is not available."""
        spec = DorisTarget(fe_host="localhost", database="test", table="test_table")
        key_fields = [_mock_field("id", "Int64")]

        # Vector without dimension
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"), dimension=None
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        value_fields = [
            FieldSchema(name="embedding", value_type=EnrichedValueType(type=basic_type))
        ]

        # Request vector index on field without dimension
        index_options = IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[
                VectorIndexDef(
                    field_name="embedding",
                    metric=VectorSimilarityMetric.L2_DISTANCE,
                )
            ],
        )

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        # Vector index should be skipped
        assert state.vector_indexes is None or len(state.vector_indexes) == 0

    def test_state_compatibility(self) -> None:
        """Test schema compatibility checking."""
        state1 = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        state2 = _State(
            key_fields_schema=[_mock_field("id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        assert (
            _Connector.check_state_compatibility(state1, state2)
            == op.TargetStateCompatibility.COMPATIBLE
        )

        # Key change is incompatible
        state3 = _State(
            key_fields_schema=[_mock_field("new_id", "Int64")],
            value_fields_schema=[_mock_field("content", "Str")],
        )
        assert (
            _Connector.check_state_compatibility(state1, state3)
            == op.TargetStateCompatibility.NOT_COMPATIBLE
        )

    def test_timeout_config_propagated(self) -> None:
        """Test that timeout configs are propagated from DorisTarget to _State."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
            schema_change_timeout=120,  # Non-default
            index_build_timeout=600,  # Non-default
        )
        key_fields = [_mock_field("id", "Int64")]
        value_fields = [_mock_field("content", "Str")]
        index_options = IndexOptions(primary_key_fields=["id"])

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.schema_change_timeout == 120
        assert state.index_build_timeout == 600


# ============================================================
# RETRY LOGIC TESTS
# ============================================================


class TestRetryLogic:
    """Test retry configuration and behavior."""

    @pytest.mark.asyncio
    async def test_retry_succeeds_on_first_try(self) -> None:
        """Test retry logic when operation succeeds immediately."""
        call_count = 0

        async def successful_op() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await with_retry(
            successful_op,
            config=RetryConfig(max_retries=3),
            retryable_errors=(Exception,),
        )

        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_succeeds_after_failures(self) -> None:
        """Test retry logic with transient failures."""
        import asyncio

        call_count = 0

        async def flaky_op() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise asyncio.TimeoutError("Transient error")
            return "success"

        result = await with_retry(
            flaky_op,
            config=RetryConfig(max_retries=3, base_delay=0.01),
            retryable_errors=(asyncio.TimeoutError,),
        )

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_error(self) -> None:
        """Test retry logic when all retries fail."""
        import asyncio

        call_count = 0

        async def always_fails() -> str:
            nonlocal call_count
            call_count += 1
            raise asyncio.TimeoutError("Always fails")

        with pytest.raises(DorisConnectionError) as exc_info:
            await with_retry(
                always_fails,
                config=RetryConfig(max_retries=2, base_delay=0.01),
                retryable_errors=(asyncio.TimeoutError,),
            )

        assert call_count == 3  # Initial + 2 retries
        assert "failed after 3 attempts" in str(exc_info.value)


class TestMySQLErrorRetry:
    """Test MySQL error retry functionality."""

    def test_retryable_mysql_error_codes(self) -> None:
        """Test that specific MySQL error codes are identified as retryable."""
        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        # Retryable error codes (connection issues)
        retryable_codes = [2003, 2006, 2013, 1040, 1205]
        for code in retryable_codes:
            error = pymysql.err.OperationalError(code, f"Test error {code}")
            assert _is_retryable_mysql_error(error), (
                f"Error code {code} should be retryable"
            )

        # Non-retryable error codes
        non_retryable_codes = [
            1064,
            1146,
            1045,
        ]  # Syntax error, table not found, access denied
        for code in non_retryable_codes:
            error = pymysql.err.OperationalError(code, f"Test error {code}")
            assert not _is_retryable_mysql_error(error), (
                f"Error code {code} should not be retryable"
            )

    def test_non_mysql_error_not_retryable(self) -> None:
        """Test that non-MySQL errors are not identified as retryable."""
        assert not _is_retryable_mysql_error(ValueError("test"))
        assert not _is_retryable_mysql_error(RuntimeError("test"))

    @pytest.mark.asyncio
    async def test_with_retry_handles_mysql_errors(self) -> None:
        """Test that with_retry retries on MySQL connection errors."""
        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        call_count = 0

        async def mysql_flaky_op() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise pymysql.err.OperationalError(2006, "MySQL server has gone away")
            return "success"

        result = await with_retry(
            mysql_flaky_op,
            config=RetryConfig(max_retries=3, base_delay=0.01),
        )

        assert result == "success"
        assert call_count == 3
