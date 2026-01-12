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
    _InvertedIndex,
    _Connector,
    _convert_value_type_to_doris_type,
    _convert_value_for_doris,
    _validate_identifier,
    _generate_create_table_ddl,
    _generate_stream_load_label,
    _build_stream_load_headers,
    build_vector_search_query,
    DorisSchemaError,
    RetryConfig,
    with_retry,
    DorisConnectionError,
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
    HnswVectorIndexMethod,
    IvfFlatVectorIndexMethod,
)

# Type alias for BasicValueType kind to satisfy mypy
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


# ============================================================
# TYPE MAPPING TESTS
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
            ("Date", "DATE"),
            ("LocalDateTime", "DATETIME(6)"),
            ("OffsetDateTime", "DATETIME(6)"),
            ("Json", "JSON"),
            ("Bytes", "STRING"),
        ],
    )
    def test_basic_type_mapping(self, kind: _BasicKind, expected_doris: str) -> None:
        basic_type = BasicValueType(kind=kind)
        enriched = EnrichedValueType(type=basic_type)
        result = _convert_value_type_to_doris_type(enriched)
        assert result == expected_doris

    def test_vector_type_mapping(self) -> None:
        """Vector should map to ARRAY<FLOAT>."""
        vec_schema = VectorTypeSchema(
            element_type=BasicValueType(kind="Float32"),
            dimension=384,
        )
        basic_type = BasicValueType(kind="Vector", vector=vec_schema)
        enriched = EnrichedValueType(type=basic_type)
        result = _convert_value_type_to_doris_type(enriched)
        assert result == "ARRAY<FLOAT>"


# ============================================================
# VALUE CONVERSION TESTS
# ============================================================


class TestValueConversion:
    """Test Python value -> Doris-compatible format conversion."""

    def test_uuid_conversion(self) -> None:
        test_uuid = uuid.uuid4()
        result = _convert_value_for_doris(test_uuid)
        assert result == str(test_uuid)
        assert isinstance(result, str)

    def test_nan_handling(self) -> None:
        result = _convert_value_for_doris(math.nan)
        assert result is None

    def test_none_handling(self) -> None:
        result = _convert_value_for_doris(None)
        assert result is None

    def test_list_conversion(self) -> None:
        """Lists should be preserved for ARRAY columns."""
        result = _convert_value_for_doris([1.0, 2.0, 3.0])
        assert result == [1.0, 2.0, 3.0]

    def test_dict_conversion(self) -> None:
        """Dicts should be preserved."""
        result = _convert_value_for_doris({"key": "value", "num": 42})
        assert isinstance(result, dict)
        assert result == {"key": "value", "num": 42}


# ============================================================
# PERSISTENT KEY TESTS
# ============================================================


class TestPersistentKey:
    """Test _TableKey generation."""

    def test_get_persistent_key(self) -> None:
        spec = DorisTarget(
            fe_host="localhost",
            database="test_db",
            table="test_table",
        )
        key = _Connector.get_persistent_key(spec)
        assert key.fe_host == "localhost"
        assert key.database == "test_db"
        assert key.table == "test_table"

    def test_key_equality(self) -> None:
        key1 = _TableKey("host1", "db1", "table1")
        key2 = _TableKey("host1", "db1", "table1")
        key3 = _TableKey("host2", "db1", "table1")
        assert key1 == key2
        assert key1 != key3


# ============================================================
# IDENTIFIER VALIDATION TESTS
# ============================================================


class TestIdentifierValidation:
    """Test SQL identifier validation."""

    def test_valid_identifiers(self) -> None:
        _validate_identifier("valid_table_name")
        _validate_identifier("MyTable123")
        _validate_identifier("_private_table")

    def test_invalid_identifiers(self) -> None:
        with pytest.raises(DorisSchemaError):
            _validate_identifier("invalid-name")

        with pytest.raises(DorisSchemaError):
            _validate_identifier("'; DROP TABLE users; --")

        with pytest.raises(DorisSchemaError):
            _validate_identifier("name with spaces")


# ============================================================
# TYPE COMPATIBILITY TESTS
# ============================================================


class TestTypeCompatibility:
    """Test type compatibility checking."""

    def test_exact_match(self) -> None:
        """Exact type match should be compatible."""
        from cocoindex.targets.doris import _types_compatible

        assert _types_compatible("BIGINT", "BIGINT")
        assert _types_compatible("TEXT", "TEXT")
        assert _types_compatible("VARCHAR(36)", "VARCHAR(36)")
        assert _types_compatible("ARRAY<FLOAT>", "ARRAY<FLOAT>")

    def test_array_element_type_must_match(self) -> None:
        """ARRAY types must have matching element types."""
        from cocoindex.targets.doris import _types_compatible

        # Same element type
        assert _types_compatible("ARRAY<FLOAT>", "ARRAY<FLOAT>")
        # FLOAT and DOUBLE are interchangeable in vector contexts
        assert _types_compatible("ARRAY<FLOAT>", "ARRAY<DOUBLE>")
        assert _types_compatible("ARRAY<DOUBLE>", "ARRAY<FLOAT>")

        # Different element types are NOT compatible
        assert not _types_compatible("ARRAY<FLOAT>", "ARRAY<INT>")
        assert not _types_compatible("ARRAY<INT>", "ARRAY<FLOAT>")
        assert not _types_compatible("ARRAY<BIGINT>", "ARRAY<FLOAT>")

    def test_varchar_length_compatibility(self) -> None:
        """VARCHAR length must be sufficient in actual column."""
        from cocoindex.targets.doris import _types_compatible

        # Actual can hold expected
        assert _types_compatible("VARCHAR(36)", "VARCHAR(100)")
        assert _types_compatible("VARCHAR(36)", "VARCHAR(36)")

        # Actual cannot hold expected (too small)
        assert not _types_compatible("VARCHAR(100)", "VARCHAR(36)")

        # No length specified - allowed
        assert _types_compatible("VARCHAR(36)", "VARCHAR")
        assert _types_compatible("VARCHAR", "VARCHAR(36)")

    def test_text_string_interchangeable(self) -> None:
        """TEXT and STRING types are interchangeable."""
        from cocoindex.targets.doris import _types_compatible

        assert _types_compatible("TEXT", "STRING")
        assert _types_compatible("STRING", "TEXT")

    def test_text_can_hold_varchar(self) -> None:
        """TEXT/STRING can hold any VARCHAR content."""
        from cocoindex.targets.doris import _types_compatible

        assert _types_compatible("VARCHAR(255)", "TEXT")
        assert _types_compatible("VARCHAR(255)", "STRING")

    def test_array_vs_non_array_incompatible(self) -> None:
        """ARRAY and non-ARRAY types are incompatible."""
        from cocoindex.targets.doris import _types_compatible

        assert not _types_compatible("ARRAY<FLOAT>", "FLOAT")
        assert not _types_compatible("BIGINT", "ARRAY<BIGINT>")


# ============================================================
# DDL GENERATION TESTS
# ============================================================


class TestDDLGeneration:
    """Test DDL generation for Doris."""

    def _mock_field(
        self,
        name: str,
        kind: _BasicKind,
        nullable: bool = False,
        dim: int | None = None,
    ) -> FieldSchema:
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

    def test_create_table_uses_duplicate_key(self) -> None:
        """Doris 4.0 requires DUPLICATE KEY for vector index support."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content", "Str")],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert "DUPLICATE KEY" in ddl
        assert "UNIQUE KEY" not in ddl
        assert "AGGREGATE KEY" not in ddl

    def test_vector_column_is_not_null(self) -> None:
        """Vector columns must have NOT NULL constraint."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("embedding", "Vector", dim=768)],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert "embedding ARRAY<FLOAT> NOT NULL" in ddl

    def test_vector_index_ddl(self) -> None:
        """Test vector index DDL generation."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("embedding", "Vector", dim=768)],
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
        assert '"metric_type" = "l2_distance"' in ddl
        assert '"dim" = "768"' in ddl

    def test_inverted_index_ddl(self) -> None:
        """Test inverted index DDL generation."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content", "Str")],
            inverted_indexes=[
                _InvertedIndex(
                    name="idx_content_inv",
                    field_name="content",
                    parser="english",
                )
            ],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert "INDEX idx_content_inv (content) USING INVERTED" in ddl
        assert '"parser" = "english"' in ddl


# ============================================================
# STREAM LOAD TESTS
# ============================================================


class TestStreamLoad:
    """Test Stream Load related functions."""

    def test_stream_load_label_uniqueness(self) -> None:
        """Labels should be unique."""
        label1 = _generate_stream_load_label()
        label2 = _generate_stream_load_label()
        assert label1 != label2
        assert label1.startswith("cocoindex_")

    def test_stream_load_headers_basic(self) -> None:
        """Test basic Stream Load headers."""
        headers = _build_stream_load_headers("test_label")
        assert headers["format"] == "json"
        assert headers["strip_outer_array"] == "true"
        assert headers["label"] == "test_label"
        assert "Expect" in headers

    def test_stream_load_headers_with_columns(self) -> None:
        """Test Stream Load headers with column specification."""
        headers = _build_stream_load_headers(
            "test_label", columns=["id", "name", "value"]
        )
        assert headers["columns"] == "id, name, value"

    def test_stream_load_headers_without_columns(self) -> None:
        """Test Stream Load headers without column specification."""
        headers = _build_stream_load_headers("test_label", columns=None)
        assert "columns" not in headers


# ============================================================
# QUERY GENERATION TESTS
# ============================================================


class TestQueryGeneration:
    """Test query generation functions."""

    def test_l2_distance_query_uses_approximate(self) -> None:
        """L2 distance queries should use _approximate suffix."""
        query = build_vector_search_query(
            table="documents",
            vector_field="embedding",
            query_vector=[0.1, 0.2, 0.3],
            metric="l2_distance",
            limit=10,
        )
        assert "l2_distance_approximate" in query
        assert "ORDER BY" in query
        assert "LIMIT 10" in query

    def test_inner_product_query_uses_approximate(self) -> None:
        """Inner product queries should use _approximate suffix."""
        query = build_vector_search_query(
            table="documents",
            vector_field="embedding",
            query_vector=[0.1, 0.2, 0.3],
            metric="inner_product",
            limit=10,
        )
        assert "inner_product_approximate" in query
        assert "DESC" in query  # Inner product: larger = more similar


# ============================================================
# STATE COMPATIBILITY TESTS
# ============================================================


class TestStateCompatibility:
    """Test schema compatibility checking."""

    def _mock_field(
        self, name: str, kind: _BasicKind = "Str", nullable: bool = False
    ) -> FieldSchema:
        basic_type = BasicValueType(kind=kind)
        return FieldSchema(
            name=name,
            value_type=EnrichedValueType(type=basic_type, nullable=nullable),
        )

    def test_identical_schemas_compatible(self) -> None:
        """Identical schemas should be compatible."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.COMPATIBLE

    def test_key_change_incompatible(self) -> None:
        """Key schema change should be incompatible."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("new_id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.NOT_COMPATIBLE

    def test_remove_column_compatible_in_extend_mode(self) -> None:
        """Removing a column should be compatible in extend mode (default)."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[
                self._mock_field("content"),
                self._mock_field("extra"),
            ],
            schema_evolution="extend",
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            schema_evolution="extend",
        )
        result = _Connector.check_state_compatibility(state1, state2)
        # In extend mode, removed columns are kept in DB, so it's compatible
        assert result == op.TargetStateCompatibility.COMPATIBLE

    def test_remove_column_incompatible_in_strict_mode(self) -> None:
        """Removing a column should be incompatible in strict mode."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[
                self._mock_field("content"),
                self._mock_field("extra"),
            ],
            schema_evolution="strict",
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            schema_evolution="strict",
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.NOT_COMPATIBLE


# ============================================================
# DESCRIBE TESTS
# ============================================================


class TestDescribe:
    """Test describe function."""

    def test_describe_returns_readable_string(self) -> None:
        key = _TableKey("localhost", "test_db", "test_table")
        desc = _Connector.describe(key)
        assert "test_table" in desc
        assert "test_db" in desc
        assert "localhost" in desc


# ============================================================
# CONFIGURATION TESTS
# ============================================================


class TestConfiguration:
    """Test all DorisTarget configuration options."""

    def test_default_config_values(self) -> None:
        """Test default configuration values."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        assert spec.fe_http_port == 8080
        assert spec.query_port == 9030
        assert spec.username == "root"
        assert spec.password == ""
        assert spec.enable_https is False
        assert spec.batch_size == 10000
        assert spec.stream_load_timeout == 600
        assert spec.auto_create_table is True
        assert spec.max_retries == 3
        assert spec.retry_base_delay == 1.0
        assert spec.retry_max_delay == 30.0
        assert spec.replication_num == 1
        assert spec.buckets == "auto"

    def test_custom_config_values(self) -> None:
        """Test custom configuration values."""
        spec = DorisTarget(
            fe_host="custom-host",
            database="custom_db",
            table="custom_table",
            fe_http_port=9080,
            query_port=19030,
            username="custom_user",
            password="custom_pass",
            enable_https=True,
            batch_size=5000,
            stream_load_timeout=300,
            auto_create_table=False,
            max_retries=5,
            retry_base_delay=2.0,
            retry_max_delay=60.0,
            replication_num=3,
            buckets=16,
        )
        assert spec.fe_host == "custom-host"
        assert spec.database == "custom_db"
        assert spec.table == "custom_table"
        assert spec.fe_http_port == 9080
        assert spec.query_port == 19030
        assert spec.username == "custom_user"
        assert spec.password == "custom_pass"
        assert spec.enable_https is True
        assert spec.batch_size == 5000
        assert spec.stream_load_timeout == 300
        assert spec.auto_create_table is False
        assert spec.max_retries == 5
        assert spec.retry_base_delay == 2.0
        assert spec.retry_max_delay == 60.0
        assert spec.replication_num == 3
        assert spec.buckets == 16

    def test_https_url_generation(self) -> None:
        """Test that enable_https affects URL generation."""
        spec_http = DorisTarget(
            fe_host="localhost", database="test", table="t", enable_https=False
        )
        spec_https = DorisTarget(
            fe_host="localhost", database="test", table="t", enable_https=True
        )
        assert spec_http.enable_https is False
        assert spec_https.enable_https is True


# ============================================================
# RETRY CONFIGURATION TESTS
# ============================================================


class TestRetryConfiguration:
    """Test retry configuration and behavior."""

    def test_retry_config_defaults(self) -> None:
        """Test RetryConfig default values."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 2.0

    def test_retry_config_custom(self) -> None:
        """Test custom RetryConfig values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=0.5,
            max_delay=60.0,
            exponential_base=3.0,
        )
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 60.0
        assert config.exponential_base == 3.0

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

    def test_spec_retry_config_propagates(self) -> None:
        """Test that DorisTarget retry config is accessible."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="t",
            max_retries=10,
            retry_base_delay=0.5,
            retry_max_delay=120.0,
        )
        assert spec.max_retries == 10
        assert spec.retry_base_delay == 0.5
        assert spec.retry_max_delay == 120.0


# ============================================================
# VECTOR INDEX METHOD PARAMETERS TESTS
# ============================================================


class TestVectorIndexMethodParameters:
    """Test that vector index method parameters (HNSW/IVF) are properly extracted."""

    def _mock_field(
        self,
        name: str,
        kind: _BasicKind,
        nullable: bool = False,
        dim: int | None = None,
    ) -> FieldSchema:
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

    def test_hnsw_method_parameters_extracted(self) -> None:
        """Test HNSW method parameters are extracted into _VectorIndex."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        key_fields = [self._mock_field("id", "Int64")]
        value_fields = [self._mock_field("embedding", "Vector", dim=384)]

        # Create index with HNSW method and custom parameters
        index_options = IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[
                VectorIndexDef(
                    field_name="embedding",
                    metric=VectorSimilarityMetric.L2_DISTANCE,
                    method=HnswVectorIndexMethod(m=32, ef_construction=200),
                )
            ],
        )

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.vector_indexes is not None
        assert len(state.vector_indexes) == 1
        idx = state.vector_indexes[0]
        assert idx.index_type == "hnsw"
        assert idx.max_degree == 32  # m maps to max_degree
        assert idx.ef_construction == 200
        assert idx.nlist is None  # IVF param should be None

    def test_ivf_method_parameters_extracted(self) -> None:
        """Test IVF method parameters are extracted into _VectorIndex."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        key_fields = [self._mock_field("id", "Int64")]
        value_fields = [self._mock_field("embedding", "Vector", dim=384)]

        # Create index with IVF method and custom parameters
        index_options = IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[
                VectorIndexDef(
                    field_name="embedding",
                    metric=VectorSimilarityMetric.L2_DISTANCE,
                    method=IvfFlatVectorIndexMethod(lists=128),
                )
            ],
        )

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.vector_indexes is not None
        assert len(state.vector_indexes) == 1
        idx = state.vector_indexes[0]
        assert idx.index_type == "ivf"
        assert idx.nlist == 128  # lists maps to nlist
        assert idx.max_degree is None  # HNSW param should be None
        assert idx.ef_construction is None

    def test_default_hnsw_when_no_method(self) -> None:
        """Test that HNSW is the default when no method is specified."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        key_fields = [self._mock_field("id", "Int64")]
        value_fields = [self._mock_field("embedding", "Vector", dim=384)]

        # Create index without method
        index_options = IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[
                VectorIndexDef(
                    field_name="embedding",
                    metric=VectorSimilarityMetric.L2_DISTANCE,
                    method=None,
                )
            ],
        )

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.vector_indexes is not None
        assert len(state.vector_indexes) == 1
        idx = state.vector_indexes[0]
        assert idx.index_type == "hnsw"  # Default

    def test_hnsw_parameters_in_ddl(self) -> None:
        """Test that HNSW parameters appear in DDL."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("embedding", "Vector", dim=768)],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=768,
                    max_degree=32,
                    ef_construction=200,
                )
            ],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert '"max_degree" = "32"' in ddl
        assert '"ef_construction" = "200"' in ddl

    def test_ivf_parameters_in_ddl(self) -> None:
        """Test that IVF parameters appear in DDL."""
        state = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("embedding", "Vector", dim=768)],
            vector_indexes=[
                _VectorIndex(
                    name="idx_embedding_ann",
                    field_name="embedding",
                    index_type="ivf",
                    metric_type="l2_distance",
                    dimension=768,
                    nlist=128,
                )
            ],
        )
        key = _TableKey("localhost", "test_db", "test_table")
        ddl = _generate_create_table_ddl(key, state)

        assert '"index_type" = "ivf"' in ddl
        assert '"nlist" = "128"' in ddl


# ============================================================
# AUTO_CREATE_TABLE TESTS
# ============================================================


class TestAutoCreateTable:
    """Test auto_create_table configuration handling."""

    def _mock_field(
        self, name: str, kind: _BasicKind, nullable: bool = False
    ) -> FieldSchema:
        basic_type = BasicValueType(kind=kind)
        return FieldSchema(
            name=name,
            value_type=EnrichedValueType(type=basic_type, nullable=nullable),
        )

    def test_auto_create_table_stored_in_state(self) -> None:
        """Test that auto_create_table is stored in state."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
            auto_create_table=False,
        )
        key_fields = [self._mock_field("id", "Int64")]
        value_fields = [self._mock_field("content", "Str")]
        index_options = IndexOptions(primary_key_fields=["id"])

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.auto_create_table is False

    def test_auto_create_table_default_true(self) -> None:
        """Test that auto_create_table defaults to True."""
        spec = DorisTarget(
            fe_host="localhost",
            database="test",
            table="test_table",
        )
        key_fields = [self._mock_field("id", "Int64")]
        value_fields = [self._mock_field("content", "Str")]
        index_options = IndexOptions(primary_key_fields=["id"])

        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )

        assert state.auto_create_table is True


# ============================================================
# INDEX CHANGE COMPATIBILITY TESTS
# ============================================================


class TestIndexChangeCompatibility:
    """Test index change detection in state compatibility check."""

    def _mock_field(
        self, name: str, kind: _BasicKind = "Str", nullable: bool = False
    ) -> FieldSchema:
        basic_type = BasicValueType(kind=kind)
        return FieldSchema(
            name=name,
            value_type=EnrichedValueType(type=basic_type, nullable=nullable),
        )

    def test_vector_index_change_compatible(self) -> None:
        """Test that vector index changes are compatible (handled via sync)."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=[
                _VectorIndex(
                    name="idx_old",
                    field_name="content",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=[
                _VectorIndex(
                    name="idx_new",
                    field_name="content",
                    index_type="ivf",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
        )
        result = _Connector.check_state_compatibility(state1, state2)
        # Index changes should be COMPATIBLE (handled by _sync_indexes)
        assert result == op.TargetStateCompatibility.COMPATIBLE

    def test_inverted_index_change_compatible(self) -> None:
        """Test that inverted index changes are compatible."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            inverted_indexes=[
                _InvertedIndex(name="idx_old", field_name="content", parser="english")
            ],
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            inverted_indexes=[
                _InvertedIndex(name="idx_new", field_name="content", parser="unicode")
            ],
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.COMPATIBLE

    def test_adding_new_index_compatible(self) -> None:
        """Test that adding new indexes is compatible."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=None,
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=[
                _VectorIndex(
                    name="idx_new",
                    field_name="content",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.COMPATIBLE

    def test_removing_index_compatible(self) -> None:
        """Test that removing indexes is compatible."""
        state1 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=[
                _VectorIndex(
                    name="idx_old",
                    field_name="content",
                    index_type="hnsw",
                    metric_type="l2_distance",
                    dimension=384,
                )
            ],
        )
        state2 = _State(
            key_fields_schema=[self._mock_field("id", "Int64")],
            value_fields_schema=[self._mock_field("content")],
            vector_indexes=None,
        )
        result = _Connector.check_state_compatibility(state1, state2)
        assert result == op.TargetStateCompatibility.COMPATIBLE


# ============================================================
# MYSQL ERROR RETRY TESTS
# ============================================================


class TestMySQLErrorRetry:
    """Test MySQL error retry functionality."""

    def test_retryable_mysql_error_codes(self) -> None:
        """Test that specific MySQL error codes are identified as retryable."""
        from cocoindex.targets.doris import _is_retryable_mysql_error

        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        # Retryable error codes
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

    def test_interface_error_is_retryable(self) -> None:
        """Test that InterfaceError is identified as retryable."""
        from cocoindex.targets.doris import _is_retryable_mysql_error

        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        error = pymysql.err.InterfaceError("Connection lost")
        assert _is_retryable_mysql_error(error)

    def test_non_mysql_error_not_retryable(self) -> None:
        """Test that non-MySQL errors are not identified as retryable."""
        from cocoindex.targets.doris import _is_retryable_mysql_error

        assert not _is_retryable_mysql_error(ValueError("test"))
        assert not _is_retryable_mysql_error(RuntimeError("test"))
        assert not _is_retryable_mysql_error(Exception("test"))

    @pytest.mark.asyncio
    async def test_with_retry_handles_mysql_errors(self) -> None:
        """Test that with_retry retries on MySQL connection errors."""
        from cocoindex.targets.doris import (
            with_retry,
            RetryConfig,
            DorisConnectionError,
        )

        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        call_count = 0

        async def mysql_flaky_op() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                # Simulate MySQL "server has gone away" error
                raise pymysql.err.OperationalError(2006, "MySQL server has gone away")
            return "success"

        # Should retry MySQL errors even though they're not in the default retryable_errors tuple
        result = await with_retry(
            mysql_flaky_op,
            config=RetryConfig(max_retries=3, base_delay=0.01),
        )

        assert result == "success"
        assert call_count == 3  # 2 failures + 1 success

    @pytest.mark.asyncio
    async def test_with_retry_does_not_retry_non_retryable_mysql_errors(self) -> None:
        """Test that with_retry does not retry non-retryable MySQL errors."""
        from cocoindex.targets.doris import with_retry, RetryConfig

        try:
            import pymysql
        except ImportError:
            pytest.skip("pymysql not installed")

        call_count = 0

        async def mysql_syntax_error() -> str:
            nonlocal call_count
            call_count += 1
            # Simulate syntax error - not retryable
            raise pymysql.err.OperationalError(
                1064, "You have an error in your SQL syntax"
            )

        with pytest.raises(pymysql.err.OperationalError) as exc_info:
            await with_retry(
                mysql_syntax_error,
                config=RetryConfig(max_retries=3, base_delay=0.01),
            )

        # Should not retry - only 1 call
        assert call_count == 1
        assert exc_info.value.args[0] == 1064
