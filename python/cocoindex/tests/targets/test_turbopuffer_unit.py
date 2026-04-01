"""
Unit tests for Turbopuffer connector (no turbopuffer account required).
"""
# mypy: disable-error-code="no-untyped-def"

from typing import Literal
import pytest

from cocoindex.targets.turbopuffer import (
    Turbopuffer,
    _NamespaceKey,
    _State,
    _Connector,
    _convert_key_to_id,
    _convert_value_to_attribute,
    _is_vector_field,
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
# PERSISTENT KEY TESTS
# ============================================================


class TestGetPersistentKey:
    def test_returns_correct_namespace_key(self):
        spec = Turbopuffer(
            namespace_name="my-ns", api_key="tpuf_test", region="aws-us-east-1"
        )
        key = _Connector.get_persistent_key(spec)
        assert isinstance(key, _NamespaceKey)
        assert key.namespace_name == "my-ns"
        assert key.region == "aws-us-east-1"

    def test_uses_default_region(self):
        spec = Turbopuffer(namespace_name="my-ns", api_key="tpuf_test")
        key = _Connector.get_persistent_key(spec)
        assert key.region == "gcp-us-central1"


# ============================================================
# SETUP STATE TESTS
# ============================================================


class TestGetSetupState:
    def _make_index_options(
        self, metric: VectorSimilarityMetric = VectorSimilarityMetric.COSINE_SIMILARITY
    ) -> IndexOptions:
        return IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[VectorIndexDef(field_name="embedding", metric=metric)],
        )

    def test_single_key_single_vector(self):
        spec = Turbopuffer(namespace_name="ns", api_key="key")
        key_fields = [_mock_field("id", "Str")]
        value_fields = [
            _mock_field("embedding", "Vector", dim=384),
            _mock_field("text", "Str"),
        ]
        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, self._make_index_options()
        )
        assert state.key_field_schema == key_fields[0]
        assert state.value_fields_schema == value_fields
        assert state.distance_metric == "cosine_distance"
        assert state.api_key == "key"

    def test_rejects_multiple_keys(self):
        spec = Turbopuffer(namespace_name="ns", api_key="key")
        key_fields = [_mock_field("id1", "Str"), _mock_field("id2", "Str")]
        value_fields = [_mock_field("embedding", "Vector", dim=384)]
        with pytest.raises(ValueError, match="single key field"):
            _Connector.get_setup_state(
                spec, key_fields, value_fields, self._make_index_options()
            )

    def test_rejects_no_vector(self):
        spec = Turbopuffer(namespace_name="ns", api_key="key")
        key_fields = [_mock_field("id", "Str")]
        value_fields = [_mock_field("text", "Str")]
        with pytest.raises(ValueError, match="vector field"):
            _Connector.get_setup_state(
                spec,
                key_fields,
                value_fields,
                IndexOptions(primary_key_fields=["id"]),
            )

    def test_rejects_multiple_vectors(self):
        spec = Turbopuffer(namespace_name="ns", api_key="key")
        key_fields = [_mock_field("id", "Str")]
        value_fields = [
            _mock_field("emb1", "Vector", dim=384),
            _mock_field("emb2", "Vector", dim=768),
        ]
        with pytest.raises(ValueError, match="single vector field"):
            _Connector.get_setup_state(
                spec, key_fields, value_fields, self._make_index_options()
            )

    @pytest.mark.parametrize(
        "metric,expected",
        [
            (VectorSimilarityMetric.COSINE_SIMILARITY, "cosine_distance"),
            (VectorSimilarityMetric.L2_DISTANCE, "euclidean_squared"),
            (VectorSimilarityMetric.INNER_PRODUCT, "dot_product"),
        ],
    )
    def test_distance_metric_mapping(self, metric, expected):
        spec = Turbopuffer(namespace_name="ns", api_key="key")
        key_fields = [_mock_field("id", "Str")]
        value_fields = [_mock_field("embedding", "Vector", dim=384)]
        index_options = IndexOptions(
            primary_key_fields=["id"],
            vector_indexes=[VectorIndexDef(field_name="embedding", metric=metric)],
        )
        state = _Connector.get_setup_state(
            spec, key_fields, value_fields, index_options
        )
        assert state.distance_metric == expected


# ============================================================
# STATE COMPATIBILITY TESTS
# ============================================================


class TestCheckStateCompatibility:
    def _make_state(
        self,
        key_name: str = "id",
        metric: str = "cosine_distance",
        value_name: str = "text",
    ) -> _State:
        return _State(
            key_field_schema=_mock_field(key_name, "Str"),
            value_fields_schema=[
                _mock_field("embedding", "Vector", dim=384),
                _mock_field(value_name, "Str"),
            ],
            distance_metric=metric,
            api_key="key",
        )

    def test_compatible_when_same(self):
        s1 = self._make_state()
        s2 = self._make_state()
        assert (
            _Connector.check_state_compatibility(s1, s2)
            == op.TargetStateCompatibility.COMPATIBLE
        )

    def test_not_compatible_on_key_change(self):
        s1 = self._make_state(key_name="id")
        s2 = self._make_state(key_name="new_id")
        assert (
            _Connector.check_state_compatibility(s1, s2)
            == op.TargetStateCompatibility.NOT_COMPATIBLE
        )

    def test_not_compatible_on_metric_change(self):
        s1 = self._make_state(metric="cosine_distance")
        s2 = self._make_state(metric="euclidean_squared")
        assert (
            _Connector.check_state_compatibility(s1, s2)
            == op.TargetStateCompatibility.NOT_COMPATIBLE
        )

    def test_compatible_on_value_field_change(self):
        s1 = self._make_state(value_name="text")
        s2 = self._make_state(value_name="content")
        assert (
            _Connector.check_state_compatibility(s1, s2)
            == op.TargetStateCompatibility.COMPATIBLE
        )


# ============================================================
# DESCRIBE TESTS
# ============================================================


class TestDescribe:
    def test_format(self):
        key = _NamespaceKey(region="gcp-us-central1", namespace_name="my-ns")
        assert _Connector.describe(key) == "Turbopuffer namespace my-ns@gcp-us-central1"


# ============================================================
# HELPER FUNCTION TESTS
# ============================================================


class TestConvertKeyToId:
    def test_string_passthrough(self):
        assert _convert_key_to_id("abc") == "abc"

    def test_int_to_str(self):
        assert _convert_key_to_id(42) == "42"

    def test_float_to_str(self):
        assert _convert_key_to_id(3.14) == "3.14"

    def test_bool_to_str(self):
        assert _convert_key_to_id(True) == "True"

    def test_complex_to_json(self):
        result = _convert_key_to_id({"a": 1, "b": 2})
        assert result == '{"a": 1, "b": 2}'

    def test_list_to_json(self):
        result = _convert_key_to_id([1, 2, 3])
        assert result == "[1, 2, 3]"


class TestConvertValueToAttribute:
    def test_string_passthrough(self):
        assert _convert_value_to_attribute("hello") == "hello"

    def test_int_passthrough(self):
        assert _convert_value_to_attribute(42) == 42

    def test_float_passthrough(self):
        assert _convert_value_to_attribute(3.14) == 3.14

    def test_bool_passthrough(self):
        assert _convert_value_to_attribute(True) is True

    def test_none_returns_none(self):
        assert _convert_value_to_attribute(None) is None

    def test_complex_to_json(self):
        result = _convert_value_to_attribute({"key": "value"})
        assert result == '{"key": "value"}'

    def test_list_to_json(self):
        result = _convert_value_to_attribute([1, 2, 3])
        assert result == "[1, 2, 3]"


class TestIsVectorField:
    def test_vector_field(self):
        field = _mock_field("embedding", "Vector", dim=384)
        assert _is_vector_field(field) is True

    def test_non_vector_field(self):
        field = _mock_field("text", "Str")
        assert _is_vector_field(field) is False

    def test_int_field(self):
        field = _mock_field("count", "Int64")
        assert _is_vector_field(field) is False
