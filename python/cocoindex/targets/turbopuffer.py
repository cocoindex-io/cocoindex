import dataclasses
import json
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import turbopuffer  # type: ignore

from cocoindex import op
from cocoindex.engine_type import FieldSchema, BasicValueType
from cocoindex.index import IndexOptions, VectorSimilarityMetric

_logger = logging.getLogger(__name__)


def _get_turbopuffer() -> Any:
    """Lazily import turbopuffer to avoid import errors when not installed."""
    try:
        import turbopuffer  # type: ignore

        return turbopuffer
    except ImportError:
        raise ImportError(
            "turbopuffer is required for Turbopuffer connector. "
            "Install it with: pip install turbopuffer"
        )


_TURBOPUFFER_DISTANCE_METRIC: dict[VectorSimilarityMetric, str] = {
    VectorSimilarityMetric.COSINE_SIMILARITY: "cosine_distance",
    VectorSimilarityMetric.L2_DISTANCE: "euclidean_squared",
    VectorSimilarityMetric.INNER_PRODUCT: "dot_product",
}


class Turbopuffer(op.TargetSpec):
    namespace_name: str
    api_key: str
    region: str = "gcp-us-central1"


@dataclasses.dataclass
class _NamespaceKey:
    region: str
    namespace_name: str


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    distance_metric: str
    api_key: str


@dataclasses.dataclass
class _MutateContext:
    client: Any  # turbopuffer.Turbopuffer
    namespace: Any  # turbopuffer.lib.namespace.Namespace
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    distance_metric: str


def _get_client(spec: Turbopuffer) -> Any:
    tpuf = _get_turbopuffer()
    return tpuf.Turbopuffer(
        api_key=spec.api_key,
        region=spec.region,
    )


def _convert_key_to_id(key: Any) -> str:
    if isinstance(key, str):
        return key
    elif isinstance(key, (int, float, bool)):
        return str(key)
    else:
        return json.dumps(key, sort_keys=True, default=str)


def _convert_value_to_attribute(value: Any) -> str | int | float | bool | None:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _is_vector_field(field: FieldSchema) -> bool:
    value_type = field.value_type.type
    if isinstance(value_type, BasicValueType):
        return value_type.kind == "Vector"
    return False


@op.target_connector(
    spec_cls=Turbopuffer, persistent_key_type=_NamespaceKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: Turbopuffer) -> _NamespaceKey:
        return _NamespaceKey(
            region=spec.region,
            namespace_name=spec.namespace_name,
        )

    @staticmethod
    def get_setup_state(
        spec: Turbopuffer,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        if len(key_fields_schema) != 1:
            raise ValueError("Turbopuffer only supports a single key field")

        vector_fields = [f for f in value_fields_schema if _is_vector_field(f)]
        if not vector_fields:
            raise ValueError(
                "Turbopuffer requires a vector field in the value schema for embeddings."
            )
        if len(vector_fields) > 1:
            raise ValueError(
                f"Turbopuffer only supports a single vector field per namespace, "
                f"but found {len(vector_fields)}: {[f.name for f in vector_fields]}. "
                f"Consider using LanceDB or Qdrant for multiple vector fields."
            )

        distance_metric = "cosine_distance"  # Default
        if index_options.vector_indexes:
            if len(index_options.vector_indexes) > 1:
                raise ValueError(
                    "Turbopuffer only supports a single vector index per namespace"
                )
            vector_index = index_options.vector_indexes[0]
            distance_metric = _TURBOPUFFER_DISTANCE_METRIC.get(
                vector_index.metric, "cosine_distance"
            )

        return _State(
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            distance_metric=distance_metric,
            api_key=spec.api_key,
        )

    @staticmethod
    def describe(key: _NamespaceKey) -> str:
        return f"Turbopuffer namespace {key.namespace_name}@{key.region}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        if previous.key_field_schema != current.key_field_schema:
            return op.TargetStateCompatibility.NOT_COMPATIBLE
        if previous.distance_metric != current.distance_metric:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    def apply_setup_change(
        key: _NamespaceKey, previous: _State | None, current: _State | None
    ) -> None:
        if previous is None and current is None:
            return
        state = current or previous
        if state is None:
            return

        # Delete namespace data if previous state exists and we're removing or recreating
        if previous is not None:
            should_delete = current is None or (
                previous.key_field_schema != current.key_field_schema
                or previous.distance_metric != current.distance_metric
            )
            if should_delete:
                try:
                    tpuf = _get_turbopuffer()
                    client = tpuf.Turbopuffer(
                        api_key=state.api_key,
                        region=key.region,
                    )
                    ns = client.namespace(key.namespace_name)
                    ns.delete_all()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    _logger.debug(
                        "Namespace %s not found for deletion: %s",
                        key.namespace_name,
                        e,
                    )

        # Turbopuffer namespaces are created implicitly on first write — no setup needed.

    @staticmethod
    def prepare(
        spec: Turbopuffer,
        setup_state: _State,
    ) -> _MutateContext:
        client = _get_client(spec)
        ns = client.namespace(spec.namespace_name)

        return _MutateContext(
            client=client,
            namespace=ns,
            key_field_schema=setup_state.key_field_schema,
            value_fields_schema=setup_state.value_fields_schema,
            distance_metric=setup_state.distance_metric,
        )

    @staticmethod
    def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            if not mutations:
                continue

            ids_to_delete: list[str] = []
            rows_to_upsert: list[dict[str, Any]] = []

            # Find the vector field name
            vector_field_name: str | None = None
            for field in context.value_fields_schema:
                if _is_vector_field(field):
                    vector_field_name = field.name
                    break

            for key, value in mutations.items():
                doc_id = _convert_key_to_id(key)

                if value is None:
                    ids_to_delete.append(doc_id)
                else:
                    row: dict[str, Any] = {"id": doc_id}

                    # Extract vector
                    if vector_field_name and vector_field_name in value:
                        embedding = value[vector_field_name]
                        if embedding is None:
                            raise ValueError(
                                f"Missing embedding for document {doc_id}. "
                                f"Turbopuffer requires an embedding for each document."
                            )
                        row["vector"] = embedding

                    # Build attributes from non-vector fields
                    for field in context.value_fields_schema:
                        if field.name == vector_field_name:
                            continue
                        if field.name in value:
                            converted = _convert_value_to_attribute(value[field.name])
                            if converted is not None:
                                row[field.name] = converted

                    rows_to_upsert.append(row)

            # Execute upserts
            if rows_to_upsert:
                context.namespace.write(
                    upsert_rows=rows_to_upsert,
                    distance_metric=context.distance_metric,
                )

            # Execute deletes
            if ids_to_delete:
                context.namespace.write(
                    deletes=ids_to_delete,
                )
