"""
Pinecone target connector for CocoIndex.

Supports:
- Vector upsert/delete operations
- Metadata filtering
- Namespace isolation
"""

import asyncio
import dataclasses
import logging
from typing import Any

from cocoindex.targets._engine_builtin_specs import (
    PineconeConnection,
    Pinecone as PineconeSpec,
)

from pinecone import Pinecone as PineconeClient, ServerlessSpec  # type: ignore

from cocoindex import op
from cocoindex.auth_registry import get_auth_entry
from cocoindex.engine_type import (
    FieldSchema,
    BasicValueType,
)
from cocoindex.index import IndexOptions, VectorSimilarityMetric

_logger = logging.getLogger(__name__)

# Pinecone metric mapping
_PINECONE_VECTOR_METRIC: dict[VectorSimilarityMetric, str] = {
    VectorSimilarityMetric.COSINE_SIMILARITY: "cosine",
    VectorSimilarityMetric.L2_DISTANCE: "euclidean",
    VectorSimilarityMetric.INNER_PRODUCT: "dotproduct",
}


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema  # Single key field
    value_fields_schema: list[FieldSchema]
    vector_field_name: str  # Which field contains the embedding
    dimension: int
    metric: VectorSimilarityMetric
    namespace: str
    # Connection info needed for apply_setup_change
    api_key: str
    environment: str | None
    cloud: str
    region: str


@dataclasses.dataclass
class _IndexKey:
    """Unique identifier for Pinecone index"""

    index_name: str


@dataclasses.dataclass
class _MutateContext:
    index: Any  # Type: pinecone.Index (using Any to avoid runtime import)
    key_field_schema: FieldSchema
    vector_field_name: str
    metadata_field_names: list[str]
    namespace: str
    batch_size: int
    lock: asyncio.Lock


def _find_vector_field(value_fields_schema: list[FieldSchema]) -> tuple[str, int]:
    """
    Find the vector field and extract its dimension.

    Returns:
        Tuple of (field_name, dimension)

    Raises:
        ValueError if no vector field or multiple vector fields found
    """
    vector_fields = []
    for field in value_fields_schema:
        base_type = field.value_type.type
        if isinstance(base_type, BasicValueType) and base_type.kind == "Vector":
            if base_type.vector is not None and base_type.vector.dimension is not None:
                vector_fields.append((field.name, base_type.vector.dimension))

    if len(vector_fields) == 0:
        raise ValueError(
            "Pinecone requires exactly one vector field with fixed dimension"
        )
    if len(vector_fields) > 1:
        raise ValueError(
            f"Pinecone supports only one vector field per index, found {len(vector_fields)}: "
            f"{[name for name, _ in vector_fields]}"
        )

    return vector_fields[0]


def _convert_value_for_pinecone(value: Any) -> Any:
    """
    Convert Python values to Pinecone-compatible metadata format.

    Pinecone metadata supports: strings, numbers, booleans, and lists of strings.
    Complex types should be JSON-serialized.
    """
    if value is None:
        return None

    # Pinecone supports these types directly
    if isinstance(value, (str, int, float, bool)):
        return value

    # Lists of primitives
    if isinstance(value, (list, tuple)):
        return [_convert_value_for_pinecone(v) for v in value]

    # Convert complex types to JSON strings
    if isinstance(value, dict):
        import json

        return json.dumps(value)

    # Convert other types to strings
    return str(value)


@op.target_connector(
    spec_cls=PineconeSpec, persistent_key_type=_IndexKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: PineconeSpec) -> _IndexKey:
        return _IndexKey(index_name=spec.index_name)

    @staticmethod
    def get_setup_state(
        spec: PineconeSpec,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        # Pinecone requires exactly one key field (which is the vector ID)
        if len(key_fields_schema) != 1:
            raise ValueError("Pinecone requires exactly one key field")

        # Find the vector field and its dimension
        vector_field_name, dimension = _find_vector_field(value_fields_schema)

        # Get metric from vector indexes or use default
        metric = VectorSimilarityMetric.COSINE_SIMILARITY
        if index_options.vector_indexes:
            # Use the first vector index's metric
            for idx in index_options.vector_indexes:
                if idx.field_name == vector_field_name:
                    metric = idx.metric
                    break

        # Resolve the connection reference to get actual credentials
        connection = get_auth_entry(PineconeConnection, spec.connection)

        return _State(
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            vector_field_name=vector_field_name,
            dimension=dimension,
            metric=metric,
            namespace=spec.namespace,
            # Store connection info from resolved reference
            api_key=connection.api_key,
            environment=connection.environment,
            cloud=spec.cloud,
            region=spec.region,
        )

    @staticmethod
    def describe(key: _IndexKey) -> str:
        return f"Pinecone index {key.index_name}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        # Key field change → incompatible
        if previous.key_field_schema != current.key_field_schema:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Dimension change → incompatible (need new index)
        if previous.dimension != current.dimension:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Metric change → incompatible (need new index)
        if previous.metric != current.metric:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Vector field name change → incompatible
        if previous.vector_field_name != current.vector_field_name:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        # Metadata schema changes are OK (Pinecone is flexible)
        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _IndexKey, previous: _State | None, current: _State | None
    ) -> None:
        """
        Handle index creation/deletion.

        Note: This runs synchronously with Pinecone's sync client since
        index operations are infrequent and Pinecone doesn't have async API.
        """
        # Get state for connection (use current or previous)
        state = current or previous
        if state is None:
            return

        try:
            # Initialize Pinecone using state's api_key
            pc = PineconeClient(api_key=state.api_key)
        except Exception as e:
            _logger.error(f"Failed to initialize Pinecone client: {e}")
            raise

        # Handle index deletion
        if current is None and previous is not None:
            try:
                if key.index_name in pc.list_indexes().names():
                    _logger.info(f"Deleting Pinecone index {key.index_name}")
                    pc.delete_index(key.index_name)
            except Exception as e:
                _logger.error(f"Failed to delete Pinecone index {key.index_name}: {e}")
                raise
            return

        # Handle index creation
        if current is not None:
            try:
                existing_indexes = pc.list_indexes().names()
            except Exception as e:
                _logger.error(f"Failed to list Pinecone indexes: {e}")
                raise

            if key.index_name not in existing_indexes:
                _logger.info(
                    f"Creating Pinecone index {key.index_name} "
                    f"with dimension {current.dimension} "
                    f"and metric {current.metric}"
                )

                try:
                    # Use state's cloud and region
                    pc.create_index(
                        name=key.index_name,
                        dimension=current.dimension,
                        metric=_PINECONE_VECTOR_METRIC[current.metric],
                        spec=ServerlessSpec(cloud=state.cloud, region=state.region),
                    )
                except Exception as e:
                    _logger.error(
                        f"Failed to create Pinecone index {key.index_name}: {e}"
                    )
                    raise
            else:
                try:
                    # Validate existing index matches requirements
                    index_stats = pc.Index(key.index_name).describe_index_stats()
                    if index_stats["dimension"] != current.dimension:
                        raise ValueError(
                            f"Index {key.index_name} exists with dimension "
                            f"{index_stats['dimension']}, but expected {current.dimension}"
                        )
                except Exception as e:
                    _logger.error(
                        f"Failed to validate Pinecone index {key.index_name}: {e}"
                    )
                    raise

    @staticmethod
    async def prepare(
        spec: PineconeSpec,
        setup_state: _State,
    ) -> _MutateContext:
        """Prepare for mutations by getting index reference."""
        # Use setup_state's api_key (which has the resolved connection)
        pc = PineconeClient(api_key=setup_state.api_key)
        index = pc.Index(spec.index_name)

        # Get list of metadata field names (all non-vector fields)
        metadata_field_names = [
            field.name
            for field in setup_state.value_fields_schema
            if field.name != setup_state.vector_field_name
        ]

        return _MutateContext(
            index=index,
            key_field_schema=setup_state.key_field_schema,
            vector_field_name=setup_state.vector_field_name,
            metadata_field_names=metadata_field_names,
            namespace=setup_state.namespace,
            batch_size=spec.batch_size,
            lock=asyncio.Lock(),
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        """
        Apply mutations (upserts and deletes).

        Pinecone operations are batched for efficiency.
        """
        for context, mutations in all_mutations:
            vectors_to_upsert: list[dict[str, Any]] = []
            ids_to_delete: list[str] = []

            for key, value in mutations.items():
                # Convert key to string ID (Pinecone requires string IDs)
                vector_id = str(key)

                if value is None:
                    # Delete operation
                    ids_to_delete.append(vector_id)
                else:
                    # Upsert operation
                    # Extract vector
                    vector_values = value.get(context.vector_field_name)
                    if vector_values is None:
                        raise ValueError(
                            f"Missing vector field '{context.vector_field_name}' in value"
                        )

                    # Extract metadata (all non-vector fields)
                    # Include all fields, even with None values to ensure updates
                    metadata = {
                        field_name: _convert_value_for_pinecone(value.get(field_name))
                        for field_name in context.metadata_field_names
                    }

                    vectors_to_upsert.append(
                        {"id": vector_id, "values": vector_values, "metadata": metadata}
                    )

            # Execute operations in batches
            async with context.lock:
                # Batch upserts
                for i in range(0, len(vectors_to_upsert), context.batch_size):
                    upsert_batch = vectors_to_upsert[i : i + context.batch_size]
                    # Run in thread pool since Pinecone client is sync
                    await asyncio.to_thread(
                        context.index.upsert,
                        vectors=upsert_batch,
                        namespace=context.namespace,
                    )

                # Batch deletes
                for i in range(0, len(ids_to_delete), context.batch_size):
                    delete_batch = ids_to_delete[i : i + context.batch_size]
                    await asyncio.to_thread(
                        context.index.delete,
                        ids=delete_batch,
                        namespace=context.namespace,
                    )

    @staticmethod
    async def cleanup(context: _MutateContext) -> None:
        """Cleanup resources. Pinecone client doesn't need explicit cleanup."""
        pass


# Public helper function for querying
def get_index(
    api_key: str,
    index_name: str,
) -> Any:
    """
    Helper function to get a Pinecone index for querying.

    Usage:
        index = get_index(api_key="xxx", index_name="my-index")
        results = index.query(
            vector=[0.1, 0.2, ...],
            top_k=10,
            namespace="my-namespace"
        )
    """
    pc = PineconeClient(api_key=api_key)
    return pc.Index(index_name)
