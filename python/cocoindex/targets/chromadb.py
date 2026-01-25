import asyncio
import dataclasses
import json
import logging
from enum import Enum
from typing import Any

import chromadb  # type: ignore

from cocoindex import op
from cocoindex.engine_type import FieldSchema, BasicValueType
from cocoindex.index import IndexOptions, VectorSimilarityMetric

_logger = logging.getLogger(__name__)

_CHROMADB_DISTANCE_METRIC: dict[VectorSimilarityMetric, str] = {
    VectorSimilarityMetric.COSINE_SIMILARITY: "cosine",
    VectorSimilarityMetric.L2_DISTANCE: "l2",
    VectorSimilarityMetric.INNER_PRODUCT: "ip",
}


class ClientType(Enum):
    PERSISTENT = "persistent"
    HTTP = "http"


class HnswConfig:
    m: int | None = None
    ef_construction: int | None = None
    ef_search: int | None = None


class ChromaDB(op.TargetSpec):
    collection_name: str
    client_type: ClientType = ClientType.PERSISTENT
    path: str | None = None  # For PersistentClient
    host: str | None = None  # For HttpClient
    port: int | None = None  # For HttpClient
    ssl: bool = False  # For HttpClient
    tenant: str = chromadb.config.DEFAULT_TENANT
    database: str = chromadb.config.DEFAULT_DATABASE
    hnsw_config: HnswConfig | None = None


@dataclasses.dataclass
class _CollectionKey:
    client_type: ClientType
    location: str  # path for persistent, host:port for http
    collection_name: str
    tenant: str
    database: str


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    distance_metric: str
    hnsw_config: HnswConfig | None = None


@dataclasses.dataclass
class _MutateContext:
    client: chromadb.ClientAPI
    collection: chromadb.Collection
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    lock: asyncio.Lock


def _get_client(spec: ChromaDB) -> chromadb.ClientAPI:
    if spec.client_type == ClientType.PERSISTENT:
        path = spec.path or "./chromadb_data"
        return chromadb.PersistentClient(
            path=path,
            tenant=spec.tenant,
            database=spec.database,
        )
    else:  # HTTP client
        host = spec.host or "localhost"
        port = spec.port or 8000
        return chromadb.HttpClient(
            host=host,
            port=port,
            ssl=spec.ssl,
            tenant=spec.tenant,
            database=spec.database,
        )


def _get_location(spec: ChromaDB) -> str:
    if spec.client_type == ClientType.PERSISTENT:
        return spec.path or "./chromadb_data"
    else:
        host = spec.host or "localhost"
        port = spec.port or 8000
        return f"{host}:{port}"


def _convert_key_to_id(key: Any) -> str:
    if isinstance(key, str):
        return key
    elif isinstance(key, (int, float, bool)):
        return str(key)
    else:
        # For complex types, JSON serialize
        return json.dumps(key, sort_keys=True, default=str)


def _convert_value_to_metadata(value: Any) -> str | int | float | bool | None:
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


def _build_hnsw_metadata(
    distance_metric: str, hnsw_config: HnswConfig | None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"hnsw:space": distance_metric}

    if hnsw_config is not None:
        if hnsw_config.m is not None:
            metadata["hnsw:M"] = hnsw_config.m
        if hnsw_config.ef_construction is not None:
            metadata["hnsw:construction_ef"] = hnsw_config.ef_construction
        if hnsw_config.ef_search is not None:
            metadata["hnsw:search_ef"] = hnsw_config.ef_search

    return metadata


@op.target_connector(
    spec_cls=ChromaDB, persistent_key_type=_CollectionKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: ChromaDB) -> _CollectionKey:
        return _CollectionKey(
            client_type=spec.client_type,
            location=_get_location(spec),
            collection_name=spec.collection_name,
            tenant=spec.tenant,
            database=spec.database,
        )

    @staticmethod
    def get_setup_state(
        spec: ChromaDB,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        # Validate single key field
        if len(key_fields_schema) != 1:
            raise ValueError("ChromaDB only supports a single key field")

        # Find vector fields
        vector_fields = [f for f in value_fields_schema if _is_vector_field(f)]
        if len(vector_fields) > 1:
            raise ValueError(
                f"ChromaDB only supports a single vector field per collection, "
                f"but found {len(vector_fields)}: {[f.name for f in vector_fields]}. "
                f"Consider using LanceDB or Qdrant for multiple vector fields."
            )

        # Get distance metric from index options
        distance_metric = "cosine"  # Default
        if index_options.vector_indexes:
            if len(index_options.vector_indexes) > 1:
                raise ValueError(
                    "ChromaDB only supports a single vector index per collection"
                )
            vector_index = index_options.vector_indexes[0]
            distance_metric = _CHROMADB_DISTANCE_METRIC.get(
                vector_index.metric, "cosine"
            )

        return _State(
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            distance_metric=distance_metric,
            hnsw_config=spec.hnsw_config,
        )

    @staticmethod
    def describe(key: _CollectionKey) -> str:
        return f"ChromaDB collection {key.collection_name}@{key.location}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        # Key schema or distance metric changes require recreation
        if previous.key_field_schema != current.key_field_schema:
            return op.TargetStateCompatibility.NOT_COMPATIBLE
        if previous.distance_metric != current.distance_metric:
            return op.TargetStateCompatibility.NOT_COMPATIBLE

        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _CollectionKey, previous: _State | None, current: _State | None
    ) -> None:
        if previous is None and current is None:
            return

        # Create a temporary spec to get the client
        # Use the key to reconstruct the client configuration
        if key.client_type == ClientType.PERSISTENT:
            client = chromadb.PersistentClient(
                path=key.location,
                tenant=key.tenant,
                database=key.database,
            )
        else:
            host, port_str = key.location.rsplit(":", 1)
            client = chromadb.HttpClient(
                host=host,
                port=int(port_str),
                tenant=key.tenant,
                database=key.database,
            )

        # Delete collection if previous state exists and incompatible
        if previous is not None:
            should_delete = current is None or (
                previous.key_field_schema != current.key_field_schema
                or previous.distance_metric != current.distance_metric
            )
            if should_delete:
                try:
                    await asyncio.to_thread(
                        client.delete_collection, key.collection_name
                    )
                except Exception as e:  # pylint: disable=broad-exception-caught
                    _logger.debug(
                        "Collection %s not found for deletion: %s",
                        key.collection_name,
                        e,
                    )

        if current is None:
            return

        # Get or create collection with HNSW metadata
        metadata = _build_hnsw_metadata(current.distance_metric, current.hnsw_config)

        await asyncio.to_thread(
            client.get_or_create_collection,
            name=key.collection_name,
            metadata=metadata,
        )

    @staticmethod
    async def prepare(
        spec: ChromaDB,
        setup_state: _State,
    ) -> _MutateContext:
        client = _get_client(spec)
        collection = await asyncio.to_thread(client.get_collection, spec.collection_name)

        return _MutateContext(
            client=client,
            collection=collection,
            key_field_schema=setup_state.key_field_schema,
            value_fields_schema=setup_state.value_fields_schema,
            lock=asyncio.Lock(),
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            if not mutations:
                continue

            ids_to_delete: list[str] = []
            ids_to_upsert: list[str] = []
            embeddings_to_upsert: list[list[float]] = []
            metadatas_to_upsert: list[dict[str, Any]] = []
            documents_to_upsert: list[str | None] = []

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
                    ids_to_upsert.append(doc_id)

                    # Extract embedding
                    embedding: list[float] | None = None
                    if vector_field_name and vector_field_name in value:
                        embedding = value[vector_field_name]

                    if embedding is None:
                        raise ValueError(
                            f"Missing embedding for document {doc_id}. "
                            f"ChromaDB requires an embedding for each document."
                        )
                    embeddings_to_upsert.append(embedding)

                    # Build metadata from non-vector fields
                    metadata: dict[str, Any] = {}
                    document: str | None = None

                    for field in context.value_fields_schema:
                        if field.name == vector_field_name:
                            continue
                        if field.name in value:
                            field_value = value[field.name]
                            # Use document field for text content if present
                            if field.name in ("content", "text", "document"):
                                if isinstance(field_value, str):
                                    document = field_value
                            else:
                                converted = _convert_value_to_metadata(field_value)
                                if converted is not None:
                                    metadata[field.name] = converted

                    metadatas_to_upsert.append(metadata)
                    documents_to_upsert.append(document)

            async with context.lock:
                # Execute deletes
                if ids_to_delete:
                    await asyncio.to_thread(
                        context.collection.delete, ids=ids_to_delete
                    )

                # Execute upserts
                if ids_to_upsert:
                    await asyncio.to_thread(
                        context.collection.upsert,
                        ids=ids_to_upsert,
                        embeddings=embeddings_to_upsert,
                        metadatas=metadatas_to_upsert if any(metadatas_to_upsert) else None,
                        documents=documents_to_upsert if any(documents_to_upsert) else None,
                    )
