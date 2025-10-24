import dataclasses
import logging
import uuid
from typing import Any

try:
    import chromadb  # type: ignore
except ImportError as e:
    raise ImportError(
        "ChromaDB optional dependency not installed. "
        "Install with: pip install 'cocoindex[chromadb]'"
    ) from e

from .. import op
from ..typing import (
    FieldSchema,
    EnrichedValueType,
    BasicValueType,
    StructType,
    ValueType,
    TableType,
)
from ..index import IndexOptions

_logger = logging.getLogger(__name__)


class ChromaDB(op.TargetSpec):
    """ChromaDB target specification.
    
    Args:
        collection_name: Name of the ChromaDB collection
        client_path: Path for persistent client (if None, uses ephemeral client)
        client_settings: Optional settings dict for ChromaDB client
    """
    collection_name: str
    client_path: str | None = None
    client_settings: dict[str, Any] | None = None


@dataclasses.dataclass
class _State:
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]
    collection_name: str
    client_path: str | None = None
    client_settings: dict[str, Any] | None = None


@dataclasses.dataclass
class _TableKey:
    client_path: str
    collection_name: str


@dataclasses.dataclass
class _MutateContext:
    collection: Any  # chromadb.Collection
    key_field_schema: FieldSchema
    value_fields_schema: list[FieldSchema]


def _convert_value_for_chromadb(value_type: ValueType, v: Any) -> Any:
    """Convert value to ChromaDB-compatible format."""
    if v is None:
        return None
        
    if isinstance(value_type, BasicValueType):
        # Handle UUID conversion
        if isinstance(v, uuid.UUID):
            return str(v)
        
        # Handle Range type
        if value_type.kind == "Range":
            return {"start": v[0], "end": v[1]}
        
        # Handle Vector type - ChromaDB stores as list of floats
        if value_type.vector is not None:
            return [float(_convert_value_for_chromadb(value_type.vector.element_type, e)) for e in v]
        
        return v
    
    elif isinstance(value_type, StructType):
        return _convert_fields_for_chromadb(value_type.fields, v)
    
    elif isinstance(value_type, TableType):
        if isinstance(v, list):
            return [_convert_fields_for_chromadb(value_type.row.fields, item) for item in v]
        else:
            key_fields = value_type.row.fields[:value_type.num_key_parts]
            value_fields = value_type.row.fields[value_type.num_key_parts:]
            return [
                _convert_fields_for_chromadb(key_fields, item[:value_type.num_key_parts])
                | _convert_fields_for_chromadb(value_fields, item[value_type.num_key_parts:])
                for item in v
            ]
    
    return v


def _convert_fields_for_chromadb(fields: list[FieldSchema], v: Any) -> dict:
    """Convert fields to ChromaDB document format."""
    if isinstance(v, dict):
        return {
            field.name: _convert_value_for_chromadb(field.value_type.type, v.get(field.name))
            for field in fields
        }
    elif isinstance(v, tuple):
        return {
            field.name: _convert_value_for_chromadb(field.value_type.type, value)
            for field, value in zip(fields, v)
        }
    else:
        # Single value case
        field = fields[0]
        return {field.name: _convert_value_for_chromadb(field.value_type.type, v)}


def _extract_embedding(value_dict: dict, value_fields: list[FieldSchema]) -> list[float] | None:
    """Extract embedding vector from value fields if present."""
    for field in value_fields:
        if isinstance(field.value_type.type, BasicValueType):
            if field.value_type.type.vector is not None:
                vec = value_dict.get(field.name)
                if vec is not None:
                    return [float(x) for x in vec]
    return None


@op.target_connector(
    spec_cls=ChromaDB, persistent_key_type=_TableKey, setup_state_cls=_State
)
class _Connector:
    @staticmethod
    def get_persistent_key(spec: ChromaDB) -> _TableKey:
        return _TableKey(
            client_path=spec.client_path or ":memory:",
            collection_name=spec.collection_name
        )

    @staticmethod
    def get_setup_state(
        spec: ChromaDB,
        key_fields_schema: list[FieldSchema],
        value_fields_schema: list[FieldSchema],
        index_options: IndexOptions,
    ) -> _State:
        if len(key_fields_schema) != 1:
            raise ValueError("ChromaDB only supports a single key field")
        
        if index_options.vector_indexes is not None:
            _logger.warning(
                "Vector index configuration not yet supported in ChromaDB target (Phase 1). "
                "Embeddings will be stored but indexing options are ignored."
            )
        
        return _State(
            key_field_schema=key_fields_schema[0],
            value_fields_schema=value_fields_schema,
            collection_name=spec.collection_name,
            client_path=spec.client_path,
            client_settings=spec.client_settings,
        )

    @staticmethod
    def describe(key: _TableKey) -> str:
        return f"ChromaDB collection {key.collection_name}@{key.client_path}"

    @staticmethod
    def check_state_compatibility(
        previous: _State, current: _State
    ) -> op.TargetStateCompatibility:
        if (
            previous.key_field_schema != current.key_field_schema
            or previous.value_fields_schema != current.value_fields_schema
        ):
            return op.TargetStateCompatibility.NOT_COMPATIBLE
        return op.TargetStateCompatibility.COMPATIBLE

    @staticmethod
    async def apply_setup_change(
        key: _TableKey, previous: _State | None, current: _State | None
    ) -> None:
        latest_state = current or previous
        if not latest_state:
            return

        # Create or connect to ChromaDB client
        if latest_state.client_path and latest_state.client_path != ":memory:":
            client = chromadb.PersistentClient(
                path=latest_state.client_path,
                settings=chromadb.Settings(**(latest_state.client_settings or {}))
            )
        else:
            client = chromadb.Client(
                settings=chromadb.Settings(**(latest_state.client_settings or {}))
            )

        # Handle collection lifecycle
        if previous is not None and current is None:
            # Delete collection
            try:
                client.delete_collection(name=key.collection_name)
            except Exception as e:
                _logger.warning(
                    "Failed to delete collection %s: %s",
                    key.collection_name,
                    e
                )
            return

        if current is not None:
            # Check if schema changed (not compatible)
            reuse = previous is not None and _Connector.check_state_compatibility(
                previous, current
            ) == op.TargetStateCompatibility.COMPATIBLE

            if not reuse and previous is not None:
                # Schema changed, need to recreate
                try:
                    client.delete_collection(name=key.collection_name)
                except Exception:
                    pass  # Collection might not exist

            # Create or get collection
            try:
                collection = client.get_or_create_collection(
                    name=current.collection_name
                )
                _logger.info(
                    "ChromaDB collection %s ready with %d items",
                    current.collection_name,
                    collection.count()
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to create/open ChromaDB collection {current.collection_name}: {e}"
                ) from e

    @staticmethod
    async def prepare(
        spec: ChromaDB,
        setup_state: _State,
    ) -> _MutateContext:
        # Connect to client
        if setup_state.client_path and setup_state.client_path != ":memory:":
            client = chromadb.PersistentClient(
                path=setup_state.client_path,
                settings=chromadb.Settings(**(setup_state.client_settings or {}))
            )
        else:
            client = chromadb.Client(
                settings=chromadb.Settings(**(setup_state.client_settings or {}))
            )

        # Get collection
        collection = client.get_collection(name=spec.collection_name)

        return _MutateContext(
            collection=collection,
            key_field_schema=setup_state.key_field_schema,
            value_fields_schema=setup_state.value_fields_schema,
        )

    @staticmethod
    async def mutate(
        *all_mutations: tuple[_MutateContext, dict[Any, dict[str, Any] | None]],
    ) -> None:
        for context, mutations in all_mutations:
            ids_to_upsert = []
            metadatas_to_upsert = []
            documents_to_upsert = []
            embeddings_to_upsert = []
            ids_to_delete = []

            key_name = context.key_field_schema.name

            for key, value in mutations.items():
                # Convert key to string ID
                if isinstance(key, uuid.UUID):
                    key_id = str(key)
                else:
                    key_id = str(key)

                if value is None:
                    # Deletion
                    ids_to_delete.append(key_id)
                else:
                    # Upsert
                    ids_to_upsert.append(key_id)

                    # Convert value fields to metadata
                    metadata = {}
                    embedding = None
                    document_text = None

                    for field_schema, (field_name, field_value) in zip(
                        context.value_fields_schema, value.items()
                    ):
                        converted = _convert_value_for_chromadb(
                            field_schema.value_type.type, field_value
                        )

                        # Check if this is an embedding field
                        if isinstance(field_schema.value_type.type, BasicValueType):
                            if field_schema.value_type.type.vector is not None:
                                embedding = converted
                                continue

                        # Store as metadata (ChromaDB supports str, int, float, bool)
                        if isinstance(converted, (str, int, float, bool)):
                            metadata[field_name] = converted
                        elif converted is None:
                            metadata[field_name] = None
                        else:
                            # Convert complex types to string
                            import json
                            metadata[field_name] = json.dumps(converted)

                    # Use key as document if no specific text field
                    document_text = key_id
                    documents_to_upsert.append(document_text)
                    metadatas_to_upsert.append(metadata)
                    if embedding:
                        embeddings_to_upsert.append(embedding)

            # Execute deletions
            if ids_to_delete:
                try:
                    context.collection.delete(ids=ids_to_delete)
                except Exception as e:
                    _logger.warning("Failed to delete some IDs: %s", e)

            # Execute upserts
            if ids_to_upsert:
                if embeddings_to_upsert:
                    context.collection.upsert(
                        ids=ids_to_upsert,
                        embeddings=embeddings_to_upsert,
                        metadatas=metadatas_to_upsert,
                        documents=documents_to_upsert,
                    )
                else:
                    context.collection.upsert(
                        ids=ids_to_upsert,
                        metadatas=metadatas_to_upsert,
                        documents=documents_to_upsert,
                    )
