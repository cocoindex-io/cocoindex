"""All builtin storages."""
from dataclasses import dataclass
from typing import Sequence

from . import op
from . import index
from .auth_registry import AuthEntryReference

class Postgres(op.StorageSpec):
    """Storage powered by Postgres and pgvector."""

    database_url: str | None = None
    table_name: str | None = None

@dataclass
class Qdrant(op.StorageSpec):
    """Storage powered by Qdrant - https://qdrant.tech/."""

    collection_name: str
    grpc_url: str = "http://localhost:6334/"
    api_key: str | None = None

@dataclass
class Neo4jConnection:
    """Connection spec for Neo4j."""
    uri: str
    user: str
    password: str
    db: str | None = None

@dataclass
class TargetFieldMapping:
    """Mapping for a graph element (node or relationship) field."""
    source: str
    # Field name for the node in the Knowledge Graph.
    # If unspecified, it's the same as `field_name`.
    target: str | None = None

@dataclass
class NodeReferenceMapping:
    """Spec for a referenced graph node, usually as part of a relationship."""
    label: str
    fields: list[TargetFieldMapping]

@dataclass
class NodeStorageSpec:
    """Storage spec for a graph node."""
    primary_key_fields: Sequence[str]
    vector_indexes: Sequence[index.VectorIndexDef] = ()

@dataclass
class NodeMapping:
    """Spec to map a row to a graph node."""
    kind = "Node"

    label: str

@dataclass
class RelationshipMapping:
    """Spec to map a row to a graph relationship."""
    kind = "Relationship"

    rel_type: str
    source: NodeReferenceMapping
    target: NodeReferenceMapping
    nodes_storage_spec: dict[str, NodeStorageSpec] | None = None

class Neo4j(op.StorageSpec):
    """Graph storage powered by Neo4j."""

    connection: AuthEntryReference
    mapping: NodeMapping | RelationshipMapping
