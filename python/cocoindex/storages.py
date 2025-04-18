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
class GraphFieldMapping:
    """Mapping for a Neo4j field."""
    field_name: str
    # Field name for the node in the Knowledge Graph.
    # If unspecified, it's the same as `field_name`.
    node_field_name: str | None = None

@dataclass
class GraphRelationshipEnd:
    """Spec for a Neo4j node type."""
    label: str
    fields: list[GraphFieldMapping]

@dataclass
class GraphRelationshipNode:
    """Spec for a Neo4j node type."""
    primary_key_fields: Sequence[str]
    vector_indexes: Sequence[index.VectorIndexDef] = ()

@dataclass
class GraphNode:
    """Spec for a Neo4j node type."""
    kind = "Node"

    label: str

@dataclass
class GraphRelationship:
    """Spec for a Neo4j relationship."""
    kind = "Relationship"

    rel_type: str
    source: GraphRelationshipEnd
    target: GraphRelationshipEnd
    nodes: dict[str, GraphRelationshipNode] | None = None

class Neo4j(op.StorageSpec):
    """Graph storage powered by Neo4j."""

    connection: AuthEntryReference
    mapping: GraphNode | GraphRelationship
