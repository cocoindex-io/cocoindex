"""All builtin storages."""
from dataclasses import dataclass

from . import op
from . import index
from .auth_registry import AuthEntryReference
class Postgres(op.StorageSpec):
    """Storage powered by Postgres and pgvector."""

    database_url: str | None = None
    table_name: str | None = None

@dataclass
class Neo4jConnectionSpec:
    """Connection spec for Neo4j."""
    uri: str
    user: str
    password: str
    db: str | None = None

@dataclass
class Neo4jFieldMapping:
    """Mapping for a Neo4j field."""
    field_name: str
    # Field name for the node in the Knowledge Graph.
    # If unspecified, it's the same as `field_name`.
    node_field_name: str | None = None

@dataclass
class Neo4jRelationshipEndSpec:
    """Spec for a Neo4j node type."""
    label: str
    fields: list[Neo4jFieldMapping]

@dataclass
class Neo4jRelationshipNodeSpec:
    """Spec for a Neo4j node type."""
    primary_key_fields: list[str]
    vector_indexes: list[index.VectorIndexDef] | None = None
class Neo4jRelationship(op.StorageSpec):
    """Graph storage powered by Neo4j."""

    connection: AuthEntryReference
    rel_type: str
    source: Neo4jRelationshipEndSpec
    target: Neo4jRelationshipEndSpec
    nodes: dict[str, Neo4jRelationshipNodeSpec]
