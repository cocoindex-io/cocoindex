"""All builtin storages."""
from dataclasses import dataclass

from . import op
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
class Neo4jNodeSpec:
    """Spec for a Neo4j node type."""
    field_name: str
    label: str

class Neo4jRelationship(op.StorageSpec):
    """Graph storage powered by Neo4j."""

    connection: AuthEntryReference
    relationship: str
    source_node: Neo4jNodeSpec
    target_node: Neo4jNodeSpec
