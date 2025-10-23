"""All builtin targets."""

from dataclasses import dataclass
from typing import Sequence, Literal

from .. import op
from .. import index
from ..auth_registry import AuthEntryReference
from ..setting import DatabaseConnectionSpec


@dataclass
class PostgresColumnOptions:
    """Options for a Postgres column."""

    # Specify the specific type of the column in Postgres. Can use it to override the default type derived from CocoIndex schema.
    type: Literal["vector", "halfvec"] | None = None


class Postgres(op.TargetSpec):
    """Target powered by Postgres and pgvector."""

    database: AuthEntryReference[DatabaseConnectionSpec] | None = None
    table_name: str | None = None
    schema: str | None = None
    column_options: dict[str, PostgresColumnOptions] | None = None


class PostgresSqlCommand(op.TargetAttachmentSpec):
    """Attachment to execute specified SQL statements for Postgres targets."""

    name: str
    setup_sql: str
    teardown_sql: str | None = None


@dataclass
class QdrantConnection:
    """Connection spec for Qdrant."""

    grpc_url: str
    api_key: str | None = None


@dataclass
class Qdrant(op.TargetSpec):
    """Target powered by Qdrant - https://qdrant.tech/."""

    collection_name: str
    connection: AuthEntryReference[QdrantConnection] | None = None


@dataclass
class TargetFieldMapping:
    """Mapping for a graph element (node or relationship) field."""

    source: str
    # Field name for the node in the Knowledge Graph.
    # If unspecified, it's the same as `field_name`.
    target: str | None = None


@dataclass
class NodeFromFields:
    """Spec for a referenced graph node, usually as part of a relationship."""

    label: str
    fields: list[TargetFieldMapping]


@dataclass
class ReferencedNode:
    """Target spec for a graph node."""

    label: str
    primary_key_fields: Sequence[str]
    vector_indexes: Sequence[index.VectorIndexDef] = ()


@dataclass
class Nodes:
    """Spec to map a row to a graph node."""

    kind = "Node"

    label: str


@dataclass
class Relationships:
    """Spec to map a row to a graph relationship."""

    kind = "Relationship"

    rel_type: str
    source: NodeFromFields
    target: NodeFromFields


# For backwards compatibility only
NodeMapping = Nodes
RelationshipMapping = Relationships
NodeReferenceMapping = NodeFromFields


@dataclass
class Neo4jConnection:
    """Connection spec for Neo4j."""

    uri: str
    user: str
    password: str
    db: str | None = None


class Neo4j(op.TargetSpec):
    """Graph storage powered by Neo4j."""

    connection: AuthEntryReference[Neo4jConnection]
    mapping: Nodes | Relationships


class Neo4jDeclaration(op.DeclarationSpec):
    """Declarations for Neo4j."""

    kind = "Neo4j"
    connection: AuthEntryReference[Neo4jConnection]
    nodes_label: str
    primary_key_fields: Sequence[str]
    vector_indexes: Sequence[index.VectorIndexDef] = ()


@dataclass
class KuzuConnection:
    """Connection spec for Kuzu."""

    api_server_url: str


class Kuzu(op.TargetSpec):
    """Graph storage powered by Kuzu."""

    connection: AuthEntryReference[KuzuConnection]
    mapping: Nodes | Relationships


class KuzuDeclaration(op.DeclarationSpec):
    """Declarations for Kuzu."""

    kind = "Kuzu"
    connection: AuthEntryReference[KuzuConnection]
    nodes_label: str
    primary_key_fields: Sequence[str]
