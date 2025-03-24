"""All builtin storages."""
from . import op

class Postgres(op.StorageSpec):
    """Storage powered by Postgres and pgvector."""

    database_url: str | None = None
    table_name: str | None = None

class Qdrant(op.StorageSpec):
    """Storage powered by Qdrant - https://qdrant.tech/."""

    collection_name: str
