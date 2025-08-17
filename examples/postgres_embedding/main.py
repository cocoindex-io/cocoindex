from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector
from typing import Any
import cocoindex
import os
import sys

os.environ['RUST_BACKTRACE'] = '1'
os.environ['COCOINDEX_LOG'] = 'debug'

@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[float]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


def get_key_columns_from_env() -> list[str]:
    """
    Get key columns from environment variables.
    Ensures only one of KEY_COLUMN_FOR_SINGLE_KEY or KEY_COLUMNS_FOR_MULTIPLE_KEYS is set.
    
    Returns:
        List of key column names
        
    Raises:
        SystemExit: If configuration is invalid
    """
    single_key = os.environ.get("KEY_COLUMN_FOR_SINGLE_KEY")
    multiple_keys = os.environ.get("KEY_COLUMNS_FOR_MULTIPLE_KEYS")
    
    # Check that exactly one is set
    if single_key and multiple_keys:
        print("❌ Error: Both KEY_COLUMN_FOR_SINGLE_KEY and KEY_COLUMNS_FOR_MULTIPLE_KEYS are set")
        print("   Please set only one of them:")
        print("   - KEY_COLUMN_FOR_SINGLE_KEY=id (for single primary key)")
        print("   - KEY_COLUMNS_FOR_MULTIPLE_KEYS=product_category,product_name (for composite primary key)")
        sys.exit(1)
    
    if not single_key and not multiple_keys:
        print("❌ Error: Neither KEY_COLUMN_FOR_SINGLE_KEY nor KEY_COLUMNS_FOR_MULTIPLE_KEYS is set")
        print("   Please set one of them:")
        print("   - KEY_COLUMN_FOR_SINGLE_KEY=id (for single primary key)")
        print("   - KEY_COLUMNS_FOR_MULTIPLE_KEYS=product_category,product_name (for composite primary key)")
        sys.exit(1)
    
    if single_key:
        # Single primary key
        return [single_key.strip()]
    else:
        # Multiple primary keys (composite key)
        return [col.strip() for col in multiple_keys.split(",")]

def is_single_key() -> bool:
    """
    Check if using single key or composite key configuration.
    
    Returns:
        bool: True if using single key, False if using composite key
    """
    return bool(os.environ.get("KEY_COLUMN_FOR_SINGLE_KEY"))


@cocoindex.flow_def(name="PostgresEmbedding")
def postgres_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that reads data from a PostgreSQL table, generates embeddings,
    and stores them in another PostgreSQL table with pgvector.
    """
    # Required environment variables
    table_name = os.environ["TABLE_NAME"]
    indexing_column = os.environ["INDEXING_COLUMN"]
    
    # Get key columns from environment
    key_columns = get_key_columns_from_env()
    
    # Optional environment variables
    ordinal_column = os.environ.get("ORDINAL_COLUMN")
    
    # Only include the data column - primary keys are automatically read by the PostgreSQL source
    included_columns = [indexing_column]

    # Get source database URL for the Postgres source
    source_db_url = os.environ.get("SOURCE_DATABASE_URL")
    if not source_db_url:
        print("❌ Error: SOURCE_DATABASE_URL environment variable is required")
        print("   This should point to the database containing the source table")
        sys.exit(1)

    # Create auth entry for the source database
    source_db_conn = cocoindex.add_auth_entry(
        "source_db_conn",
        cocoindex.DatabaseConnectionSpec(
            url=source_db_url
        )
    )

    # Read from source PostgreSQL table with only the specified columns
    postgres_source_kwargs = {
        "table_name": table_name,
        "database": source_db_conn,
        "included_columns": included_columns,
    }
    if ordinal_column:
        postgres_source_kwargs["ordinal_column"] = ordinal_column
    
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.PostgresDb(**postgres_source_kwargs)
    )

    document_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as row:
        # Use the indexing column for embedding generation
        row["text_embedding"] = text_to_embedding(row[indexing_column])
        # Collect the data - include key columns and content
        collect_data = {
            "content": row[indexing_column],
            "text_embedding": row["text_embedding"],
        }
        
        # Add each key column as a separate field
        for key_col in key_columns:
            if is_single_key():
                collect_data[key_col] = row[key_col]
            else:
                collect_data[key_col] = row["_key"][key_col]
        
        document_embeddings.collect(**collect_data)

    document_embeddings.export(
        "document_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=key_columns,
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="text_embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    # Get the table name, for the export target in the postgres_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        postgres_embedding_flow, "document_embeddings"
    )
    
    # Get key columns configuration
    key_columns = get_key_columns_from_env()
    # Build SELECT clause with all key columns
    key_columns_select = ", ".join(key_columns)
    
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT content, text_embedding <=> %s::vector AS distance, {key_columns_select}
                FROM {table_name} ORDER BY distance LIMIT %s
            """,
                (query_vector, top_k),
            )
            results = []
            for row in cur.fetchall():
                result = {"content": row[0], "score": 1.0 - row[1], "key": "__".join(str(x) for x in row[2:])}
                results.append(result)
            return results


def _main() -> None:
    # Initialize the database connection pool for CocoIndex database (where embeddings are stored)
    cocoindex_db_url = os.getenv("COCOINDEX_DATABASE_URL")
    if not cocoindex_db_url:
        print("❌ Error: COCOINDEX_DATABASE_URL environment variable is required")
        print("   This should point to the database where embeddings will be stored")
        sys.exit(1)

    pool = ConnectionPool(cocoindex_db_url)

    postgres_embedding_flow.setup()
    with cocoindex.FlowLiveUpdater(postgres_embedding_flow) as updater:
        # Run queries in a loop to demonstrate the query capabilities.
        while True:
            query = input("Enter search query (or Enter to quit): ")
            if query == "":
                break
            # Run the query function with the database connection pool and the query.
            results = search(pool, query)
            print("\nSearch results:")
            for result in results:
                print(f"[{result['score']:.3f}] {result['content']} key: {result['key']}")
                print("---")
            print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
