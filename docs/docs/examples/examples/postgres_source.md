---
title:  Transform Data From Structured Source in PostgreSQL
description: Transform data from PostgreSQL table as source, transform with both AI models and non-AI data mappings, and write them into PostgreSQL/PgVector for semantic + structured search.
sidebar_class_name: hidden
slug: /examples/postgres_source
canonicalUrl: '/examples/postgres_source'
sidebar_custom_props:
  image: /img/examples/postgres_source/cover.png
  tags: [data-mapping, vector-index, postgres]
image: /img/examples/postgres_source/cover.png
tags: [data-mapping, vector-index, postgres]
---
import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/postgres_source" margin="0 0 24px 0" /
>
![PostgreSQL Product Indexing Flow](/img/examples/postgres_source/cover.png)

[CocoIndex](https://github.com/cocoindex-io/cocoindex) is one framework for building **incremental** data flows across **structured and unstructured** sources. This tutorial shows how to take data from PostgreSQL table as source, transform with both AI and non-AI data mappings, and write them into a new PostgreSQL table with PgVector for semantic + structured search.

## PostgreSQL Product Indexing Flow
![PostgreSQL Product Indexing Flow](/img/examples/postgres_source/flow.png)

- Reading data from a PostgreSQL table `source_products`.
- Computing additional fields (`total_value`, `full_description`).
- Generating embeddings for semantic search.
- Storing the results in another PostgreSQL table with a vector index using pgvector


### Connect to source

`flow_builder.add_source` reads rows from `source_products`.

```python
@cocoindex.flow_def(name="PostgresProductIndexing")
def postgres_product_indexing_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope) -> None:

    data_scope["products"] = flow_builder.add_source(
        cocoindex.sources.Postgres(
            table_name="source_products",
            # Optional. Use the default CocoIndex database if not specified.
            database=cocoindex.add_transient_auth_entry(
                cocoindex.DatabaseConnectionSpec(
                    url=os.environ["SOURCE_DATABASE_URL"],
                )
            ),
            # Optional.
            ordinal_column="modified_time",
            notification=cocoindex.sources.PostgresNotification(),
        ),
    )
```
This step adds source data from PostgreSQL table `source_products` to the flow as a `KTable`.

![Add PostgreSQL Source](/img/examples/postgres_source/source.png)

CocoIndex incrementally sync data from Postgres. When new or updated rows are found, only those rows run through the pipeline, so downstream indexes and search results reflect the latest data while unchanged rows are untouched. The following two arguments (both are optional) make this more efficient:

- `notification` enables change capture based on Postgres LISTEN/NOTIFY. Each change triggers an incremental processing on the specific row immediately.
- Regardless if `notification` is provided or not, CocoIndex still needs to scan the full table to detect changes in some scenarios (e.g. between two `update` invocation), and the `ordinal_column` provides a field that CocoIndex can use to quickly detect which row has changed without reading value columns.

Check [Postgres source](https://cocoindex.io/docs/ops/sources#postgres) for more details.

If you use the Postgres database hosted by Supabase, please click Connect on your project dashboard and find the URL there. Check [DatabaseConnectionSpec](https://cocoindex.io/docs/core/settings#databaseconnectionspec)
for more details.

## Simple Data Mapping / Transformation

Create a simple transformation to calculate the total price.

```python
@cocoindex.op.function()
def calculate_total_value(price: float, amount: int) -> float:
    """Compute total value for each product."""
    return price * amount
```

Plug into the flow:

```python
with data_scope["products"].row() as product:
     # Compute total value
    product["total_value"] = flow_builder.transform(
        calculate_total_value,
        product["price"],
        product["amount"],
    )
```

![Calculate Total Value](/img/examples/postgres_source/price.png)

### Data Transformation & AI Transformation

Create a custom function creates a `full_description` field by combining the product’s category, name, and description.

```python
@cocoindex.op.function()
def make_full_description(category: str, name: str, description: str) -> str:
    """Create a detailed product description for embedding."
    return f"Category: {category}\nName: {name}\n\n{description}"

```

Embeddings often perform better with more context. By combining fields into a single text string, we ensure that the semantic meaning of the product is captured fully.

Now plug into the flow:

```python
with data_scope["products"].row() as product:
    #.. other transformations

    # Compute full description
    product["full_description"] = flow_builder.transform(
        make_full_description,
        product["product_category"],
        product["product_name"],
        product["description"],
    )

    # Generate embeddings
    product["embedding"] = product["full_description"].transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )

    # Collect data
    indexed_product.collect(
        product_category=product["product_category"],
        product_name=product["product_name"],
        description=product["description"],
        price=product["price"],
        amount=product["amount"],
        total_value=product["total_value"],
        embedding=product["embedding"],
    )
```

This takes each product row, and does the following:

1. builds a rich description.

    ![Make Full Description](/img/examples/postgres_source/description.png)

2. turns it into an embedding

    ![Embed Full Description](/img/examples/postgres_source/embed.png)

3. collects the embedding along with structured fields (category, name, price, etc.).

    ![Collect Embedding](/img/examples/postgres_source/collector.png)


## Export

```python
indexed_product.export(
    "output",
    cocoindex.targets.Postgres(),
    primary_key_fields=["product_category", "product_name"],
    vector_indexes=[
        cocoindex.VectorIndexDef(
            field_name="embedding",
            metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
        )
    ],
)
```

All transformed rows are collected and exported to a new PostgreSQL table with a vector index, ready for semantic search.


## Field lineage
When the transform flow starts to getting complex, it's hard to understand how each field is derived.
CocoIndex provides a way to visualize the lineage of each field, to make it easier to trace and troubleshoot field origins and downstream dependencies.

For example, the following image shows the lineage of the `embedding` field, you can click from the final output backward all the way to the source fields, step by step.

![Field Lineage](/img/examples/postgres_source/lineage.png)


## Running the Pipeline

1. Set up dependencies:

    ```bash
    pip install -e .
    ```

2. Create the source table with sample data:

    ```bash
    psql "postgres://cocoindex:cocoindex@localhost/cocoindex" -f ./prepare_source_data.sql
    ```

3. Setup tables and update the index:

    ```bash
    cocoindex update --setup main.py
    ```

4. Run CocoInsight:

    ```bash
    cocoindex server -ci main
    ```
    You can walk through the project step by step in CocoInsight to see exactly how each field is constructed and what happens behind the scenes. It connects to your local CocoIndex server, with zero pipeline data retention.


## Continuous Updating

For continuous updating when the source changes, add `-L`:

```bash
cocoindex server -ci -L main
```
Check [live updates](https://cocoindex.io/docs/tutorials/live_updates) for more details.

## Search and Query the Index

### Query

Runs a semantic similarity search over the indexed products table, returning the top matches for a given query.

```python
def search(pool: ConnectionPool, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    # Get the table name, for the export target in the text_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        postgres_product_indexing_flow, "output"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        register_vector(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                    product_category,
                    product_name,
                    description,
                    amount,
                    total_value,
                    (embedding <=> %s) AS distance
                FROM {table_name}
                ORDER BY distance ASC
                LIMIT %s
            """,
                (query_vector, top_k),
            )
            return cur.fetchall()
```
This function

- Converts the query text into an embedding (`query_vector`).
- Compares it with each product’s stored embedding (`embedding`) using vector distance.
- Returns the closest matches, including both metadata and the similarity score (`distance`).

### Create an command-line interactive loop

```python
def _main() -> None:
    # Initialize the database connection pool.
    pool = ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        results = search(pool, query)
        print("\nSearch results:")
        for result in results:
            score = 1.0 - result["distance"]
            print(
                f"[{score:.3f}] {result['product_category']} | {result['product_name']} | {result['amount']} | {result['total_value']}"
            )
            print(f"    {result['description']}")
            print("---")
        print()

if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
```

### Run as a Service

This [example](https://cocoindex.io/docs/examples/image_search#fast-api-application) runs as a service using Fast API.


## Why One Framework for Structured + Unstructured?

- Unified workflow: All data— files, APIs, or databases—is processed through a single, consistent system, and AI operations are handled alongside standard data transformations.

- True incremental processing with live updates: Out-of-box incremental support from the framework, process only what’s changed, avoiding redundant computation and ensuring faster updates to downstream indexes.

- Reliable consistency: Embeddings and derived data always reflect the accurate, transformed state of each row, ensuring results are dependable and current in a single flow.

- Streamlined operations: A single deployment manages everything, providing clear data lineage and reducing the complexity of the data stack.
