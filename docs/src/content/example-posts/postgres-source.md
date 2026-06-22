---
title: Postgres as a *Source*
description: 'Use an existing Postgres table as a CocoIndex source — read product rows, derive fields, embed each row, and write the vectors back to Postgres with pgvector.'
slug: postgres-source
image: https://cocoindex.io/blobs/docs-v1/img/examples/postgres-source/cover.png
tags: [postgres-source, vector-index]
---

![Postgres as a source with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/postgres-source/cover.png)

Most data already lives in a database. This example takes an existing Postgres table of products, reads it row by row, derives a couple of fields, [embeds](https://cocoindex.io/docs/ops/sentence_transformers/) each row, and writes the result — including the vector — back into Postgres with [pgvector](https://github.com/pgvector/pgvector). Point it at any table and you have a semantic index over your structured data, kept in sync as the table changes.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed targets — runs in a Rust engine underneath, so only the rows that changed get re-embedded and re-upserted.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/postgres_source)

## Flow overview

![CocoIndex Postgres source flow: read product rows from a Postgres table, derive fields and embed each row, and store the vectors in Postgres with pgvector](https://cocoindex.io/blobs/docs-v1/img/examples/postgres-source/flow-v1.png)

From a high level, these are the steps:

1. Read product rows from an existing Postgres table with [`PgTableSource`](https://cocoindex.io/docs/connectors/postgres/).
2. For each row, derive a description and a `total_value`, then [embed](https://cocoindex.io/docs/ops/sentence_transformers/) the description.
3. Store the enriched rows and their embeddings in Postgres (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

> **New to embeddings?** An [*embedding*](https://cocoindex.io/docs/ops/sentence_transformers/) is a list of numbers (a vector) that captures the *meaning* of a piece of text, so rows with similar meaning land close together in vector space. A [*vector index*](https://cocoindex.io/docs/common_resources/vector_schema/) stores those vectors and finds the nearest ones to your query fast. That's what lets search match by meaning instead of exact words.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. The same instance can hold both the source table and the target — or set `SOURCE_DATABASE_URL` to read from a separate database.

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  export SOURCE_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- Install CocoIndex and the dependencies this example uses:

  ```sh
  pip install -U "cocoindex[postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

- A source table to read from. Create `source_products` with the sample rows from the repo:

  ```sh
  psql "$SOURCE_DATABASE_URL" -f ./prepare_source_data.sql
  ```

## Define the data and shared resources

[Apps](https://cocoindex.io/docs/programming_guide/app/) are the top-level runnable unit in CocoIndex. Before the App, we set up the data shapes and the [shared resources](https://cocoindex.io/docs/programming_guide/context/) the rest of the code builds on. `SourceProduct` describes one row read from the source table; `OutputProduct` describes one row written to the target, with the two derived fields and the embedding vector. `coco_lifespan` provides everything every step needs — a Postgres pool for the target, a pool for the source, and the embedding model — once at startup.

```python title="main.py"
DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex")
SOURCE_DATABASE_URL = os.getenv("SOURCE_DATABASE_URL", DATABASE_URL)
TABLE_NAME = "output"
PG_SCHEMA_NAME = "coco_examples_v1"

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("postgres_source_db")
SOURCE_POOL = coco.ContextKey[asyncpg.Pool]("source_pool")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@dataclass
class SourceProduct:
    product_category: str
    product_name: str
    description: str
    price: float
    amount: int


@dataclass
class OutputProduct:
    product_category: str
    product_name: str
    description: str
    price: float
    amount: int
    total_value: float
    embedding: Annotated[NDArray, EMBEDDER]


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with (
        asyncpg.create_pool(DATABASE_URL) as target_pool,
        asyncpg.create_pool(SOURCE_DATABASE_URL) as source_pool,
    ):
        builder.provide(PG_DB, target_pool)
        builder.provide(SOURCE_POOL, source_pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, CocoIndex notices (`detect_change=True`) and re-embeds.

## Process a row

![One processing component per row: each source row is derived and embedded, producing an OutputProduct row written to Postgres](https://cocoindex.io/blobs/docs-v1/img/examples/postgres-source/stage-file-process.png)

`process_product` runs once per source row. It builds a `full_description` from the category, name, and body, computes `total_value`, embeds the description, and declares the target row.

```python title="main.py"
@coco.fn(memo=True)
async def process_product(
    product: SourceProduct,
    table: postgres.TableTarget[OutputProduct],
) -> None:
    full_description = f"Category: {product.product_category}\nName: {product.product_name}\n\n{product.description}"
    total_value = product.price * product.amount
    embedding = await coco.use_context(EMBEDDER).embed(full_description)
    table.declare_row(
        row=OutputProduct(
            product_category=product.product_category,
            product_name=product.product_name,
            description=product.description,
            price=product.price,
            amount=product.amount,
            total_value=total_value,
            embedding=embedding,
        ),
    )
```

We embed the composed description rather than the raw body, so the category and name carry weight in the vector — a search for "wireless audio" matches even when the body never says it. We use [`SentenceTransformerEmbedder`](https://cocoindex.io/docs/ops/sentence_transformers/) with `all-MiniLM-L6-v2`, a small, fast model that runs locally with no API key; there are 12k+ sentence-transformer models on [Hugging Face](https://huggingface.co/models?other=sentence-transformers), so swap in whichever you prefer.

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a row's content and this function's code are both unchanged, the row is skipped on the next run. `table.declare_row` declares the row as a [target state](https://cocoindex.io/docs/programming_guide/target_state/); CocoIndex handles inserting, updating, or deleting it to match.

## Define the main function

`app_main` wires the source to the target. It mounts the Postgres target table, opens the source table, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per source row.

```python title="main.py"
@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            OutputProduct,
            primary_key=["product_category", "product_name"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    source = postgres.PgTableSource(
        coco.use_context(SOURCE_POOL),
        table_name="source_products",
        row_type=SourceProduct,
    )

    await coco.mount_each(
        process_product,
        source.fetch_rows().items(lambda p: (p.product_category, p.product_name)),
        target_table,
    )


app = coco.App(
    coco.AppConfig(name="PostgresSourceV1"),
    app_main,
)
```

[`PgTableSource`](https://cocoindex.io/docs/connectors/postgres/) reads the table — passing `row_type=SourceProduct` maps each row straight into the dataclass and selects exactly its fields. `fetch_rows().items(...)` streams rows over a cursor and tags each one with a [stable key](https://cocoindex.io/docs/programming_guide/processing_component/), here the `(product_category, product_name)` composite primary key. `mount_table_target` creates and manages the Postgres target table for you: schema, idempotent upserts, and orphan cleanup when a source row disappears. `mount_each` runs one component per row so the engine can track and update them independently.

## Setup and run

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build and update the index. The Postgres source runs as a one-shot catch-up — it scans the source table, syncs the target, and exits:

```sh
cocoindex update main
```

## Query the index

Match user text against the index with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python title="main.py"
async def query_once(pool, embedder, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                product_category, product_name, description,
                amount, total_value,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['product_category']} | {r['product_name']} | {r['amount']} | {r['total_value']}")
        print(f"    {r['description']}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print the derived fields alongside the description. Run a search straight from the command line:

```bash
python main.py "wireless headphones"
```

The most semantically similar products come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

## Incremental updates

CocoIndex keeps the target in sync with the source table and does the **minimum work** to get there. You never compute a diff or write update logic: the source row changes, and CocoIndex works out exactly what to re-embed, upsert, and delete. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a row is skipped when its content and the function's code are both unchanged. `mount_table_target` decides what to *write* — each output row's primary key is derived from the source row's `(product_category, product_name)`, so it upserts only the rows that actually changed and deletes rows whose source is gone.

- **A row is added** — only that row is derived and embedded, and it is inserted. The rest is untouched.
- **A row is edited** — it is re-derived; if the embedded description changed it is re-embedded, and the target row is updated in place.
- **A row is deleted** — its row is removed from the target automatically.

The same machinery covers **logic** changes too: tweak how `full_description` is composed or swap the embedding model, and CocoIndex compares the new output against what is already in Postgres and applies only the difference. Each `cocoindex update main` does this once and exits; re-run it after the source table changes to bring the index back in sync.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/postgres_source](https://github.com/cocoindex-io/cocoindex/tree/main/examples/postgres_source). If this is useful, a ⭐ on [GitHub](https://github.com/cocoindex-io/cocoindex) helps, and the [Discord](https://discord.com/invite/zpA9S2DR7s) is the place to ask questions and share what you build.
