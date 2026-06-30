---
title: Semantic Search with *LanceDB*
description: 'The Semantic Search 101 pipeline with CocoIndex V1, pointed at LanceDB instead of Postgres — chunk Markdown, embed each chunk, and store the vectors in an embedded, file-based store with zero server to run.'
slug: text-embedding-lancedb
image: https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding-lancedb/cover.png
tags: [vector-index, lancedb]
---

![Semantic Search with LanceDB and CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding-lancedb/cover.png)

This is the [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) example with one thing changed: the vectors land in [LanceDB](https://lancedb.github.io/lancedb/) instead of Postgres. LanceDB is an embedded, file-based vector store — no server to stand up, no `POSTGRES_URL`, just a directory on disk you can copy to move. Everything else — read Markdown, chunk, embed each chunk — is identical, so this post focuses on the one part that differs: the connector.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed targets — runs in a Rust engine underneath, so only what changed gets re-embedded and re-upserted.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_lancedb)

## Flow overview

![CocoIndex text embedding flow with LanceDB: read Markdown, split into chunks, embed each chunk, and store the vectors in an embedded LanceDB table](https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding-lancedb/flow-v1.png)

From a high level, these are the steps:

1. Read Markdown files from a local directory.
2. [Split each file into overlapping chunks](https://cocoindex.io/docs/ops/text/), then [embed](https://cocoindex.io/docs/ops/sentence_transformers/) every chunk.
3. Store the chunks and their embeddings in a LanceDB table (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

The chunk-and-embed half is unchanged from the base example — `RecursiveSplitter` cuts each file into overlapping Markdown chunks, and a local [`SentenceTransformerEmbedder`](https://cocoindex.io/docs/ops/sentence_transformers/) (`all-MiniLM-L6-v2`, no API key) turns each chunk into a 384-d vector. See the [base walkthrough](https://cocoindex.io/docs/examples/text-embedding/) for the chunk/embed details. What changes here is the target.

## Connect to LanceDB

LanceDB is embedded, so the "connection" is just a path on disk — the directory is created on first run. The [shared resources](https://cocoindex.io/docs/programming_guide/context/) the rest of the code builds on are the LanceDB connection and the embedding model, both provided once at startup in [`coco.lifespan`](https://cocoindex.io/docs/programming_guide/context/). `DocEmbedding` defines one output row — each chunk becomes one row.

```python title="main.py"
import cocoindex as coco
from cocoindex.connectors import lancedb, localfs
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "doc_embeddings"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("text_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield
```

Compared to the Postgres version, the only difference is the resource: `lancedb.connect_async(LANCEDB_URI)` instead of an `asyncpg` pool, and a `LanceAsyncConnection` context key instead of `asyncpg.Pool`. `embedding: Annotated[NDArray, EMBEDDER]` still ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, CocoIndex notices (`detect_change=True`) and re-embeds.

## Mount the LanceDB table

`app_main` wires the source to the target. It mounts the LanceDB table, walks the source directory, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await lancedb.mount_table_target(
        LANCE_DB,
        table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(
            DocEmbedding, primary_key=["id"]
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # watch for changes; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

`lancedb.mount_table_target` is the LanceDB counterpart to the Postgres `mount_table_target` — same call shape, same managed-table guarantees: it creates and manages the table for you, handles idempotent upserts keyed on the primary key, and cleans up orphan rows when a file disappears. `process_file` and `process_chunk` take a `lancedb.TableTarget[DocEmbedding]` and call `table.declare_row(...)` exactly as before; only the import changed. `live=True` makes the [filesystem source](https://cocoindex.io/docs/connectors/localfs/) [watch for changes](https://cocoindex.io/docs/programming_guide/live_mode/), and `mount_each` runs one component per file.

The App binds it all together and points at the Markdown folder:

```python title="main.py"
app = coco.App(
    coco.AppConfig(name="TextEmbeddingLanceDBV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)
```

## Setup and run

No database to install — LanceDB writes to `./lancedb_data/`, created on first run. Install the example's dependencies and grab a few `.md` files (the repo ships [sample files](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_lancedb/markdown_files), or drop your own into `markdown_files/`):

```sh
pip install -U "cocoindex[sentence_transformers,lancedb]" python-dotenv
```

Then build and update the index with the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching for file changes
cocoindex update -L main
```

## Query the index

Query with LanceDB's async search API, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python title="main.py"
async def query_once(conn, embedder, query_text: str, *, top_k: int = TOP_K) -> None:
    query_vec = await embedder.embed(query_text)
    table = await conn.open_table(TABLE_NAME)
    search = await table.search(query_vec, vector_column_name="embedding")
    results = await search.limit(top_k).to_list()
    for r in results:
        score = 1.0 - r["_distance"]
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")
```

`table.search(...).limit(top_k)` returns the nearest vectors; `_distance` is LanceDB's distance, which we turn into a similarity score. Run a search straight from the command line:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

## Incremental updates

The incremental story is identical to the [base example](https://cocoindex.io/docs/examples/text-embedding/): `@coco.fn(memo=True)` decides what to *recompute* (a file is skipped when its content and the function's code are both unchanged), and `lancedb.mount_table_target` decides what to *write* — each row's [`id`](https://cocoindex.io/docs/common_resources/id_generation/) is derived from its chunk's text, so it upserts only the rows that actually changed and deletes rows whose source is gone.

- **A file is added** — only that file is chunked and embedded, and its rows are inserted.
- **A file is edited** — it is re-chunked; unchanged chunks keep their `id` and embedding, genuinely new chunks are embedded and inserted, and chunks that no longer exist are deleted.
- **A file is deleted** — its rows are removed from the LanceDB table automatically.

A catch-up run (`cocoindex update main`) does this once and exits; live mode (`cocoindex update -L main`) keeps watching and applies each change with low latency.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/text_embedding_lancedb](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_lancedb). For the chunk-and-embed walkthrough, see [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) — same flow, Postgres as the target.

If this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex) and come say hi in our [Discord community](https://discord.com/invite/zpA9S2DR7s).
