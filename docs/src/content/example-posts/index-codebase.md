---
title: Index Your Codebase for AI Agents
description: 'Index a codebase for RAG and AI coding agents with CocoIndex V1 and Tree-sitter — language-aware chunking, embeddings, semantic search, and a live vector index.'
slug: index-codebase
image: https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/cover.png
tags: [data-indexing, semantic-search]
---

![Index Your Codebase for AI Agents with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/cover.png)

In this tutorial, we'll build a live semantic index over a codebase with [CocoIndex](https://github.com/cocoindex-io/cocoindex). Point it at a repo, and you get a vector index you can search in natural language ("where do we embed chunks?") that updates itself as you edit — the kind of fresh, low-latency context an agent needs.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — incremental processing, change tracking, managed targets — runs in a Rust engine underneath, so only what changed gets re-embedded and re-upserted.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/code_embedding)

## Use cases

![CocoIndex codebase indexing use cases](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/usecase-agents.png)

- **Code context for agents** — semantic context for Claude, Codex, OpenCode, Factory instead of file-by-file reading.
- **Code search** — natural-language and semantic search over your repo.
- **Review & refactor agents** — context for code review, security analysis, and large-scale refactoring.

## Why CocoIndex for codebase indexing

A codebase is hard to keep indexed well, and it exercises most of what CocoIndex was built for:

- **Syntax-aware chunking** is built in. Tree-sitter integration means chunks follow real code structure (functions, classes, blocks) instead of arbitrary line windows, for every major language.
- **Incremental updates** suit code that changes constantly. CocoIndex re-embeds only the chunks that changed and re-upserts only the rows that moved — no full re-index on a one-line edit.
- **Live updates** keep the index current. With `live=True` on the filesystem source and `cocoindex update -L`, the index keeps watching and applies changes with low latency.
- **Plain Python** keeps it customizable. Pick your embedding model, chunking strategy, and vector database.
- **Consistent indexing and query.** The same embedder is shared between the indexing path and the query path, so what you index is what you search against.

![Why CocoIndex for codebase indexing: syntax-aware chunking, incremental updates, live updates, a Rust core, plain Python, and a consistent embedder across indexing and query.](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/why-cocoindex.png)

## Flow overview

![CocoIndex flow for code embedding](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/flow-v1.png)

From a high level, these are the steps:

1. Read code files from a local directory.
2. Split each file into syntax-aware chunks with Tree-sitter, then embed every chunk.
3. Store the chunks and their embeddings in Postgres (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. CocoIndex supports [many targets](https://cocoindex.io/docs/connectors/postgres/), so you can pick another store.

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- Install CocoIndex and the dependencies this example uses:

  ```sh
  pip install -U "cocoindex[postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

## Define the data and shared resources

[Apps](https://cocoindex.io/docs/programming_guide/app/) are the top-level runnable unit in CocoIndex. Before the App, we set up two things the rest of the code builds on. `CodeEmbedding` defines one row of the output table — each chunk of code becomes one row, with its text, location, and embedding vector. `coco_lifespan` provides the [shared resources](https://cocoindex.io/docs/programming_guide/context/) every step needs — the Postgres connection pool and the embedding model — once at startup.

```python
import os
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator

DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex")
TABLE_NAME = "code_embeddings"
PG_SCHEMA_NAME = "coco_examples"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

PG_DB = coco.ContextKey[asyncpg.Pool]("code_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class CodeEmbedding:
    id: int
    filename: str
    code: str
    embedding: Annotated[NDArray, EMBEDDER]
    start_line: int
    end_line: int


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically.

## Process a file

![One processing component per file: each file is chunked with Tree-sitter and embedded, producing CodeEmbedding rows.](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/stage-file-process.png)

`process_file` runs once per file. It reads the file, detects the language so Tree-sitter can parse it, [splits the code](https://cocoindex.io/docs/ops/text/) along the syntax tree, and maps each chunk to `process_chunk`.

```python
@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    text = await file.read_text()
    language = detect_code_language(filename=str(file.file_path.path.name))
    chunks = _splitter.split(
        text,
        chunk_size=1000,
        min_chunk_size=300,
        chunk_overlap=300,
        language=language,
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

CocoIndex uses Tree-sitter to chunk code along its actual syntax structure rather than arbitrary line breaks. Because each chunk is a coherent syntactic unit, retrieval returns whole functions or blocks instead of fragments cut mid-statement. All major languages are supported; unknown types fall back to plain text.

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a file's content and this function's code are both unchanged, the whole file is skipped on the next run. `coco.map` fans out to one `process_chunk` call per chunk.

Here is what chunking produces: each file is split into syntactic chunks, each with its own location and text.

![Each file split into chunks, with the location and text of every chunk](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/flow-chunk.png)

## Process a chunk

`process_chunk` embeds the chunk with the shared embedder and declares the target row.

```python
@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
    table.declare_row(
        row=CodeEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            code=chunk.text,
            embedding=embedding,
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )
```

We use [`SentenceTransformerEmbedder`](https://cocoindex.io/docs/ops/sentence_transformers/) with `all-MiniLM-L6-v2`; there are 12k+ sentence-transformer models on [Hugging Face](https://huggingface.co/models?other=sentence-transformers), so swap in whichever you prefer. `chunk.start.line` and `chunk.end.line` carry through, so search results point straight at the lines that matched.

## Define the main function

![mount_each fans out one processing component per file, from the codebase source to the Postgres target.](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/stage-main-function.png)

`app_main` wires the source to the target. It mounts the Postgres table (with a [vector index](https://cocoindex.io/docs/common_resources/vector_schema/)), walks the codebase, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            CodeEmbedding, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["**/*.py", "**/*.rs", "**/*.toml", "**/*.md", "**/*.mdx"],
            excluded_patterns=["**/.*", "**/target", "**/node_modules"],
        ),
        live=True,  # watch for changes; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

`mount_table_target` creates and manages the Postgres table for you: schema, the pgvector index, idempotent upserts, and orphan cleanup when a file disappears. `live=True` makes the [filesystem source](https://cocoindex.io/docs/connectors/localfs/) [watch for changes](https://cocoindex.io/docs/programming_guide/live_mode/), and `mount_each` runs one component per file so the engine can track and update them independently.

## Create the App

![A CocoIndex App binds source, transform, and target state into one runnable unit.](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/stage-create-app.png)

Bind `app_main` into a `coco.App` and point it at the codebase root.

```python
app = coco.App(
    coco.AppConfig(name="CodeEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # index from repo root
)
```

That is the entire indexing path.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to set up and update the index. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching for file changes
cocoindex update -L main
```

## Query the index

Match user text against the index with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python
async def query_once(pool, embedder, query: str, *, top_k: int = 5) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT filename, code, embedding <=> $1 AS distance, start_line, end_line
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} (L{r['start_line']}-L{r['end_line']})")
        print(f"    {r['code']}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print the filename, the matched line range, and the code snippet.

```bash
python main.py "embedding"
```

The search results print in the terminal:

![Search results in the terminal](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/search-results.png)

## Incremental updates

CocoIndex keeps the index in sync with the codebase and does the **minimum work** to get there. You never compute a diff or write update logic: you change something, and CocoIndex works out exactly what to embed, upsert, and delete. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged, and an embedding is reused when its chunk text is unchanged. `mount_table_target` decides what to *write* — each row's `id` is derived from its chunk's content, so it upserts only the rows that actually changed and deletes rows whose source is gone.

The same machinery covers two kinds of change: changes to your **data** (the code being indexed) and changes to your **logic** (the pipeline itself).

**Data changes.**

- **A file is added** — only that file is chunked and embedded, and its rows are inserted. The rest of the repo is untouched.
- **A file is deleted** — its rows are removed from the target.
- **A file is edited** — the file is re-chunked, and the new chunks usually overlap the old ones. Chunks whose text is unchanged keep their `id` and embedding, so they are left as-is; genuinely new chunks are embedded and inserted; chunks that no longer exist are deleted. Edit one function and only that function's chunks are re-embedded, even though the whole file was re-read.

![A file edited and re-chunked: unchanged chunks are reused with no re-embedding, a removed chunk's row is deleted, and a new chunk is embedded and inserted.](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/incremental-diff.png)

**Logic changes.** A pipeline change is reconciled the same way — CocoIndex compares the new output against what is already in Postgres and applies only the difference.

- **Change the file patterns** (`included_patterns` / `excluded_patterns`) — files that now match are added automatically; files that no longer match have their rows deleted automatically.
- **Tune the chunking** (chunk size, overlap) — files are re-chunked, producing the same partial-overlap diff shown above: unchanged chunks are no-ops, new chunks are embedded and inserted, dropped chunks are deleted.
- **Swap the embedding model** — the vectors genuinely change, so all embeddings are recomputed, but row identity is stable: it is an in-place update of the `embedding` column, with no rows added or removed.

A catch-up run (`cocoindex update main`) does this once and exits; live mode (`cocoindex update -L main`) keeps watching and applies each change with low latency, so the index stays current as you code.

## Run it
The full, runnable example is in the CocoIndex repo: [examples/code_embedding](https://github.com/cocoindex-io/cocoindex/tree/main/examples/code_embedding).

## CocoIndex Code
If you'd rather not wire the pipeline yourself, [CocoIndex Code](https://github.com/cocoindex-io/cocoindex-code) is an end-to-end implementation of exactly this indexing, packaged as a CLI and an MCP server. It does the same thing this example does (AST-aware chunking, incremental re-index on file changes, local embeddings by default), hardened for production.

![CocoIndex Code: semantic code search for coding agents, as a CLI and MCP server](https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/cocoindex-code.png)

You can plug it straight into your coding agent or code-review agent:

- **Claude Code skill:** `npx skills add cocoindex-io/cocoindex-code`, then invoke `/ccc`.
- **MCP server:** `claude mcp add cocoindex-code -- ccc mcp` (Codex, OpenCode, Cursor, and any MCP client work the same way).
- **CLI:** `ccc index` to build the index, `ccc search "where we embed chunks"` to query it.
