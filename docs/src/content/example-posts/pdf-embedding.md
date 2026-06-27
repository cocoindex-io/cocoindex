---
title: Semantic Search over PDFs
description: 'Build a vector index from local PDFs with CocoIndex V1 — convert to Markdown with docling on a GPU runner, chunk, embed with sentence-transformers, and store the vectors in Postgres with pgvector, then query in natural language.'
slug: pdf-embedding
image: https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/cover.png
tags: [vector-index, pdf]
---

![Semantic Search over PDFs with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/cover.png)

We'll take a folder of PDFs — papers, RFCs, manuals, contracts — and turn it into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English. The trick PDFs add over plain text: they have to be *parsed* first. We use [docling](https://github.com/docling-project/docling) to convert each PDF to clean Markdown, then chunk, embed, and store the vectors in Postgres.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed targets — runs in a Rust engine underneath, so only changed PDFs get re-parsed and re-embedded. The one genuinely expensive step (PDF parsing) runs on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/) so it doesn't block the event loop.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/pdf_embedding)

## Flow overview

![CocoIndex PDF embedding flow: walk a folder of PDFs, convert each to Markdown with docling on a GPU runner, split into chunks, embed each chunk, and store the vectors in Postgres with pgvector](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/flow-v1.png)

From a high level, these are the steps:

1. Read PDF files from a local directory (live).
2. [Convert each PDF to Markdown](https://github.com/docling-project/docling) with docling, [split it into overlapping chunks](https://cocoindex.io/docs/ops/text/), then [embed](https://cocoindex.io/docs/ops/sentence_transformers/) every chunk.
3. Store the chunks and their embeddings in Postgres (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. The repo ships a compose file:

  ```sh
  docker compose -f dev/postgres.yaml up -d
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- Install CocoIndex and the dependencies this example uses (docling pulls in the PDF parser):

  ```sh
  pip install -U "cocoindex[postgres,sentence_transformers]" asyncpg pgvector numpy docling python-dotenv
  ```

- A few PDFs to index. The example ships a `pdf_files/` folder with a couple of papers and an RFC — or drop your own in.

## Define the data and shared resources

`PdfEmbedding` defines one row of the output table — each chunk of text becomes one row, with its filename, character offsets, text, and embedding vector. `coco_lifespan` provides the [shared resources](https://cocoindex.io/docs/programming_guide/context/) every step needs — the Postgres connection pool and the embedding model — once at startup.

```python title="main.py"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("pdf_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class PdfEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(os.environ["POSTGRES_URL"]) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, CocoIndex notices (`detect_change=True`) and re-embeds.

## Convert PDFs to Markdown

This is the one step text embedding doesn't have. [docling](https://github.com/docling-project/docling) reads the PDF and exports clean Markdown — preserving headings, tables, and reading order, which is exactly what makes the downstream chunks coherent.

```python title="main.py"
@functools.cache
def pdf_converter() -> DocumentConverter:
    pipeline_options = PdfPipelineOptions(
        accelerator_options=AcceleratorOptions(device=AcceleratorDevice.CPU)
    )
    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )


@coco.fn.as_async(runner=coco.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="input.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()
```

Two things make this hold up at scale:

- **`@coco.fn.as_async(runner=coco.GPU)`** wraps a *synchronous*, CPU/GPU-heavy function so CocoIndex runs it on a dedicated [GPU runner](https://cocoindex.io/docs/programming_guide/function/) instead of blocking the async event loop. PDF parsing is the slow part of this pipeline; offloading it keeps the rest of the flow responsive.
- **`@functools.cache`** builds the docling `DocumentConverter` once and reuses it across every PDF — model load happens a single time, not per file.

## Process a file

![One processing component per PDF: convert to Markdown, chunk, embed each chunk, and declare PdfEmbedding rows into Postgres](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/stage-file-process.png)

`process_file` runs once per PDF. It converts the PDF to Markdown, [splits the text](https://cocoindex.io/docs/ops/text/) into overlapping chunks, and maps each chunk to `process_chunk`.

```python title="main.py"
@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    markdown = await pdf_to_markdown(await file.read())
    chunks = _splitter.split(
        markdown, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a PDF's content and this function's code are both unchanged, the whole file is skipped on the next run — so you never re-run docling on a PDF you've already parsed. `coco.map` fans out to one `process_chunk` call per chunk.

## Process a chunk

`process_chunk` embeds the chunk with the shared embedder and declares the target row.

```python title="main.py"
@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    table.declare_row(
        row=PdfEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        ),
    )
```

We use [`SentenceTransformerEmbedder`](https://cocoindex.io/docs/ops/sentence_transformers/) with `all-MiniLM-L6-v2` — a small, fast model that runs locally with no API key. `table.declare_row` declares the row as a target state; CocoIndex handles inserting, updating, or deleting it to match. Each row's `id` is derived from the chunk text, so a chunk that survives a re-parse keeps its row.

## Define the main function

![mount_each fans out one processing component per PDF, from the filesystem source to the Postgres target](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/stage-main-function.png)

`app_main` wires the source to the target. It mounts the Postgres table, walks the source directory for PDFs, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            PdfEmbedding, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,   # "coco_examples"
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,  # watch for changes; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_file, files.items(), target_table)


app = coco.App(
    coco.AppConfig(name="PdfEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
)
```

`mount_table_target` creates and manages the Postgres table for you — schema, idempotent upserts, and orphan cleanup when a PDF disappears. `live=True` makes the [filesystem source](https://cocoindex.io/docs/connectors/localfs/) [watch for changes](https://cocoindex.io/docs/programming_guide/live_mode/), and `mount_each` runs one component per file so the engine can track and update them independently.

> **No vector index here.** To keep the example minimal, this flow doesn't declare a vector index, so queries do a sequential scan — fine for a few PDFs. For a larger corpus, add one line — `target_table.declare_vector_index(column="embedding")` — exactly as the [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) example does, and pgvector serves approximate-nearest-neighbor queries instead.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build and update the index. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching for file changes
cocoindex update -L main
```

## Query the index

Match user text against the index with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python title="main.py"
async def query_once(pool, embedder, query: str, *, top_k: int = 5) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT filename, text, embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print the filename and the matching chunk. Run a search straight from the command line:

```bash
python main.py "what is attention?"
```

With the sample papers indexed, the most semantically similar passages come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

## Incremental updates

CocoIndex keeps the index in sync with your PDFs and does the **minimum work** to get there. You never compute a diff or write update logic. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a PDF is skipped when its bytes and the function's code are both unchanged, so docling never re-parses an unchanged file. `mount_table_target` decides what to *write* — each row's [`id`](https://cocoindex.io/docs/common_resources/id_generation/) is derived from its chunk's text, so it upserts only the rows that actually changed and deletes rows whose source is gone.

- **A PDF is added** — only that file is parsed, chunked, and embedded; its rows are inserted. The rest is untouched.
- **A PDF is replaced** — it is re-parsed and re-chunked; chunks whose text is unchanged keep their `id` and embedding, genuinely new chunks are embedded and inserted, and chunks that no longer exist are deleted.
- **A PDF is deleted** — its rows are removed from the target automatically.

The same machinery covers **logic** changes too: tune the chunk size or swap the embedding model, and CocoIndex compares the new output against what's already in Postgres and applies only the difference. A catch-up run (`cocoindex update main`) does this once and exits; live mode (`cocoindex update -L main`) keeps watching and applies each change with low latency.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/pdf_embedding](https://github.com/cocoindex-io/cocoindex/tree/main/examples/pdf_embedding). If your inputs are already plain text or Markdown, [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) is the same flow without the docling step; if you want the Markdown itself as the output, see [PDF → Markdown](https://cocoindex.io/docs/examples/pdf-to-markdown/).

Got a folder of papers, reports, or scanned docs you want to search by meaning? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
