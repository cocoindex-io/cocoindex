<p align="center">
  <a href="https://cocoindex.io/docs/examples/pdf-embedding/" title="Build a vector index from local PDFs with CocoIndex — convert to Markdown with docling on a GPU runner, chunk, embed, and store in Postgres pgvector, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/pdf-embedding/cover.svg" alt="Semantic search over PDFs with CocoIndex — walk a folder of PDFs, convert each to Markdown with docling on a GPU runner, split into chunks, embed with sentence-transformers, and store the vectors in Postgres with pgvector" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Semantic search over a folder of <em>PDFs</em>.</h1>

<p align="center">
  <b>Convert each PDF to Markdown with <em>docling</em> on a GPU runner, <em>chunk</em> and <em>embed</em> it, and store the vectors in Postgres pgvector.</b><br/>
  Papers, RFCs, manuals, contracts — searchable in plain English, in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/pdf-embedding/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Take a folder of PDFs and turn it into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English. The trick PDFs add over plain text: they have to be *parsed* first. This pipeline uses [docling](https://github.com/docling-project/docling) to convert each PDF to clean Markdown, then chunks, embeds, and stores the vectors in Postgres. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-parsed and re-embedded.

## How it works

The one genuinely expensive step is PDF parsing, so it runs on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/) and the docling converter is built once with `@functools.cache`. `process_file` converts the PDF to Markdown, splits it into overlapping chunks, and maps each chunk to `process_chunk` for embedding. Read it in [`main.py`](main.py):

```python
@coco.fn.as_async(runner=coco.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="input.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()

@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[PdfEmbedding]) -> None:
    markdown = await pdf_to_markdown(await file.read())
    chunks = _splitter.split(markdown, chunk_size=2000, chunk_overlap=500, language="markdown")
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

`@coco.fn.as_async(runner=coco.GPU)` wraps the *synchronous*, GPU-heavy parse so it runs off the async event loop. Each chunk's row `id` is derived from its text, so a chunk that survives a re-parse keeps its row.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/pdf-embedding/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the docling converter, the GPU runner, the row schema, and the query.
</p>

## Why it's worth a star ⭐

- **Parsing where text embedding has none.** docling reads the PDF and exports Markdown that preserves headings, tables, and reading order — which is exactly what makes the downstream chunks coherent.
- **The slow step, off the event loop.** `@coco.fn.as_async(runner=coco.GPU)` offloads PDF parsing to a dedicated GPU runner; `@functools.cache` loads the docling model once, not per file.
- **Incremental by default.** `@coco.fn(memo=True)` skips a PDF whose bytes and code are unchanged, so docling never re-parses a file you've already converted; `mount_table_target` upserts only changed rows and deletes rows whose source is gone.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and added, replaced, or deleted PDFs are picked up as they change.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** (docling pulls in the PDF parser):

```sh
cp .env.example .env     # set POSTGRES_URL
pip install -e .
```

**3. Build the index** — the example ships a `pdf_files/` folder of sample papers/RFCs; catch-up or live:

```sh
cocoindex update main        # catch-up
cocoindex update -L main     # live: keep watching for file changes
```

**4. Search from the command line:**

```sh
python main.py "what is attention?"
```

With the sample papers indexed, the most semantically similar passages come back ranked — even when they share none of the words in your query. This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan. For a larger corpus, add `target_table.declare_vector_index(column="embedding")` exactly as [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) does.

---

<p align="center">
  If this made your PDFs searchable, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/pdf-embedding/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/pdf_embedding" alt="" width="1" height="1" />
