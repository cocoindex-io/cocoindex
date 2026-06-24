<p align="center">
  <a href="https://cocoindex.io/docs/examples/paper-metadata/" title="Index academic PDFs into structured metadata with CocoIndex — LLM-extract title/authors/abstract, embed for semantic search, store in Postgres pgvector, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/paper-metadata/cover.svg" alt="Turn a folder of academic PDFs into structured metadata with CocoIndex — read the first page, LLM-extract title, authors, and abstract into typed rows, embed the title and abstract for semantic search, and store it all in Postgres with pgvector" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn a folder of papers into <em>structured</em> metadata.</h1>

<p align="center">
  <b>Read just the first page, LLM-extract <em>title, authors, abstract</em> into typed rows, then embed the metadata so you can search papers by <em>meaning</em> — in plain async Python.</b><br/>
  One PDF fans out into three Postgres tables, and CocoIndex keeps all three in sync as the folder changes.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/paper-metadata/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

The first page of a paper holds almost everything you'd want to query — title, authors, abstract — but it's locked in PDF prose. This pipeline reads just that page, hands the text to an LLM with a strict schema, and gets back clean typed JSON; the same metadata is then embedded so you can search by meaning, not exact words. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-extracted and re-embedded.

## How it works

One PDF flows through three small functions and fans into three tables:

1. **`extract_basic_info`** slices the first page out of the PDF and counts the pages; **`pdf_to_markdown`** pulls the text off that page with [pypdf](https://github.com/py-pdf/pypdf).
2. **`extract_metadata`** hands that text to `gpt-4o` (via the `openai` SDK) with `response_format={"type": "json_object"}` and `temperature=0`, then `model_validate_json` parses it into a typed `PaperMetadataModel` — a malformed response fails loudly instead of writing junk.
3. **`process_file`** declares the rows: one metadata row, one author-index row per author, one embedding row for the title plus one per abstract chunk.

Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    metadata_table: postgres.TableTarget[PaperMetadataRow],
    author_table: postgres.TableTarget[AuthorPaperRow],
    embedding_table: postgres.TableTarget[MetadataEmbeddingRow],
) -> None:
    content = await file.read()
    basic_info = extract_basic_info(content)
    metadata = extract_metadata(pdf_to_markdown(basic_info.first_page))

    metadata_table.declare_row(row=PaperMetadataRow(
        filename=str(file.file_path.path), title=metadata.title,
        authors=[a.model_dump() for a in metadata.authors],
        abstract=metadata.abstract, num_pages=basic_info.num_pages,
    ))
    for author in metadata.authors:
        if author.name:
            author_table.declare_row(row=AuthorPaperRow(
                author_name=author.name, filename=str(file.file_path.path)))

    title_embedding = await coco.use_context(EMBEDDER).embed(metadata.title)
    embedding_table.declare_row(row=MetadataEmbeddingRow(
        id=uuid.uuid4(), filename=str(file.file_path.path),
        location="title", text=metadata.title, embedding=title_embedding))
    for chunk in _abstract_splitter.split(metadata.abstract, chunk_size=500, ...):
        embedding_table.declare_row(row=MetadataEmbeddingRow(
            id=uuid.uuid4(), filename=str(file.file_path.path), location="abstract",
            text=chunk.text, embedding=await coco.use_context(EMBEDDER).embed(chunk.text)))
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically. `app_main` mounts the three tables (with different primary keys), walks the source for `*.pdf`, and runs one `process_file` component per file with `mount_each`.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/paper-metadata/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the Pydantic schema, the three-table fan-out, the abstract splitter, and the pgvector query.
</p>

## Why it's worth a star ⭐

- **One file, three tables, kept in sync.** Paper metadata, an author-to-paper index, and embeddings — `mount_table_target` upserts only what changed and removes rows whose PDF is gone, across all three.
- **First page only, capped at 4000 chars.** That's almost always enough for the title block and abstract, and it keeps token cost flat regardless of paper length.
- **Typed extraction, validated loud.** `gpt-4o` returns JSON, `PaperMetadataModel.model_validate_json` rejects anything off-schema — junk never reaches Postgres.
- **Incremental by default.** `@coco.fn(memo=True)` skips a PDF entirely when its bytes and the function's code are unchanged, so you never re-pay for the LLM call or the embeddings on a file you've seen.
- **Honest cache busting.** `EMBEDDER` is declared with `detect_change=True`, so swapping the embedding model re-embeds everything with no cache to clear by hand.

## Run it

**1. Start Postgres (with pgvector):**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — the example ships a `papers/` folder of well-known papers:

```sh
cp .env.example .env     # set POSTGRES_URL and OPENAI_API_KEY
pip install -e .
```

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
cocoindex update main       # catch-up run
cocoindex update -L main    # live run — watch the papers/ folder for changes
```

This reads each PDF's first page, LLM-extracts the metadata, embeds the title and abstract chunks, and writes the `coco_examples_v1` schema's three tables.

**4. Search by meaning** — a plain SQL query over pgvector's cosine distance, reusing the *same* embedder:

```sh
python main.py "graph neural networks"
```

The most semantically similar titles and abstracts come back ranked — even when they share none of the query's words. Note: to keep the example minimal it declares **no vector index**, so queries do a sequential scan (fine for a handful of papers).

---

<p align="center">
  If this turned your PDFs into searchable rows, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/paper-metadata/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/paper_metadata" alt="" width="1" height="1" />
