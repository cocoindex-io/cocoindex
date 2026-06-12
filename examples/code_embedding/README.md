<p align="center">
  <a href="https://cocoindex.io/docs/examples/index-codebase/" title="Index your codebase for AI agents with CocoIndex — Tree-sitter chunking, embeddings, and a live pgvector index">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/cover.png" alt="Index your codebase for AI agents with CocoIndex and Tree-sitter — language-aware chunking, embeddings, semantic search, and a live vector index in plain async Python" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Index your <em>codebase</em> for AI agents.</h1>

<p align="center">
  <b>A live, syntax-aware vector index over your repo — in ~100 lines of plain async Python.</b><br/>
  Point it at a directory, search it in natural language, and it re-embeds only what changes as you edit.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/index-codebase/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

You declare the transformation in native Python and your own types — `target_state = transformation(source_state)`. The heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so a one-line edit re-embeds one chunk, not the repo.

```python
query: "where do we embed chunks?"

[0.582] examples/code_embedding/main.py (L66-L83)
    @coco.fn
    async def process_chunk(chunk, filename, id_gen, table):
        embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
        ...
```

## How it works

Walk a repo → detect language → split along the **syntax tree** with Tree-sitter → embed each chunk → upsert into Postgres (pgvector). With `live=True`, the source keeps watching and the index stays fresh as you code.

<p align="center">
  <img src="https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/flow-v1.png" alt="CocoIndex code-embedding flow: localfs.walk_dir source → per-file processing component (detect language, Tree-sitter RecursiveSplitter, coco.map → embed each chunk → declare CodeEmbedding rows) → Postgres pgvector target with a cosine vector index" width="70%"/>
</p>

The whole indexing path is the snippet below — read it top-to-bottom in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[CodeEmbedding]) -> None:
    text = await file.read_text()
    language = detect_code_language(filename=str(file.file_path.path.name))
    chunks = _splitter.split(text, chunk_size=1000, min_chunk_size=300,
                             chunk_overlap=300, language=language)   # Tree-sitter, syntax-aware
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@coco.fn
async def process_chunk(chunk, filename, id_gen, table) -> None:
    embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
    table.declare_row(row=CodeEmbedding(
        id=await id_gen.next_id(chunk.text), filename=str(filename), code=chunk.text,
        embedding=embedding, start_line=chunk.start.line, end_line=chunk.end.line,
    ))

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    table = await postgres.mount_table_target(PG_DB, table_name=TABLE_NAME, ...)
    table.declare_vector_index(column="embedding")
    files = localfs.walk_dir(sourcedir, recursive=True,
                             path_matcher=PatternFilePathMatcher(included_patterns=["**/*.py", ...]),
                             live=True)
    await coco.mount_each(process_file, files.items(), table)
```

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/index-codebase/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the data model, the lifespan, chunking, embedding, the App, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Syntax-aware chunking, built in.** Tree-sitter splits along real code structure — functions, classes, blocks — so retrieval returns whole units, not fragments cut mid-statement. Every major language; unknown types fall back to plain text.
- **Incremental by default.** `@coco.fn(memo=True)` skips unchanged files and reuses embeddings for unchanged chunks; `mount_table_target` upserts only the rows that moved and deletes orphans. Edit one function → one chunk is re-embedded.
- **Live updates.** `live=True` + `cocoindex update -L` keeps watching the filesystem and applies changes with low latency — always-fresh context for an agent.
- **Plain Python, your stack.** Swap the embedding model (12k+ on Hugging Face), the chunking, or the [vector store](https://cocoindex.io/docs/). No DSL.
- **Consistent index & query.** The same embedder is shared by the indexing and query paths, so what you index is what you search.

<p align="center">
  <img src="https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/incremental-diff.png" alt="A file edited and re-chunked: unchanged chunks are reused with no re-embedding, a removed chunk's row is deleted, and a new chunk is embedded and inserted" width="78%"/>
</p>

## Run it

**1. Postgres + pgvector.** If you don't have one, start a local instance with the compose file in this repo:

```sh
docker compose -f ../../dev/postgres.yaml up -d
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

**2. Install deps:**

```sh
pip install -e .
```

**3. Build / update the index** (writes rows into Postgres) — pick one:

```sh
cocoindex update main       # catch-up: scan, sync changes, exit
cocoindex update -L main    # live: catch up, then keep watching for edits
```

**4. Query it** — semantic search from the terminal:

```sh
python main.py "embedding"
```

<p align="center">
  <img src="https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/search-results.png" alt="Semantic search results in the terminal: similarity score, filename, matched line range, and the code snippet" width="80%"/>
</p>

Each result carries `start_line`/`end_line`, so hits point straight at the lines that matched. Query uses pgvector's `<=>` cosine distance, turned into a similarity score.

## Want it production-ready, not DIY?

[**CocoIndex Code**](https://github.com/cocoindex-io/cocoindex-code) is this exact pipeline — AST-aware chunking, incremental re-index, local embeddings — hardened and packaged as a CLI and an MCP server you can plug straight into a coding or code-review agent.

<p align="center">
  <a href="https://github.com/cocoindex-io/cocoindex-code" title="CocoIndex Code — semantic code search for coding agents, as a CLI and MCP server">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/index-codebase/cocoindex-code.png" alt="CocoIndex Code — semantic code search for coding agents, as a CLI and MCP server" width="80%"/>
  </a>
</p>

```sh
npx skills add cocoindex-io/cocoindex-code     # Claude Code skill, then /ccc
claude mcp add cocoindex-code -- ccc mcp        # MCP: Codex, OpenCode, Cursor, any client
ccc index && ccc search "where we embed chunks" # CLI
```

---

<p align="center">
  If this made your agents smarter, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/index-codebase/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>
