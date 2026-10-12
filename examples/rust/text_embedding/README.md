# Text Embedding (Rust)

Rust port of the Python [`text_embedding`](../../text_embedding) example.

Walks local markdown files, chunks each file (markdown-aware), embeds the chunks,
and stores them in Postgres/pgvector — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                      |
| ---------------- | ---------------------------------------- | -------------------------------------------------------- |
| Environment      | `@coco.lifespan`                         | `#[cocoindex::lifespan]`                                 |
| App              | `coco.App(...)`                          | `#[cocoindex::app(name = "TextEmbeddingRust")]`          |
| Source           | `localfs.walk_dir(..., live=True)`       | `walk_dir(..).live()`                                    |
| Per-file compute | `coco.mount_each(process_file, ...)`     | `ctx.mount_each_live(..)` + `#[cocoindex::function(memo)]` |
| Chunking         | `RecursiveSplitter` (Markdown)           | `ops::text::RecursiveSplitter` (Markdown)                |
| Embeddings       | `SentenceTransformerEmbedder.embed`      | `SentenceTransformerEmbedder::embed(&ctx, text)`         |
| Target           | `postgres.TableTarget` + pgvector index  | `postgres::TableTarget` + `declare_vector_index`          |
| Row schema       | Dataclass annotations                    | `#[derive(SchemaFields)]` + runtime model dimension       |

Incrementality: each markdown file is its own component, and `process_file` is
memoized on the file, so an unchanged file is not re-chunked. The embedder
memoizes individual texts and automatically groups concurrent cache misses into
batches of up to 64. Rows of a removed or edited file are reconciled away by
the managed `TableTarget`.

## Run

Start Postgres with pgvector and point the example at it:

```bash
docker compose -f ../../../dev/postgres.yaml up -d
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex
```

Install the `cocoindex` CLI from this repository (or run it with
`cargo run -p cocoindex_cli --` from the repository root):

```bash
cargo install --path ../../../rust/cli
```

Then, from this directory:

```bash
cocoindex update                   # build, then walk ./markdown_files -> chunk -> embed -> pgvector
cocoindex update -L                # keep watching ./markdown_files for changes (Ctrl-C to stop)
cocoindex update --preview         # planned changes (see the note below)
cocoindex show                     # inspect the persisted state
cocoindex drop                     # drop the app's state
cargo run -- query "your query"    # cosine similarity search
```

`TEXT_EMBEDDING_SOURCE_DIR` points the app at another directory. Without the
CLI, `cargo run -- index [DIR]` runs one catch-up pass through the same
`#[cocoindex::lifespan]` (`Environment::from_registered`).

Two limits, both described in `dev/plans/rust-cli.md`:

- `cocoindex update --preview` stops with an engine error here: preview does not
  yet support target actions under child providers, such as the rows of a
  Postgres table.
- `cocoindex drop` clears the app's state but leaves the Postgres table in
  place: dropping does not yet revert targets in the Rust SDK.
