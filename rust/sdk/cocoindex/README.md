# cocoindex

Declarative, incremental data pipelines in Rust.

CocoIndex keeps a target in sync with its source. You declare what the output
should look like (files, rows, vectors, graph nodes) as a function of the
input, and the engine works out what changed since the last run and applies
the minimal set of creates, updates and deletes. This crate is the Rust SDK;
it runs the same core engine as the Python package
[`cocoindex`](https://pypi.org/project/cocoindex/) and shares its concepts and
state database.

- [Documentation](https://cocoindex.io/docs), with the
  [Rust quickstart](https://cocoindex.io/docs/getting_started/rust_quickstart)
- [Showcase](https://github.com/cocoindex-io/cocoindex/blob/main/rust/sdk/SHOWCASE.md):
  a tour of every part of the SDK
- [Examples](https://github.com/cocoindex-io/cocoindex/tree/main/examples/rust)
- [API reference](https://docs.rs/cocoindex)

## Example

A pipeline that upper-cases every `.txt` file under `./input` into `./output`.
On the second run only the files that changed are processed, and a file
removed from `./input` is removed from `./output`.

```rust
use cocoindex::prelude::*;

// One processing component per file: it declares that file's output, so the
// output is synced, and removed, with the file.
#[cocoindex::function(memo)]
async fn upper(ctx: &Ctx, file: FileEntry, target: DirTarget) -> Result<()> {
    let text = file.content_str()?.to_uppercase();
    target.declare_file(ctx, &file.key(), text.as_bytes())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = App::open("upper_case", "./.cocoindex_db").await?;
    let stats = app
        .run(|ctx| async move {
            let target = DirTarget::mount(&ctx, "./output")?;
            let files = walk_items("./input", &["**/*.txt"])?;
            mount_each!(files, |file| upper(ctx, file, target)).await?;
            Ok(())
        })
        .await?;
    println!("{stats}");
    Ok(())
}
```

## Features

The crate has no default features. Every connector and op is behind a feature
so that a pipeline compiles only what it uses.

| Feature | Enables | Needs at build time |
|---|---|---|
| `text` | `ops::text`: language detection, recursive and separator chunking | a C compiler (tree-sitter grammars) |
| `code_match` | `ops::code`: structural code matching over tree-sitter ASTs | a C compiler |
| `fastembed` | `ops::sentence_transformers`, `ops::image`: local embeddings | the ONNX Runtime, downloaded by `ort-sys`; models download at run time |
| `embed_api` | `ops::api`: embeddings and transcription over an OpenAI-compatible HTTP API | |
| `fs_live` | `DirWalker::live`: follow a directory with native file events | |
| `chrono`, `rust_decimal` | `MemoInput` for those crates' types | |
| `postgres` | Postgres source and target (pgvector) | |
| `sqlite` | SQLite target | |
| `doris` | Apache Doris target | |
| `surrealdb` | SurrealDB target | |
| `lancedb` | LanceDB target (embedded) | |
| `qdrant` | Qdrant target | `protoc` |
| `turbopuffer` | turbopuffer target | |
| `zvec` | Zvec target (embedded) | the prebuilt `libzvec_c_api`, downloaded by `zvec-rust-sys` |
| `valkey` | Valkey/Redis (RediSearch) target | OpenSSL headers on Linux |
| `neo4j`, `falkordb` | graph targets | |
| `bigquery`, `snowflake` | data warehouse targets | |
| `kafka`, `iggy` | message stream sources and targets | |
| `amazon_s3`, `azure_blob`, `google_drive`, `oci_object_storage` | object store and drive sources | |
| `testing` | `cocoindex::testing`: an `Environment` on a temporary database for tests | |

Features that speak HTTP through `reqwest` use the platform's native TLS,
which on Linux needs the OpenSSL development headers.

## License

Apache-2.0
