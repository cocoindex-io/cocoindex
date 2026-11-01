# Ergonomic Rust SDK

CocoIndex's Rust SDK provides the same declarative, incremental processing
model as the Python SDK. Rust pipelines use attribute and function-like macros
for logic tracking, memoization, batching, and mounting, while an explicit
`&Ctx` carries component and resource state.

## Open and run an app

`App::open` is asynchronous. `App::open_blocking` is available when an async
entry point is not convenient.

```rust
use cocoindex::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let app = App::open("my_app", ".cocoindex_db").await?;
    let stats = app
        .run(|ctx| async move {
            // Declare the desired target state using ctx.
            Ok(())
        })
        .await?;

    println!("{stats}");
    Ok(())
}
```

Use `App::builder(name).db_path(path).build().await` for single-app
configuration. When an app needs shared resources, build an `Environment`,
provide the resources, and then create the app:

```rust
let app = Environment::builder()
    .db_path(".cocoindex_db")
    .provide_key(&DB, database)
    .build()
    .await?
    .app("my_app")
    .await?;
```

## Declare typed context resources

`context_key!` gives a resource a stable name and type without requiring users
to write their own `LazyLock<ContextKey<_>>`. The Rust identifier is the key
name by default. Use `key = "..."` when the persistent identity should differ
from the identifier or survive an identifier rename.

```rust
use cocoindex::connectors::postgres;

#[derive(cocoindex::MemoInput)]
struct AppConfig {
    model: String,
    debug: bool,
}

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(static CONFIG: AppConfig, detect_change);
cocoindex::context_key!(static CLIENT: ApiClient);

// Use an explicit key only when its persisted identity should differ from
// the Rust identifier or remain stable across an identifier rename.
cocoindex::context_key!(
    static LEGACY_DB: postgres::Database,
    key = "app_database"
);
```

- The plain form provides a typed resource without change tracking.
- `detect_change` uses the value type's `MemoInput` implementation. Derive
  `MemoInput` for ordinary structs and enums; each field is handled
  recursively, so the containing type does not need `Serialize`. Resource
  types can define a stable identity and external-state validation themselves.
- `MemoInput` composes through `Option`, sequences, tuples, maps, sets, `Box`,
  and `Arc`. UUID support is always available; enable the standalone
  `serde_json`, `chrono`, or `rust_decimal` feature for those leaf types.
  Connector features enable the corresponding leaf feature automatically.
  Wrap other third-party leaf types in a newtype when they appear inside
  derived data.
- When both optional arguments are present, write `key = "..."` before
  `detect_change`.

`Vec<u8>` follows normal element-by-element `Vec<T>` semantics. Use
`bytes::Bytes` when a large byte buffer should be hashed in bulk, or pass a
precomputed `Fingerprint` when one is already available.

Provide resources with `EnvironmentBuilder::provide_key` and read them with
`ctx.get_key(&KEY)`. Reads inside memoized functions are tracked as
dependencies when the key uses a change-detecting form.

## Define functions

`#[cocoindex::function]` tracks the function's logic. Adding `memo` caches the
result by the function logic, `MemoInput` arguments, and context dependencies:

```rust
#[cocoindex::function(memo)]
async fn parse_file(_ctx: &Ctx, file: FileEntry) -> Result<Vec<Section>> {
    parse(file.content_str()?)
}
```

Use `memo_key(...)` when an argument's default `MemoInput` identity is too
broad or the type does not implement `MemoInput`. A transform replaces that
argument's memo key and external-state validation; `skip` (also spelled
`None`) excludes it:

```rust
fn entry_identity(entry: &Entry) -> (String, u64) {
    (entry.name.clone(), entry.version)
}

#[cocoindex::function(
    memo,
    memo_key(entry = entry_identity, client = skip)
)]
async fn fetch(_ctx: &Ctx, entry: &Entry, client: &ApiClient) -> Result<String> {
    client.fetch(&entry.name).await
}
```

Only skip an argument when changes to it cannot affect the result, or when the
equivalent dependency is tracked through a context key. Use `ctx.memo(...)`
for a memoized block within a function; prefer `#[function(memo)]` for a whole
function because the macro tracks its logic automatically.

## Batch item-shaped calls

A function marked `batching` has a batch-shaped implementation but an
item-shaped call site. Concurrent calls are coalesced automatically, so callers
do not construct or manage a `Batched` value:

```rust
#[cocoindex::function(memo, batching, max_batch_size = 64)]
async fn embed_batch(
    ctx: &Ctx,
    texts: Vec<String>,
    model: String,
) -> Result<Vec<Vec<f32>>> {
    ctx.get_key(&EMBEDDING_CLIENT)?.embed(texts, &model).await
}

let embeddings = ctx
    .map(texts, {
        let ctx = ctx.clone();
        move |text| {
            let ctx = ctx.clone();
            let model = model.clone();
            async move { embed_batch(&ctx, text, model).await }
        }
    })
    .await?;
```

The body receives only the items in the current batch. With `memo`, cache hits
are returned per item and only misses enter the batch. Without `memo`, every
call is processed. `max_batch_size` caps each physical request. Additional
parameters must implement `Serialize` because they identify compatible calls
for batching; with `memo`, they must also implement `MemoInput` unless their
memo identity is transformed or skipped.

A physical batch does not inherit any individual caller's deadline. A batch
error currently fails every item in that physical batch; the Rust SDK does not
yet retry smaller sub-batches automatically. A batching function must also not
call itself recursively from its own body, because that waits on the batcher
which is already executing the body.

The built-in `SentenceTransformerEmbedder::embed(&ctx, text)` already uses
this pattern: concurrent cache misses are batched up to 64 texts and repeated
texts are memoized.

## Mount processing components

For normal function calls, prefer the macros that include function logic and
arguments in a component-memo fingerprint:

```rust
let summary = use_mount!(summarize(ctx, document)).await?;

let outputs = mount_each!(files, |file| process_file(ctx, file, target)).await?;
```

`mount_each!` accepts `(key, value)` items, creates one child component per
key, and runs them concurrently. Without an explicit prefix, the entry
function's name is used; the prefixed form is
`mount_each!("documents", files, |file| process_file(ctx, file))`.

Use `ctx.scope(key, body)` or `ctx.mount_each(items, key_fn, body)` only when
the component key and closure are deliberately dynamic and no automatic
function/argument fingerprint is wanted. These methods always execute their
closures; they still provide stable child ownership and reconciliation.

## Read files and declare output files

`walk_items` produces stable `(relative_path, FileEntry)` pairs ready for
`mount_each!`. `FileEntry` implements `MemoInput`, including content freshness
validation, so a memoized function can use it directly or inside a container.

```rust
#[cocoindex::function]
async fn render_file(ctx: &Ctx, file: FileEntry, target: DirTarget) -> Result<()> {
    let markdown = render(file.content_str()?);
    target.declare_file(ctx, &format!("{}.md", file.stem()), markdown.as_bytes())?;
    Ok(())
}

let target = DirTarget::mount(&ctx, "./output")?;
let files = walk_items("./input", &["**/*.txt"])?;
mount_each!(files, |file| render_file(ctx, file, target.clone())).await?;
```

`DirTarget` is declarative: new and changed files are written, unchanged files
are skipped, and files no longer declared by their owning components are
removed during reconciliation.

## Derive connector schemas from row types

`SchemaFields` keeps the Rust row and connector schema in one place. A bare
`#[coco(vector)]` marks a vector whose runtime dimension can be supplied after
the embedding model is loaded:

```rust
#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct DocEmbedding {
    id: i64,
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

let dim = ctx.get_key(&EMBEDDER)?.dimension();
let schema = postgres::TableSchema::from_row::<DocEmbedding>(["id"])?
    .with_vector_dim("embedding", dim)?;
```

The current end-to-end reference is
[`examples/rust/text_embedding`](../../examples/rust/text_embedding). It walks
Markdown files, splits them into chunks, memoizes and batches embeddings, and
declares Postgres/pgvector rows using a schema derived from the Rust row type.
