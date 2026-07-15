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
to write their own `LazyLock<ContextKey<_>>`. The name is persistent identity;
do not derive it from the Rust module path.

```rust
use cocoindex::connectors::postgres;

cocoindex::context_key!(
    static DB: postgres::Database = "app_database",
    state = postgres::Database::state_id
);
cocoindex::context_key!(static CONFIG: AppConfig = "app_config", detect_change);
cocoindex::context_key!(static CLIENT: ApiClient = "api_client");
```

- The plain form provides a typed resource without change tracking.
- `detect_change` fingerprints the complete serializable value.
- `state = expression` fingerprints only a stable derived state, which is
  useful for connections and model clients whose runtime handles are not
  serializable.

Provide resources with `EnvironmentBuilder::provide_key` and read them with
`ctx.get_key(&KEY)`. Reads inside memoized functions are tracked as
dependencies when the key uses a change-detecting form.

## Define functions

`#[cocoindex::function]` tracks the function's logic. Adding `memo` caches the
result by the function logic, serializable arguments, and context dependencies:

```rust
#[cocoindex::function(memo)]
async fn parse_file(_ctx: &Ctx, file: FileEntry) -> Result<Vec<Section>> {
    parse(file.content_str()?)
}
```

Use `memo_key(...)` when the default representation of an argument is either
too broad or not serializable. A transform replaces that argument's memo key;
`skip` (also spelled `None`) excludes it:

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
call is processed. `max_batch_size` caps each physical request.

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
`mount_each!`. `FileEntry` is serializable, so a mounted or memoized function
can use it directly as an input.

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
