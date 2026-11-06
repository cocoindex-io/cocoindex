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

## Run apps with the `cocoindex` CLI

Register the environment setup and the app entry point, and the standalone
`cocoindex` binary (`rust/cli`) builds the crate and drives it: `cocoindex
update`, `update -L`, `update --preview`, `ls`, `show`, `drop`. The same
lifespan runs when your own code calls `Environment::from_registered()`, like
`@coco.lifespan` in Python.

```rust
use cocoindex::prelude::*;

#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    let database = postgres::Database::connect(&url).await?;
    let pool = database.pool().clone();
    Ok(builder
        .db_path(".cocoindex_db")
        .provide_key(&DB, database)
        .on_shutdown(move || async move { pool.close().await }))
}

#[cocoindex::app(name = "MyApp")]
async fn app_main(ctx: Ctx) -> Result<()> {
    // Declare the desired target state using ctx.
    Ok(())
}

#[cocoindex::main]
async fn main() -> Result<()> {
    // `cocoindex ...` invocations are handled before this body runs.
    let env = Environment::from_registered().await?;
    let stats = env.app("MyApp").await?.run(app_main).await?;
    println!("{stats}");
    env.shutdown().await;
    Ok(())
}
```

## Inspect stored state

`cocoindex::inspect` reads what an app has stored in the state db: its stable
paths, the details of each one (processor, version, target states), and the
tracked target states with their owner component. It is the Rust form of
Python's `cocoindex.inspect`, and `cocoindex show` prints the same data. Each
function takes an `App`; its `_by_name` form takes an `Environment` and an app
name.

```rust
use cocoindex::inspect::{self, StablePath, StablePathQuery};
use futures::TryStreamExt;

for path in inspect::list_stable_paths(&app).await? {
    println!("{path}");
}

// Streams read in one transaction and buffer nothing.
let mut entries = std::pin::pin!(inspect::iter_target_states(&app).await);
while let Some(entry) = entries.try_next().await? {
    println!("{} owner:{}", entry.readable_path, entry.owner_component_path);
}

// One path, with the paths under it.
let query = StablePathQuery { include_children: true, ..Default::default() };
let details = inspect::query_stable_path_details(&app, &StablePath::root(), query).await?;

// Without the `App`: an environment and the app's name.
let details = inspect::get_stable_path_detail_by_name(&env, "MyApp", &StablePath::root()).await?;
```

Both SDKs write one state db format. The Python CLI reads a db that a Rust app
wrote (`cocoindex show --db PATH --app-name NAME`), and the Rust CLI reads a
Python app's db the same way.

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
  and `Arc`. UUID and `serde_json::Value` support is always available; enable
  the standalone `chrono` or `rust_decimal` feature for those leaf types.
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

`mount!` is the background form, like `coco.mount` in Python: the parent does
not wait for the child and takes no value from it. The child runs on its own
task, its target states sync when it finishes, and in live mode it refreshes
independently. The handle waits for the child when the parent needs that:

```rust
let handle = mount!(process_file(ctx, file, target)).await?;
// ... other work ...
handle.ready().await?;
```

`ctx.mount(key, body)` is the dynamic form. A failing background child does not
fail its parent or siblings: the failure is logged, or routed through the
exception handlers in scope.

### Nested component paths

`ctx.with_subpath(&"process")` returns a context whose child mounts go under
`process`, the Rust spelling of a `with coco.component_subpath("process"):`
block. Chain calls for a multi-part prefix. `use_state` is not available on a
subpath context, as in Python.

```rust
let process = ctx.with_subpath(&"process");
for file in files {
    let ctx = &process;
    mount!(file.name(), process_file(ctx, file, target.clone())).await?;
}
```

### Exception handlers

A handler sees the failures of background-mounted components (`mount!`,
`ctx.mount`, live components, `auto_refresh`). It returns `Ok(())` to swallow a
failure or `Err(_)` to pass it to the next outer handler; when every handler
returns `Err`, the last error reaches the mount's `handle.ready()`. Install
one for the whole environment on the builder, like Python's
`builder.set_exception_handler`, or for one scope with
`ctx.with_exception_handler`, like `async with coco.exception_handler(...)`:

```rust
#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    Ok(builder.exception_handler(|err, exc| {
        tracing::error!("{} failed at {}: {err}", exc.env_name, exc.stable_path);
        Ok(())
    }))
}

let scoped = ctx.with_exception_handler(|err, exc| match exc.source {
    ExceptionSource::Component => Err(Error::user(DeadLetter(err.to_string()))),
    ExceptionSource::Handler => Ok(()),
});
let handle = scoped.mount(&"child", process).await?;
handle.ready().await?; // Err(DeadLetter) when the child fails
```

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

## Read blobs from Azure Blob Storage

The `azure_blob` feature lists and reads the blobs of a container over the Blob
service REST API, with an account key, a SAS token or a Microsoft Entra ID
access token. `AzureBlobFile` implements `FileLike` and `MemoInput`: a memoized
function runs again only for a blob whose content changed.

```rust
use cocoindex::connectors::azure_blob::{self, AzureBlobFile, ContainerClient, Credential, ListOptions};

#[cocoindex::function(memo)]
async fn index_blob(ctx: &Ctx, file: AzureBlobFile) -> Result<usize> {
    Ok(file.read_text().await?.len())
}

let client = ContainerClient::new(
    "https://myaccount.blob.core.windows.net",
    "docs",
    Credential::AccountKey(account_key),
)?;
let files = azure_blob::list_blobs(
    &client,
    ListOptions { prefix: "handbook".to_string(), ..Default::default() },
)
.items()
.await?;
mount_each!(files, |file| index_blob(ctx, file)).await?;
```

A prefix names a folder: `"handbook"` lists the blobs under `handbook/`, keyed
by the rest of their names. `azure_blob::get_blob` fetches one blob and
`azure_blob::read` reads one by name. The end-to-end reference is
[`examples/rust/azure_blob_embedding`](../../examples/rust/azure_blob_embedding).

## Store documents in an embedded Zvec collection

The `zvec` feature declares a [Zvec](https://github.com/alibaba/zvec) collection
and its documents as target states. Zvec runs in the process, with no server.
The collection schema comes from the row type, with dense vector and full-text
search fields.

```rust
use cocoindex::connectors::zvec::{self, CollectionSchema, ConnectOptions, ManagedConnection, ZvecFtsType};

cocoindex::context_key!(static ZVEC: ManagedConnection);

#[derive(Serialize, SchemaFields)]
struct Doc {
    id: String,
    body: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

// At startup: provide the connection with `.provide_key(&ZVEC, conn.clone())`.
let conn = zvec::connect("./zvec_data", ConnectOptions::default())?;

// In the app: the collection and its documents follow what is declared.
let schema = CollectionSchema::from_row::<Doc>(["id"])?
    .with_vector_dim("embedding", 384)?
    .with_fts("body", ZvecFtsType::default())?;
let docs = zvec::mount_collection_target(&ctx, &ZVEC, "docs", schema).await?;
docs.declare_row(&ctx, &doc)?;
```

The `zvec-rust` build script downloads the prebuilt `libzvec_c_api` shared
library. `cargo run` and `cargo test` find it; a binary that runs on its own
needs it on the library path, or a `build.rs` that calls
`zvec_rust_build::configure()`.

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
