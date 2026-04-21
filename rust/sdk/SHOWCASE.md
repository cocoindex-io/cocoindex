# Ergonomic Rust SDK

**What is the use case?**

An idiomatic Rust SDK for CocoIndex with the same incremental pipeline capabilities as the Python SDK — memoized computation, scoped components, file walking with fingerprinting — but using proc macros instead of decorators and explicit `&Ctx` instead of hidden globals.

**Describe the solution you'd like**

### `App`

Entry point. Open an LMDB-backed app and run a pipeline:

```rust
let app = cocoindex::App::open("my_app", ".cocoindex_db")?;

let stats = app.run(|ctx| async move {
    // pipeline logic using ctx
    Ok(())
}).await?;

println!("{stats}");  // "processed 5, wrote 3, skipped 2 in 0.4s"
```

| Method | Signature |
|--------|-----------|
| `App::open` | `(name: &str, db_path: impl Into<PathBuf>) -> Result<App>` |
| `App::builder` | `(name: &str) -> AppBuilder` — configure `.db_path()`, `.provide()`, `.build()` |
| `app.run` | `(F: FnOnce(Ctx) -> Future<Result<()>>) -> Result<RunStats>` |

### `Ctx`

Pipeline context, threaded explicitly through every function:

```rust
impl Ctx {
    /// Create a named sub-component. Scopes track what changed between runs.
    pub async fn scope(&self, key: &impl Display, f: impl FnOnce(Ctx) -> Future<Result<T>>) -> Result<T>;

    /// Memoized computation. Skips the closure if the key hasn't changed since last run.
    pub async fn memo(&self, key: &impl Serialize, f: impl FnOnce() -> Future<Result<T>>) -> Result<T>;

    /// Batch-process items with per-item memoization.
    /// Cache hits return stored values. Cache misses are collected and passed to `f`
    /// as a single batch. Results stored back and merged in original order.
    pub async fn batch(&self, items: I, key_fn: impl Fn(&Item) -> K, f: impl FnOnce(Vec<Item>) -> Future<Result<Vec<T>>>) -> Result<Vec<T>>;

    /// Run a closure concurrently for each item, creating a child scope per item.
    pub async fn mount_each(&self, items: I, key_fn: impl Fn(&Item) -> K, f: impl Fn(Ctx, Item) -> Fut) -> Result<Vec<T>>;

    /// Run a closure concurrently for each item within the current scope (no child scopes).
    pub async fn map(&self, items: I, f: impl Fn(Item) -> Fut) -> Result<Vec<T>>;

    /// Get a shared resource by type, returning a typed error if missing.
    pub fn get_or_err<T: Send + Sync + 'static>(&self) -> Result<&T>;

    /// Try to get a shared resource by type.
    pub fn try_get<T: Send + Sync + 'static>(&self) -> Option<&T>;

    /// Write a file output. CocoIndex tracks it for incremental updates.
    pub fn write_file(&self, path: impl AsRef<Path>, content: &[u8]) -> Result<()>;
}
```

### `FileEntry`

Returned by `fs::walk()`. Lazy content, eager fingerprint — the fingerprint is used as a memo key so unchanged files skip processing entirely:

```rust
impl FileEntry {
    pub fn path(&self) -> PathBuf;
    pub fn relative_path(&self) -> &Path;
    pub fn stem(&self) -> &str;
    pub fn fingerprint(&self) -> impl Serialize;
    pub fn content(&self) -> Result<Vec<u8>>;
    pub fn content_str(&self) -> Result<String>;
}
```

```rust
let files = cocoindex::fs::walk(&dir, &["**/*.rs", "**/*.py"])?;
```

### `RunStats`

```rust
pub struct RunStats {
    pub processed: u64,
    pub skipped: u64,
    pub written: u64,
    pub deleted: u64,
    pub elapsed: Duration,
}
```

### `#[cocoindex::function]`

Mark a pipeline function. Emits a compile-time code hash constant (`__COCO_FN_HASH_<NAME>`) for automatic cache invalidation when the function body changes:

```rust
#[cocoindex::function]
async fn process(ctx: &Ctx, file: &FileEntry) -> Result<String> {
    // function body — code hash is computed at compile time
    Ok(file.content_str()?)
}
```

The constant can be included in manual `ctx.memo()` / `ctx.batch()` keys so that changing the function body automatically invalidates the cache — even when the function uses non-serializable resources:

```rust
#[cocoindex::function]
async fn analyze(ctx: &Ctx, file: &FileEntry) -> Result<Info> {
    let client = ctx.get_or_err::<Client>()?.clone(); // non-serializable — not in key
    let content = file.content_str()?;
    ctx.memo(&(__COCO_FN_HASH_ANALYZE, file.fingerprint()), move || async move {
        client.call(&content).await // only serializable data in the key
    }).await
}
```

### `#[cocoindex::function(memo)]`

Memoize a function by its arguments. The first `&Ctx` parameter is recognized automatically; remaining parameters become the cache key. The code hash is prepended to the key, so changing the function body automatically invalidates the cache.

Best for pure computations where all parameters are `Serialize`:

```rust
#[cocoindex::function(memo)]
async fn extract_pub_fns(ctx: &Ctx, file: &FileEntry) -> Result<Vec<String>> {
    Ok(file.content_str()?.lines()
        .filter(|l| l.trim_start().starts_with("pub fn "))
        .map(|l| l.trim().to_string())
        .collect())
}
```

Conceptually similar to:

```rust
pub const __COCO_FN_HASH_EXTRACT_PUB_FNS: u64 = 0x...;

async fn extract_pub_fns(ctx: &Ctx, file: &FileEntry) -> Result<Vec<String>> {
    let __coco_key = build_fingerprint(__COCO_FN_HASH_EXTRACT_PUB_FNS, file);
    cached_by_fingerprint(ctx, __coco_key, {
        let file = file.clone();
        move || async move { /* original body */ }
    }).await
}
```

Optional `version` parameter for manual cache busting:

```rust
#[cocoindex::function(memo, version = 2)]
async fn analyze(ctx: &Ctx, file: &FileEntry) -> Result<Info> { ... }
```

### `#[cocoindex::function(batching)]`

Batch processing without caching. The first non-ctx parameter is the items collection. The function body receives **all** items every time — no per-item cache probing.

Use this when you always want to reprocess all items (e.g., a batch API call where caching is handled externally, or when results depend on the full set of items).

```rust
#[cocoindex::function(batching)]
async fn embed_all(ctx: &Ctx, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
    let client = ctx.get::<EmbeddingClient>().clone();
    // Always processes all texts — no caching
    client.embed_batch(&texts).await
}
```

### `#[cocoindex::function(memo, batching)]`

Batch processing **with** per-item memoization. The first non-ctx parameter is the items collection. The macro wraps the function body in `ctx.batch()` — cache hits return stored values, cache misses are collected and passed to the body as a single batch.

The code hash is included in each item's cache key, so changing the function body invalidates all cached results.

The function body **can** access `ctx` (e.g., `ctx.get::<T>()` for non-serializable resources).

```rust
#[cocoindex::function(memo, batching)]
async fn extract(ctx: &Ctx, files: Vec<FileEntry>) -> Result<Vec<Info>> {
    let client = ctx.get::<Client>().clone();
    // `files` here is only the cache misses
    let mut results = Vec::new();
    for file in &files {
        results.push(client.analyze(&file.content_str()?).await?);
    }
    Ok(results)
}
```

Conceptually similar to:

```rust
pub const __COCO_FN_HASH_EXTRACT: u64 = 0x...;

async fn extract(ctx: &Ctx, files: Vec<FileEntry>) -> Result<Vec<Info>> {
    batch_by_fingerprint(
        files,
        |__coco_item| build_fingerprint(__COCO_FN_HASH_EXTRACT, __coco_item),
        move |files| async move { /* original body */ }
    ).await
}
```

Extra parameters beyond the items collection are cloned into the closure and included in the per-item cache key:

```rust
#[cocoindex::function(memo, batching)]
async fn extract(ctx: &Ctx, files: Vec<FileEntry>, model: String) -> Result<Vec<Info>> {
    // `model` is included in each item's key: (hash, &model, item.clone())
    // ...
}
```

**Additional context**

### Example: Multi-Codebase Summarization

The current checked-in example is `rust/sdk/examples/multi-codebase-summarization`. It scans project subdirectories, memoizes per-file extraction, memoizes per-project aggregation, writes markdown summaries, and removes stale outputs when a project disappears.

Key patterns demonstrated:
- **module-level `OnceLock<LlmClient>`** — shared `reqwest`-based LLM client usable from `#[cocoindex::function(memo)]` bodies
- **`#[cocoindex::function(memo)]`** — per-file extraction cached by file fingerprint
- **`#[cocoindex::function(memo)]`** — project aggregation cached until the input file summaries change
- **`ctx.mount_each()`** — concurrent per-file extraction within each project
- **`ctx.write_file()` + stale-output cleanup** — incremental markdown writes plus removal of deleted-project outputs

```toml
# Cargo.toml
[dependencies]
cocoindex = { path = "../../cocoindex" }
dotenvy = "0.15"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

```rust
use cocoindex::prelude::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::OnceLock;

struct LlmClient { /* fields omitted */ }
static LLM: OnceLock<LlmClient> = OnceLock::new();

fn init_llm() { /* initialize reqwest client from env */ }
fn llm() -> &'static LlmClient { LLM.get().unwrap() }

#[cocoindex::function(memo)]
async fn extract_file_info(_ctx: &Ctx, file: &FileEntry) -> Result<CodebaseInfo> {
    let content = file.content_str()?;
    let file_path = file.key();
    llm().extract(/* prompt using file_path + content */).await
}

#[cocoindex::function(memo)]
async fn aggregate_project_info(
    _ctx: &Ctx,
    project_name: String,
    file_infos: Vec<CodebaseInfo>,
) -> Result<CodebaseInfo> {
    if file_infos.len() <= 1 {
        /* shortcut small projects */
    }
    llm().extract(/* aggregation prompt */).await
}

#[tokio::main]
async fn main() -> cocoindex::Result<()> {
    init_llm();
    let app = cocoindex::App::open("multi_codebase_summarization", ".cocoindex_db")?;

    let stats = app
        .run(|ctx| async move {
            let mut active_projects = HashSet::new();
            for entry in std::fs::read_dir(&root_dir)? {
                let project_name = entry?.file_name().to_string_lossy().to_string();
                let project_dir = entry?.path();
                let files = cocoindex::fs::walk(&project_dir, &["*.py", "**/*.py"])?;

                let file_infos = ctx
                    .mount_each(
                        files,
                        |f| format!("{project_name}/{}", f.key()),
                        |child_ctx, file| async move { extract_file_info(&child_ctx, &file).await },
                    )
                    .await?;

                let project_info =
                    aggregate_project_info(&ctx, project_name.clone(), file_infos.clone()).await?;
                let markdown =
                    generate_markdown(&ctx, project_name.clone(), project_info, file_infos).await?;
                ctx.write_file(output_dir.join(format!("{project_name}.md")), markdown.as_bytes())?;
                active_projects.insert(project_name);
            }

            cleanup_stale_outputs(&output_dir, &active_projects)?;
            Ok(())
        })
        .await?;

    println!("{stats}");
    Ok(())
}
```

**What's happening:**

| Run | Behavior |
|-----|----------|
| First | All Python files are cache misses. Full extraction + aggregation cost. |
| Second (no changes) | Every file fingerprint and project summary hits cache. Zero LLM calls. |
| After editing one `*.py` file | Only that file is re-extracted, and only that project's aggregation reruns. |
| After deleting a project directory | The stale `output/<project>.md` file is removed on the next run. |

---
Contributors, please refer to [Contributing Guide](https://cocoindex.io/docs/about/contributing).
Unless the PR can be sent immediately (e.g. just a few lines of code), we recommend you to leave a comment on the issue like **`I'm working on it`** or **`Can I work on this issue?`** to avoid duplicating work. Our [Discord server](https://discord.com/invite/zpA9S2DR7s) is always open and friendly.
