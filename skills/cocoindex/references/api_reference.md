# CocoIndex API Reference

Quick reference for the most commonly used CocoIndex APIs.

## Import Convention

```python
import cocoindex as coco
```

All APIs live directly on the `coco` module. There is no separate `cocoindex.asyncio` module.

---

## `@coco.fn` Decorator

Mark a function as a CocoIndex processing function.

```python
@coco.fn
async def my_function(arg1: str) -> None: ...

@coco.fn(memo=True, version=1)
async def expensive_fn(data: str) -> Result: ...

# Force async interface for a sync function (useful for batching)
@coco.fn.as_async(memo=True, batching=True)
def batch_embed(texts: list[str]) -> list[NDArray]: ...
```

**Parameters:**
- `memo: bool = False` -- Enable memoization (skip if inputs/code unchanged)
- `version: int | None = None` -- Explicit version bump to force re-execution
- `logic_tracking: "full" | "self" | None = "full"` -- How code changes are tracked
- `batching: bool = False` -- Auto-batch concurrent calls (async only)
- `max_batch_size: int | None = None` -- Max batch size
- `runner: Runner | None = None` -- e.g. `coco.GPU` for serialized GPU execution

---

## Mount APIs (all async)

All mount APIs accept an optional `ComponentSubpath` as their first argument. When omitted, the subpath is auto-derived from `Symbol(fn.__name__)`. Provide an explicit subpath when mounting the same function multiple times, using multi-part paths, or needing a specific path name.

### `coco.mount()`

Mount a processing component in the background.

```python
# Subpath auto-derived from fn.__name__
handle = await coco.mount(processor_fn, *args, **kwargs)

# Explicit subpath
handle = await coco.mount(coco.component_subpath("name"), processor_fn, *args, **kwargs)

await handle.ready()  # Optional: wait until component finishes
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `processor_fn` -- Function (or LiveComponent class) to run.
- `*args, **kwargs` -- Arguments passed to the function.

**Returns:** `ComponentMountHandle`

### `coco.use_mount()`

Mount a dependent component and return its result. Parent depends on the child.

```python
# Subpath auto-derived from fn.__name__
result = await coco.use_mount(init_fn, *args, **kwargs)

# Explicit subpath
result = await coco.use_mount(coco.component_subpath("setup"), init_fn, *args, **kwargs)
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `processor_fn` -- Function to run.
- `*args, **kwargs` -- Arguments passed to the function.

**Returns:** The return value of `processor_fn`.

### `coco.mount_each()`

Mount one component per item in a keyed iterable. Preferred for processing lists.

```python
# Subpath auto-derived from fn.__name__
await coco.mount_each(process_file, files.items(), *extra_args)

# Explicit subpath
await coco.mount_each(coco.component_subpath("process"), process_file, files.items(), table)
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `fn` -- Function to run per item. Item value is passed as first argument.
- `items` -- Keyed iterable of `(StableKey, T)` pairs, or a `LiveMapFeed` for live mode.
- `*args, **kwargs` -- Additional arguments passed to `fn` after the item.

**Returns:** `ComponentMountHandle`

### `coco.mount_target()`

Mount a target state, ensuring the container is applied before returning the child provider.

```python
provider = await coco.mount_target(target_state)
```

Prefer connector convenience methods (`postgres.mount_table_target()`, etc.) which call this internally.

### `coco.map()`

Run a function concurrently on each item. No processing components are created -- pure concurrent execution within the current component.

```python
results = await coco.map(process_chunk, chunks, *extra_args)
```

**Parameters:**
- `fn` -- Async function to apply. Item is passed as first argument.
- `items` -- Iterable or async iterable.
- `*args, **kwargs` -- Additional arguments passed to `fn` after the item.

**Returns:** `list[T]`

### `coco.component_subpath()`

Create a stable component path for mounting.

```python
coco.component_subpath("setup")
coco.component_subpath("file", str(file_path))
coco.component_subpath("record", record.id)

# Chaining with /
subpath = coco.component_subpath("a") / "b" / "c"

# Context manager form (applies to all nested mount calls)
with coco.component_subpath("process"):
    for f in files:
        await coco.mount(coco.component_subpath(str(f.path)), process_file, f)
```

**StableKey types:** `str | int | bool | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]`

---

## Context System

### `coco.ContextKey[T]`

Type-safe key for sharing resources. The `key` string is the stable identity across runs.

```python
PG_DB = coco.ContextKey[asyncpg.Pool]("pg_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")
```

- `detect_change=True` -- Opt in to auto-invalidate dependent memos when value changes (models, configs)
- `detect_change=False` (default) -- For resources not affecting computation (DB connections, loggers)

### `builder.provide()`

Register a resource in context (used in lifespan).

```python
builder.provide(PG_DB, pool)
builder.provide_with(KEY, context_manager)         # Sync CM
await builder.provide_async_with(KEY, async_cm)    # Async CM
```

### `coco.use_context()`

Retrieve a resource from context inside a processing function.

```python
pool = coco.use_context(PG_DB)
embedder = coco.use_context(EMBEDDER)
```

---

## Lifespan

### `@coco.lifespan`

Define environment setup/teardown. Registered to the default environment.

```python
@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(MODEL))
        yield
```

Can also be sync:

```python
@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./my.db")
    yield
```

---

## App

### `coco.App`

```python
app = coco.App(
    coco.AppConfig(name="MyApp"),
    main_fn,
    **params,
)

# Async
await app.update()
handle = app.update(live=True)  # Live mode
await app.drop()

# Sync (blocking)
app.update_blocking(report_to_stdout=True)
app.drop_blocking()
```

### `coco.AppConfig`

```python
coco.AppConfig(
    name="MyApp",                    # Required
    environment=env,                 # Optional: custom Environment
    max_inflight_components=1024,    # Optional: concurrency limit
)
```

### Start/Stop (for programmatic usage)

```python
# Context manager (preferred)
async with coco.runtime():
    await app.update()

# Or manually
await coco.start()
try:
    await app.update()
finally:
    await coco.stop()

# Sync variants
with coco.runtime():
    app.update_blocking()
```

---

## Exception Handlers

### Global (in lifespan)

```python
builder.set_exception_handler(my_handler)
```

### Scoped (in processing functions)

```python
async with coco.exception_handler(my_handler):
    await coco.mount_each(process_file, files.items(), table)
```

### Handler Signature

```python
async def my_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    logger.error(f"Error in {ctx.stable_path}: {exc}")
```

`ExceptionContext` provides: `env_name`, `stable_path`, `processor_name`, `mount_kind`, `parent_stable_path`, `is_background`, `source`, `original_exception`.

---

## Live Mode

Run the pipeline continuously, streaming changes from sources:

```python
# CLI
cocoindex update main.py -L

# Programmatic
handle = app.update(live=True)
async for snapshot in handle.watch():
    print(snapshot.stats)
```

Sources that support live mode:
- `localfs.walk_dir(..., live=True)` -- File watching
- `kafka.topic_as_map(consumer, topics)` -- Kafka topic consumption

---

## CLI Commands

```bash
cocoindex init [PROJECT_NAME]              # Create new project
cocoindex update APP_TARGET                # Run app once
cocoindex update APP_TARGET -L             # Run in live mode
cocoindex update APP_TARGET --full-reprocess  # Force reprocess all
cocoindex update APP_TARGET --reset        # Reset state before running
cocoindex drop APP_TARGET [-f]             # Drop app and all state
cocoindex ls [APP_TARGET] [--db PATH]      # List apps
cocoindex show APP_TARGET [--tree]         # Show component paths
```

**APP_TARGET format:** `main.py`, `main.py:app_name`, `my_module:app_name`

---

## Text Operations

**Import:** `from cocoindex.ops.text import ...`

### `detect_code_language()`

```python
language = detect_code_language(filename="example.py")  # -> "python"
```

### `RecursiveSplitter`

```python
splitter = RecursiveSplitter()
chunks = splitter.split(
    text,
    chunk_size=1000,
    chunk_overlap=200,
    min_chunk_size=300,
    language="python",  # Syntax-aware splitting
)
# Each chunk: Chunk(text=..., start=TextPosition(...), end=TextPosition(...))
```

### `SeparatorSplitter`

```python
splitter = SeparatorSplitter(separators_regex=[r"\n\n", r"\n"])
chunks = splitter.split(text)
```

---

## Embedding Operations

**Import:** `from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder`

```python
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
embedding = await embedder.embed(text)  # -> NDArray (float32)
```

**As VectorSchemaProvider:**

```python
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]  # Auto-infer dimensions
```

---

## File Resources

**Import:** `from cocoindex.resources.file import ...`

### `FileLike` (async file object)

```python
text = await file.read_text()
data = await file.read()  # bytes, lazy/cached
fp = await file.content_fingerprint()
```

### `PatternFilePathMatcher`

```python
matcher = PatternFilePathMatcher(
    included_patterns=["**/*.py", "**/*.md"],
    excluded_patterns=[".*/**", "__pycache__/**"],
)
```

---

## ID Generation

**Import:** `from cocoindex.resources.id import ...`

```python
# Deterministic: same dep -> same ID
chunk_id = await generate_id(chunk.content)
chunk_uuid = generate_uuid(chunk.content)

# Distinct per call (even with same dep)
id_gen = IdGenerator()
chunk_id = await id_gen.next_id(chunk.content)

uuid_gen = UuidGenerator()
chunk_uuid = uuid_gen.next_uuid(chunk.content)
```

---

## Vector Schema

**Import:** `from cocoindex.resources.schema import VectorSchema`

```python
# Via ContextKey (preferred -- auto-infer from embedder)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]

# Explicit dimensions
schema = VectorSchema(dtype=np.float32, size=384)

@dataclass
class Record:
    vector: Annotated[NDArray, schema]
```

---

## See Also

- [Connectors Reference](./connectors.md) -- Database and system connectors
- [Patterns Reference](./patterns.md) -- Common pipeline patterns
- [Setup Project](./setup_project.md) -- Project setup guide
- [Setup Database](./setup_database.md) -- Database setup guide
