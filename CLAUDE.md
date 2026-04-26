# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with code in this repository.

## Build and Test Commands

This project uses [uv](https://docs.astral.sh/uv/) for Python project management.

### Building

```bash
uv run maturin develop   # Build Rust code and install Python package (required after Rust changes)
```

### Testing

```bash
cargo test               # Run Rust tests
uv run mypy              # Type check Python code
uv run pytest python/    # Run Python tests (use after both Rust and Python changes)
```

### Code Formatting and Linting

```bash
uv run ruff format .           # Format Python code
uv run ruff format --check .   # Check formatting without making changes (same as CI)
uv run ruff check .            # Lint Python code
```

### Workflow Summary

| Change Type | Commands to Run |
|-------------|-----------------|
| Rust code only | `uv run maturin develop && cargo test` |
| Python code only | `uv run mypy && uv run pytest python/` |
| Both Rust and Python | Run all commands from both categories above |
| Python formatting | `uv run ruff format .` |

## Code Structure

```
cocoindex/
├── rust/                       # Rust crates (workspace)
│   ├── core/                   # Core engine crate
│   │   └── src/
│   │       ├── engine/         # Core engine
│   │       ├── state/          # States of the core engine
│   │       └── inspect/        # Database inspection utilities
│   ├── py/                     # Python bindings (PyO3)
│   ├── py_utils/               # Python-Rust utility helpers (error, convert, future)
│   ├── utils/                  # General utilities: error, batching, fingerprint, etc.
│   └── ops_text/               # Text processing operations (splitter, language detection)
│
├── python/
│   ├── cocoindex/              # Python package
│   │   ├── __init__.py         # Package entry point
│   │   ├── cli.py              # CLI commands
│   │   ├── _internal/          # Internal implementation for the core engine
│   │   │   ├── api.py          # Public API: mount, use_mount, mount_each, map, mount_target, App, fn, start/stop
│   │   │   ├── app.py          # App base implementation
│   │   │   ├── context_keys.py # ContextKey and ContextProvider
│   │   │   ├── environment.py  # Environment and lifespan handling
│   │   │   ├── function.py     # @coco.fn decorator implementation
│   │   │   ├── component_ctx.py # ComponentContext and component_subpath
│   │   │   ├── target_state.py # Target state implementation
│   │   │   └── core.pyi        # Type stubs for the Rust extension module (update when PyO3 APIs change)
│   │   ├── connectors/         # External system connectors (localfs, postgres, qdrant, lancedb, google_drive)
│   │   ├── connectorkits/      # Connector building utilities
│   │   ├── resources/          # Abstractions: file.py (FileLike), chunk.py (Chunk), schema.py
│   │   └── ops/                # Operations: text.py (RecursiveSplitter), sentence_transformers.py
│   └── tests/                  # Python tests
│
├── examples/                   # Example applications
├── docs/                       # Documentation
└── dev/                        # Development utilities
```

## Key Concepts

### Mental model: declarative data pipelines

CocoIndex uses a **declarative** programming model — you specify *what* your output should look like (target states), not *how* to incrementally update it. The engine handles change detection and applies minimal updates automatically.

Think of it like:

* **React**: declare UI as function of state → React re-renders what changed
* **Spreadsheets**: declare formulas → cells recompute when inputs change
* **CocoIndex**: declare target states as function of source → engine syncs what changed

### Core concepts

**App** — The top-level runnable unit. Bundles a main function with its arguments. When you call `app.update()`, the main function runs as the root processing component.

**Processing Component** — The unit of execution that owns a set of target states. Created by `mount()` or `use_mount()` at a specific component path. When a component finishes, its target states sync atomically to external systems.

**Component Path** — Stable identifier for a processing component across runs. Created via `coco.component_subpath("process", filename)`. CocoIndex uses component paths to:

* Match components to their previous runs for change detection
* Determine ownership of target states (if a path disappears, its target states are cleaned up)

**Target State** — What you want to exist in an external system (a file, a database row, a table). You *declare* target states; CocoIndex keeps them in sync — creating, updating, or removing as needed.

**Target** — The API object used to declare target states (e.g., `DirTarget`, `TableTarget`). Targets can be nested: a container target state (directory/table) provides a Target for declaring child target states (files/rows).

**Function** — A Python function decorated with `@coco.fn`. Use `memo=True` to enable memoization (skip execution when inputs and code are unchanged).

**Context** — React-style provider mechanism for sharing resources. Define keys with `ContextKey[T]`, provide values in lifespan via `builder.provide()`, use in functions via `coco.use_context(key)`.

### Key APIs

```python
# Mounting processing components (subpath auto-derived from fn.__name__)
await coco.mount(fn, *args, **kw)                                       # child runs independently
result = await coco.use_mount(fn, *args, **kw)                          # returns value directly

# Explicit subpath (for multi-part paths or multiple mounts of same function)
await coco.mount(coco.component_subpath("process", filename), fn, *args, **kw)
result = await coco.use_mount(coco.component_subpath("name"), fn, *args, **kw)

# Component subpath composition
subpath = coco.component_subpath("process", filename)  # multiple parts
subpath = coco.component_subpath("a") / "b" / "c"      # chaining with /

# Using component_subpath as context manager (applies to all nested mount calls)
with coco.component_subpath("process"):
    for f in files:
        await coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)

# Declaring target states (typically via Target methods)
dir_target.declare_file(filename=name, content=data)
table_target.declare_row(row=MyRow(...))

# Using context values
db = coco.use_context(PG_DB)  # retrieve value provided in lifespan

# Explicit context management (for ThreadPoolExecutor)
ctx = coco.get_component_context()
with ctx.attach():
    # coco APIs work correctly in this thread
    coco.mount(...)
```

**Mount handles:**

* `mount()` → `ComponentMountHandle`: call `await handle.ready()` to wait until target states are synced
* `use_mount()` → returns the result value directly (awaitable)

### How syncing works

When a processing component finishes, CocoIndex compares its declared target states with those from the previous run at the same component path:

* New target states → create (insert row, create file)
* Changed target states → update
* Missing target states → delete

Changes are applied atomically per component. If a source item is deleted (path no longer mounted), all its target states are cleaned up automatically.

### Example

```python
@coco.fn(memo=True)
async def process_file(file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    target.declare_file(filename=outname, content=html)

@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = await coco.use_mount(localfs.declare_dir_target, outdir)

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"])
    )
    await coco.mount_each(process_file, files.items(), target)


app = coco.App(
    coco.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./docs"),
    outdir=pathlib.Path("./out"),
)
app.update_blocking(report_to_stdout=True)
```

## Code Conventions

### Internal vs External Modules

We distinguish between **internal modules** (under packages with `_` prefix, e.g. `_internal.*` or `connectors.*._source`) and **external modules** (which users can directly import).

**External modules** (user-facing, e.g. `cocoindex/ops/sentence_transformers.py`):

* Be strict about not leaking implementation details
* Use `__all__` to explicitly list public exports
* Prefix ALL non-public symbols with `_`, including:
  * Standard library imports: `import threading as _threading`, `import typing as _typing`
  * Third-party imports: `import numpy as _np`, `from numpy.typing import NDArray as _NDArray`
  * Internal package imports: `from cocoindex.resources import schema as _schema`
* Exception: `TYPE_CHECKING` imports for type hints don't need prefixing

**Internal modules** (e.g. `cocoindex/_internal/component_ctx.py`, `**/_target.py`):

* Less strict since users shouldn't import these directly
* Standard library and internal imports don't need underscore prefix
* Only prefix symbols that are truly private to the module itself (e.g. `_context_var` for a module-private ContextVar)

### Type Annotations

Avoid `Any` whenever feasible. Use specific types — including concrete types from third-party libraries. Only use `Any` when the type is truly generic and no downstream code needs to downcast it.

### Prefer stronger types; validate and exchange early

Prefer strongly-typed values over weakly-typed representations (strings, `Any`, raw identifiers). When a value enters the system in a weaker form, validate and convert it to the stronger type at the earliest point where the conversion is possible — don't propagate the weak form further than necessary.

Example: connector handlers receive a `ContextKey` string as part of a target-state key. Rather than storing `_db_key: str` and re-resolving it via `context_provider.get(key_str, ConnType)` on every `_apply_actions` call, the parent handler resolves the key once (when constructing the child handler) and passes the typed connection directly. The child stores `_pool: asyncpg.Pool`, not `_db_key: str`.

### Multi-Value Returns

For functions returning multiple values, use `NamedTuple` instead of plain tuples. At call sites, access fields by name (`result.can_reuse`) rather than positional unpacking — this prevents misreading fields in the wrong order.

### Exceptions are for exceptional situations

Reserve exceptions (Python) and errors (Rust) for truly unexpected failures — not for normal control flow like signaling completion, end-of-iteration, or expected state transitions. When a condition is part of the normal, expected operation of the system, represent it with an explicit return value (e.g., a sentinel value, an enum variant, or `None`) rather than raising/throwing. This keeps exception handling clean and makes it easy to distinguish real errors from routine signals.

### Testing Guidelines

We prefer end-to-end tests on user-facing APIs, over unit tests on smaller internal functions. With this said, there're cases where unit tests are necessary, e.g. for internal logic with various situations and edge cases, in which case it's usually easier to cover various scenarios with unit tests.

#### Test Environment Setup

Use `common.create_test_env(__file__)` to create a CocoIndex `Environment` for tests. It derives a unique `db_path` from the test file path and picks up the current event loop automatically.

* **Sync tests** (module-level creation): Call at module level — the Environment creates a background loop for async callbacks.

  ```python
  coco_env = common.create_test_env(__file__)
  ```

* **Async tests with async resources** (e.g., asyncpg pools): Call inside an async fixture so the Environment binds to the test's running event loop (same loop the pool is on). Use the `suffix` parameter when each test needs its own Environment:

  ```python
  @pytest_asyncio.fixture
  async def pg_env(request: pytest.FixtureRequest) -> Any:
      pool = await asyncpg.create_pool(dsn)
      coco_env = common.create_test_env(__file__, suffix=request.node.name)
      coco_env.context_provider.provide(DB_KEY, pool)
      yield pool, coco_env
      await pool.close()
  ```

  The `suffix` ensures each test gets a unique `db_path`, avoiding "environment already open" errors.

#### Testcontainers for Database Tests

Use `testcontainers[postgres]` (in the `build-test` dependency group) to spin up real database instances automatically — no manual setup or environment variables needed. Use a module-scoped sync fixture for the container and a function-scoped async fixture for per-test resources:

```python
@pytest.fixture(scope="module")
def pg_dsn() -> Any:
    with PostgresContainer("postgres:16-alpine") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        yield dsn

@pytest_asyncio.fixture
async def pool(pg_dsn: str) -> Any:
    p = await asyncpg.create_pool(pg_dsn)
    yield p
    await p.close()
```

### Sync vs Async

The Rust core (`rust/core`, `rust/utils`) uses **async-first** design with Tokio. The `rust/py` crate bridges Rust async to Python, offering both sync and async APIs:

* Rust core exposes async functions
* `rust/py` provides sync wrappers that use `block_on()` to call async Rust from sync Python
* Python's `cocoindex` API is **async-first**: mount APIs (`mount`, `use_mount`, `mount_each`, `map`) are all `async def`; `App.update()`/`App.drop()` are async; sync entry points (`App.update_blocking()`, `App.drop_blocking()`, `start_blocking()`, `stop_blocking()`) are available for CLI and blocking contexts

When adding new functionality that involves I/O or concurrency:

* Implement async in Rust
* Bridge to Python via `rust/py`, providing both sync and async variants if needed

## Versioning

The current codebase is for CocoIndex v1, which is a fundamental redesign from CocoIndex v0. Currently the `v1` branch is the main branch for CocoIndex v1 code.

## Docs diagrams

All diagrams embedded in docs pages are built as inline Astro components under `docs/src/components/diagrams/`, not exported SVG files. See [docs/src/components/diagrams/README.md](docs/src/components/diagrams/README.md) for the palette, primitives, shared CSS classes, animation conventions, and step-by-step authoring guidelines. Follow that guide for any new or updated diagram.
