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

### Workflow Summary

| Change Type | Commands to Run |
|-------------|-----------------|
| Rust code only | `uv run maturin develop && cargo test` |
| Python code only | `uv run mypy && uv run pytest python/` |
| Both Rust and Python | Run all commands from both categories above |

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
│   │   ├── asyncio.py          # Async App and APIs (import cocoindex.asyncio as coco_aio)
│   │   ├── _internal/          # Internal implementation for the core engine
│   │   │   ├── api.py          # Shared API definitions
│   │   │   ├── api_sync.py     # Sync APIs: App, mount, mount_run
│   │   │   ├── api_async.py    # Async APIs: App, mount, mount_run
│   │   │   ├── app.py          # App base implementation
│   │   │   ├── context_keys.py # ContextKey and ContextProvider
│   │   │   ├── environment.py  # Environment and lifespan handling
│   │   │   ├── function.py     # @coco.function decorator implementation
│   │   │   ├── component_ctx.py # ComponentContext and component_subpath
│   │   │   └── target_state.py # Target state implementation
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

**Processing Component** — The unit of execution that owns a set of target states. Created by `mount()` or `mount_run()` at a specific component path. When a component finishes, its target states sync atomically to external systems.

**Component Path** — Stable identifier for a processing component across runs. Created via `coco.component_subpath("process", filename)`. CocoIndex uses component paths to:

* Match components to their previous runs for change detection
* Determine ownership of target states (if a path disappears, its target states are cleaned up)

**Target State** — What you want to exist in an external system (a file, a database row, a table). You *declare* target states; CocoIndex keeps them in sync — creating, updating, or removing as needed.

**Target** — The API object used to declare target states (e.g., `DirTarget`, `TableTarget`). Targets can be nested: a container target state (directory/table) provides a Target for declaring child target states (files/rows).

**Function** — A Python function decorated with `@coco.function`. Use `memo=True` to enable memoization (skip execution when inputs and code are unchanged).

**Context** — React-style provider mechanism for sharing resources. Define keys with `ContextKey[T]`, provide values in lifespan via `builder.provide()`, use in functions via `coco.use_context(key)`.

### Key APIs

```python
# Mounting processing components (subpath first, then function)
coco.mount(coco.component_subpath("name"), fn, *args, **kw)      # child runs independently
coco.mount_run(coco.component_subpath("name"), fn, *args, **kw)  # returns value, creates dependency

# Component subpath composition
subpath = coco.component_subpath("process", filename)  # multiple parts
subpath = coco.component_subpath("a") / "b" / "c"      # chaining with /

# Using component_subpath as context manager (applies to all nested mount calls)
with coco.component_subpath("process"):
    for f in files:
        coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)

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

* `mount()` → `ProcessingUnitMountHandle`: call `wait_until_ready()` to block until target states are synced
* `mount_run()` → `ProcessingUnitMountRunHandle[T]`: call `result()` to get return value (implicitly waits)

### How syncing works

When a processing component finishes, CocoIndex compares its declared target states with those from the previous run at the same component path:

* New target states → create (insert row, create file)
* Changed target states → update
* Missing target states → delete

Changes are applied atomically per component. If a source item is deleted (path no longer mounted), all its target states are cleaned up automatically.

### Example

```python
@coco.function(memo=True)
def process_file(file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(filename=outname, content=html)

@coco.function
def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(
        coco.component_subpath("setup"), localfs.declare_dir_target, outdir
    ).result()

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    with coco.component_subpath("process"):
        for f in files:
            coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)


app = coco.App(
    coco.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./docs"),
    outdir=pathlib.Path("./out"),
)
app.update(report_to_stdout=True)
```

## Code Conventions

### Internal vs External Modules

We distinguish between **internal modules** (under packages with `_` prefix, e.g. `_internal.*`) and **external modules** (which users can directly import).

**External modules** (user-facing, e.g. `cocoindex/ops/sentence_transformers.py`):

* Be strict about not leaking implementation details
* Use `__all__` to explicitly list public exports
* Prefix ALL non-public symbols with `_`, including:
  * Standard library imports: `import threading as _threading`, `import typing as _typing`
  * Third-party imports: `import numpy as _np`, `from numpy.typing import NDArray as _NDArray`
  * Internal package imports: `from cocoindex.resources import schema as _schema`
* Exception: `TYPE_CHECKING` imports for type hints don't need prefixing

**Internal modules** (e.g. `cocoindex/_internal/component_ctx.py`):

* Less strict since users shouldn't import these directly
* Standard library and internal imports don't need underscore prefix
* Only prefix symbols that are truly private to the module itself (e.g. `_context_var` for a module-private ContextVar)

### Testing Guidelines

We prefer end-to-end tests on user-facing APIs, over unit tests on smaller internal functions. With this said, there're cases where unit tests are necessary, e.g. for internal logic with various situations and edge cases, in which case it's usually easier to cover various scenarios with unit tests.
