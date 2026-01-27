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
│   └── extra_text/             # Text processing utilities (splitter, language detection)
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
│   │   │   ├── scope.py        # Scope implementation
│   │   │   └── target_state.py # Target state implementation
│   │   ├── connectors/         # External system connectors (localfs, postgres, qdrant, lancedb, google_drive)
│   │   ├── connectorkits/      # Connector building utilities
│   │   ├── resources/          # Abstractions: file.py (FileLike), chunk.py (Chunk), schema.py
│   │   └── extras/             # Utilities: text.py (RecursiveSplitter), sentence_transformers.py
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

**Processing Component** — The unit of execution that owns a set of target states. Created by `mount()` or `mount_run()` at a specific scope. When a component finishes, its target states sync atomically to external systems.

**Scope** — Stable identifier for a processing component across runs (like a path in a tree: `scope / "process" / filename`). CocoIndex uses scopes to:

* Match components to their previous runs for change detection
* Determine ownership of target states (if a scope disappears, its target states are cleaned up)

**Target State** — What you want to exist in an external system (a file, a database row, a table). You *declare* target states; CocoIndex keeps them in sync — creating, updating, or removing as needed.

**Target** — The API object used to declare target states (e.g., `DirTarget`, `TableTarget`). Targets can be nested: a container target state (directory/table) provides a Target for declaring child target states (files/rows).

**Function** — A Python function decorated with `@coco.function`. Use `memo=True` to enable memoization (skip execution when inputs and code are unchanged).

**Context** — React-style provider mechanism for sharing resources. Define keys with `ContextKey[T]`, provide values in lifespan via `builder.provide()`, use in functions via `scope.use()`.

### Key APIs

```python
# Mounting processing components
coco.mount(fn, scope, *args, **kw)      # child runs independently, no data dependency
coco.mount_run(fn, scope, *args, **kw)  # returns value, creates data dependency

# Scope composition (stable identifiers)
child_scope = scope / "setup"
file_scope  = scope / "process" / filename

# Declaring target states (typically via Target methods)
dir_target.declare_file(scope, filename=name, content=data)
table_target.declare_row(scope, row=MyRow(...))
```

**Mount handles:**

* `mount()` → `ProcessingUnitMountHandle`: call `wait_until_ready()` to block until target states are synced
* `mount_run()` → `ProcessingUnitMountRunHandle[T]`: call `result()` to get return value (implicitly waits)

### How syncing works

When a processing component finishes, CocoIndex compares its declared target states with those from the previous run at the same scope:

* New target states → create (insert row, create file)
* Changed target states → update
* Missing target states → delete

Changes are applied atomically per component. If a source item is deleted (scope no longer mounted), all its target states are cleaned up automatically.

### Example

```python
@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(scope, filename=outname, content=html)

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(localfs.declare_dir_target, scope / "setup", outdir).result()

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for f in files:
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, target)


app = coco.App(
    app_main,
    coco.AppConfig(name="FilesTransform"),
    sourcedir=pathlib.Path("./docs"),
    outdir=pathlib.Path("./out"),
)
app.update(report_to_stdout=True)
```

## Principles

* We prefer end-to-end tests on user-facing APIs, over unit tests on smaller internal functions. With this said, there're cases where unit tests are necessary, e.g. for internal logic with various situations and edge cases, in which case it's usually easier to cover various scenarios with unit tests.

## Python Code Conventions

* Avoid leaking internal symbols in public modules. Import modules with underscore prefix and reference their symbols:
  * `import typing as _typing`, then reference as `_typing.Literal`, `_typing.Optional`, etc.
  * `from cocoindex._internal import core as _core`
  * `from cocoindex.resources import chunk as _chunk`
