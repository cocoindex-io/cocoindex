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
uv run dmypy run         # Type check Python code (uses mypy daemon)
uv run pytest python/    # Run Python tests (use after both Rust and Python changes)
```

### Workflow Summary

| Change Type | Commands to Run |
|-------------|-----------------|
| Rust code only | `uv run maturin develop && cargo test` |
| Python code only | `uv run dmypy run && uv run pytest python/` |
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
│   └── cocoindex/              # The legacy version of the core engine. PLEASE IGNORE THIS.
│
├── python/
│   └── cocoindex/              # Python package
│       ├── __init__.py         # Package entry point
│       ├── cli.py              # CLI commands
│       ├── _internal/          # Internal implementation for the core engine
│       │   ├── api.py          # API definitions (sync and async)
│       │   ├── app.py          # App implementation
│       │   ├── datatype.py     # Data type definitions
│       │   ├── effect.py       # Effect implementation
│       │   ├── environment.py  # Environment handling
│       │   ├── function.py     # Function decorator implementation
│       │   └── scope.py        # Scope implementation
│       ├── connectors/         # External system connectors
│       ├── connectorkits/      # Connector building utilities
│       ├── resources/          # Abstractions for various resources (files, tables, chunks, etc.)
│       ├── extras/             # Convenience utilities for various types of data processing, etc.
│       └── tests/              # Python tests
│
├── examples/                   # Example applications
├── docs/                       # Documentation
└── dev/                        # Development utilities
```

## Key Concepts

### Mental model (state-reconcile engine)

* **Declare desired state** (Target States) inside **Processing Components**; the engine **reconciles** external systems to match (create/update/delete/publish).
* **Processing Components are long-lived instances** keyed by a stable path; individual *runs* are ephemeral.
* Composition is **tree-shaped** (parent mounts children); diffs and external Actions are computed **per component** and applied atomically when possible.

### Core nouns

* **Scope**: pure value that identifies a component instance and its place in the tree.
* **Function**: a Python function decorated with `@coco.function` that can be called normally but gains tracking (deps, memoization, tracing).
* **Processing Component**: a mounted instance of a Coco function at a specific **Scope**.
* **Target State**: a **unit of desired external state** (e.g., a table, a table row, a blob, a message). The engine turns diffs into **Actions** (insert/update/delete/publish) to keep targets in sync.
* **App**: bundles a top-level function and arguments; the top-level function is **mounted as the root component** each run.
* **(Reserved) Context**: future React-style provider mechanism (typed keys; provide/use). Do **not** overload "Context" to mean Scope.

### Canonical API shapes (free functions; Scope first)

```python
# Mounting & effects
coco.mount(scope: Scope, fn, *args, **kw) -> ComponentHandle                # no data dependency
coco.mount_run(scope: Scope, fn, *args, **kw) -> ComponentRunHandle[T]      # creates dependency; one up-to-date run
coco.declare_target_state(scope: Scope, effect: Effect) -> None                   # scope-owned external outcome

# Scope composition
child_scope = scope / "setup"
file_scope  = scope / "process" / (kind, arg)
```

**Handles**

* `ComponentHandle` (from `mount`): exposes `ready()` to wait (join) until the child is **FRESH** for the current epoch; **does not** create a parent→child data dependency.
* `ComponentRunHandle[T]` (from `mount_run`): exposes a `result()` method to block on the result of the component, which creates a data dependency.

### Target States → Actions

* "A **Target State** is a unit of desired external state. Users declare Target States; CocoIndex executes **Actions** on external systems to keep them in sync (inserts, updates, deletes, publishes)."
* When a component re-runs, CocoIndex diffs **current run's declared Target States vs previous run's** at the same Scope and applies a **bundled change**.

### Example

```python
@coco.function
def process_file(scope: coco.Scope, file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(scope, filename=outname, content=html)

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(localfs.dir_target, scope / "setup", outdir).result()

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
