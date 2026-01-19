---
title: Local File System
toc_max_heading_level: 4
description: CocoIndex connector for reading from and writing to the local file system.
---

# Local File System

The `localfs` connector provides utilities for reading files from and writing files to the local file system.

```python
from cocoindex.connectors import localfs
```

## As Source

Use `walk_dir()` to iterate over files in a directory. It returns a `DirWalker` that supports both synchronous and asynchronous iteration.

```python
def walk_dir(
    path: str | Path,
    *,
    recursive: bool = False,
    path_matcher: FilePathMatcher | None = None,
) -> DirWalker
```

**Parameters:**

- `path` — The root directory path to walk through.
- `recursive` — If `True`, recursively walk subdirectories.
- `path_matcher` — Optional filter for files and directories. See [PatternFilePathMatcher](../resource_types.md#patternfilepathmatcher).

**Returns:** A `DirWalker` that can be used with both `for` and `async for` loops.

### Synchronous Iteration

Sync iteration yields `File` objects implementing the [`FileLike`](../resource_types.md#filelike--asyncfilelike) protocol:

```python
from cocoindex.connectors import localfs

for file in localfs.walk_dir("/path/to/documents", recursive=True):
    text = file.read_text()
    ...
```

### Asynchronous Iteration

Async iteration yields `AsyncFile` objects implementing the [`AsyncFileLike`](../resource_types.md#filelike--asyncfilelike) protocol:

```python
from cocoindex.connectors import localfs

async for file in localfs.walk_dir("/path/to/documents", recursive=True):
    text = await file.read_text()
    ...
```

The async variant runs file I/O in a thread pool, keeping the event loop responsive.

### Filtering Files

Use `PatternFilePathMatcher` to filter which files and directories are included:

```python
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

# Include only .py and .md files, exclude hidden directories and test files
matcher = PatternFilePathMatcher(
    included_patterns=["*.py", "*.md"],
    excluded_patterns=["**/.*", "**/test_*", "**/__pycache__/**"],
)

for file in localfs.walk_dir("/path/to/project", recursive=True, path_matcher=matcher):
    process(file)
```

### Example

```python
import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    matcher = PatternFilePathMatcher(included_patterns=["*.md"])

    for file in localfs.walk_dir(sourcedir, recursive=True, path_matcher=matcher):
        coco.mount(process_file, scope / "file" / str(file.relative_path), file)

@coco.function(memo=True)
def process_file(scope: coco.Scope, file: localfs.File) -> None:
    text = file.read_text()
    # ... process the file content ...
```

## As Target (Effects)

The `localfs` connector provides effect-based APIs for writing files. Effects ensure that CocoIndex tracks what files should exist and automatically handles creation, updates, and deletion.

### Effect Hierarchy

File writing follows a two-level effect hierarchy:

- **Parent effect:** Directory exists — declared via `declare_dir_target()`
- **Child effects:** Files in the directory — declared via `DirTarget.declare_file()`

```
output/                      ← Parent effect (directory)
├── intro.html               ← Child effect (file)
├── chapter1.html            ← Child effect (file)
└── chapter2.html            ← Child effect (file)
```

The directory effect must be declared and resolved before files can be declared within it.

### declare_dir_target

Declares a directory as an effect target. Returns a `DirTarget` for declaring files.

```python
@coco.function
def declare_dir_target(
    scope: coco.Scope,
    path: pathlib.Path,
    *,
    stable_key: coco.StableKey | None = None,
    managed_by: Literal["system", "user"] = "system",
) -> DirTarget[coco.PendingS]
```

**Parameters:**

- `path` — The filesystem path for the directory.
- `stable_key` — Optional stable key for identifying the directory across path changes.
- `managed_by` — Whether CocoIndex manages the directory lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `DirTarget`. Use `mount_run(...).result()` to wait for resolution.

### DirTarget.declare_file

Declares a file to be written within the directory.

```python
def declare_file(
    self,
    scope: coco.Scope,
    *,
    filename: str,
    content: bytes | str,
) -> None
```

**Parameters:**

- `filename` — The name of the file (relative to the directory).
- `content` — The file content (bytes or str).
