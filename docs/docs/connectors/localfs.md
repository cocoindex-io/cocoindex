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

## Stable memoization with FilePath

A key feature of the `localfs` connector is **stable memoization** through `FilePath`. When you move your entire project directory, memoization keys remain stable as long as you use the same `ContextKey` key string for the base directory.

### Using ContextKey for stable base directories

Define a `ContextKey[pathlib.Path]` with a stable string identifier. Provide the actual path in your app's lifespan, then pass the key directly to `walk_dir()`, `declare_dir_target()`, and related functions.

```python
import pathlib
import cocoindex as coco
from cocoindex.connectors import localfs

# Define a stable key (the string "source_dir" is the stable memo identifier)
SOURCE_DIR = coco.ContextKey[pathlib.Path]("source_dir", tracked=False)

@coco.function
async def app_main() -> None:
    async for file in localfs.walk_dir(SOURCE_DIR, recursive=True):
        # file.file_path has a stable memo key based on "source_dir"
        await process(file)

# In your lifespan, provide the actual path:
async def lifespan(builder: coco.EnvBuilder) -> None:
    builder.provide(SOURCE_DIR, pathlib.Path("./data"))
```

When you move your project to a different location, just update the path in `builder.provide()` — memoization keys remain the same because they're based on the stable key string (`"source_dir"`), not the filesystem path.

### FilePath

`FilePath` combines an optional base directory key and a relative path. Passing a `ContextKey[Path]` directly to any localfs function is equivalent to constructing `FilePath(base_dir=key)`.

`FilePath` supports all `pathlib.PurePath` operations:

```python
SOURCE_DIR = coco.ContextKey[pathlib.Path]("source_dir", tracked=False)
source = localfs.FilePath(base_dir=SOURCE_DIR)

# Create paths using the / operator
config_path = source / "config" / "settings.json"

# Access path properties
print(config_path.name)      # "settings.json"
print(config_path.suffix)    # ".json"
print(config_path.parent)    # FilePath pointing to "config/"

# Resolve to absolute path (requires active component context)
abs_path = config_path.resolve()  # pathlib.Path
```

See [FilePath](../resource_types.md#filepath) in Resource Types for full details.

## As source

Use `walk_dir()` to iterate over files in a directory. It returns a `DirWalker` that supports both synchronous and asynchronous iteration.

```python
def walk_dir(
    path: FilePath | Path | ContextKey[Path],
    *,
    recursive: bool = False,
    path_matcher: FilePathMatcher | None = None,
) -> DirWalker
```

**Parameters:**

- `path` — The root directory path to walk through. Can be a `FilePath`, a `pathlib.Path`, or a `ContextKey[Path]` (equivalent to `FilePath(base_dir=path)`).
- `recursive` — If `True`, recursively walk subdirectories.
- `path_matcher` — Optional filter for files and directories. See [PatternFilePathMatcher](../resource_types.md#patternfilepathmatcher).

**Returns:** A `DirWalker` that supports async iteration via `async for`.

### Iterating files

`walk_dir()` returns a `DirWalker` that supports async iteration, yielding `File` objects (implementing the [`FileLike`](../resource_types.md#filelike) base class):

```python
async for file in localfs.walk_dir("/path/to/documents", recursive=True):
    text = await file.read_text()
    ...
```

File I/O runs in a thread pool, keeping the event loop responsive.

### Keyed iteration with `items()`

`DirWalker.items()` yields keyed `(str, File)` pairs, useful for associating each file with a stable string key (its relative path):

```python
async for key, file in localfs.walk_dir("/path/to/dir", recursive=True).items():
    content = await file.read()
```

### Filtering files

Use `PatternFilePathMatcher` to filter which files and directories are included:

```python
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

# Include only .py and .md files, exclude hidden directories and test files
matcher = PatternFilePathMatcher(
    included_patterns=["**/*.py", "**/*.md"],
    excluded_patterns=["**/.*", "**/test_*", "**/__pycache__"],
)

async for file in localfs.walk_dir("/path/to/project", recursive=True, path_matcher=matcher):
    await process(file)
```

### Example

```python
import pathlib
import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

SOURCE_DIR = coco.ContextKey[pathlib.Path]("source_dir", tracked=False)

@coco.fn
async def app_main() -> None:
    matcher = PatternFilePathMatcher(included_patterns=["**/*.md"])

    async for file in localfs.walk_dir(SOURCE_DIR, recursive=True, path_matcher=matcher):
        await coco.mount(
            coco.component_subpath("file", str(file.file_path.path)),
            process_file,
            file,
        )

@coco.fn(memo=True)
async def process_file(file: FileLike) -> None:
    text = await file.read_text()
    # ... process the file content ...
```

## As target

The `localfs` connector provides target state APIs for writing files. CocoIndex tracks what files should exist and automatically handles creation, updates, and deletion.

### declare_file

Declare a single file target. This is the simplest way to write a file.

```python
@coco.fn
def declare_file(
    path: FilePath | Path | ContextKey[Path],
    content: bytes | str,
    *,
    create_parent_dirs: bool = False,
) -> None
```

**Parameters:**

- `path` — The filesystem path for the file. Can be a `FilePath`, a `pathlib.Path`, or a `ContextKey[Path]`.
- `content` — The file content (bytes or str).
- `create_parent_dirs` — If `True`, create parent directories if they don't exist.

**Example:**

```python
OUTPUT_DIR = coco.ContextKey[pathlib.Path]("output_dir", tracked=False)

@coco.fn
def app_main() -> None:
    coco.mount(
        coco.component_subpath("readme"),
        localfs.declare_file,
        localfs.FilePath("readme.txt", base_dir=OUTPUT_DIR),
        content="Hello, world!",
        create_parent_dirs=True,
    )
```

### declare_dir_target

Declare a directory target for writing multiple files. Returns a `DirTarget` for declaring files within.

```python
@coco.fn
def declare_dir_target(
    path: FilePath | Path | ContextKey[Path],
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[coco.PendingS]
```

**Parameters:**

- `path` — The filesystem path for the directory. Can be a `FilePath`, a `pathlib.Path`, or a `ContextKey[Path]`.
- `create_parent_dirs` — If `True`, create parent directories if they don't exist. Defaults to `True`.

**Returns:** A pending `DirTarget`. Use `await coco.mount_target(...)` or the convenience wrapper `await localfs.mount_dir_target(path)` to resolve.

### DirTarget.declare_file

Declares a file to be written within the directory.

```python
def declare_file(
    self,
    filename: str | PurePath,
    content: bytes | str,
    *,
    create_parent_dirs: bool = False,
) -> None
```

**Parameters:**

- `filename` — The name of the file (can include subdirectory path).
- `content` — The file content (bytes or str).
- `create_parent_dirs` — If `True`, create parent directories within the target directory.

### DirTarget.declare_dir_target

Declares a subdirectory target within the directory.

```python
def declare_dir_target(
    self,
    path: str | PurePath,
    *,
    create_parent_dirs: bool = False,
) -> DirTarget[coco.PendingS]
```

**Parameters:**

- `path` — The path of the subdirectory (relative to this directory).
- `create_parent_dirs` — If `True`, create parent directories.

**Returns:** A `DirTarget` for the subdirectory.

### Target example

```python
import pathlib
import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

SOURCE_DIR = coco.ContextKey[pathlib.Path]("source_dir", tracked=False)
OUTPUT_DIR = coco.ContextKey[pathlib.Path]("output_dir", tracked=False)

@coco.fn
async def app_main() -> None:
    # Declare output directory target using context key
    target = await localfs.mount_dir_target(OUTPUT_DIR)

    # Process files and write outputs
    await coco.mount_each(process_file, localfs.walk_dir(SOURCE_DIR, recursive=True).items(), target)

@coco.fn(memo=True)
async def process_file(file: FileLike, target: localfs.DirTarget) -> None:
    # Transform the file
    content = (await file.read_text()).upper()

    # Write to output with same relative path
    target.declare_file(
        filename=file.file_path.path,
        content=content,
        create_parent_dirs=True,
    )
```
