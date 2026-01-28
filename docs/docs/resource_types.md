---
title: Resource Types
description: Common data types for files shared across CocoIndex connectors and utilities.
---

The `cocoindex.resources` package provides common data models and abstractions shared across connectors and utility modules, ensuring a consistent interface for working with data.

## File

The file module (`cocoindex.resources.file`) defines protocols and utilities for working with file-like objects.

### FileLike / AsyncFileLike

`FileLike` is a protocol for file objects with synchronous read access. `AsyncFileLike` is its async counterpart with the same properties but async read methods.

```python
from cocoindex.resources.file import FileLike

def process_file(file: FileLike) -> str:
    text = file.read_text()
    ...
    return text
```

```python
from cocoindex.resources.file import AsyncFileLike

async def process_file_async(file: AsyncFileLike) -> str:
    text = await file.read_text()
    ...
    return text
```

**Properties:**

- `file_path` — A `FilePath` object representing the file's path. Access the relative path via `file_path.path` (`PurePath`).
- `size` — File size in bytes
- `modified_time` — File modification time (`datetime`)

**Methods:**

- `read(size=-1)` — Read file content as bytes. Pass `size` to limit bytes read.
- `read_text(encoding=None, errors="replace")` — Read as text. Auto-detects encoding via BOM if not specified.

**Memoization:**

`FileLike` objects provide a memoization key based on `file_path` and `modified_time`. When used as arguments to a [memoized function](./programming_guide/function.md#memoization), CocoIndex can detect when a file has changed and skip recomputation for unchanged files.

### FilePath

`FilePath` is a base class that combines a **base directory** (with a stable key) and a **relative path**. This enables stable memoization even when the entire directory tree is moved to a different location.

```python
from cocoindex.resources.file import FilePath
```

Each connector provides its own `FilePath` subclass (e.g., `localfs.FilePath`). The base class defines the common interface.

**Properties:**

- `base_dir` — A `KeyedConnection` object that holds the base directory. The `base_dir.key` is used for stable memoization.
- `path` — The path relative to the base directory (`PurePath`).

**Methods:**

- `resolve()` — Resolve to the full path (type depends on the connector, e.g., `pathlib.Path` for local filesystem).

**Path Operations:**

`FilePath` supports most `pathlib.PurePath` operations:

```python
# Join paths with /
config_path = source_dir / "config" / "settings.json"

# Access path properties
config_path.name      # "settings.json"
config_path.stem      # "settings"
config_path.suffix    # ".json"
config_path.parts     # ("config", "settings.json")
config_path.parent    # FilePath pointing to "config/"

# Modify path components
config_path.with_name("other.json")
config_path.with_suffix(".yaml")
config_path.with_stem("config")

# Pattern matching
config_path.match("*.json")  # True

# Convert to POSIX string
config_path.as_posix()  # "config/settings.json"
```

**Memoization:**

`FilePath` provides a memoization key based on `(base_dir.key, path)`. This means:

- Two `FilePath` objects with the same base directory key and relative path have the same memo key
- Moving the entire project directory doesn't invalidate memoization, as long as you re-register with the same key

For connector-specific usage (e.g., `register_base_dir`), see the individual connector documentation like [Local File System](./connectors/localfs.md).

### FilePathMatcher

`FilePathMatcher` is a protocol for filtering files and directories during traversal.

```python
from cocoindex.resources.file import FilePathMatcher

class MyMatcher(FilePathMatcher):
    def is_dir_included(self, path: PurePath) -> bool:
        """Return True to traverse this directory."""
        return not path.name.startswith(".")

    def is_file_included(self, path: PurePath) -> bool:
        """Return True to include this file."""
        return path.suffix in (".py", ".md")
```

#### PatternFilePathMatcher

A built-in `FilePathMatcher` implementation using glob patterns:

```python
from cocoindex.resources.file import PatternFilePathMatcher

# Include only Python and Markdown files, exclude tests and hidden dirs
matcher = PatternFilePathMatcher(
    included_patterns=["*.py", "*.md"],
    excluded_patterns=["**/test_*", "**/.*"],
)
```

**Parameters:**

- `included_patterns` — Glob patterns for files to include. If `None`, all files are included.
- `excluded_patterns` — Glob patterns for files/directories to exclude. Excluded directories are not traversed.
