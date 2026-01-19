---
title: Resource Types
description: Common data types for files shared across CocoIndex connectors and utilities.
---

# Resource Types

The `cocoindex.resources` package provides common data types shared across connectors and utility modules, providing a consistent interface for working with data.

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

- `relative_path` — The relative path of the file (`PurePath`)
- `size` — File size in bytes
- `modified_time` — File modification time (`datetime`)

**Methods:**

- `read(size=-1)` — Read file content as bytes. Pass `size` to limit bytes read.
- `read_text(encoding=None, errors="replace")` — Read as text. Auto-detects encoding via BOM if not specified.

**Memoization:**

`FileLike` objects provide a memoization key based on `relative_path` and `modified_time`. When used as arguments to a [memoized function](./programming_guide/function.md#memoization), CocoIndex can detect when a file has changed and skip recomputation for unchanged files.

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
