---
title: Unpickle Safety
description: How CocoIndex restricts deserialization to prevent code execution, and how to register your types.
---

# Unpickle Safety

## Why this matters

CocoIndex uses Python's `pickle` internally to serialize and deserialize data — for example, cached return values of [memoized functions](../programming_guide/function.md) and [target state](../programming_guide/target_state.md) tracking records.

By default, `pickle.loads()` can instantiate **any** Python class, which means a crafted pickle payload could execute arbitrary code (e.g., `os.system("rm -rf /")`) during deserialization.

To prevent this, CocoIndex uses a **restricted unpickler** that only allows deserialization into an explicit allow-list of types. Any type not on the list is rejected with an error.

## What's already allowed

You don't need to do anything for the following types — they're pre-registered automatically:

| Category | Types |
|----------|-------|
| **Python builtins** | `bool`, `int`, `float`, `complex`, `str`, `bytes`, `bytearray`, `list`, `tuple`, `dict`, `set`, `frozenset`, `None` |
| **Paths** | `pathlib.Path`, `pathlib.PurePath`, `pathlib.PosixPath`, `pathlib.PurePosixPath`, `pathlib.PureWindowsPath` |
| **Date/time** | `datetime.datetime`, `datetime.date`, `datetime.time`, `datetime.timedelta`, `datetime.timezone` |
| **Other stdlib** | `uuid.UUID` |
| **NumPy** | `numpy.ndarray`, `numpy.dtype` (when numpy is installed) |
| **CocoIndex internals** | All built-in connector and resource types |

## When you need `@coco.unpickle_safe`

If you define a **custom type** that gets serialized by CocoIndex, you must register it. The most common case is a custom type returned by a memoized function:

```python
import cocoindex as coco
from dataclasses import dataclass

@coco.unpickle_safe
@dataclass
class Summary:
    title: str
    word_count: int

@coco.fn(memo=True)
async def summarize(text: str) -> Summary:
    # ... expensive computation ...
    return Summary(title="Example", word_count=len(text.split()))
```

Without `@coco.unpickle_safe`, CocoIndex can serialize `Summary` into the cache, but will reject it when trying to deserialize the cached result on a subsequent run.

:::tip When is registration needed?
If your custom type is **returned by a memoized function** or **used as a target state value**, it needs `@coco.unpickle_safe`. If it's only used as an input argument or within a non-memoized function, registration is not required.
:::

## Usage

### Decorator for your own types

Apply `@coco.unpickle_safe` to any class — dataclasses, NamedTuples, or regular classes:

```python
import cocoindex as coco
from dataclasses import dataclass
from typing import NamedTuple

@coco.unpickle_safe
@dataclass
class ChunkMetadata:
    source: str
    page: int

@coco.unpickle_safe
class SearchResult(NamedTuple):
    score: float
    text: str
```

### Registering third-party types

For types from third-party libraries that you don't control, use `coco.add_unpickle_safe_global()`:

```python
import cocoindex as coco
from some_library import SomeType

coco.add_unpickle_safe_global(
    SomeType.__module__,
    SomeType.__qualname__,
    SomeType,
)
```

## Troubleshooting

### `UnpicklingError: Forbidden global during unpickling`

```
_pickle.UnpicklingError: Forbidden global during unpickling: myapp.models.Summary
```

This means CocoIndex tried to deserialize a `Summary` object but the type is not registered. The fix is to add `@coco.unpickle_safe` to the class definition:

```python
@coco.unpickle_safe  # Add this
@dataclass
class Summary:
    title: str
    word_count: int
```

:::note Existing cached data
If you see this error after adding the restricted unpickler to an existing project, it means previously cached data references types that aren't yet registered. Adding `@coco.unpickle_safe` to the type and re-running will fix it — the stale cache entry is skipped and recomputed.
:::
