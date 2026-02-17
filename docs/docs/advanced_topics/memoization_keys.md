# Function Memoization Keys

CocoIndex memoizes (caches) results using a **stable, deterministic key** derived from CocoIndex function call’s inputs.

## How CocoIndex derives keys

For each argument value, CocoIndex derives a “key fragment” with this precedence:

1. If the object implements **`__coco_memo_key__()`**, CocoIndex uses its return value.
2. Otherwise, if you registered a **memo key function for the object’s type**, CocoIndex uses that.
3. Otherwise, CocoIndex falls back to structural canonicalization for a limited set of primitives/containers.

The final memoization key is a deterministic fingerprint of the full call key.

## Automatic support for dataclasses and Pydantic models

CocoIndex automatically supports Python dataclasses and Pydantic v2 models without requiring `__coco_memo_key__()`:

**Dataclasses**: All fields are included in definition order.

```python
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int

# Works automatically - no __coco_memo_key__ needed
@coco.function(memo=True)
def process_point(p: Point) -> str:
    return f"Point({p.x}, {p.y})"
```

**Pydantic v2 models**: All fields are included (set and unset), preserving field definition order.

```python
from pydantic import BaseModel

class Config(BaseModel):
    name: str
    value: int = 42

# Works automatically - no __coco_memo_key__ needed
@coco.function(memo=True)
def process_config(cfg: Config) -> str:
    return f"Config {cfg.name} = {cfg.value}"
```

### Override automatic handling

If you need custom behavior, implement `__coco_memo_key__()` - it takes precedence over automatic handling:

```python
@dataclass
class Point:
    x: int
    y: int
    transient_data: str  # Don't include in memo key

    def __coco_memo_key__(self) -> object:
        return (self.x, self.y)  # Only x and y matter for memoization
```

## Define `__coco_memo_key__` (preferred when you control the type)

Implement a method on your class:

```python
class MyType:
    def __coco_memo_key__(self) -> object:
        # Must return a stable, deterministic value across processes.
        # Prefer small primitives / tuples.
        return (...)
```

### What should you return?

Return something that uniquely identifies the **semantic content** your function depends on.

- **Good**: small tuples of primitives, e.g. `(stable_id, version)`
- **Bad**: memory addresses, unstable UUIDs, open file handles, timestamps “now”, or large raw payloads

### Examples

**File-like resource (include freshness):**

```python
import os
from pathlib import Path

class LocalFile:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __coco_memo_key__(self) -> object:
        p = self.path.resolve()
        st = os.stat(p)
        return (str(p), st.st_mtime_ns, st.st_size)
```

**DB row (include row version / updated_at):**

```python
class UserRow:
    def __init__(self, user_id: int, updated_at: int) -> None:
        self.user_id = user_id
        self.updated_at = updated_at

    def __coco_memo_key__(self) -> object:
        return ("users", self.user_id, self.updated_at)
```

## Register a key function for a type (when you don’t control the type)

If you can’t add `__coco_memo_key__` (stdlib / third-party types), register a handler:

```python
from cocoindex import register_memo_key_function

def path_key(p) -> object:
    p = p.resolve()
    st = p.stat()
    return (str(p), st.st_mtime_ns, st.st_size)

register_memo_key_function(__import__("pathlib").Path, path_key)
```

Notes:

- Registration is **MRO-aware**: if you register both a base class and a subclass, the **most specific** match wins.
- Your key function must return the same kinds of stable objects as `__coco_memo_key__` (small primitives/tuples).

## Prevent memoization for stateful types

Some types maintain internal state that makes memoization semantically incorrect. For example, a generator that tracks call counts would produce wrong results if memoized.

### Inherit from `NotMemoizable` (when you control the type)

```python
import cocoindex as coco

class MyStatefulGenerator(coco.NotMemoizable):
    def __init__(self) -> None:
        self._counter = 0

    def next_value(self) -> int:
        self._counter += 1
        return self._counter
```

### Register as not memoizable (when you don't control the type)

For third-party types you can't modify:

```python
import cocoindex as coco
from some_library import StatefulGenerator

coco.register_not_memoizable(StatefulGenerator)
```

In either case, attempting to use the type as a memo key raises a clear error.

## Best practices

- **Always include freshness for external resources**: e.g. file `(path, mtime_ns, size)`, HTTP `(url, etag)`, DB `(pk, updated_at)`.
- **Keep keys small**: use identifiers + versions, not full payloads.
- **Keys must be deterministic across processes**: no `id(obj)`, no pointer addresses, no random values.
- **Mark stateful types as `NotMemoizable`**: prevent subtle bugs from incorrect memoization.
