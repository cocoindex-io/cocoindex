# Function Memoization Keys

CocoIndex memoizes (caches) results using a **stable, deterministic key** derived from CocoIndex function call’s inputs.

## How CocoIndex derives keys

For each argument value, CocoIndex derives a “key fragment” with this precedence:

1. If the object implements **`__coco_memo_key__()`**, CocoIndex uses its return value.
2. Otherwise, if you registered a **memo key function for the object’s type**, CocoIndex uses that.
3. Otherwise, CocoIndex falls back to structural canonicalization for a limited set of primitives/containers.

The final memoization key is a deterministic fingerprint of the full call key.

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

## Best practices

- **Always include freshness for external resources**: e.g. file `(path, mtime_ns, size)`, HTTP `(url, etag)`, DB `(pk, updated_at)`.
- **Keep keys small**: use identifiers + versions, not full payloads.
- **Keys must be deterministic across processes**: no `id(obj)`, no pointer addresses, no random values.
