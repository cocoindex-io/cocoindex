---
title: Memoization Keys & States
description: Customize how CocoIndex identifies and validates memoized function inputs using keys and state validation.
---

# Memoization Keys & States

When a CocoIndex function has `memo=True`, the engine caches results and skips re-execution when the inputs haven't changed. This page explains how to customize two aspects of that process:

- **Memoization keys** — how CocoIndex fingerprints your inputs to find a cached result.
- **Memo states** — how CocoIndex validates that a cached result is still fresh, *after* a fingerprint match.

## How memoization works

For each argument value, CocoIndex derives a "key fragment" with this precedence:

1. If the object implements **`__coco_memo_key__()`**, CocoIndex uses its return value.
2. Otherwise, if you registered a **memo key function** for the object's type, CocoIndex uses that.
3. Otherwise, CocoIndex falls back to structural canonicalization for a limited set of primitives/containers.

The key fragments are combined into a deterministic fingerprint. If the fingerprint matches a cached entry, the cached result is reused — unless **memo states** indicate it's stale (see [Memo state validation](#memo-state-validation) below).

## Customizing the memoization key

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
### Define `__coco_memo_key__` (when you control the type)

Implement a method on your class that returns a stable, deterministic value:

```python
class MyType:
    def __coco_memo_key__(self) -> object:
        # Return small primitives / tuples.
        return (...)
```

Return something that uniquely identifies the **semantic content** your function depends on:

- **Good**: small tuples of primitives, e.g. `(stable_id, version)`
- **Bad**: memory addresses, unstable UUIDs, open file handles, `datetime.now()`, or large raw payloads

**Example — DB row:**

```python
class UserRow:
    def __init__(self, user_id: int, updated_at: int) -> None:
        self.user_id = user_id
        self.updated_at = updated_at

    def __coco_memo_key__(self) -> object:
        return ("users", self.user_id, self.updated_at)
```

### Register a key function (when you don't control the type)

If you can't add `__coco_memo_key__` (stdlib / third-party types), register a handler:

```python
from pathlib import Path
from cocoindex import register_memo_key_function

def path_key(p: Path) -> object:
    p = p.resolve()
    st = p.stat()
    return (str(p), st.st_mtime_ns, st.st_size)

register_memo_key_function(Path, path_key)
```

- Registration is **MRO-aware**: if you register both a base class and a subclass, the **most specific** match wins.
- Your key function must return the same kinds of stable objects as `__coco_memo_key__` (small primitives/tuples).

## Memo state validation

Sometimes fingerprint matching alone isn't enough to decide whether a cached result is valid. For example:

- **Multi-level validation**: for files, check the modified time first (cheap), and only read the file for a content fingerprint when the time doesn't match.
- **Async validation**: for an S3 object, send a HEAD request to check freshness — an inherently async operation.
- **Stateful validation**: for HTTP resources, store the last fetch time and use `If-Modified-Since` on the next run.

Memo state validation addresses these by letting you attach a **state function** to your objects. It runs *after* a fingerprint match, giving you a chance to check freshness before the cached result is reused.

### How it works

When CocoIndex finds a fingerprint match, it calls each state function with the stored state from the previous run:

1. **First run** (no previous state): `prev_state` is `coco.NON_EXISTENCE`. Use `coco.is_non_existence(prev_state)` to detect this.
2. **Subsequent runs**: `prev_state` is whatever you returned last time.

Your state function returns a `coco.MemoStateOutcome(state=..., memo_valid=...)`:

- **`state`** — the current state value. CocoIndex stores it for the next run.
- **`memo_valid`** (`bool`) — whether the cached result is still valid.

This decouples "has the state changed?" from "can we reuse the memo?":

- `MemoStateOutcome(state=same_state, memo_valid=True)` → nothing changed, cached result reused, no state update needed.
- `MemoStateOutcome(state=new_state, memo_valid=True)` → state changed but cached result is still valid (e.g. mtime changed but content hash unchanged). The new state is persisted so the next run uses the updated state.
- `MemoStateOutcome(state=new_state, memo_valid=False)` → something changed that invalidates the cache. Function re-executes, new state is stored.

### Define `__coco_memo_state__` (when you control the type)

Add a `__coco_memo_state__` method alongside `__coco_memo_key__`:

```python
import os
import hashlib
from pathlib import Path
import cocoindex as coco

class LocalFile:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __coco_memo_key__(self) -> object:
        # Identity only — which file is it?
        return str(self.path.resolve())

    def __coco_memo_state__(self, prev_state: object) -> coco.MemoStateOutcome:
        st = os.stat(self.path)
        new_mtime = st.st_mtime_ns
        if coco.is_non_existence(prev_state):
            # First run — compute initial state
            content_hash = hashlib.sha256(self.path.read_bytes()).hexdigest()
            return coco.MemoStateOutcome(state=(new_mtime, content_hash), memo_valid=True)

        prev_mtime, prev_hash = prev_state
        if new_mtime == prev_mtime:
            # mtime unchanged — definitely reusable, no content read needed
            return coco.MemoStateOutcome(state=prev_state, memo_valid=True)
        # mtime changed — read content and check hash
        content_hash = coco.connectorkits.fingerprint_bytes(self.path.read_bytes())
        return coco.MemoStateOutcome(state=(new_mtime, content_hash), memo_valid=content_hash == prev_hash)
```

:::tip Keys vs states for files
Without state validation, you'd include `mtime` and `size` directly in the memo key:
```python
def __coco_memo_key__(self):
    st = os.stat(self.path)
    return (str(self.path.resolve()), st.st_mtime_ns, st.st_size)
```
This works for simple cases. State validation becomes useful when you need multi-level checks (e.g. check mtime first, then content hash only if it differs), async operations, or stored metadata like ETags. With the `MemoStateOutcome` return, you can update the state (e.g. new mtime) without invalidating the cache when the content hasn't actually changed.
:::

### Register a state function (when you don't control the type)

Pass a `state_fn` keyword argument to `register_memo_key_function`. The state function receives the object as its first argument and `prev_state` as its second:

```python
from pathlib import Path
from cocoindex import register_memo_key_function

def path_key(p: Path) -> object:
    return str(p.resolve())

def path_state(p: Path, prev_state: object) -> coco.MemoStateOutcome:
    st = p.stat()
    new_state = (st.st_mtime_ns, st.st_size)
    memo_valid = not coco.is_non_existence(prev_state) and new_state == prev_state
    return coco.MemoStateOutcome(state=new_state, memo_valid=memo_valid)

register_memo_key_function(Path, path_key, state_fn=path_state)
```

### Async state methods

A state method can return an `Awaitable`. CocoIndex handles this automatically:

- **In an async CocoIndex function**: awaitables from all state methods are gathered concurrently.
- **In a sync CocoIndex function**: if no event loop is running, CocoIndex uses `asyncio.run()`. If a loop is already running, it raises an error — switch to an async function or use `@coco.fn.as_async`.

```python
import cocoindex as coco

class S3Object:
    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key

    def __coco_memo_key__(self) -> object:
        return (self.bucket, self.key)

    async def __coco_memo_state__(self, prev_state: object) -> coco.MemoStateOutcome:
        etag = await self._head_object()
        memo_valid = not coco.is_non_existence(prev_state) and etag == prev_state
        return coco.MemoStateOutcome(state=etag, memo_valid=memo_valid)

    async def _head_object(self) -> str:
        ...  # boto3 / aioboto3 HEAD call
```

## Preventing memoization

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

```python
import cocoindex as coco
from some_library import StatefulGenerator

coco.register_not_memoizable(StatefulGenerator)
```

In either case, attempting to use the type as a memo key raises a clear error.

## Best practices

- **Keep keys small and deterministic**: use identifiers and versions, not full payloads. No `id(obj)`, pointer addresses, or random values.
- **Separate identity from freshness**: put stable identifiers (file path, URL, primary key) in the key. Put freshness checks (mtime, ETag, version) in the state.
- **Use state validation for expensive checks**: if freshness validation is costly (content hashing, network calls), a state function lets you do it only when the fingerprint matches, and only when a cheap pre-check (mtime) fails.
- **Use `MemoStateOutcome(state=new_state, memo_valid=True)` for cheap state updates**: when a cheap property changes (mtime) but the expensive check (content hash) confirms nothing meaningful changed, return `memo_valid=True` while updating the state. This avoids re-executing the function and avoids re-checking the expensive property next time.
- **Mark stateful types as `NotMemoizable`**: prevent subtle bugs from incorrect memoization of types with side effects.
