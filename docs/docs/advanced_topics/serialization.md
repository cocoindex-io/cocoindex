---
title: Serialization
description: How CocoIndex serializes and deserializes Python values, and how to provide type annotations for proper reconstruction.
---

# Serialization

## Overview

CocoIndex serializes Python values in several situations:

- **Memoized function return values** — cached so unchanged functions can skip re-execution.
- **Memo states** — stored between runs for freshness validation (see [Memo state validation](./memoization_keys.md#memo-state-validation)).
- **Tracking records** — stored by target state handlers to detect changes (see [Custom Target Connector](./custom_target_connector.md)).

CocoIndex uses [msgspec](https://jcristharif.com/msgspec/) (msgpack format) as the default serialization engine. Most Python types — dataclasses, NamedTuples, primitives, collections, and Pydantic models — are handled automatically without any special registration.

## Type annotations

CocoIndex uses type annotations to properly reconstruct typed objects during deserialization. Without annotations, values may deserialize as basic Python types (`dict`, `list`, `str`, etc.) instead of their original types.

**Add type annotations in these three places:**

1. **Memoized function return types** — annotate the return type of `@coco.fn(memo=True)` functions.

   ```python
   @coco.fn(memo=True)
   async def process_chunk(chunk: Chunk) -> Embedding:  # return type annotation
       return embed(chunk.text)
   ```

   See [Function — Memoization](../programming_guide/function.md#memoization).

2. **`__coco_memo_state__` `prev_state` parameter** — annotate with the state type you return in `MemoStateOutcome(state=...)`.

   ```python
   def __coco_memo_state__(self, prev_state: tuple[int, str] | coco.NonExistenceType) -> coco.MemoStateOutcome:
       ...
   ```

   See [Memoization Keys & States — Memo state validation](./memoization_keys.md#memo-state-validation).

3. **`reconcile()` `prev_possible_records` parameter** — annotate with `Collection[YourTrackingRecord]`.

   ```python
   def reconcile(
       self,
       key: str,
       desired_state: MyValue | NonExistenceType,
       prev_possible_records: Collection[MyTrackingRecord],  # tracking record type
       prev_may_be_missing: bool,
       /,
   ) -> ...:
   ```

   See [Custom Target Connector — TargetHandler](./custom_target_connector.md#targethandler-you-implement).

## What works automatically

You don't need any special registration for the following types — they are serialized with msgspec by default:

| Category | Types |
|----------|-------|
| **Primitives** | `bool`, `int`, `float`, `str`, `bytes`, `None` |
| **Collections** | `list`, `tuple`, `dict`, `set`, `frozenset` |
| **Dataclasses** | Any `@dataclass` (including frozen) |
| **NamedTuples** | Any `NamedTuple` subclass |
| **Pydantic models** | Any `pydantic.BaseModel` subclass |
| **Date/time** | `datetime.datetime`, `datetime.date`, `datetime.time`, `datetime.timedelta`, `datetime.timezone` |
| **Other stdlib** | `uuid.UUID` |

These types also work when nested inside collections or other dataclasses.

## Types requiring pickle

Some types can't be serialized with msgspec. These are handled with pickle automatically:

| Category | Types |
|----------|-------|
| **Complex numbers** | `complex` |
| **Paths** | `pathlib.Path`, `pathlib.PurePath`, and all subclasses |
| **NumPy** | `numpy.ndarray`, `numpy.dtype` (when numpy is installed) |

### `@coco.serialize_by_pickle`

For your own types that need pickle serialization, use the `@coco.serialize_by_pickle` decorator:

```python
import cocoindex as coco

@coco.serialize_by_pickle
class MySpecialType:
    """A type with custom __reduce__ that msgspec can't handle."""
    def __init__(self, data):
        self.data = data

    def __reduce__(self):
        return (MySpecialType, (self.data,))
```

This decorator:
- Routes serialization through pickle instead of msgspec
- Automatically registers the type as safe to unpickle (equivalent to also applying `@coco.unpickle_safe`)

### `@coco.unpickle_safe` (backward compatibility)

The `@coco.unpickle_safe` decorator only affects deserialization — it adds a type to the restricted unpickle allow-list without changing how the type is serialized. This is kept for backward compatibility with previously cached data.

Most users won't need this. If you previously used `@coco.unpickle_safe` on dataclasses or NamedTuples, you can remove it — msgspec handles these types natively now.

### Registering third-party types

For types from third-party libraries that need pickle serialization, use `coco.serialize_by_pickle()` as a regular function call:

```python
import cocoindex as coco
from some_library import SomeType

coco.serialize_by_pickle(SomeType)
```

## Backward compatibility

Old data serialized with pickle (before this change) is readable indefinitely. Pickle-format data starts with byte `0x80`, which CocoIndex recognizes and routes to the restricted unpickler automatically.

## Troubleshooting

### `DeserializationError: Failed to deserialize msgspec payload`

This usually means the type annotation doesn't match the serialized data. Common causes:

- **Missing return type annotation** on a memoized function — add `-> YourType` to the function signature.
- **Changed type structure** between runs — if you renamed or restructured a dataclass, the cached data won't match. Run with `full_reprocess=True` to rebuild the cache.
- **Forward reference not resolved** — if your type annotation uses a string forward reference, ensure the type is defined before the function is first called.

### `UnpicklingError: Forbidden global during unpickling`

```
_pickle.UnpicklingError: Forbidden global during unpickling: myapp.models.Summary
```

This means CocoIndex tried to deserialize a type via pickle but it's not in the allow-list. Fix by either:

1. Using `@coco.serialize_by_pickle` (if you want the type to use pickle going forward)
2. Using `@coco.unpickle_safe` (if you only need to read old cached data)
3. Converting to a dataclass or NamedTuple (recommended — msgspec handles these automatically)

:::note Existing cached data
If you see this error after upgrading from an older version, it means previously cached data references types that aren't yet in the allow-list. Adding `@coco.unpickle_safe` to the type and re-running will fix it — the stale cache entry is skipped and recomputed.
:::

### Migration from `@coco.unpickle_safe`

If you previously decorated types with `@coco.unpickle_safe`:

- **Dataclasses and NamedTuples**: Remove `@coco.unpickle_safe` — msgspec handles these natively. Old cached data from before the migration is still readable.
- **Types needing pickle**: Switch to `@coco.serialize_by_pickle` (which also registers for unpickle safety).
- **Third-party types**: Use `coco.serialize_by_pickle(SomeType)` for types that need pickle serialization.
