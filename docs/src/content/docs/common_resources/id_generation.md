---
title: ID Generation
description: Utilities for generating stable unique IDs and UUIDs that persist across incremental updates.
---

# ID Generation

The ID module (`cocoindex.resources.id`) provides utilities for generating stable unique IDs and UUIDs that persist across incremental updates.

In an incremental pipeline, using random IDs (like `uuid.uuid4()`) means every reprocessing run generates different IDs for the same data — causing unnecessary churn in your targets (deleting old rows, inserting identical ones with new IDs). CocoIndex's ID utilities produce **stable** IDs: the same inputs produce the same IDs across runs, so unchanged data keeps its identity and targets only see real changes.

## Choosing the right API

| API | Same `dep` produces... | Use when... |
|-----|------------------------|-------------|
| `generate_id(dep)` | **Same** ID every time | Each unique input maps to exactly one ID |
| `IdGenerator.next_id(dep)` | **Distinct** ID each call | You need multiple IDs for potentially non-distinct inputs |

The same distinction applies to `generate_uuid` vs `UuidGenerator`.

## generate_id / generate_uuid

Async functions that return the **same** ID/UUID for the **same** `dep` value. These are idempotent: calling multiple times with identical `dep` yields identical results.

```python
from cocoindex.resources.id import generate_id, generate_uuid

async def process_item(item: Item) -> Row:
    # Same item.key always gets the same ID
    item_id = await generate_id(item.key)
    return Row(id=item_id, data=item.data)

async def process_document(doc: Document) -> Row:
    # Same doc.path always gets the same UUID
    doc_uuid = await generate_uuid(doc.path)
    return Row(id=doc_uuid, content=doc.content)
```

**Parameters:**

- `dep` — Dependency value that determines the ID/UUID. The same `dep` always produces the same result within a component. Defaults to `None`.

**Returns:**

- `generate_id` returns an `int` (IDs start from 1; 0 is reserved)
- `generate_uuid` returns a `uuid.UUID`

## IdGenerator / UuidGenerator

Classes that return a **distinct** ID/UUID on each call, even when called with the same `dep` value. The sequence is stable across runs.

Use these when you need multiple IDs for potentially non-distinct inputs, such as splitting text into chunks where chunks may have identical content but still need unique IDs.

```python
from cocoindex.resources.id import IdGenerator, UuidGenerator

async def process_document(doc: Document) -> list[Row]:
    # Use doc.path to distinguish generators within the same processing component
    id_gen = IdGenerator(deps=doc.path)
    rows = []
    for chunk in split_into_chunks(doc.content):
        # Each call returns a distinct ID, even if chunks are identical
        chunk_id = await id_gen.next_id(chunk.content)
        rows.append(Row(id=chunk_id, content=chunk.content))
    return rows

async def process_with_uuids(doc: Document) -> list[Row]:
    # Use doc.path to distinguish generators within the same processing component
    uuid_gen = UuidGenerator(deps=doc.path)
    rows = []
    for chunk in split_into_chunks(doc.content):
        # Each call returns a distinct UUID, even if chunks are identical
        chunk_uuid = await uuid_gen.next_uuid(chunk.content)
        rows.append(Row(id=chunk_uuid, content=chunk.content))
    return rows
```

**Constructor:**

- `IdGenerator(deps=None)` / `UuidGenerator(deps=None)` — Create a generator. The `deps` parameter distinguishes generators within the same processing component. Use distinct `deps` values for different generator instances.

**Methods:**

- `async IdGenerator.next_id(dep=None)` — Generate the next unique integer ID (distinct on each call)
- `async UuidGenerator.next_uuid(dep=None)` — Generate the next unique UUID (distinct on each call)
