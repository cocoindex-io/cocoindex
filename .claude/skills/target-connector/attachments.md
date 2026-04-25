# Attachment Providers

## Overview

Attachment providers allow a target handler to expose auxiliary child target states that coexist with regular children under the same parent. For example, a database table handler manages rows as regular children, but also supports vector indexes and SQL command attachments as auxiliary states.

Attachment providers use **symbol keys** (prefixed with `@`) to namespace-separate them from regular children, avoiding path conflicts.

## When to Use

Use attachment providers when a target has auxiliary state beyond its primary children:
- Database indexes (vector indexes, B-tree indexes)
- SQL commands (triggers, materialized views)
- Any metadata or configuration that lives alongside the primary data

## Path Hierarchy

Attachment target states live under symbol-keyed sub-providers within the same parent:

```
table "my_table"               (root target state — table)
├── row "id=1"                  (regular child — row)
├── row "id=2"                  (regular child — row)
├── @vector_index               (attachment namespace)
│   └── "embedding_idx"         (attachment target state — vector index)
└── @sql_command_attachment      (attachment namespace)
    └── "custom_idx"            (attachment target state — SQL command)
```

The `@` prefix is a symbol key that separates attachment namespaces from regular child keys.

## How It Works

1. **Parent handler** implements `attachment(att_type)` returning a handler for that attachment type (or `None` if unsupported)
2. **User code** calls `provider.attachment(att_type)` on a resolved child provider to get an attachment sub-provider
3. **Target states** declared under the attachment provider are tracked independently from regular children
4. Attachment providers are **cached** — calling `.attachment("x")` twice returns the same provider

## Implementation

### Step 1: Define Attachment Types

Define spec, action, and tracking record types for each attachment kind:

```python
class _VectorIndexSpec(NamedTuple):
    column: str
    metric: str
    method: str
    lists: int | None
    m: int | None
    ef_construction: int | None
```

### Step 2: Implement Attachment Handler

Create a handler class with `reconcile()` method:

```python
class _VectorIndexHandler:
    def __init__(self, pool, table_name, schema_name):
        self._pool = pool
        self._table_name = table_name
        self._schema_name = schema_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _VectorIndexSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_VectorIndexFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_VectorIndexAction, _VectorIndexFingerprint] | None:
        # Compare desired state with previous, return action or None
        ...
```

### Step 3: Add `attachment()` to Parent Handler

The parent handler (e.g., `_RowHandler`) returns the appropriate attachment handler:

```python
class _RowHandler:
    def attachment(self, att_type: str) -> _VectorIndexHandler | _SqlCommandHandler | None:
        if att_type == "vector_index":
            return _VectorIndexHandler(self._pool, self._table_name, self._schema_name)
        elif att_type == "sql_command_attachment":
            return _SqlCommandHandler(self._pool, self._table_name, self._schema_name)
        return None
```

### Step 4: Expose User-Facing API

Wrap the attachment provider in a convenient method on the target class:

```python
class TableTarget:
    def declare_vector_index(self, *, name, column, metric="cosine", method="ivfflat", ...):
        spec = _VectorIndexSpec(column=column, metric=metric, method=method, ...)
        att_provider = self._provider.attachment("vector_index")
        coco.declare_target_state(att_provider.target_state(name, spec))

    def declare_sql_command_attachment(self, *, name, setup_sql, teardown_sql=None):
        spec = _SqlCommandSpec(setup_sql=setup_sql, teardown_sql=teardown_sql)
        att_provider = self._provider.attachment("sql_command_attachment")
        coco.declare_target_state(att_provider.target_state(name, spec))
```

## Tracking Record Design

Choose the tracking record type based on whether teardown recovery is needed:

| Approach | Tracking Record | When to Use |
|----------|----------------|-------------|
| **Fingerprint** | `bytes` (content hash) | No teardown needed; change detection only (e.g., vector index — just DROP + CREATE) |
| **Full spec** | The spec itself (e.g., `_SqlCommandSpec`) | Teardown requires info from previous state (e.g., SQL command — need `teardown_sql` from previous run) |

**Fingerprint example** (vector index): Only needs to detect whether the spec changed. On change or delete, the action is always DROP + CREATE — no previous state info needed.

```python
tracking_record = fingerprint_object(desired_state)  # bytes
```

**Full spec example** (SQL command): On change or delete, the previous `teardown_sql` must be executed before the new `setup_sql`. The full spec is stored so `prev_possible_records` contains recoverable teardown information.

```python
tracking_record = desired_state  # _SqlCommandSpec (the spec itself)
```

## Reference Implementations

- `python/cocoindex/connectors/postgres/_target.py` — `_VectorIndexHandler`, `_SqlCommandHandler`, `_RowHandler.attachment()`, `TableTarget.declare_vector_index()`, `TableTarget.declare_sql_command_attachment()`
