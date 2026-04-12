---
name: target-connector
description: This skill should be used when creating a new target connector for CocoIndex to integrate with external systems. It provides guidance on implementing TargetHandler, TargetActionSink, and related types for declarative target state synchronization with change detection and automatic cleanup.
---

# Target Connector

## Overview

A target connector connects CocoIndex's declarative target state system to external systems. It handles synchronization by determining what changed and applying changes to the external system.

## When to Use

Use this skill when creating a new target connector for any external system (databases, file systems, cloud storage, APIs, etc.).

## Key Data Types

### What You Implement

| Type | Purpose |
| ---- | ------- |
| `TargetHandler` | Implements `reconcile()` — compares desired state with previous tracking records. Optionally implements `attachment(att_type)` for auxiliary child states. |
| `TargetActionSink` | Executes actions against the external system |
| Tracking Record | Persisted state for change detection (typically a frozen dataclass) |
| Action | Describes what operation to perform on the external system |

### What CocoIndex Provides

| Type | Purpose |
| ---- | ------- |
| `TargetStateProvider` | Factory that creates `TargetState` objects from your handler |
| `TargetState` | Wrapper that holds the key and spec |
| `register_root_target_states_provider()` | Registers a root handler and returns a provider |
| `declare_target_state()` | Declares a leaf target state for reconciliation |
| `declare_target_state_with_child()` | Declares a target state and returns a child provider |

## Implementation Workflow

### Root Target States

1. **Define types**: Key, Spec, TrackingRecord, Action
2. **Implement TargetHandler**: The `reconcile()` method must be non-blocking
3. **Create TargetActionSink**: Use `TargetActionSink.from_fn()` or `from_async_fn()`. The callback receives `context_provider: ContextProvider` as its first positional argument, followed by `actions`
4. **Register provider**: Call `register_root_target_states_provider(name, handler)`
5. **Create user-facing API**: Wrap the provider in a user-friendly class

### Non-Root (Child) Target States

For targets nested inside another target (e.g., files inside a directory):

1. Parent sink returns `ChildTargetDef(handler=...)` when executed
2. Call `declare_target_state_with_child(parent_ts)` to get an unresolved child provider
3. CocoIndex resolves the child provider when parent's sink executes

### Child Invalidation

For container targets, set `child_invalidation` in `TargetReconcileOutput` when a container change affects its children:

| Value | When to Use | Effect on Children |
| ----- | ----------- | ------------------ |
| `None` (default) | No impact on children (e.g., only new columns added) | Normal change detection |
| `"destructive"` | Container rebuilt from scratch (e.g., table dropped and recreated due to primary key change or table type switch) | All previous tracking records ignored; children treated as new and re-declared |
| `"lossy"` | Data loss possible but container not fully rebuilt (e.g., column removed or type changed) | All children get `prev_may_be_missing=True`, forcing upsert even if content appears unchanged |

**Pattern for two-level (table/row) connectors using `statediff.diff_composite`:**

```python
# After computing main_action and column_actions via statediff.diff_composite:
child_invalidation: Literal["destructive", "lossy"] | None = None
if main_action == "replace":
    # Table dropped and recreated — all rows are destroyed.
    child_invalidation = "destructive"
elif main_action is None and any(a != "insert" for a in column_actions.values()):
    # Column changes other than adding new columns may lose existing row data.
    child_invalidation = "lossy"

return coco.TargetReconcileOutput(
    action=_TableAction(...),
    sink=self._sink,
    tracking_record=_TableTrackingRecord(...),
    child_invalidation=child_invalidation,
)
```

For connectors without column-level diffs (e.g., a collection that is either intact or fully replaced), only `"destructive"` applies:

```python
child_invalidation: Literal["destructive"] | None = (
    "destructive" if main_action == "replace" else None
)
```

## TargetHandler Protocol

```python
class TargetHandler(Protocol[ValueT, TrackingRecordT, OptChildHandlerT]):
    def reconcile(
        self,
        key: StableKey,
        desired_target_state: ValueT | NonExistenceType,
        prev_possible_records: Collection[TrackingRecordT],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[Any, TrackingRecordT, OptChildHandlerT] | None:
        ...

    # Optional: override to support attachment types
    def attachment(self, att_type: str) -> TargetHandler | None:
        return None
```

**Parameters:**

- `key`: `StableKey` — a union of `None | bool | int | str | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]`
- `desired_target_state`: What the user declared, or `NON_EXISTENCE` if no longer declared
- `prev_possible_records`: Tracking records from previous runs (may have multiple)
- `prev_may_be_missing`: If `True`, the target state might not exist in the external system

**Returns:**

- `TargetReconcileOutput(action, sink, tracking_record, child_invalidation=None)` if an action is needed (generic params: `[ActionT, TrackingRecordT, OptChildHandlerT]`)
- `None` if no changes are required

The optional `child_invalidation` field is only relevant for container targets — see [Child Invalidation](#child-invalidation).

**Important:** The `reconcile()` method must be non-blocking. It should only compare states and return an action — actual I/O happens in the sink.

## Best Practices

### Use `ContextKey` for External Resource Identity

When a target connector manages state in an external resource (database, object store, etc.), use a `ContextKey` string as part of the target state key — not connection parameters like host, port, or credentials.

**Why:** Target state keys must be stable across runs for correct reconciliation. CocoIndex uses keys to match current declarations with previously tracked states. If the key is stable, previously tracked states are associated with the current target, so CocoIndex can correctly reconcile — e.g., deleting rows that are no longer declared. If the key changes (because a connection parameter changed), CocoIndex cannot associate previous tracked states with the current target, and treats the target as being in a cleared state — losing the ability to clean up old data.

**Pattern:**

```python
# User creates a stable logical name for the resource
db = coco.ContextKey[asyncpg.Pool]("my_pg")

# Target connector uses db.key (the string "my_pg") in the target state key
class _TableKey(NamedTuple):
    db_key: str           # Stable — from ContextKey.key
    schema_name: str | None
    table_name: str

key = _TableKey(db_key=db.key, ...)

# At action time, resolve the live connection from context_provider
pool = context_provider.get(key.db_key, asyncpg.Pool)
```

This decouples target identity from transient connection details — changing a password, switching replicas, or rotating credentials won't invalidate tracked states.

**Reference:** See `_TableKey` in `python/cocoindex/connectors/postgres/_target.py` and `python/cocoindex/connectors/surrealdb/_target.py`.

### Idempotent Actions

Actions should be idempotent:

```python
# Good
path.mkdir(parents=True, exist_ok=True)
path.unlink(missing_ok=True)
await conn.execute("INSERT ... ON CONFLICT DO UPDATE ...")

# Bad
path.mkdir()  # Fails if exists
await conn.execute("INSERT ...")  # Fails on duplicate key
```

### Handle Multiple Previous States

Due to interrupted updates, `prev_possible_records` may contain multiple records:

```python
if not prev_may_be_missing and all(
    prev.fingerprint == target_fp for prev in prev_possible_records
):
    return None  # Safe to skip
```

### Fingerprinting for Change Detection

Use the `connectorkits.fingerprint` utilities for content-based change detection:

```python
from cocoindex.connectorkits.fingerprint import fingerprint_bytes, fingerprint_str, fingerprint_object

# For raw bytes
fp = fingerprint_bytes(content)

# For strings
fp = fingerprint_str(text)

# For arbitrary objects (uses memo key mechanism)
fp = fingerprint_object(obj)
```

### Shared Action Sinks

Create module-level shared sinks when all handler instances use the same action logic. The callback must accept `context_provider: ContextProvider` as its first positional argument:

```python
def _apply_actions(
    context_provider: ContextProvider, actions: Sequence[MyAction]
) -> list[coco.ChildTargetDef[MyChildHandler] | None] | None:
    for action in actions:
        conn = context_provider.get(action.key.db_key, ConnType)
        ...

_shared_sink = coco.TargetActionSink.from_fn(_apply_actions)
```

### Input Safety

When building queries from user-provided names (table, column, index) or values (record IDs, keys), you must guard against injection and ensure correctness. See [input_safety.md](input_safety.md) for patterns on identifier validation, parameterized queries, and value escaping.

## Completion Checklist

After implementing the connector code, complete these additional steps:

### 1. Optional Dependencies

If the connector requires third-party packages, update `pyproject.toml`:

```toml
[project.optional-dependencies]
# Add new optional dependency group
myconnector = ["some-package>=1.0.0"]

# Add to the 'all' group
all = [
    # ... existing deps ...
    "some-package>=1.0.0",
]

[[tool.mypy.overrides]]
# Add to mypy ignore list if package lacks type stubs
module = [
    # ... existing modules ...
    "some_package",
    "some_package.*",
]
ignore_missing_imports = true
```

### 2. Documentation

Create connector documentation at `docs/docs/connectors/<connector_name>.md`:

- Follow the structure of existing connector docs (e.g., `postgres.md`, `sqlite.md`)
- Include: connection setup, target state APIs, schema definition, type mappings, examples
- Add a note about optional dependencies if applicable

Update `docs/sidebars.ts` to include the new connector:

```typescript
{
  type: 'category',
  label: 'Connectors',
  items: [
    // ... existing connectors ...
    'connectors/<connector_name>',  // Add in alphabetical order
  ],
},
```

### 3. Tests

Create tests at `python/tests/connectors/test_<connector_name>_target.py`:

**Test structure:**

```python
import pytest
import cocoindex as coco
from tests import common

# Check for optional dependency availability
try:
    import optional_package
    HAS_OPTIONAL = True
except ImportError:
    HAS_OPTIONAL = False

requires_optional = pytest.mark.skipif(
    not HAS_OPTIONAL, reason="optional-package is not installed"
)

coco_env = common.create_test_env(__file__)
```

**Required test cases:**

| Category | Test Cases |
| -------- | ---------- |
| Basic CRUD | Create target, insert data, update data, delete data |
| Schema | Different column types, schema with extra columns |
| Lifecycle | Drop/cleanup when target no longer declared |
| Optimization | No-op when data unchanged |
| Multiple targets | Multiple tables/directories in same connection |
| User-managed | `managed_by="user"` mode if supported |
| Optional features | Vector support, special types (skip if dependency missing) |

**Test pattern:**

```python
DB_KEY = coco.ContextKey[connector.ConnectionType]("test_db")

def test_insert_and_update(connector_fixture: tuple[Connection, Path]) -> None:
    conn, _ = connector_fixture
    source_rows: list[RowType] = []

    coco_env.context_provider.provide(DB_KEY, conn)

    async def declare_target() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            connector.declare_table_target,
            DB_KEY,
            "test_table",
            await connector.TableSchema.from_class(RowType, primary_key=["id"]),
        )
        for row in source_rows:
            table.declare_row(row=row)

    app = coco.App(
        coco.AppConfig(name="test_insert", environment=coco_env),
        declare_target,
    )

    # Insert
    source_rows.append(RowType(id="1", name="Alice"))
    app.update()
    assert read_data(conn, "test_table") == [{"id": "1", "name": "Alice"}]

    # Update
    source_rows[0] = RowType(id="1", name="Alice Updated")
    app.update()
    assert read_data(conn, "test_table") == [{"id": "1", "name": "Alice Updated"}]
```

**Optional feature tests:**

```python
@requires_optional
def test_vector_support(connector_with_vec: tuple[Connection, Path]) -> None:
    """Tests that require optional dependencies should be skipped when unavailable."""
    # ... test vector functionality ...
```

**Reference implementations:**

- `python/tests/connectors/test_sqlite_target.py` - SQLite tests with vector support

## Attachment Providers

For targets with auxiliary child states (e.g., indexes on a database table), see [attachments.md](attachments.md) for the full reference on implementing attachment providers.

## Resources

For complete implementation details and examples, see:

- `docs/docs/advanced_topics/custom_target_connector.md` - Full documentation
- `python/cocoindex/connectors/localfs/_target.py` - File system target connector (sync API, nested directory targets)
- `python/cocoindex/connectors/sqlite/_target.py` - SQLite target connector (sync API, two-level table/row targets, vector support)
- `python/cocoindex/connectors/postgres/_target.py` - PostgreSQL target connector (async API, two-level table/row targets, vector support, attachment providers)
- `python/cocoindex/connectors/doris/_target.py` - Doris target connector (async API, two-level table/row targets, Stream Load bulk inserts)
