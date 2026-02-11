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
| `TargetHandler` | Implements `reconcile()` — compares desired state with previous tracking records |
| `TargetActionSink` | Executes actions against the external system |
| Tracking Record | Persisted state for change detection (typically a frozen dataclass) |
| Action | Describes what operation to perform on the external system |

### What CocoIndex Provides

| Type | Purpose |
| ---- | ------- |
| `TargetStatesProvider` | Factory that creates `TargetState` objects from your handler |
| `TargetState` | Wrapper that holds the key and spec |
| `register_root_target_states_provider()` | Registers a root handler and returns a provider |
| `declare_target_state()` | Declares a leaf target state for reconciliation |
| `declare_target_state_with_child()` | Declares a target state and returns a child provider |

## Implementation Workflow

### Root Target States

1. **Define types**: Key, Spec, TrackingRecord, Action
2. **Implement TargetHandler**: The `reconcile()` method must be non-blocking
3. **Create TargetActionSink**: Use `TargetActionSink.from_fn()` or `from_async_fn()`
4. **Register provider**: Call `register_root_target_states_provider(name, handler)`
5. **Create user-facing API**: Wrap the provider in a user-friendly class

### Non-Root (Child) Target States

For targets nested inside another target (e.g., files inside a directory):

1. Parent sink returns `ChildTargetDef(handler=...)` when executed
2. Call `declare_target_state_with_child(parent_ts)` to get an unresolved child provider
3. CocoIndex resolves the child provider when parent's sink executes

## TargetHandler Protocol

```python
class TargetHandler(Protocol[KeyT, ValueT, TrackingRecordT, OptChildHandlerT]):
    def reconcile(
        self,
        key: KeyT,
        desired_state: ValueT | NonExistenceType,
        prev_possible_states: Collection[TrackingRecordT],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[ActionT, TrackingRecordT, OptChildHandlerT] | None:
        ...
```

**Parameters:**

- `key`: Unique identifier for the target state
- `desired_state`: What the user declared, or `NON_EXISTENCE` if no longer declared
- `prev_possible_states`: Tracking records from previous runs (may have multiple)
- `prev_may_be_missing`: If `True`, the target state might not exist in the external system

**Returns:**

- `TargetReconcileOutput(action, sink, tracking_record)` if an action is needed
- `None` if no changes are required

**Important:** The `reconcile()` method must be non-blocking. It should only compare states and return an action — actual I/O happens in the sink.

## Best Practices

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

Due to interrupted updates, `prev_possible_states` may contain multiple records:

```python
if not prev_may_be_missing and all(
    prev.fingerprint == target_fp for prev in prev_possible_states
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

Create module-level shared sinks when all handler instances use the same action logic:

```python
_shared_sink = coco.TargetActionSink.from_fn(_apply_actions)
```

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
def test_insert_and_update(connector_fixture: tuple[Connection, Path]) -> None:
    conn, _ = connector_fixture
    source_rows: list[RowType] = []

    with connector.register_db("test_db", conn) as db:

        async def declare_target() -> None:
            table = await coco_aio.mount_run(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                "test_table",
                await connector.TableSchema.from_class(RowType, primary_key=["id"]),
            ).result()
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

## Resources

For complete implementation details and examples, see:

- `docs/docs/advanced_topics/custom_target_connector.md` - Full documentation
- `python/cocoindex/connectors/localfs/_target.py` - File system target connector (sync API, nested directory targets)
- `python/cocoindex/connectors/sqlite/_target.py` - SQLite target connector (sync API, two-level table/row targets, vector support)
- `python/cocoindex/connectors/postgres/_target.py` - PostgreSQL target connector (async API, two-level table/row targets, vector support)
