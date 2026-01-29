---
title: Custom Target Connector
description: Learn how to create custom target connectors to integrate CocoIndex with external systems.
toc_max_heading_level: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

A **custom target connector** is the mechanism that connects CocoIndex's declarative target state system to external systems. When you call methods like `dir_target.declare_file()` or `table_target.declare_row()`, a target connector handles the actual synchronization — determining what changed and applying those changes to the external system.

## When to Create a Custom Target Connector

Most users will use built-in connectors (like `localfs` or `postgres`) and never need to create their own. Consider creating a custom target connector when:

- You need to integrate with an external system not covered by existing connectors
- You need custom change detection logic (e.g., content-based fingerprinting)
- You need to manage hierarchical target states (containers with children)

:::tip Start Simple
For simple use cases where you just need to write data to an external system without sophisticated change tracking, consider using a regular function with memoization instead. Target states providers are most valuable when you need CocoIndex to track and clean up target states automatically.
:::

## Key Data Types

This section introduces the key data types. Each is marked as either **you implement** or **CocoIndex provides** to clarify responsibilities.

### TargetHandler *(you implement)*

A `TargetHandler` implements the reconciliation logic. It's a protocol with a single method:

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

**Type Parameters:**

- `KeyT`: The type used to identify target states (e.g., filename, primary key tuple)
- `ValueT`: The specification for the target state (e.g., file content, row data)
- `TrackingRecordT`: What's stored to detect changes on future runs
- `OptChildHandlerT`: The child handler type, or `None` for leaf targets

**Parameters:**

- `key`: Unique identifier for this target state
- `desired_state`: What the user declared, or `NON_EXISTENCE` if no longer declared
- `prev_possible_states`: Tracking records from previous runs (may have multiple due to interrupted updates)
- `prev_may_be_missing`: If `True`, the target state might not exist in the external system

**Returns:**

- `TargetReconcileOutput` if an action is needed
- `None` if no changes are required

:::warning Non-blocking
The `reconcile()` method must be **non-blocking**. It should only compare states and return an action — actual I/O operations happen later in the `TargetActionSink`.
:::

### Tracking Record *(you define)*

A **tracking record** captures the essential information needed to detect changes. Good tracking records:

- Are **minimal**: Only include what's needed for change detection
- Are **deterministic**: Same input always produces the same record
- Are **serializable**: Must be persistable (typically a NamedTuple or dataclass)

```python
# Example: File tracking record
@dataclass(frozen=True, slots=True)
class _FileTrackingRecord:
    fingerprint: bytes  # Content hash for change detection
```

:::tip Fingerprinting
For content-based change detection, use a hash function like `blake2b`. This lets you detect changes without storing the full content:

```python
from hashlib import blake2b

def _compute_fingerprint(content: bytes) -> bytes:
    return blake2b(content).digest()
```

:::

### Action and TargetActionSink *(you implement)*

An **action** (you define) describes what operation to perform on the external system:

```python
# Example: File action
class _FileAction(NamedTuple):
    path: pathlib.Path
    content: bytes | None  # None means delete
```

A **TargetActionSink** batches and executes actions:

```python
# Sync sink
sink = coco.TargetActionSink.from_fn(apply_actions)

# Async sink
sink = coco.TargetActionSink.from_async_fn(apply_actions_async)
```

The sink function receives a sequence of actions and applies them. For container targets, it returns child handler definitions:

```python
def apply_actions(
    actions: Sequence[_FileAction],
) -> list[coco.ChildTargetDef[_ChildHandler] | None]:
    outputs = []
    for action in actions:
        if action.content is None:
            action.path.unlink(missing_ok=True)
            outputs.append(None)
        else:
            action.path.write_bytes(action.content)
            # Return child handler for directories
            if action.is_directory:
                outputs.append(coco.ChildTargetDef(handler=_ChildHandler(action.path)))
            else:
                outputs.append(None)
    return outputs
```

### TargetReconcileOutput *(you return)*

`TargetReconcileOutput` bundles what `reconcile()` returns when an action is needed:

```python
class TargetReconcileOutput(NamedTuple):
    action: ActionT                           # What to do
    sink: TargetActionSink[ActionT, ...]      # How to execute it
    tracking_record: TrackingRecordT | NonExistenceType  # What to remember
```

### TargetStatesProvider *(CocoIndex provides)*

A `TargetStatesProvider` is a factory that creates `TargetState` objects. You don't implement this class — CocoIndex gives you one when you register a handler or declare a target state with children.

```python
# You get a provider from registration
provider = coco.register_root_target_states_provider("my.target", handler)

# Or from declaring a parent target state
child_provider = coco.declare_target_state_with_child(parent_target_state)
```

### TargetState *(CocoIndex provides)*

A `TargetState` wraps a key and spec. You create these using the provider, then declare them:

```python
# Create a target state
target_state = provider.target_state(key, spec)

# Declare it for reconciliation
coco.declare_target_state(target_state)
```

## Implementing Root Target States

This section covers root target states — those not nested inside another target.

### Life of a Root Target State

Understanding what happens at runtime:

1. **Registration**: You define a `TargetHandler` and call `register_root_target_states_provider()`. CocoIndex returns a `TargetStatesProvider` — a factory for creating target states associated with your handler.

2. **Declaration**: During execution, user code calls `provider.target_state(key, spec)` to create `TargetState` objects, then `declare_target_state()` to declare them. CocoIndex collects all declared target states.

3. **Reconciliation**: When the processing unit finishes, CocoIndex calls your handler's `reconcile()` method for each target state. For declared target states, `desired_state` contains the spec; for previously declared but now missing states, `desired_state` is `NON_EXISTENCE` (triggering cleanup). Your `reconcile()` compares the desired state with previous records and returns `TargetReconcileOutput` if an action is needed, or `None` if no changes are required.

4. **Action Execution**: CocoIndex batches actions by their `TargetActionSink` and executes them. The sink applies changes to the external system (database writes, file operations, API calls, etc.).

5. **Tracking Persistence**: After successful execution, CocoIndex persists the new tracking records. On the next run, these become the `prev_possible_states` for change detection.

:::note Multiple Previous States
Due to interrupted updates, `prev_possible_states` may contain multiple records. CocoIndex tracks all possible states until a successful update confirms the current state. Your reconciliation logic should handle this by generating actions that work correctly regardless of which previous state is actual.
:::

### Step 1: Define Your Types

Start by defining the types for your provider:

```python
from typing import NamedTuple, Collection
from dataclasses import dataclass
import cocoindex as coco

# Key: How to identify a target state
_RowKey = tuple[str, ...]  # Primary key values

# Value: What the user declares
@dataclass
class _RowSpec:
    data: dict[str, Any]

# Tracking Record: What to persist for change detection
@dataclass(frozen=True, slots=True)
class _RowTrackingRecord:
    fingerprint: bytes

# Action: What operation to perform
class _RowAction(NamedTuple):
    key: _RowKey
    data: dict[str, Any] | None  # None = delete
```

### Step 2: Implement the Handler

```python
class _RowHandler(coco.TargetHandler[_RowKey, _RowSpec, _RowTrackingRecord]):
    """Handler for database rows."""

    def __init__(self, connection: DatabaseConnection, table: str):
        self._conn = connection
        self._table = table
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
        for action in actions:
            if action.data is None:
                await self._conn.delete(self._table, action.key)
            else:
                await self._conn.upsert(self._table, action.key, action.data)

    def _compute_fingerprint(self, data: dict[str, Any]) -> bytes:
        import json
        from hashlib import blake2b
        serialized = json.dumps(data, sort_keys=True, default=str)
        return blake2b(serialized.encode()).digest()

    def reconcile(
        self,
        key: _RowKey,
        desired_state: _RowSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_RowTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowTrackingRecord] | None:
        # Handle deletion
        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None  # Nothing to delete
            return coco.TargetReconcileOutput(
                action=_RowAction(key=key, data=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Handle upsert
        target_fp = self._compute_fingerprint(desired_state.data)

        # Skip if unchanged
        if not prev_may_be_missing and all(
            prev.fingerprint == target_fp for prev in prev_possible_states
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(key=key, data=desired_state.data),
            sink=self._sink,
            tracking_record=_RowTrackingRecord(fingerprint=target_fp),
        )
```

### Step 3: Register the Provider

For root-level target states (not nested within another target), register a provider:

```python
_row_provider = coco.register_root_target_states_provider(
    "mycompany.io/mydb/row",  # Unique provider name
    _RowHandler(connection, table),
)
```

### Step 4: Create User-Facing APIs

Wrap the provider in a user-friendly API:

```python
class TableTarget:
    """User-facing API for declaring rows."""

    def __init__(self, provider: coco.TargetStateProvider[_RowKey, _RowSpec, None]):
        self._provider = provider

    def declare_row(self, *, row: dict[str, Any], key: tuple[str, ...]) -> None:
        spec = _RowSpec(data=row)
        target_state = self._provider.target_state(key, spec)
        coco.declare_target_state(target_state)
```

## Implementing Container Targets

Container targets (directories, tables) have children (files, rows). This section covers how non-root target states work and how to implement them.

### Non-Root Target States

For targets **nested inside another target** (e.g., files inside a directory), the lifecycle is similar to root targets but **how you get the provider is different**.

For root targets, you call `register_root_target_states_provider()` and immediately get a provider with your handler. For non-root targets, the handler comes from the **parent's sink execution**:

1. **Declaration**: Call `declare_target_state_with_child(parent_ts)` — returns an **unresolved** child provider immediately
2. **Resolution**: When the parent reconciles and its sink executes, the sink returns `ChildTargetDef(handler=...)`. CocoIndex resolves the child provider with this handler.
3. **Usage**: The child provider can now create child target states, which follow the same reconciliation → execution → tracking flow as root targets.

The child handler often needs context from the parent's action execution. For example, a file handler needs to know the directory path that was created. By returning the handler from the parent's sink, the handler has access to this runtime context.

### Step 1: Define Parent and Child Handlers

The parent handler reconciles the container itself. The child handler reconciles entries within it:

```python
# Parent handler for directory
class _DirHandler(coco.TargetHandler[_DirKey, _DirSpec, _DirTrackingRecord]):
    def reconcile(self, key, desired_state, prev_possible_states, prev_may_be_missing, /):
        # Reconcile the directory itself
        ...

# Child handler for entries within a directory
class _EntryHandler(coco.TargetHandler[str, _EntrySpec, _EntryTrackingRecord]):
    def __init__(self, base_path: pathlib.Path):
        self._base_path = base_path

    def reconcile(self, key, desired_state, prev_possible_states, prev_may_be_missing, /):
        # Reconcile files/subdirs within the directory
        path = self._base_path / key
        ...
```

### Step 2: Return Child Handlers from the Sink

The parent's sink creates the container and returns child handlers:

```python
def _apply_dir_actions(
    actions: Sequence[_DirAction],
) -> list[coco.ChildTargetDef[_EntryHandler] | None]:
    outputs = []
    for action in actions:
        if action.should_delete:
            shutil.rmtree(action.path, ignore_errors=True)
            outputs.append(None)  # No child handler for deleted directories
        else:
            action.path.mkdir(parents=True, exist_ok=True)
            # Return child handler with the created path
            outputs.append(coco.ChildTargetDef(handler=_EntryHandler(action.path)))
    return outputs
```

### Step 3: Create User-Facing API

The user-facing API uses `declare_target_state_with_child()` and exposes methods for declaring children:

```python
class DirTarget:
    """User-facing API for declaring files in a directory."""

    def __init__(self, provider: coco.TargetStatesProvider[str, _EntrySpec, None]):
        self._provider = provider

    def declare_file(self, filename: str, content: bytes) -> None:
        spec = _EntrySpec(content=content)
        target_state = cast(
            coco.TargetState[None],
            self._provider.target_state(filename, spec),
        )
        coco.declare_target_state(target_state)


@coco.function
def declare_dir_target(path: pathlib.Path) -> DirTarget:
    """Declare a directory target and return an API for declaring files."""
    parent_ts = _root_provider.target_state(
        key=_DirKey(path=str(path)),
        spec=_DirSpec(),
    )
    # Child provider is pending until parent sink runs
    child_provider = coco.declare_target_state_with_child(parent_ts)
    return DirTarget(child_provider)
```

## Best Practices

### Idempotent Actions

Actions should be idempotent — applying the same action multiple times should have the same effect as applying it once:

```python
# Good: Idempotent
path.mkdir(parents=True, exist_ok=True)
path.unlink(missing_ok=True)
await conn.execute("INSERT ... ON CONFLICT DO UPDATE ...")

# Bad: Not idempotent
path.mkdir()  # Fails if exists
await conn.execute("INSERT ...")  # Fails on duplicate key
```

### Handle Multiple Previous States

Due to interrupted updates, `prev_possible_states` may contain multiple records. Design your reconciliation logic to handle this:

```python
# Check if ALL previous states match (conservative approach)
if not prev_may_be_missing and all(
    prev.fingerprint == target_fp for prev in prev_possible_states
):
    return None  # Safe to skip
```

### Efficient Change Detection

Choose tracking records that enable efficient change detection without storing full content:

| Scenario | Tracking Record |
|----------|-----------------|
| File content | Content hash (fingerprint) |
| Database row | Row data hash |
| Schema/structure | Schema definition |
| Directory existence | `None` (presence is enough) |

### Shared Action Sinks

If all instances of a handler use the same action logic, create a shared sink:

```python
# Module-level shared sink
_shared_sink = coco.TargetActionSink.from_fn(_apply_actions)

class _MyHandler(coco.TargetHandler[...]):
    def reconcile(self, ...):
        return coco.TargetReconcileOutput(
            action=...,
            sink=_shared_sink,  # Reuse the same sink
            tracking_record=...,
        )
```

## Complete Example: Local File System

Here's a simplified version of the `localfs` connector showing the complete pattern:

```python
from __future__ import annotations
import pathlib
from dataclasses import dataclass
from hashlib import blake2b
from typing import Collection, Literal, NamedTuple, Sequence
import cocoindex as coco


# Types
_FileName = str
_FileContent = bytes
_FileFingerprint = bytes


class _FileAction(NamedTuple):
    path: pathlib.Path
    content: _FileContent | None  # None = delete


@dataclass(frozen=True, slots=True)
class _FileTrackingRecord:
    fingerprint: _FileFingerprint


# Action execution
def _apply_actions(actions: Sequence[_FileAction]) -> None:
    for action in actions:
        if action.content is None:
            action.path.unlink(missing_ok=True)
        else:
            action.path.parent.mkdir(parents=True, exist_ok=True)
            action.path.write_bytes(action.content)


_file_sink = coco.TargetActionSink[_FileAction, None].from_fn(_apply_actions)


def _compute_fingerprint(content: bytes) -> _FileFingerprint:
    return blake2b(content).digest()


# Handler
class _FileHandler(coco.TargetHandler[_FileName, _FileContent, _FileTrackingRecord]):
    __slots__ = ("_base_path",)
    _base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self._base_path = base_path

    def reconcile(
        self,
        key: _FileName,
        desired_state: _FileContent | coco.NonExistenceType,
        prev_possible_states: Collection[_FileTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_FileAction, _FileTrackingRecord] | None:
        path = self._base_path / key

        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_FileAction(path=path, content=None),
                sink=_file_sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = _compute_fingerprint(desired_state)

        if not prev_may_be_missing and all(
            prev.fingerprint == target_fp for prev in prev_possible_states
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_FileAction(path=path, content=desired_state),
            sink=_file_sink,
            tracking_record=_FileTrackingRecord(fingerprint=target_fp),
        )
```

See the full implementations in:

- [`cocoindex/connectors/localfs/target.py`](https://github.com/cocoindex-io/cocoindex/blob/v1/python/cocoindex/connectors/localfs/_target.py) — File system targets
- [`cocoindex/connectors/postgres/target.py`](https://github.com/cocoindex-io/cocoindex/blob/v1/python/cocoindex/connectors/postgres/_target.py) — PostgreSQL tables and rows
