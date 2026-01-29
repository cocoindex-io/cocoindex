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

## Resources

For complete implementation details and examples, see:

- `docs/docs/advanced_topics/custom_target_connector.md` - Full documentation
- `python/cocoindex/connectors/localfs/_target.py` - Real-world implementation example (localfs connector)
