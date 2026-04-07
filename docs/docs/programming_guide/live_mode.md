---
title: Live Mode
description: Make your app react to source changes continuously, instead of only processing in full sweeps.
---

# Live Mode

By default, calling `app.update()` runs one processing cycle — it scans all sources, processes what changed since the last run (memoized components are skipped, so unchanged work is not redone), syncs target states, and returns. To pick up further changes, you call `update()` again.

So updates are already incremental — but each call still has to scan sources to discover what changed, and changes are only picked up when you trigger a new run.

**Live mode** keeps the app running after that initial scan and lets components stream changes continuously from their sources (e.g. a file system watcher or a database change feed), applying them to target states with very low latency. This is useful when:

- You want near-real-time reactions to source changes, instead of waiting for the next `update()` call
- Your sources can push changes more efficiently than a full rescan can discover them

Two things are needed for live mode to work: the app must be **enabled** to stay running, and somewhere in the component tree a component must **react** to changes.

## Enabling live mode

Pass `live=True` when updating the app:

```python
app.update_blocking(live=True)

# Or async
handle = app.update(live=True)
await handle.result()
```

From the CLI:

```bash
cocoindex update --live my_app.py
# or
cocoindex update -L my_app.py
```

The `live` flag propagates top-down through the component tree — both `coco.mount()` and `coco.use_mount()` inherit `live` from the parent, so children are live when the app is live.

Without `live=True` on the app, everything completes after the initial scan — even if a source supports live watching.

## Reacting to changes

Enabling live mode keeps the app running, but something in the component tree needs to actually watch for changes. That something is a [**LiveComponent**](../advanced_topics/live_component.md) — a component with a long-running `process_live()` method that delivers incremental updates.

You rarely need to write a `LiveComponent` manually. The most common pattern is:

### Sources with `LiveMapView` or `LiveMapFeed`

Source connectors can provide live capabilities via two [protocols](../advanced_topics/live_component.md#live-map):

- **`LiveMapView`** — the source has scannable current state (e.g., a directory or database table). It does a full scan first, then watches for changes. Example: [`localfs.walk_dir(live=True).items()`](../connectors/localfs.md#live-file-watching).
- **`LiveMapFeed`** — the source only streams changes, with no snapshot to scan (e.g., a Kafka consumer). All data arrives via the change stream. Example: [`kafka.topic_as_map()`](../connectors/kafka.md#as-source).

When `mount_each()` receives either, it automatically creates a `LiveComponent` internally that:

1. **Scans current state** (if available) — iterates all items and mounts a processing component for each
2. **Signals readiness** — the initial scan is complete (or the stream has caught up), target states are synced
3. **Watches for changes** — the source delivers incremental updates:
   - New or modified items → re-mount the affected component
   - Deleted items → remove the component and its target states

CocoIndex handles change detection, memoization, and target state reconciliation the same way as in batch mode.

Without live support on the source, `mount_each()` does a one-time iteration — items are processed in batch and that's it.

## Examples

### `localfs` — file watching with `LiveMapView`

The [`localfs`](../connectors/localfs.md) connector supports live mode via `walk_dir(..., live=True)`, which watches for file system changes using `watchfiles`:

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # items() returns a LiveMapView
    )
    await coco.mount_each(process_file, files.items(), outdir)

app = coco.App(coco.AppConfig(name="FilesTransform"), app_main, sourcedir=..., outdir=...)
app.update_blocking(live=True)
```

**Non-live compatibility:** `LiveMapView` sources also work without `live=True` — they do the initial full scan and exit cleanly. You can write your pipeline once and choose batch or live at run time.

For a complete working example, see [`files_transform`](https://github.com/cocoindex-io/cocoindex/tree/v1/examples/files_transform).

### `kafka` — consuming a topic with `LiveMapFeed`

The [`kafka`](../connectors/kafka.md) connector treats a topic as a live keyed map — each message is an upsert or delete for a key. Since there's no snapshot to scan, it returns a `LiveMapFeed`:

```python
from confluent_kafka.aio import AIOConsumer
from cocoindex.connectors import kafka

@coco.fn
async def app_main() -> None:
    consumer = AIOConsumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-group",
        "enable.auto.commit": "false",
    })
    items = kafka.topic_as_map(consumer, ["my-topic"])
    await coco.mount_each(process_message, items)

app = coco.App(coco.AppConfig(name="KafkaConsumer"), app_main)
app.update_blocking(live=True)
```

## Going deeper

The abstractions behind live mode, from most general to most specific:

- **[LiveComponent](../advanced_topics/live_component.md)** — the underlying protocol for components that react to changes incrementally. Most flexible — full control over the lifecycle.
- **[LiveMapFeed / LiveMapView](../advanced_topics/live_component.md#live-map)** — represents a changing collection of keyed items. `mount_each()` uses it to construct a `LiveComponent` automatically. Connector authors implement this to add live support.
- **Source connectors** — provide `LiveMapView` (e.g., [`localfs`](../connectors/localfs.md)) or `LiveMapFeed` (e.g., [`kafka`](../connectors/kafka.md)) from their source APIs. Users just pass the result to `mount_each()`.

For custom change feeds, fine-grained lifecycle control, or implementing live map protocols on your own connector, see [Live Components](../advanced_topics/live_component.md).
