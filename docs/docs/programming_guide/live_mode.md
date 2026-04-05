---
title: Live Mode
description: Make your app react to source changes continuously, instead of only processing in full sweeps.
---

# Live Mode

By default, calling `app.update()` runs a full processing cycle — it scans all sources, processes everything, syncs target states, and returns. To process again, you call `update()` again.

**Live mode** keeps the app running after the initial scan, so components can watch for changes and process them incrementally — without rescanning everything. This is useful when:

- You have a large dataset and only a few items change at a time
- You want near-real-time reactions to source changes (e.g., file system watcher, database change feed)

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

### Sources with `LiveItemsView`

Some source connectors can provide a [`LiveItemsView`](../advanced_topics/live_component.md#liveitems-view) — a collection that can be iterated for a full scan *and* watched for changes. When `mount_each()` receives a `LiveItemsView`, it automatically creates a `LiveComponent` internally:

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # source provides LiveItemsView when live
    )
    await coco.mount_each(process_file, files.items(), outdir)  # outdir passed to process_file
```

The internally created `LiveComponent`:

1. **Full scan** — iterates all items and mounts a processing component for each
2. **Signals readiness** — the initial scan is complete, target states are synced
3. **Watches for changes** — the source delivers incremental updates:
   - New or modified items → re-mount the affected component
   - Deleted items → remove the component and its target states

CocoIndex handles change detection, memoization, and target state reconciliation the same way as in batch mode.

Without live support on the source, `mount_each()` does a one-time iteration — items are processed in batch and that's it.

**Non-live compatibility:** Live-capable sources work in non-live mode too — they do the initial full scan and exit cleanly, no watching occurs. This means you can write your pipeline once and choose batch or live at run time.

How live mode is activated varies by connector — some use a flag, others may require external configuration (e.g., subscribing to a change notification service). Check each connector's documentation for details.

## Example: `localfs` with live file watching

The [`localfs`](../connectors/localfs.md) connector supports live mode via `walk_dir(..., live=True)`, which watches for file system changes using `watchfiles`. Here's a pipeline that transforms Markdown files to HTML and reacts to file changes:

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), outdir)

app = coco.App(coco.AppConfig(name="FilesTransform"), app_main, sourcedir=..., outdir=...)
app.update_blocking(live=True)
```

For a complete working example, see [`files_transform`](https://github.com/cocoindex-io/cocoindex/tree/v1/examples/files_transform).

## Going deeper

The abstractions behind live mode, from most general to most specific:

- **[LiveComponent](../advanced_topics/live_component.md)** — the underlying protocol for components that react to changes incrementally. Most flexible — full control over the lifecycle.
- **[LiveItemsView](../advanced_topics/live_component.md#liveitems-view)** — represents a changing collection of keyed items. `mount_each()` uses it to construct a `LiveComponent` automatically. Connector authors implement this to add live support.
- **Source connectors** (e.g., `localfs.walk_dir(live=True)`) — provide `LiveItemsView` from their `items()` method. Users just flip a flag.

For custom change feeds, fine-grained lifecycle control, or implementing `LiveItemsView` on your own connector, see [Live Components](../advanced_topics/live_component.md).
