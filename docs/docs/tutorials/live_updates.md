---
title: Live Updates
description: "Keep your indexes up-to-date with live updates in CocoIndex."
---

# Live Updates

CocoIndex is designed to keep your indexes synchronized with your data sources. This is achieved through a feature called **live updates**, which automatically detects changes in your sources and updates your indexes accordingly. This ensures that your search results and data analysis are always based on the most current information.

## How Live Updates Work

Live updates in CocoIndex can be triggered in two main ways:

1.  **Refresh Interval:** You can configure a `refresh_interval` for any data source. CocoIndex will then periodically check the source for any new, updated, or deleted data. This is a simple and effective way to keep your index fresh, especially for sources that don't have a built-in change notification system.

2.  **Change Capture Mechanisms:** Some data sources offer more sophisticated ways to track changes. For example:
    *   **Amazon S3:** You can configure an SQS queue to receive notifications whenever a file is added, modified, or deleted in your S3 bucket. CocoIndex can listen to this queue and trigger an update instantly.
    *   **Google Drive:** The Google Drive source can be configured to poll for recent changes, which is more efficient than a full refresh.

When a change is detected, CocoIndex performs an **incremental update**. This means it only re-processes the data that has been affected by the change, without having to re-index your entire dataset. This makes the update process fast and efficient.

Here's an example of how to set up a source with a `refresh_interval`:

```python
@cocoindex.flow_def(name="LiveUpdateExample")
def live_update_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # Source: local files in the 'data' directory
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data"),
        refresh_interval=cocoindex.timedelta(seconds=5),
    )
    # ...
```

By setting `refresh_interval` to 5 seconds, we're telling CocoIndex to check for changes in the `data` directory every 5 seconds.

## Implementing Live Updates

You can enable live updates using either the CocoIndex CLI or the Python library.

### Using the CLI

To start a live update process from the command line, use the `update` command with the `-L` or `--live` flag:

```bash
cocoindex update -L your_flow_definition_file.py
```

This will start a long-running process that continuously monitors your data sources for changes and updates your indexes in real-time. You can stop the process by pressing `Ctrl+C`.

### Using the Python Library

For more control over the live update process, you can use the `FlowLiveUpdater` class in your Python code. This is particularly useful when you want to integrate CocoIndex into a larger application.

The `FlowLiveUpdater` can be used as a context manager, which automatically starts the updater when you enter the `with` block and stops it when you exit. The `wait()` method will block until the updater is aborted (e.g., by pressing `Ctrl+C`).

Here's how you can use `FlowLiveUpdater` to start and manage a live update process:

```python
import cocoindex

# Create a FlowLiveUpdater instance
with cocoindex.FlowLiveUpdater(live_update_flow, cocoindex.FlowLiveUpdaterOptions(print_stats=True)) as updater:
    print("Live updater started. Press Ctrl+C to stop.")
    # The updater runs in the background.
    # The wait() method blocks until the updater is stopped.
    updater.wait()

print("Live updater stopped.")
```

#### Getting Status Updates

You can also get status updates from the `FlowLiveUpdater` to monitor the update process. The `next_status_updates()` method blocks until there is a new status update.

```python
import cocoindex

updater = cocoindex.FlowLiveUpdater(live_update_flow)
updater.start()

while True:
    updates = updater.next_status_updates()

    if not updates.active_sources:
        print("All sources have finished processing.")
        break

    for source_name in updates.updated_sources:
        print(f"Source '{source_name}' has been updated.")

updater.wait()
```

This allows you to react to updates in your application, for example, by notifying users or triggering downstream processes.

## Example

Let's walk through an example of how to set up a live update flow. For the complete, runnable code, see the [live updates example](https://github.com/cocoindex-io/cocoindex/tree/main/examples/live_updates) in the CocoIndex repository.

### 1. Setting up the Source

The first step is to define a source and configure a `refresh_interval`. In this example, we'll use a `LocalFile` source to monitor a directory named `data`.

```python
@cocoindex.flow_def(name="LiveUpdateExample")
def live_update_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # Source: local files in the 'data' directory
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data"),
        refresh_interval=cocoindex.timedelta(seconds=5),
    )

    # Collector
    collector = data_scope.add_collector()
    with data_scope["documents"].row() as doc:
        collector.collect(filename=doc["filename"], content=doc["content"])

    # Target: Postgres database
    collector.export(
        "documents_index",
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename"]
    )
```

By setting `refresh_interval` to 5 seconds, we're telling CocoIndex to check for changes in the `data` directory every 5 seconds.

### 2. Running the Live Updater

Once the flow is defined, you can use the `FlowLiveUpdater` to start the live update process.

```python
def main():
    # Initialize CocoIndex
    cocoindex.init()

    # Setup the flow
    live_update_flow.setup(report_to_stdout=True)

    # Start the live updater
    with cocoindex.FlowLiveUpdater(live_update_flow, cocoindex.FlowLiveUpdaterOptions(print_stats=True)) as updater:
        print("Live updater started. Watching for changes in the 'data' directory.")
        updater.wait()

if __name__ == "__main__":
    main()
```

The `FlowLiveUpdater` will run in the background, and the `updater.wait()` call will block until the process is stopped.

## Conclusion

Live updates is a powerful feature of CocoIndex that ensures your indexes are always fresh. By using a combination of refresh intervals and source-specific change capture mechanisms, you can build responsive, real-time applications that are always in sync with your data.

For more detailed information on the `FlowLiveUpdater` and other live update options, please refer to the [Run a Flow documentation](https://cocoindex.io/docs/core/flow_methods#live-update).
