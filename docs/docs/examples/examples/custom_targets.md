---
title: Export markdown files to local Html with Custom Targets
description: Simple example to export Markdown files to local HTML files using Custom Targets.
sidebar_class_name: hidden
slug: /examples/custom_targets
canonicalUrl: '/examples/custom_targets'
sidebar_custom_props:
  image: /img/examples/custom_targets/cover.png
  tags: [custom-building-blocks]
image: /img/examples/custom_targets/cover.png
tags: [custom-building-blocks]
---
import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/custom_output_files" margin="0 0 24px 0" />

![Custom Targets](/img/examples/custom_targets/cover.png)

## Overview

Let’s walk through a simple example—exporting `.md` files as `.html` using a custom file-based target. This project monitors folder changes and continuously converts markdown to HTML incrementally. The overall flow is simple and primarily focuses on how to configure your custom target.


## Ingest files

Ingest a list of markdown files:

```python
@cocoindex.flow_def(name="CustomOutputFiles")
def custom_output_files(
flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
	data_scope["documents"] = flow_builder.add_source(
		cocoindex.sources.LocalFile(path="data", included_patterns=["*.md"]),
		refresh_interval=timedelta(seconds=5),
	)
```
This ingestion creates a table with `filename` and `content` fields.
<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" />

## Process each file and collect

Define custom function that converts markdown to HTML

```python
@cocoindex.op.function()
def markdown_to_html(text: str) -> str:
    return _markdown_it.render(text)
```

<DocumentationButton url="https://cocoindex.io/docs/custom_ops/custom_functions" text="Custom Function" margin="0 0 16px 0" />

Define data collector and transform each document to html.

```python
output_html = data_scope.add_collector()
with data_scope["documents"].row() as doc:
    doc["html"] = doc["content"].transform(markdown_to_html)
    output_html.collect(filename=doc["filename"], html=doc["html"])
```
![Convert markdown to html](/img/examples/custom_targets/convert.png)


##  Define the custom target

### Define the target spec

<DocumentationButton url="https://cocoindex.io/docs/custom_ops/custom_targets#target-spec" text="Target Spec" margin="0 0 16px 0" />

The target spec contains a directory for output files:

```python
class LocalFileTarget(cocoindex.op.TargetSpec):
    directory: str
```


### Implement the connector

<DocumentationButton url="https://cocoindex.io/docs/custom_ops/custom_targets#target-connector" text="Target Connector" margin="0 0 16px 0" />

`get_persistent_key()` defines the persistent key,
which uniquely identifies the target for change tracking and incremental updates. Here, we simply use the target directory as the key (e.g., `./data/output`).

```python
@cocoindex.op.target_connector(spec_cls=LocalFileTarget)
class LocalFileTargetConnector:
    @staticmethod
    def get_persistent_key(spec: LocalFileTarget, target_name: str) -> str:
        """Use the directory path as the persistent key for this target."""
        return spec.directory

```

The `describe()` method returns a human-readable string that describes the target, which is displayed in the CLI logs.
For example, it prints:

`Target: Local directory ./data/output`

```python
@staticmethod
def describe(key: str) -> str:
    """(Optional) Return a human-readable description of the target."""
    return f"Local directory {key}"
```

`apply_setup_change()` applies setup changes to the backend. The previous and current specs are passed as arguments,
and the method is expected to update the backend setup to match the current state.

A `None` spec indicates non-existence, so when `previous` is `None`, we need to create it,
and when `current` is `None`, we need to delete it.


```python
@staticmethod
def apply_setup_change(
    key: str, previous: LocalFileTarget | None, current: LocalFileTarget | None
) -> None:
    """
    Apply setup changes to the target.

    Best practice: keep all actions idempotent.
    """

    # Create the directory if it didn't exist.
    if previous is None and current is not None:
        os.makedirs(current.directory, exist_ok=True)

    # Delete the directory with its contents if it no longer exists.
    if previous is not None and current is None:
        if os.path.isdir(previous.directory):
            for filename in os.listdir(previous.directory):
                if filename.endswith(".html"):
                    os.remove(os.path.join(previous.directory, filename))
            os.rmdir(previous.directory)
```

The `mutate()` method is called by CocoIndex to apply data changes to the target,
batching mutations to potentially multiple targets of the same type.
This allows the target connector flexibility in implementation (e.g., atomic commits, or processing items with dependencies in a specific order).

Each element in the batch corresponds to a specific target and is represented by a tuple containing:
- the target specification
- all mutations for the target, represented by a `dict` mapping primary keys to value fields. Value fields can be represented by a dataclass—`LocalFileTargetValues` in this case:

```python
@dataclasses.dataclass
class LocalFileTargetValues:
    """Represents value fields of exported data. Used in `mutate` method below."""

    html: str
```

The value type of the `dict` is `LocalFileTargetValues | None`,
where a non-`None` value means an upsert and `None` value means a delete. Similar to `apply_setup_changes()`,
idempotency is expected here.

```python
@staticmethod
def mutate(
    *all_mutations: tuple[LocalFileTarget, dict[str, LocalFileTargetValues | None]],
) -> None:
    """
    Mutate the target.
    """
    for spec, mutations in all_mutations:
        for filename, mutation in mutations.items():
            full_path = os.path.join(spec.directory, filename) + ".html"
            if mutation is None:
                # Delete the file
                try:
                    os.remove(full_path)
                except FileNotFoundError:
                    pass
            else:
                # Create/update the file
                with open(full_path, "w") as f:
                    f.write(mutation.html)
```

### Use it in the Flow

```python
output_html.export(
    "OutputHtml",
    LocalFileTarget(directory="output_html"),
    primary_key_fields=["filename"],
)
```

## Run the example

```bash
pip install -e .
cocoindex update --setup main.py
```

You can add, modify, or remove files in the `data/` directory — CocoIndex will only reprocess the changed files and update the target accordingly.

For **real-time updates**, run in live mode:

```bash
cocoindex update --setup -L main.py
```

This keeps your knowledge graph continuously synchronized with your document source — perfect for fast-changing environments like internal wikis or technical documentation.

## Best Practices

- **Idempotency matters**: `apply_setup_change()` and `mutate()` should be safe to run multiple times without unintended effects.
- **Prepare once, mutate many**: If you need setup (such as establishing a connection), use `prepare()` to avoid repeating work.
- **Use structured types**: For primary keys or values, CocoIndex supports simple types as well as dataclasses and NamedTuples.

## Why Custom Targets?

### Integration with internal system
Sometimes there may be an internal/homegrown tool or API (e.g. within a company) that's not publicly available.
These can only be connected through custom targets.

### Faster adoption of new export logic
When a new tool, database, or API joins your stack, simply define a Target Spec and Target Connector — start exporting right away, with no pipeline refactoring required.
