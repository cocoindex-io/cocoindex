---
title: Quickstart
description: Get started with CocoIndex in 5 minutes
---

import { GitHubButton, DocumentationButton } from '@site/src/components/ActionButtons';

# Quickstart

In this tutorial, we'll build a simple flow that converts Markdown files to HTML and saves them to a local directory.

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/v1/examples/files_transform" />

## Flow Overview

1. Read Markdown files from a local directory
2. Convert each file to HTML
3. Save the HTML files to an output directory (as **effects**)

CocoIndex automatically tracks changes — when you add, modify, or delete source files, only the affected outputs are updated.

## Setup

1. Install CocoIndex and dependencies:

    ```bash
    pip install 'cocoindex>=1.0.0a1' 'markdown-it-py[linkify,plugins]'
    ```

2. Create a new directory for your project:

    ```bash
    mkdir cocoindex-quickstart
    cd cocoindex-quickstart
    ```

3. Create a `data/` directory with a sample Markdown file:

    ```bash
    mkdir data
    ```

Add a sample file `data/hello.md`:

```markdown
# Hello World

This is a simple **Markdown** file.

- Item 1
- Item 2
- Item 3
```

## Define the App

Create a new file `main.py`:

```python
import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")
```

### Configure the Environment

Use `@coco.lifespan` to configure CocoIndex settings:

```python
@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield
```

This sets up a local database (`cocoindex.db`) for incremental processing.

### Define File Processing

Use `@coco.function` with `memo=True` to create a memoized function that processes each file:

```python
@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(scope, filename=outname, content=html)
```

Key concepts:

- **`scope`**: A handle that carries the stable path and context for declaring effects
- **`memo=True`**: Skips recomputation when inputs haven't changed
- **`target.declare_file()`**: Declares an **effect** — the desired state of an output file

<DocumentationButton url="/docs-v1/programming_guide/function" text="Function" />
<DocumentationButton url="/docs-v1/programming_guide/effect" text="Effect" />

### Define the Main Function

```python
@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    # Declare the output directory effect and get a target provider
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", outdir
    ).result()

    # Walk source files and mount a processing unit for each
    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for f in files:
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, target)
```

Key concepts:

- **`coco.mount_run()`**: Mounts a processing unit and waits for its result (the directory target)
- **`coco.mount()`**: Mounts a processing unit for each file to process
- **`scope / "process" / ...`**: Creates a stable path to identify each processing unit

<DocumentationButton url="/docs-v1/programming_guide/processing_unit" text="Processing Unit" />

### Create the App

```python
app = coco.App(
    app_main,
    coco.AppConfig(name="FilesTransform"),
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)
```

## Run the Pipeline

Run the pipeline:

```bash
cocoindex update main.py
```

CocoIndex will:

1. Create the `output_html/` directory
2. Convert `data/hello.md` to `output_html/hello.md.html`

Check the output:

```bash
ls output_html/
# hello.md.html
```

## Incremental Updates

The power of CocoIndex is **incremental processing**. Try these:

**Add a new file:**

```bash
echo "# New File" > data/world.md
cocoindex update main.py
```

Only the new file is processed.

**Modify a file:**

```bash
echo "# Updated Hello" > data/hello.md
cocoindex update main.py
```

Only the changed file is reprocessed.

**Delete a file:**

```bash
rm data/hello.md
cocoindex update main.py
```

The corresponding HTML is automatically removed.

## Next Steps

- Learn more about [Core Concepts](/programming_guide/core_concepts)
- Explore [Functions](/programming_guide/function) and memoization
- Understand [Effects](/programming_guide/effect) and how they sync to external systems
- Browse more [examples](https://github.com/cocoindex-io/cocoindex/tree/v1/examples)
