---
title: Quickstart
description: Get started with CocoIndex in 5 minutes
---

import { GitHubButton, DocumentationButton } from '@site/src/components/ActionButtons';

# Quickstart

In this tutorial, we'll build a simple app that converts PDF files to Markdown and saves them to a local directory.

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/v1/examples/pdf_to_markdown" />

## Overview

![App example showing PDF to Markdown conversion](/img/concept/app-example.svg)

1. Read PDF files from a local directory
2. Convert each file to Markdown using Docling
3. Save the Markdown files to an output directory (as **target states**)

You declare the transformation logic with native Python without worrying about changes.

Think: **target_state = transformation(source_state)**

When your source data is updated, or your processing logic is changed (for example, switching parsers or tweaking conversion settings), CocoIndex performs smart incremental processing that only reprocesses the minimum. And it keeps your Markdown files always up to date.

## Setup

1. Install CocoIndex and dependencies:

    ```bash
    pip install 'cocoindex>=1.0.0a1' docling
    ```

2. Create a new directory for your project:

    ```bash
    mkdir cocoindex-quickstart
    cd cocoindex-quickstart
    ```

3. Create a `pdf_files/` directory and add your PDF files:

    ```bash
    mkdir pdf_files
    ```
    You can download sample PDF files from the [git repo](https://github.com/cocoindex-io/cocoindex/tree/v1/examples/pdf_to_markdown).

4. Create a `.env` file to configure the database path:

    ```bash
    echo "COCOINDEX_DB=./cocoindex.db" > .env
    ```

## Define the app

![App definition](/img/quickstart/app-def.svg)

Create a new file `main.py`:

```python title="main.py"
import pathlib

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher
from docling.document_converter import DocumentConverter

app = coco_aio.App(
    "PdfToMarkdown",
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
```
This defines a CocoIndex App — the top-level runnable unit in CocoIndex.

<DocumentationButton url="/docs-v1/programming_guide/app" text="CocoIndex App" />

### Define the main function

![Processing components](/img/quickstart/components.svg)

```python title="main.py"
@coco.function
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await coco_aio.mount_each(process_file, files.items(), outdir)
```

`mount_each()` mounts one processing component per file. Each item from `files.items()` is a `(key, file)` pair — the key (the file's relative path) becomes the component subpath automatically.

It's up to you to pick the process granularity — it can be at directory level, at file level, or at page level. In this example, because we want to independently convert each file to Markdown, the file level is the most natural choice.

<DocumentationButton url="/docs-v1/programming_guide/processing_component" text="Processing Component" />

### Define file processing

![File processing](/img/quickstart/file-process.svg)

This function converts a single PDF to Markdown:

```python title="main.py"
_converter = DocumentConverter()

@coco.function(memo=True)
def process_file(
    file: localfs.File,
    outdir: pathlib.Path,
) -> None:
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()
    outname = file.file_path.path.stem + ".md"
    localfs.declare_file(outdir / outname, markdown, create_parent_dirs=True)
```

- **`memo=True`** — Caches results; unchanged files are skipped on re-runs
- **`localfs.declare_file()`** — Declares a file target state; auto-deleted if source is removed

<DocumentationButton url="/docs-v1/programming_guide/function" text="Function" />
<DocumentationButton url="/docs-v1/programming_guide/target_state" text="Target State" />

## Run the pipeline

Run the pipeline:

```bash
cocoindex update main.py
```

CocoIndex will:

1. Create the `out/` directory
2. Convert each PDF in `pdf_files/` to Markdown in `out/`

Check the output:

```bash
ls out/
# example.md (one .md file for each input PDF)
```

## Incremental updates

The power of CocoIndex is **incremental processing**. Try these:

**Add a new file:**

Add a new PDF to `pdf_files/`, then run:

```bash
cocoindex update main.py
```

Only the new file is processed.

**Modify a file:**

Replace a PDF in `pdf_files/` with an updated version, then run:

```bash
cocoindex update main.py
```

Only the changed file is reprocessed.

**Delete a file:**

```bash
rm pdf_files/example.pdf
cocoindex update main.py
```

The corresponding Markdown file is automatically removed.

## Next steps

- Learn more about [Core Concepts](/programming_guide/core_concepts)
- Explore [Functions](/programming_guide/function) and memoization
- Understand [Target States](/programming_guide/target_state) and how they sync to external systems
- Browse more [examples](https://github.com/cocoindex-io/cocoindex/tree/v1/examples)
