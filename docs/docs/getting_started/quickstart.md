---
title: Quickstart
description: Get started with CocoIndex in 5 minutes
---

import { GitHubButton, DocumentationButton } from '@site/src/components/ActionButtons';
import useBaseUrl from '@docusaurus/useBaseUrl';

# Quickstart

In this tutorial, we'll build a simple app that converts PDF files to Markdown and saves them to a local directory.

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/v1/examples/pdf_to_markdown" />

## Overview

![App example showing PDF to Markdown conversion](/img/concept/app-example.svg)


1. Read PDF files from a local directory
2. Convert each file to Markdown using Docling
3. Save the Markdown files to an output directory (as **target states**)

CocoIndex automatically tracks changes — when you add, modify, or delete source files, only the affected outputs are updated.

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
you can download the pdf files from the [git repo](https://github.com/cocoindex-io/cocoindex/tree/v1/examples/pdf_to_markdown).

## Define the App
![App Definition](/img/quickstart/app-def.svg)

Create a new file `main.py`:

```python
import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher
from docling.document_converter import DocumentConverter

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield

app = coco.App(
    app_main,
    coco.AppConfig(name="PdfToMarkdown"),
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
```
This defines a CocoIndex App.
- An App is the top-level runnable unit in CocoIndex.
- Use `@coco.lifespan` to configure CocoIndex settings: sets up a local database (`cocoindex.db`) for incremental processing.

<DocumentationButton url="/docs-v1/programming_guide/app" text="CocoIndex App" />

### Define the Main Function

![App Definition](/img/quickstart/components.svg)

```python
@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    # Declare the output directory target state and get a target provider
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", outdir
    ).result()

    # Walk source files and mount a processing component for each
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )
    for f in files:
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, target)
```
**`coco.mount()`**: Mounts a processing component for each file to process

<DocumentationButton url="/docs-v1/programming_guide/processing_component" text="Processing Component" />


### Define File Processing

![File Process](/img/quickstart/file-process.svg)

Use `@coco.function` with `memo=True` to create a memoized function that processes each file:

```python
_converter = DocumentConverter()

@coco.function(memo=True)
def process_file(
    scope: coco.Scope,
    file: localfs.File,
    target: localfs.DirTarget,
) -> None:
    markdown = _converter.convert(file.path).document.export_to_markdown()
    outname = file.relative_path.stem + ".md"
    target.declare_file(scope, filename=outname, content=markdown)
```


<DocumentationButton url="/docs-v1/programming_guide/function" text="Function" />
<DocumentationButton url="/docs-v1/programming_guide/target_state" text="Target State" />




## Run the Pipeline

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

## Incremental Updates

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

## Next Steps

- Learn more about [Core Concepts](/programming_guide/core_concepts)
- Explore [Functions](/programming_guide/function) and memoization
- Understand [Target States](/programming_guide/target_state) and how they sync to external systems
- Browse more [examples](https://github.com/cocoindex-io/cocoindex/tree/v1/examples)
