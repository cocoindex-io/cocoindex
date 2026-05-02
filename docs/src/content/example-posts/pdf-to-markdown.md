---
title: PDF to Markdown
description: 'Convert PDF files to Markdown with incremental processing'
slug: pdf-to-markdown
image: https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/cover.png
tags: [pdf, custom-building-blocks]
last_reviewed: 2026-04-20
---

![PDF to Markdown](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/cover.png)

In this tutorial, we'll build a simple app that converts PDF files to Markdown and saves them to a local directory.


## Overview

![App example showing PDF to Markdown conversion](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/app-example.svg)


1. Read PDF files from a local directory
2. Convert each file to Markdown using Docling
3. Save the Markdown files to an output directory (as **target states**)

You declare the transformation logic with native Python without worrying about changes.

Think:
**target_state = transformation(source_state)**

When your source data is updated, or your processing logic is changed (for example, switching parsers or tweaking conversion settings), CocoIndex performs smart incremental processing that only reprocesses the minimum. And it keeps your Markdown files always up to date in production.

## Setup

1. Install CocoIndex and dependencies:

    ```bash
    pip install 'cocoindex>=1.0.0' docling
    ```

2. Create a new directory for your project:

    ```bash
    mkdir pdf-to-markdown
    cd pdf-to-markdown
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

Define a CocoIndex App — the top-level runnable unit in CocoIndex.

![App Definition](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/app-def.svg)

```python title="main.py"

from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher
from docling.document_converter import DocumentConverter

app = coco.App(
    coco.AppConfig(name="PdfToMarkdown"),
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
```

[→ CocoIndex App](/docs/programming_guide/app)

### Define the main function

![App Definition](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/components.svg)

In the main function, we walk through each file in the source directory and process it.

```python title="main.py"
@coco.function
def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )
    for f in files:
        coco.mount(
            coco.component_subpath("process", str(f.file_path.path)),
            process_file,
            f,
            outdir,
        )
```
For each file, `coco.mount()` mounts a processing component. It's up to you to pick the process granularity, for example it can be
- at directory level,
- at file level,
- at page level.

In this example, because we want to independently convert each file to Markdown, it is the most natural to pick it at the file level.

[→ Processing Component](/docs/programming_guide/processing_component)


### Define file processing

![File Process](https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/file-process.svg)

For a file, we use Docling to convert it to Markdown.

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

We use `@coco.function` with `memo=True` to create a memoized function that processes each file.

[→ Function](/docs/programming_guide/function)

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
