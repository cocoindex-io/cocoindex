---
title: Transform a *Folder of Files*
description: 'The smallest end-to-end CocoIndex V1 pipeline — watch a directory of Markdown, render each file to HTML with markdown-it-py, and write the .html outputs to a local folder incrementally. Plain async Python, no external services.'
slug: files-transform
image: https://cocoindex.io/blobs/docs-v1/img/examples/files-transform/cover.png
tags: [file-transform, incremental]
---

![Transform a folder of files with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/files-transform/cover.png)

We'll take a folder of Markdown files and render each one to HTML, writing the results to a second folder that stays in sync with the source. No database, no embeddings, no API keys — just files in, files out. It's the smallest complete CocoIndex pipeline, and the cleanest way to see the **source → transform → target** shape that every larger example is built from.

The transform is your own ordinary `async` function. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, watching the directory, and keeping the output folder in sync — runs in a Rust engine underneath, so only the files that actually changed get re-rendered and re-written.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/files_transform)

## Flow overview

![CocoIndex files transform flow: watch a directory of Markdown, render each file to HTML with markdown-it-py, and write the .html outputs to a local folder](https://cocoindex.io/blobs/docs-v1/img/examples/files-transform/flow-v1.png)

From a high level, these are the steps:

1. Read Markdown files from a local directory, [watching for changes](https://cocoindex.io/docs/programming_guide/live_mode/).
2. Render each file to HTML with [markdown-it-py](https://github.com/executablebooks/markdown-it-py).
3. Write each `.html` file to an output folder (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)) on the [local filesystem](https://cocoindex.io/docs/connectors/localfs/).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Process a file

![One processing component per file: each Markdown file is rendered to HTML and written as a file target on the local filesystem](https://cocoindex.io/blobs/docs-v1/img/examples/files-transform/stage-file-process.png)

`process_file` runs once per file. It reads the Markdown, renders it to HTML, derives an output name from the source path, and declares the output file as a target state.

```python title="main.py"
import pathlib

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


@coco.fn(memo=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    localfs.declare_file(outdir / outname, html, create_parent_dirs=True)
```

The transform itself is just two lines: read the text, render it. The output name joins the source path parts with `__` so `subdir/file.md` becomes `subdir__file.html` — a flat, collision-free name in the output folder.

[`localfs.declare_file`](https://cocoindex.io/docs/connectors/localfs/) declares the `.html` file as a [target state](https://cocoindex.io/docs/programming_guide/target_state/) on the local filesystem. You describe the file you *want to exist*; CocoIndex handles writing it, overwriting it when the content changes, and deleting it when the source Markdown is gone.

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a file's content and this function's code are both unchanged, the whole file is skipped on the next run, and its HTML output is left exactly as it is.

## Define the main function

`app_main` wires the source to the target. It walks the source directory for Markdown files and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), outdir)
```

[`walk_dir`](https://cocoindex.io/docs/connectors/localfs/) lists the source folder, filtered to `*.md` by the [`PatternFilePathMatcher`](https://cocoindex.io/docs/connectors/localfs/). `live=True` makes the [filesystem source](https://cocoindex.io/docs/connectors/localfs/) [watch for changes](https://cocoindex.io/docs/programming_guide/live_mode/), and [`mount_each`](https://cocoindex.io/docs/programming_guide/processing_component/) runs one component per file so the engine can track and update each one independently — add, edit, or delete a Markdown file and only that file's HTML moves.

## Create the App

Bind `app_main` into a [`coco.App`](https://cocoindex.io/docs/programming_guide/app/), pointing it at the source folder and the output folder.

```python title="main.py"
app = coco.App(
    coco.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)
```

That is the entire pipeline — about 25 lines.

## Setup

- No external services required. Install CocoIndex and markdown-it-py:

  ```sh
  pip install -U cocoindex "markdown-it-py[linkify,plugins]"
  ```

- A few `.md` files to convert. Grab the [sample files](https://github.com/cocoindex-io/cocoindex/tree/main/examples/files_transform/data) from the repo, or drop your own notes into a `data/` directory.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build the output folder. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching for file changes
cocoindex update -L main
```

The converted files appear in `./output_html/`, one `.html` per source `.md`.

## Incremental updates

CocoIndex keeps the output folder in sync with your source files and does the **minimum work** to get there. You never compute a diff or write update logic: you change something, and CocoIndex works out exactly what to re-render and re-write. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged. `localfs.declare_file` decides what to *write* — the output file is created, overwritten, or deleted to match the declared target state.

- **A file is added** — only that file is rendered, and its `.html` is written. The rest is untouched.
- **A file is edited** — it is re-rendered and its `.html` is overwritten in place.
- **A file is deleted** — its `.html` output is removed from the target folder automatically.

The same machinery covers **logic** changes too: change the markdown-it preset or the output naming, and CocoIndex compares the new output against what is already on disk and applies only the difference. A catch-up run (`cocoindex update main`) does this once and exits; live mode (`cocoindex update -L main`) keeps watching and applies each change with low latency.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/files_transform](https://github.com/cocoindex-io/cocoindex/tree/main/examples/files_transform). This is the minimal building block — once it clicks, swap the transform for chunking and embedding and you have [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/), or point the same flow at a Postgres or vector target.

If this helped, [give CocoIndex a star on GitHub](https://github.com/cocoindex-io/cocoindex) and come say hi in our [Discord](https://discord.com/invite/zpA9S2DR7s) — we'd love to see what you build.
