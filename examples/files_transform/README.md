<p align="center">
  <a href="https://cocoindex.io/docs/examples/files-transform/" title="The smallest end-to-end CocoIndex pipeline — watch a folder of Markdown, render each file to HTML with markdown-it-py, and write the outputs incrementally, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/files-transform/cover.svg" alt="Transform a folder of files with CocoIndex — watch a directory of Markdown, render each file to HTML with markdown-it-py, and write the .html outputs to a local folder that stays in sync with the source" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">The smallest <em>source → transform → target</em> pipeline.</h1>

<p align="center">
  <b>Watch a folder of Markdown, render each file to HTML with <em>markdown-it-py</em>, and write the <code>.html</code> outputs to a folder that stays in sync.</b><br/>
  No database, no embeddings, no API keys — files in, files out, in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/files-transform/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Take a folder of Markdown files, render each one to HTML, and write the results to a second folder that stays in sync with the source. It's the smallest complete CocoIndex pipeline, and the cleanest way to see the **source → transform → target** shape that every larger example is built from. You declare the transformation in native Python — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, watching the directory, keeping the output folder in sync) runs in a Rust engine underneath, so only the files that actually changed get re-rendered.

## How it works

The whole pipeline is about 25 lines. `process_file` reads the Markdown, renders it to HTML, derives a flat output name from the source path, and declares the output file as a target state; `app_main` walks the source folder for `*.md` and mounts one component per file. Read all of [`main.py`](main.py):

```python
_markdown_it = MarkdownIt("gfm-like")

@coco.fn(memo=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    localfs.declare_file(outdir / outname, html, create_parent_dirs=True)

@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), outdir)
```

The transform itself is just two lines: read the text, render it. The output name joins the source path parts with `__`, so `subdir/file.md` becomes `subdir__file.html` — a flat, collision-free name. `localfs.declare_file` describes the file you *want to exist*; CocoIndex writes it, overwrites it on change, and deletes it when the source Markdown is gone.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/files-transform/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the transform, the main function, the App, and how incremental updates work.
</p>

## Why it's worth a star ⭐

- **The whole shape, minimized.** Source → transform → target in ~25 lines, no database or embeddings — once it clicks, every larger example reads the same way.
- **Your transform is just a function.** `_markdown_it.render` is plain Python; swap it for any function and you have a different pipeline.
- **Managed file targets.** `localfs.declare_file` handles writing, overwriting on change, and deleting the `.html` when the source `.md` disappears — you never write file I/O glue.
- **Incremental by default.** `@coco.fn(memo=True)` skips a file whose content and code are unchanged; add, edit, or delete one Markdown file and only that file's HTML moves.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and it keeps watching the directory, applying each change with low latency.

## Run it

**1. Install** (no external services required):

```sh
pip install -e .
```

**2. Add some Markdown** — the example ships a `data/` folder of sample files, or drop your own in. The `.env` sets `COCOINDEX_DB=./cocoindex.db` for internal state.

**3. Build the output folder** — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
cocoindex update main        # catch-up
cocoindex update -L main     # live: keep watching for file changes
```

The converted files appear in `./output_html/`, one `.html` per source `.md` (named by the source path parts joined with `__`, e.g. `subdir__file.html`).

**4. Try incremental updates** — add, edit, or delete a `.md` in `data/` and re-run: only the changed file is re-rendered, and a removed source's `.html` is deleted automatically.

---

<p align="center">
  If this clicked, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/files-transform/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/files_transform" alt="" width="1" height="1" />
