<p align="center">
  <a href="https://cocoindex.io/docs/examples/pdf-to-markdown/" title="Convert a folder of PDFs to Markdown with docling and CocoIndex — incremental, files in and files out, in plain Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/pdf-to-markdown/cover.svg" alt="Convert PDFs to Markdown with CocoIndex — walk a folder of PDFs, convert each one to clean Markdown with docling, and write the .md files to an output folder that stays in sync with the source" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Convert a folder of PDFs to <em>Markdown</em>.</h1>

<p align="center">
  <b>Walk a directory of PDFs, convert each one to clean Markdown with <em>docling</em>, and write the <code>.md</code> files to an output folder that stays in sync.</b><br/>
  No database, no embeddings — just files in, files out, in plain Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/pdf-to-markdown/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Convert a folder of PDFs to Markdown and write the results to a second folder that stays in sync with the source. No database, no embeddings, no API keys — just files in, files out. You declare the transformation in native Python — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed file targets) runs in a Rust engine underneath, so switching parsers or replacing one PDF reprocesses only the minimum.

## How it works

A single docling `DocumentConverter` is built once and pinned to CPU for portability across machines. `process_file` runs once per PDF: it converts the file to Markdown, derives the output name by swapping the extension, and declares the `.md` file as a [target state](https://cocoindex.io/docs/programming_guide/target_state/). Read it in [`main.py`](main.py):

```python
_converter = DocumentConverter(
    format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=_pipeline_options)}
)

@coco.fn(memo=True)
def process_file(file: localfs.File, outdir: pathlib.Path) -> None:
    markdown = _converter.convert(file.file_path.resolve()).document.export_to_markdown()
    outname = file.file_path.path.stem + ".md"
    localfs.declare_file(outdir / outname, markdown, create_parent_dirs=True)

@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await coco.mount_each(process_file, files.items(), outdir)
```

`mount_each` runs one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per PDF, so the engine tracks and updates each file independently — it's up to you to pick the granularity (directory, file, or page); file level is the natural choice here.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/pdf-to-markdown/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the App definition, the docling converter, the per-file component, and incremental updates.
</p>

## Why it's worth a star ⭐

- **Clean Markdown, not raw text dumps.** docling preserves headings, tables, and reading order — the structure that makes the output actually usable.
- **Managed file targets.** `localfs.declare_file` describes the file you *want to exist*; CocoIndex writes it, overwrites it when the source changes, and deletes its `.md` when the source PDF is gone.
- **Incremental by default.** `@coco.fn(memo=True)` skips a PDF whose content and code are unchanged, so docling never re-parses a file you've already converted — add one PDF and only that file is processed.
- **Pick your granularity.** `mount_each` mounts one component per file here, but the same shape works at directory or page level — your choice.
- **No services, runs anywhere.** Pure local CPU processing, no database or API keys to set up.

## Run it

**1. Install:**

```sh
pip install -e .
```

**2. Add some PDFs** — the example ships a `pdf_files/` folder (the "Attention Is All You Need" paper), or drop your own in. The `.env` sets `COCOINDEX_DB=./cocoindex.db` for internal state.

**3. Convert** — writes Markdown into `out/`, one `.md` per input PDF:

```sh
cocoindex update main
```

**4. Check the output:**

```sh
ls out/      # e.g. 1706.03762v7.md
```

Add, replace, or delete a PDF in `pdf_files/` and re-run `cocoindex update main` — only the changed file is reprocessed, and a removed PDF's `.md` is deleted automatically.

---

<p align="center">
  If this saved you a parsing pipeline, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/pdf-to-markdown/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/pdf_to_markdown" alt="" width="1" height="1" />
