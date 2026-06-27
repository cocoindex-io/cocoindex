<p align="center">
  <a href="https://cocoindex.io/docs/examples/multi-format-indexing/" title="Index PDFs and images into one searchable Qdrant collection with ColPali + MaxSim — no text extraction, in plain async Python with CocoIndex">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/multi-format-indexing/cover.svg" alt="Index any format together with CocoIndex and ColPali — render every PDF page to an image, embed pages and standalone images alike into multi-vector embeddings, and retrieve the most relevant page from one Qdrant MaxSim collection, no text extraction" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Index PDFs and images <em>together</em>, no parsing.</h1>

<p align="center">
  <b>Render every PDF page to an image, embed pages and images alike with multi-vector ColPali, and retrieve the most relevant <em>page</em> with MaxSim — whatever format it came from.</b><br/>
  No OCR, no text extraction, no brittle per-format parsers — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/multi-format-indexing/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Real document sets are a mix — scanned reports, slide exports, screenshots, and PDFs all jumbled together. Parsing each format into clean text is brittle and loses the layout (tables, charts, figures) that often *is* the answer. This pipeline sidesteps parsing entirely: render every PDF page to an image, embed pages and standalone images alike with the multi-vector [ColPali](https://huggingface.co/vidore/colpali-v1.2) model, and store them in one [Qdrant](https://qdrant.tech/) collection. You declare the transformation in native Python — `target_state = transformation(source_state)` — the slow per-page inference runs on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/), and the Rust engine handles incremental processing, so adding a document embeds only its pages.

## How it works

A file fans out to **pages**, so the shape is *file → N pages → N points*:

- **Walk** a folder of PDFs and images (live), matching `.pdf` / `.jpg` / `.jpeg` / `.png`.
- **Split** each file into pages — a PDF renders to one image per page via [`pdf2image`](https://github.com/Belval/pdf2image); a standalone image is a single page; anything else is skipped.
- **Embed** every page with ColPali into a multi-vector and store one MaxSim Qdrant point per page, tagged with filename and page number.

One file-splitting function handles every format, and `process_file` fans each page out with [`coco.map`](https://cocoindex.io/docs/programming_guide/app/). Read it in [`main.py`](main.py):

```python
@coco.fn.as_async(runner=coco.GPU)
def file_to_pages(filename: str, content: bytes) -> list[Page]:
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type == "application/pdf":
        return [Page(page_number=i + 1, image=_to_png(img))
                for i, img in enumerate(convert_from_bytes(content, dpi=PDF_RENDER_DPI))]
    if mime_type and mime_type.startswith("image/"):
        return [Page(page_number=None, image=content)]
    return []

@coco.fn(memo=True)   # unchanged file is never re-rendered or re-embedded
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    filename = str(file.file_path.path)
    pages = await file_to_pages(filename, await file.read())
    await coco.map(process_page, pages, filename, target)   # one point per page
```

The Qdrant collection is declared with a `MultiVectorSchema` and `multivector_comparator="max_sim"`, so a text query is scored against the *best-matching patch* of each page — the same query reaches pages from PDFs and standalone images alike.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/multi-format-indexing/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the file-to-pages split, the GPU runner, the multi-vector MaxSim collection, and cross-format search.
</p>

## Why it's worth a star ⭐

- **One index, every format.** PDFs and images funnel into the same Qdrant collection through one `file_to_pages` path — a query reaches them all, no per-format retrievers.
- **No parsing, no OCR.** ColPali embeds the rendered *page image*, so tables, charts, and figures stay intact — exactly the layout that OCR-and-embed throws away.
- **MaxSim multi-vector retrieval.** A `MultiVectorSchema` + `max_sim` comparator scores a query against each page's best-matching patches, late-interaction style.
- **Fan-out done right.** A file expands to N pages with `coco.map`, each its own point keyed by `(filename, page)` — re-running reconciles cleanly instead of duplicating.
- **Incremental on a GPU runner.** Slow per-page inference runs on `coco.GPU`; `@coco.fn(memo=True)` means adding a document embeds only its pages and leaves the rest untouched.

## Run it

> Needs **Qdrant** plus the ColPali deps (`torch`, `transformers`, `pdf2image`). `pdf2image` needs **poppler** installed for PDF rendering (`brew install poppler` / `apt install poppler-utils`).

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Build the index** — the example ships a `source_files/` folder mixing PDFs (papers) and images (financial report pages). A PDF expands to one point per page (the sample BERT paper alone is 16 pages):

```sh
cocoindex update main        # or: cocoindex update -L main   (keep watching the folder)
```

**4. Search across formats** — embed a text query with ColPali; the same query reaches pages from PDFs and standalone images alike:

```sh
python main.py "revenue growth"
```

On the sample set, *"revenue growth"* ranks the two financial-report images at the top (Sweetgreen, then Restaurant Brands), above an unrelated healthcare page — MaxSim matching the query against the most relevant patches of each page, with zero text extraction.

---

<p align="center">
  If this made your mixed-format documents searchable, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/multi-format-indexing/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/multi_format_indexing" alt="" width="1" height="1" />
