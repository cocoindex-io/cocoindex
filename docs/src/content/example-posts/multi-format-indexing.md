---
title: Index Any Format Together
description: 'Index PDFs and images into one searchable Qdrant collection with CocoIndex V1 — render every PDF page to an image, embed pages and images with the multi-vector ColPali model, and retrieve the most relevant page with MaxSim, whatever format it came from.'
slug: multi-format-indexing
image: https://cocoindex.io/blobs/docs-v1/img/examples/multi-format-indexing/cover.png
tags: [multimodal, colpali]
---

![Index PDFs and images together with ColPali and CocoIndex](https://cocoindex.io/blobs/docs-v1/img/examples/multi-format-indexing/cover.png)

Real document sets are a mix — scanned reports, slide exports, screenshots, and PDFs all jumbled together. Parsing each format into clean text is brittle and loses the layout (tables, charts, figures) that often *is* the answer. In this tutorial we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that sidesteps parsing entirely: render every PDF page to an image, embed pages and standalone images alike with the multi-vector [ColPali](https://huggingface.co/vidore/colpali-v1.2) model, and store them in one [Qdrant](https://qdrant.tech/) collection. A text query then retrieves the most relevant *page*, no matter what format it started as.

The whole pipeline is ordinary `async` Python. The slow per-page model inference runs on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/), and the Rust engine handles [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) — add a document and only its pages get embedded.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/multi_format_indexing)

## Why ColPali (and multi-vector search)

A normal embedding squashes a whole page into one vector — fine for a paragraph, lossy for a dense report page with tables and figures. [ColPali](https://github.com/illuin-tech/colpali) instead emits a *bag* of vectors (one per image patch) and matches a query token-against-patch with **MaxSim**. The cost is more vectors per page; the payoff is retrieval that holds up on visually dense, text-heavy pages — exactly the documents that defeat plain OCR-and-embed.

## Flow overview

![CocoIndex flow: walk a folder of PDFs and images, render each PDF to per-page images, embed every page with ColPali, and store one multi-vector Qdrant point per page](https://cocoindex.io/blobs/docs-v1/img/examples/multi-format-indexing/flow-v1.png)

A file fans out to **pages**, so the shape is *file → N pages → N points*:

1. Walk a folder of PDFs and images (live).
2. Render each PDF to one image per page; an image is a single page.
3. Embed every page with ColPali and store one multi-vector Qdrant point per page, tagged with filename and page number.

## Split any file into pages

One function handles every format: PDFs go through [`pdf2image`](https://github.com/Belval/pdf2image), images pass through as a single page, anything else is skipped.

```python title="main.py"
@coco.fn.as_async(runner=coco.GPU)
def file_to_pages(filename: str, content: bytes) -> list[Page]:
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type == "application/pdf":
        return [
            Page(page_number=i + 1, image=_to_png(image))
            for i, image in enumerate(convert_from_bytes(content, dpi=PDF_RENDER_DPI))
        ]
    if mime_type and mime_type.startswith("image/"):
        return [Page(page_number=None, image=content)]
    return []
```

## Embed pages and fan out

`process_file` splits a file into pages, then maps each page through `process_file`'s helper, which embeds it with ColPali and declares one multi-vector Qdrant point:

```python title="main.py"
@coco.fn
async def process_page(page: Page, filename: str, target: qdrant.CollectionTarget) -> None:
    embedding = await embed_page(page.image)          # list[list[float]] — multi-vector
    target.declare_point(
        qdrant.PointStruct(
            id=_page_id(filename, page.page_number),
            vector=embedding,
            payload={"filename": filename, "page": page.page_number},
        )
    )


@coco.fn(memo=True)
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    pages = await file_to_pages(str(file.file_path.path), await file.read())
    await coco.map(process_page, pages, str(file.file_path.path), target)
```

`embed_page` runs the ColPali model (loaded once via `@functools.cache`) and returns a *list of* vectors — the multi-vector representation. [`coco.map`](https://cocoindex.io/docs/programming_guide/app/) fans out one `process_page` per page, and [`@coco.fn(memo=True)`](https://cocoindex.io/docs/programming_guide/function/) skips files that haven't changed.

## The multi-vector Qdrant collection

The collection is declared with a [`MultiVectorSchema`](https://cocoindex.io/docs/connectors/qdrant/) and a MaxSim comparator — that's what makes Qdrant score a query against the *best-matching patch* of each page:

```python title="main.py"
target_collection = await qdrant.mount_collection_target(
    QDRANT_DB,
    collection_name=QDRANT_COLLECTION,
    schema=await qdrant.CollectionSchema.create(
        vectors=qdrant.QdrantVectorDef(
            schema=MultiVectorSchema(
                vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=dim)
            ),
            distance="cosine",
            multivector_comparator="max_sim",
        )
    ),
)
```

## Run the pipeline

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
export QDRANT_URL="http://localhost:6334/"
pip install -e .          # cocoindex[colpali,qdrant], pdf2image, torch, … (needs poppler for PDFs)
cocoindex update main
```

The example ships a `source_files/` folder mixing PDFs (papers) and images (financial report pages). A PDF expands to one point per page — the sample BERT paper alone is 16 pages.

## Search across formats

Embed a text query with ColPali and search Qdrant; the same query reaches pages from PDFs and standalone images alike:

```sh
python main.py "revenue growth"
```

On the sample set, *"revenue growth"* ranks the two financial-report images at the top (Sweetgreen, then Restaurant Brands), above an unrelated healthcare page — MaxSim matching the query against the most relevant patches of each page, with zero text extraction.

## Incremental updates

- **Add a file** — only its pages are rendered and embedded; existing points are untouched.
- **Edit a file** — pages reconcile against what's in Qdrant; unchanged pages keep their points.
- **Delete a file** — every page from it is removed.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/multi_format_indexing](https://github.com/cocoindex-io/cocoindex/tree/main/examples/multi_format_indexing). For the image-only version with a web UI, see [Search Images by Text · ColPali](https://github.com/cocoindex-io/cocoindex/tree/main/examples/image_search_colpali); for a text-extraction pipeline over PDFs instead, see [Semantic Search over PDFs](https://cocoindex.io/docs/examples/pdf-embedding/).

Got a pile of mixed-format documents to make searchable? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
