<p align="center">
  <a href="https://cocoindex.io/docs/examples/image-search/" title="Search a folder of images by text with CocoIndex — CLIP embeddings into Qdrant, live, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/image-search/cover.svg" alt="Search images by text with CocoIndex and CLIP — embed every image into the same vector space as your words, store the vectors in Qdrant, and type 'long neck' to get the giraffe back, no tags or captions" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Search a photo folder by <em>meaning</em>, not tags.</h1>

<p align="center">
  <b>CLIP embeds images <em>and</em> text into the <em>same</em> vector space — so "long neck" lands next to the giraffe, with no captions, no labels, no manual tagging.</b><br/>
  Vectors live in Qdrant, the index runs live inside a FastAPI app, and it's all plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/image-search/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A folder of photos is searchable by *meaning* the moment you stop relying on filenames and tags. [CLIP](https://huggingface.co/openai/clip-vit-large-patch14) is the trick: it embeds an image and its caption into the *same* space, so a text query and a matching picture land near each other. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, the managed Qdrant collection) runs in a Rust engine underneath, in **live mode** inside the API server, so dropping a new photo into the folder updates the index within a second.

## How it works

The indexing path is short — there's no text to chunk, just one embedding per image:

- **Walk** a local image folder (live), matching `.jpg` / `.jpeg` / `.png`.
- **Embed** each image with the CLIP image encoder.
- **Store** the vector as a Qdrant point, keyed by a stable `uuid5` of the path, with the filename in the payload.

The whole point is one shared space: the **same** CLIP model embeds images at index time and text at query time, so a cosine search with a text vector finds the nearest *image* vectors. Each image runs as its own [processing component](https://cocoindex.io/docs/programming_guide/processing_component/), so delete a photo and its point is removed automatically. Read it in [`pipeline.py`](pipeline.py):

```python
@coco.fn(memo=True)   # unchanged image is never re-embedded
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    content = await file.read()
    embedding = embed_image_bytes(content)
    point = qdrant.PointStruct(
        id=_image_id(file.file_path.path),                  # uuid5 of the path — stable
        vector=embedding,
        payload={"filename": str(file.file_path.path)},
    )
    target.declare_point(point)

def embed_query(text: str) -> list[float]:                  # query side — same model, text encoder
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        out = model.get_text_features(**inputs)
    return _projected_features(out)[0].tolist()
```

`api.py` is a FastAPI app whose [lifespan](https://fastapi.tiangolo.com/advanced/events/) starts the flow in live mode, blocks startup until the initial sweep is `READY`, then keeps watching `img/` while serving `/search`. There's no separate "build the index" step.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/image-search/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the shared image-text space, the Qdrant collection setup, the live-mode API server, and the React frontend.
</p>

## Why it's worth a star ⭐

- **One model, two encoders.** CLIP embeds images at index time and text at query time into the *same* 768-d space — search matches by meaning, never by metadata.
- **Live by default.** The flow runs in [live mode](https://cocoindex.io/docs/programming_guide/live_mode/) inside the API server; drop a photo into `img/` and it's searchable within a second, no rebuild step.
- **Incremental & self-cleaning.** `@coco.fn(memo=True)` skips unchanged images; each photo is its own processing component, so deleting one removes its Qdrant point automatically.
- **Managed Qdrant target.** `mount_collection_target` creates and reconciles the collection — the vector size comes straight from `model.config.projection_dim`, so swapping CLIP variants just works.
- **Plain Python, your stack.** FastAPI + React + Qdrant, no DSL — the indexing logic is a handful of ordinary async functions.

## Run it

> Needs **Qdrant** (vector store) and the CLIP model deps (`torch`, `transformers`, `pillow`), all pulled in by `pip install -e .`.

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Run it as a service** — the example ships an `img/` folder (a cat, a dog, an elephant, a giraffe). The server runs the index in live mode in the background and blocks startup until the first sweep finishes, so there's no separate indexing command:

```sh
python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

**4. Open the frontend:**

```sh
cd frontend && npm install && npm run dev   # http://localhost:5173
```

Query *"long neck"* and the giraffe ranks first, then the other animals by CLIP similarity — none of which was ever tagged with a word. That's the whole point of a shared image-text space: the match is by *meaning*.

---

<p align="center">
  If this made your photo folder searchable, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/image-search/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/image_search" alt="" width="1" height="1" />
