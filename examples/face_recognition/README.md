<p align="center">
  <a href="https://cocoindex.io/docs/examples/face-recognition/" title="Build your own photo face search with CocoIndex — detect every face, embed with face_recognition (dlib), index in Qdrant, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/face-recognition/cover.svg" alt="Build your own photo face search with CocoIndex — detect every face in a folder of photos, embed each into a 128-d vector with face_recognition (dlib), and index them in Qdrant so a query face finds the same person across photos" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Search your photos <em>by face</em>.</h1>

<p align="center">
  <b>Detect every face in a folder of photos, embed each into a 128-d vector with <code>face_recognition</code> (dlib), and index them in Qdrant — then a query face finds the same person across photos.</b><br/>
  The core of "find every photo of this person," with no labels or tags — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/face-recognition/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A folder of group photos has every person hiding in plain sight — the same face shows up across shots, but that knowledge is locked in pixels. This pipeline makes it searchable: detect every face, crop it, embed it into a 128-d vector with [`face_recognition`](https://github.com/ageitgey/face_recognition) (dlib), and index the faces in [Qdrant](https://qdrant.tech/). You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — while incremental processing, change tracking, and the managed Qdrant collection run in a Rust engine underneath, and the slow detection/embedding steps run on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/) instead of blocking the event loop.

## How it works

Unlike a one-embedding-per-image index, an image here fans out to **many** faces — so the shape is *image → N faces → N points*:

- **Walk** a local image folder (live), matching `.jpg` / `.jpeg` / `.png`.
- **Detect** every face in each image (CNN detector, downscaling large images first), and crop it.
- **Embed** each face into a 128-d vector and store one Qdrant point per face, keyed by `(filename, bounding box)`, with the source filename and box in the payload.

The dlib calls are synchronous and CPU/GPU-heavy, so each is wrapped with `@coco.fn.as_async(runner=coco.GPU)`. `process_file` detects a photo's faces, then maps each through `process_face` with [`coco.map`](https://cocoindex.io/docs/programming_guide/app/). Read it in [`main.py`](main.py):

```python
@coco.fn
async def process_face(face: Face, filename: str, target: qdrant.CollectionTarget) -> None:
    embedding = await embed_face(face.image)
    target.declare_point(
        qdrant.PointStruct(
            id=_face_id(filename, face.rect),     # uuid5 of (filename, box) — stable
            vector=embedding,
            payload={"filename": filename, "min_x": face.rect.min_x, "min_y": face.rect.min_y,
                     "max_x": face.rect.max_x, "max_y": face.rect.max_y},
        )
    )

@coco.fn(memo=True)   # unchanged photo is never re-detected
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    faces = await extract_faces(await file.read())
    await coco.map(process_face, faces, str(file.file_path.path), target)
```

The collection is sized to the 128-d face vector with **Euclidean** distance — dlib's own rule of thumb is that a distance under ~0.6 means "same person."

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/face-recognition/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with face detection and embedding, the image → faces fan-out, the Euclidean Qdrant collection, and searching by face.
</p>

## Why it's worth a star ⭐

- **Image → many faces.** Each photo fans out to one Qdrant point per detected face with `coco.map` — the multi-face equivalent of chunking a document.
- **Recognition without labels.** dlib's 128-d encodings put the same person close together; a Euclidean search under ~0.6 means "same person," with no tags or training.
- **The box travels with the match.** Each point's payload carries the bounding box, so a search hit tells you *where* in the source image the face is.
- **Incremental & self-cleaning.** `@coco.fn(memo=True)` skips unchanged photos; each image is its own processing component, so deleting a photo removes all its faces from Qdrant automatically.
- **Heavy work off the event loop.** CNN detection and embedding run on a `coco.GPU` runner; large images are downscaled for detection, then boxes are mapped back to full size.

## Run it

> Needs **Qdrant** plus the `face_recognition` library (it depends on **dlib** — see its [install notes](https://github.com/ageitgey/face_recognition#installation) if the build needs CMake/boost).

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Build the index** — the example ships a handful of famous group photos in `images/` (the 1927 Solvay physics conference, Steve Jobs & Bill Gates, …):

```sh
cocoindex update main        # or: cocoindex update -L main   (keep watching the folder)
```

On the sample set this indexes **36 faces** — 29 from the Solvay conference alone — each a Qdrant point keyed by `(filename, bounding box)`.

**4. Search by face** — embed a query face the same way and find the nearest indexed faces:

```sh
python main.py query images/einplanck3.jpg
```

Because Einstein appears in *both* the Einstein–Planck photo and the Solvay conference, the query pulls his Solvay face back as a close match — a Euclidean distance around `0.46`, comfortably under dlib's ~0.6 same-person threshold. That's face recognition across photos, with no labels or tags.

---

<p align="center">
  If this made your photo library searchable by face, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/face-recognition/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/face_recognition" alt="" width="1" height="1" />
