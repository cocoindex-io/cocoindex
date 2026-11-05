<h1 align="center">Search the text <em>and</em> the pictures inside your PDFs.</h1>

<p align="center">
  <b>Split each PDF page into text and images, embed each with its own model, and store both in <em>Qdrant</em>.</b><br/>
  One question searches the passages and the pictures — incrementally, in plain async Python.
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

A PDF holds two kinds of content: prose and pictures. A text model cannot see a diagram, and an image model reads prose badly. This example gives each element type its own model:

- **Text**: SentenceTransformers [`all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) (384-dim), into the `PdfElementsText` collection.
- **Images**: CLIP [`openai/clip-vit-large-patch14`](https://huggingface.co/openai/clip-vit-large-patch14) (ViT-L/14, 768-dim), into the `PdfElementsImage` collection.

Every point carries its file name and page number, so a hit takes you back to the page.

## How it works

Read in [`main.py`](main.py):

1. `localfs.walk_dir` lists the PDFs in `source_files/`. `coco.mount_each` runs `process_file` once per file.
2. `extract_pdf_elements` reads each page with `pypdf`: the page text, plus every image of 16 px or more on a side, made into a thumbnail of at most 512×512.
3. `RecursiveSplitter` cuts the page text into chunks (600 characters, 100 overlap). `process_chunk` embeds each chunk and declares one point in the text collection.
4. `process_image` embeds each image with CLIP and declares one point in the image collection. The thumbnail goes in the payload (base64), so a UI can show the hit.
5. `qdrant.mount_collection_target` creates both collections. The text collection takes its vector size from the embedder; the image collection takes it from the CLIP config.

You declare the points. CocoIndex inserts, updates and deletes them to match the PDFs.

## Prerequisites

- Python 3.11 or later.
- A Qdrant server. Start one with Docker (HTTP on `6333`, gRPC on `6334`):

  ```sh
  docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
  ```

  The example connects over gRPC to `http://localhost:6334/`. Set `QDRANT_URL` in `.env` to use another server.
- About 2 GB of disk for the models. The first run downloads them from Hugging Face. A GPU is optional; CLIP runs on the CPU too, more slowly.

## Run it

**1. Configure and install:**

```sh
cp .env.example .env
pip install -e .
```

**2. Get sample PDFs.** The script downloads three board game rulebooks into `source_files/`:

```sh
./fetch_manuals.sh
```

You can also put your own PDFs in `source_files/`.

**3. Build the index:**

```sh
cocoindex update main          # catch-up: scan, sync, exit
cocoindex update -L main       # live: keep watching source_files/ for changes
```

**4. Query.** One question goes to both collections. The text collection gets a MiniLM vector; the image collection gets a CLIP text vector, so words find pictures:

```sh
python main.py "how do players trade goods?"
```

The output lists the top 5 text passages and the top 5 images, each with its score, file and page. You can also browse the points in the [Qdrant dashboard](http://localhost:6333/dashboard).

## What a re-run does

Run `cocoindex update main` again, and CocoIndex does only the work that changed:

- **No change**: `process_file` is memoized (`memo=True`). An unchanged PDF is skipped: no parse, no embedding, no write to Qdrant.
- **A PDF changes**: only that file is processed again. `process_image` is memoized too, so an image that did not change is not sent to CLIP again. Points for text or images that are gone are deleted.
- **A PDF is deleted**: all its points are deleted from both collections.
- **The text model changes**: `EMBEDDER` is a context key with `detect_change=True`, so all text is embedded again. `clip_embed_image` names `CLIP_MODEL` in `deps`, so a CLIP model change embeds all images again.

---

<p align="center">
  Want the pictures in your PDFs to be searchable too? <a href="https://github.com/cocoindex-io/cocoindex"><b>Give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>
