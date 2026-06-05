# Image search (CLIP + Qdrant, Rust)

Rust port of the Python [`image_search`](../../image_search) example. Embeds
images with the **CLIP ViT-B/32** vision tower and indexes them in Qdrant;
queries embed text with the matching CLIP text tower (same 512-dim space), so
you can search images by natural language.

Both embedders run locally via [`fastembed`](https://github.com/Anush008/fastembed-rs)
(ONNX, no Python) — the vision tower through `cocoindex::ops::image::ImageEmbedder`
and the text tower through `cocoindex::ops::sentence_transformers`.

## Prerequisites

- A running Qdrant (gRPC on `:6334`):
  ```bash
  docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant
  ```
  Override with `QDRANT_URL` (default `http://localhost:6334`).
- `protoc` on `PATH` (the `qdrant-client` crate compiles protobufs).
- Some images in `img/` (`.jpg/.jpeg/.png/.webp/.gif/.bmp`). The CLIP ONNX
  models are downloaded automatically on first run.

## Run

```bash
# Index every image under img/ (or a directory you pass).
cargo run -- index            # defaults to ./img
cargo run -- index /path/to/images

# Search by text — CLIP text vector against indexed image vectors.
cargo run -- query "a dog on a beach"
```

Re-running `index` is incremental: unchanged images are memo-skipped and removed
images are deleted from the collection.
