# ColPali multi-vector image search (Qdrant, Rust)

Rust port of the Python [`image_search_colpali`](../../image_search_colpali)
example. Embeds each image with **ColPali** into a *list* of vectors
(late-interaction), indexes them in a Qdrant **MAX_SIM multi-vector** collection
(`cocoindex::qdrant::CollectionSchema::multivector` +
`declare_multivector_point`), and searches with `qdrant::multivector_search`.

ColPali has no pure-Rust model, so inference is offloaded to a small external
ColPali HTTP service; the indexing pipeline and the Qdrant multi-vector wiring
are native Rust.

## Prerequisites

- A running Qdrant (gRPC `:6334`): `docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant`
  (override with `QDRANT_URL`).
- `protoc` on `PATH` (`qdrant-client` compiles protobufs).
- A ColPali HTTP service on `COLPALI_URL` (default `http://localhost:8000`)
  exposing the contract below.
- Images in `img/`.

### ColPali service contract

```
POST /embed-image   body: raw image bytes      -> {"embedding": [[f32; 128]; N]}
POST /embed-query   body: {"query": "<text>"}  -> {"embedding": [[f32; 128]; M]}
```

Reference server (Python, mirrors the Python example's model usage):

```python
# pip install colpali-engine fastapi uvicorn pillow torch
import io
from fastapi import FastAPI, Request
from PIL import Image
import torch
from colpali_engine import ColPali, ColPaliProcessor
from colpali_engine.utils.torch_utils import get_torch_device, unbind_padded_multivector_embeddings

model = ColPali.from_pretrained("vidore/colpali-v1.2")
proc = ColPaliProcessor.from_pretrained("vidore/colpali-v1.2")
device = get_torch_device("auto"); model = model.to(device).eval()
app = FastAPI()

def post(emb): return unbind_padded_multivector_embeddings(
    emb, padding_side=getattr(proc.tokenizer, "padding_side", "right"))[0].cpu().tolist()

@app.post("/embed-image")
async def embed_image(request: Request):
    img = Image.open(io.BytesIO(await request.body())).convert("RGB")
    batch = proc.process_images([img]).to(device)
    with torch.no_grad(): emb = model(**batch)
    return {"embedding": post(emb)}

@app.post("/embed-query")
async def embed_query(body: dict):
    batch = proc.process_queries(texts=[body["query"]]).to(device)
    with torch.no_grad(): emb = model(**batch)
    return {"embedding": post(emb)}
# uvicorn server:app --port 8000
```

## Run

```bash
cargo run -- index             # defaults to ./img
cargo run -- query "an invoice with a total amount"
```

Re-running `index` is incremental: unchanged images are memo-skipped; removed
images are deleted from the collection.
