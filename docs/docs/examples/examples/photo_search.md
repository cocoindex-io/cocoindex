---
title: Photo Search with Face Detection
description: Covers extracting and embedding faces from images, structuring data for visual search, and exporting to a vector database for face similarity queries.
sidebar_class_name: hidden
slug: /examples/photo_search
canonicalUrl: '/examples/photo_search'
sidebar_custom_props:
  image: /img/examples/photo_search/cover.png
  tags: [vector-index, multi-modal]
image: /img/examples/photo_search/cover.png
tags: [vector-index, multi-modal]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/face_recognition" margin="0 0 24px 0" />

![Photo Search](/img/examples/photo_search/cover.png)

## Overview
We’ll walk through a comprehensive example of building a scalable face recognition pipeline. We’ll
- Detect all faces in the image and extract their bounding boxes
- Crop and encode each face image into a 128-dimensional face embedding
- Store metadata and vectors in a structured index to support queries like:
“Find all similar faces to this one” or “Search images that include this person”

With this, you can build your own photo search app with face detection and search.

## Flow Overview
![Flow Overview](/img/examples/photo_search/flow.png)

1. Ingest the images.
2. For each image,
    - Extract faces from the image.
    - Compute embeddings for each face.
3. Export following fields to a table in Postgres with PGVector:
    - Filename, rect, embedding for each face.

## Setup
- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

- Install Qdrant
    ```sh
    docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
    ```

- Install dependencies:
    ```sh
    pip install -e .
    ```

## Add source

We monitor an `images/` directory using the built-in `LocalFile` source. All newly added files are automatically processed and indexed.

```python
@cocoindex.flow_def(name="FaceRecognition")
def face_recognition_flow(flow_builder, data_scope):
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="images", binary=True),
        refresh_interval=datetime.timedelta(seconds=10),
    )
```

This creates a table with `filename` and `content` fields. 📂


You can connect it to your [S3 Buckets](https://cocoindex.io/docs/ops/sources#amazons3) (with SQS integration, [example](https://cocoindex.io/blogs/s3-incremental-etl))
or [Azure Blob store](https://cocoindex.io/docs/ops/sources#azureblob).

## Detect and Extract Faces

We use the `face_recognition` library under the hood, powered by dlib’s CNN-based face detector. Since the model is slow on large images, we downscale wide images before detection.

```python
@cocoindex.op.function(
    cache=True,
    behavior_version=1,
    gpu=True,
    arg_relationship=(cocoindex.op.ArgRelationship.RECTS_BASE_IMAGE, "content"),
)
def extract_faces(content: bytes) -> list[FaceBase]:
    orig_img = Image.open(io.BytesIO(content)).convert("RGB")

    # The model is too slow on large images, so we resize them if too large.
    if orig_img.width > MAX_IMAGE_WIDTH:
        ratio = orig_img.width * 1.0 / MAX_IMAGE_WIDTH
        img = orig_img.resize(
            (MAX_IMAGE_WIDTH, int(orig_img.height / ratio)),
            resample=Image.Resampling.BICUBIC,
        )
    else:
        ratio = 1.0
        img = orig_img

    # Extract face locations.
    locs = face_recognition.face_locations(np.array(img), model="cnn")

    faces: list[FaceBase] = []
    for min_y, max_x, max_y, min_x in locs:
        rect = ImageRect(
            min_x=int(min_x * ratio),
            min_y=int(min_y * ratio),
            max_x=int(max_x * ratio),
            max_y=int(max_y * ratio),
        )

        # Crop the face and save it as a PNG.
        buf = io.BytesIO()
        orig_img.crop((rect.min_x, rect.min_y, rect.max_x, rect.max_y)).save(
            buf, format="PNG"
        )
        face = buf.getvalue()
        faces.append(FaceBase(rect, face))

    return faces
```

We transform the image content:

```python
with data_scope["images"].row() as image:
    image["faces"] = image["content"].transform(extract_faces)
```

After this step, each image has a list of detected faces and bounding boxes.
Each detected face is cropped from the original image and stored as a PNG.

![Extracted Faces](/img/examples/photo_search/extraction.png)

## Compute Face Embeddings

We encode each cropped face using the same library. This generates a 128-dimensional vector representation per face.

```python
@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def extract_face_embedding(
    face: bytes,
) -> cocoindex.Vector[cocoindex.Float32, typing.Literal[128]]:
    """Extract the embedding of a face."""
    img = Image.open(io.BytesIO(face)).convert("RGB")
    embedding = face_recognition.face_encodings(
        np.array(img),
        known_face_locations=[(0, img.width - 1, img.height - 1, 0)],
    )[0]
    return embedding
```

We plug the embedding function into the flow:

```python
with image["faces"].row() as face:
    face["embedding"] = face["image"].transform(extract_face_embedding)
```

After this step, we have embeddings ready to be indexed!


## Collect and Export Embeddings

We now collect structured data for each face: filename, bounding box, and embedding.

```python
face_embeddings = data_scope.add_collector()

face_embeddings.collect(
    id=cocoindex.GeneratedField.UUID,
    filename=image["filename"],
    rect=face["rect"],
    embedding=face["embedding"],
)
```

We export to a Qdrant collection:

```python
face_embeddings.export(
    QDRANT_COLLECTION,
    cocoindex.targets.Qdrant(
        collection_name=QDRANT_COLLECTION
    ),
    primary_key_fields=["id"],
)
```

Now you can run cosine similarity queries over facial vectors.

CocoIndex supports 1-line switch with other vector databases.
<DocumentationButton url="https://cocoindex.io/docs/ops/targets#postgres" text="Postgres" />

## Query the Index

You can now build facial search apps or dashboards. For example:
- Given a new face embedding, find the most similar faces
- Find all face images that appear in a set of photos
- Cluster embeddings to group visually similar people


For querying embeddings, check out [Image Search project](https://cocoindex.io/blogs/live-image-search).

If you’d like to see a full example on the query path with image match, give it a shout at
[our group](https://discord.com/invite/zpA9S2DR7s).

## CocoInsight
CocoInsight is a tool to help you understand your data pipeline and data index. It can now visualize identified sections of an image based on the bounding boxes and makes it easier to understand and evaluate AI extractions - seamlessly attaching computed features in the context of unstructured visual data.

You can walk through the project step by step in [CocoInsight](https://www.youtube.com/watch?v=MMrpUfUcZPk) to see exactly how each field is constructed and what happens behind the scenes.

```sh
cocoindex server -ci main
```

Follow the url `https://cocoindex.io/cocoinsight`.  It connects to your local CocoIndex server, with zero pipeline data retention.
