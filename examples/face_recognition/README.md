# Recognize faces in images and build embedding index
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)


In this example, we will recognize faces in images and build embedding index.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps
### Indexing Flow

1. We will ingest a list of images.
2. For each image, we:
   - Extract faces from the image.
   - Compute embeddings for each face.
3. We will export to the following tables in Postgres with PGVector:
   - Filename, rect, embedding for each face.


## Prerequisite

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Install Qdrant
    ```bash
    docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
    ```

3.  Install dependencies:

    ```bash
    pip install -e .
    ```

## Run

Update index, which will also setup the tables at the first time:

```bash
cocoindex update --setup main.py
```

You can also run the command with `-L`, which will watch for file changes and update the index automatically.

```bash
cocoindex update --setup -L main.py
```

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
