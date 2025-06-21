# Build text embedding and semantic search 🔍 with Qdrant

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

CocoIndex supports Qdrant natively - [documentation](https://cocoindex.io/docs/ops/targets#qdrant). In this example, we will build index flow from text embedding from local markdown files, and query the index. We will use **Qdrant** as the vector database.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="860" alt="CocoIndex supports Qdrant" src="https://github.com/user-attachments/assets/a9deecfa-dd94-4b97-a1b1-90488d8178df" />

## Steps
### Indexing Flow
<img width="480" alt="Index flow for text embedding" src="https://github.com/user-attachments/assets/44d47b5e-b49b-4f05-9a00-dcb8027602a1" />

1. We will ingest a list of local files.
2. For each file, perform chunking (recursively split) and then embedding.
3. We will save the embeddings and the metadata in Postgres with PGVector.

### Query
We use Qdrant client to query the index, and reuse the embedding operation in the indexing flow.

## Pre-requisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one. Although the target store is Qdrant, CocoIndex uses Postgress to track the data lineage for incremental processing.

- Run Qdrant.

   ```bash
   docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
   ```

## Run

- Install dependencies:

   ```bash
   pip install -e .
   ```

- Setup:

   ```bash
   cocoindex setup main.py
   ```

   It will automatically create a collection in Qdrant.
   You can view the collections and data with the Qdrant dashboard at <http://localhost:6333/dashboard>.

- Update index:

   ```bash
   cocoindex update main.py
   ```

- Run:

   ```bash
   python main.py
   ```

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci main.py
```

Open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
