# Build embedding index from PDF files and query with natural language
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)


In this example, we will build index flow for text embedding from local PDF files, and query the index.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps
### Indexing Flow

<img width="662" alt="PDF indexing flow" src="https://github.com/user-attachments/assets/5e132dd9-7120-4b28-bc57-88d6b5583ef4" />

1. We will ingest a list of PDF files.
2. For each file:
   - convert it to markdown, and then
   - perform chunking (recursively split) and then embed each chunk.
3. We will save the embeddings and the metadata in Postgres with PGVector.

### Query
We will match against user-provided text by a SQL query, and reuse the embedding operation in the indexing flow.



## Prerequisite
[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
cocoindex setup main.py
```

Update index:

```bash
cocoindex update main.py
```

Run:

```bash
python main.py
```

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
