# Build embedding index from PDF files and query with natural language
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)


In this example, we will build a bunch of tables for papers in PDF files, including:

- Metadata (title, authors, abstract) for each paper.
- Author-to-paper mapping, for author-based query.
- Embeddings for titles and abstract chunks, for semantics search. 

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps
### Indexing Flow

1. We will ingest a list of papers in PDF.
2. For each file, we:
   - Extract the first page of the paper.
   - Convert the first page to Markdown.
   - Extract metadata (title, authors, abstract) from the first page.
   - Split the abstract into chunks, and compute embeddings for each chunk.
3. We will export to the following tables in Postgres with PGVector:
   - Metadata (title, authors, abstract) for each paper.
   - Author-to-paper mapping, for author-based query.
   - Embeddings for titles and abstract chunks, for semantics search. 


## Prerequisite

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Install dependencies:

    ```bash
    pip install -e .
    ```
3.  Create a `.env` file from `.env.example`, and fill `OPENAI_API_KEY`.

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
