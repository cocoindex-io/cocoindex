# Build real-time index for codebase
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

CocoIndex provides built-in support for code base chunking, using Tree-sitter to keep syntax boundary. In this example, we will build real-time index for codebase using CocoIndex.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

![Build embedding index for codebase](https://github.com/user-attachments/assets/6dc5ce89-c949-41d4-852f-ad95af163dbd)

[Tree-sitter](https://en.wikipedia.org/wiki/Tree-sitter_%28parser_generator%29) is a parser generator tool and an incremental parsing library. It is available in Rust 🦀 - [GitHub](https://github.com/tree-sitter/tree-sitter). CocoIndex has built-in Rust integration with Tree-sitter to efficiently parse code and extract syntax trees for various programming languages. Check out the list of supported languages [here](https://cocoindex.io/docs/ops/functions#splitrecursively) - in the `language` section.


## Tutorials
- Step by step tutorial - Check out the [blog](https://cocoindex.io/blogs/index-code-base-for-rag).
- Video tutorial - [Youtube](https://youtu.be/G3WstvhHO24?si=Bnxu67Ax5Lv8b-J2).

## Steps

### Indexing Flow
<p align='center'>
  <img width="434" alt="Screenshot 2025-05-19 at 10 14 36 PM" src="https://github.com/user-attachments/assets/3a506034-698f-480a-b653-22184dae4e14" />
</p>

1. We will ingest CocoIndex codebase.
2. For each file, perform chunking (Tree-sitter) and then embedding.
3. We will save the embeddings and the metadata in Postgres with PGVector.

### Query:
We will match against user-provided text by a SQL query, reusing the embedding operation in the indexing flow.


## Prerequisite
[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

- Install dependencies:
  ```bash
  pip install -e .
  ```

- Setup:

  ```bash
  cocoindex setup main.py
  ```

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
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run the following command to start CocoInsight:

```
cocoindex server -ci main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

<img width="1305" alt="Chunking Visualization" src="https://github.com/user-attachments/assets/8e83b9a4-2bed-456b-83e5-b5381b28b84a" />
