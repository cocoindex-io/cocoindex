<p align="center">
    <img src="https://cocoindex.io/images/github.svg" alt="CocoIndex">
</p>

<h2 align="center">Extract, Transform, Index Data. Easy and Fresh. 🌴</h2>

<div align="center">

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
[![Documentation](https://img.shields.io/badge/Documentation-394e79?logo=readthedocs&logoColor=00B9FF)](https://cocoindex.io/docs/getting_started/quickstart)
[![License](https://img.shields.io/badge/license-Apache%202.0-5B5BD6?logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://img.shields.io/pypi/v/cocoindex?color=5B5BD6)](https://pypi.org/project/cocoindex/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/cocoindex)](https://pypistats.org/packages/cocoindex)

[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)
[![Discord](https://img.shields.io/discord/1314801574169673738?logo=discord&color=5B5BD6&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)
</div>

CocoIndex is ultra performant data transformation framework, core engine written in Rust. The problem it tries to solve is to make it easy to prepare fresh data for AI - either embedding, knowledge graph, or a series of data transformation - and take the real-time data pipeline beyond traditional SQL.

<p align="center">
    <img src="https://cocoindex.io/images/cocoindex-features.png" alt="CocoIndex Features" width="500">
</p>

The philosophy is to have the framework handle the source updates, and having developers only focus on defining a series of data transformation, inspired by spreadsheet.

## Data Flow programming
CocoIndex follows [Data flow](https://en.wikipedia.org/wiki/Dataflow_programming) programming model. Compare with traditional orchestration framework, where data is opaque. In CocoIndex data and data operation are first class citizen, and there's no side effects for each data operation. All data are observable in each transformation, with lineage out of the box.

Particularly, user don't define data operations like creation, update, deletion. But rather, they define something like - for a set of source data, this is the transformation or formula. The framework takes care of the data operations like when to create, update, or delete. For example:

```python
// ingest
data['content'] = flow_builder.add_source(...) 

// transform
data['out'] = data['content'] 
    .transform(...)
    .transform(...)

// collect data
collector.collect(...)

// export to db, vector db, graph db ...
collector.export(...)
```

## Data Freshness
As a data framework, CocoIndex takes it to the next level on data freshness. Incremental processing is one of the core values provided by CocoIndex.

<p align="center">
    <img src="https://github.com/user-attachments/assets/4ead7275-8df6-42aa-ba06-81248aa7f8bf" alt="Incremental Processing" width="700">
</p>

The frameworks takes care of
- Change data capture
- Figuring out what exactly needs to be updated, and only updating that without having to recompute everything throughout.
This makes it fast to reflect any source updates to the target store. If you have concerns with surfacing stale data to AI agents and are spending lots of efforts working on infra piece to optimize the latency, the framework actually handles it for you.


## Quick Start:
If you're new to CocoIndex, we recommend checking out 
- 📖 [Documentation](https://cocoindex.io/docs)
- ⚡  [Quick Start Guide](https://cocoindex.io/docs/getting_started/quickstart)
- 🎬 [Quick Start Video Tutorial](https://youtu.be/gv5R8nOXsWU?si=9ioeKYkMEnYevTXT) 

### Setup 

1. Install CocoIndex Python library

```bash
pip install -U cocoindex
```

2. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one. CocoIndex uses it for incremental processing.


### Define data flow

Follow [Quick Start Guide](https://cocoindex.io/docs/getting_started/quickstart) to define your first indexing flow. An example flow looks like:

```python
@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # Add a data source to read files from a directory
    data_scope["documents"] = flow_builder.add_source(cocoindex.sources.LocalFile(path="markdown_files"))

    # Add a collector for data to be exported to the vector index
    doc_embeddings = data_scope.add_collector()

    # Transform data of each document
    with data_scope["documents"].row() as doc:
        # Split the document into chunks, put into `chunks` field
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown", chunk_size=2000, chunk_overlap=500)

        # Transform data of each chunk
        with doc["chunks"].row() as chunk:
            # Embed the chunk, put into `embedding` field
            chunk["embedding"] = chunk["text"].transform(
                cocoindex.functions.SentenceTransformerEmbed(
                    model="sentence-transformers/all-MiniLM-L6-v2"))

            # Collect the chunk into the collector.
            doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                                   text=chunk["text"], embedding=chunk["embedding"])

    # Export collected data to a vector index.
    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

It defines an index flow like this:

<img width="363" alt="Data Flow" src="https://github.com/user-attachments/assets/2ea7be6d-3d94-42b1-b2bd-22515577e463" />


## Examples and demo

| Example | Description |
|---------|-------------|
| [Text Embedding](examples/text_embedding) | Index text documents with embeddings for semantic search |
| [Code Embedding](examples/code_embedding) | Index code embeddings for semantic search |
| [PDF Embedding](examples/pdf_embedding) | Parse PDF and index text embeddings for semantic search |
| [Manuals LLM Extraction](examples/manuals_llm_extraction) | Extract structured information from a manual using LLM |
| [Google Drive Text Embedding](examples/gdrive_text_embedding) | Index text documents from Google Drive |
| [Docs to Knowledge Graph](examples/docs_to_knowledge_graph) | Extract relationships from Markdown documents and build a knowledge graph |
| [Embeddings to Qdrant](examples/text_embedding_qdrant) | Index documents in a Qdrant collection for semantic search |
| [FastAPI Server with Docker](examples/fastapi_server_docker) | Run the semantic search server in a Dockerized FastAPI setup | 

More coming and stay tuned! If there's any specific examples you would like to see, please let us know in our [Discord community](https://discord.com/invite/zpA9S2DR7s) 🌱.

## 📖 Documentation
For detailed documentation, visit [CocoIndex Documentation](https://cocoindex.io/docs), including a [Quickstart guide](https://cocoindex.io/docs/getting_started/quickstart).

## 🤝 Contributing
We love contributions from our community ❤️. For details on contributing or running the project for development, check out our [contributing guide](https://cocoindex.io/docs/about/contributing).

## 👥 Community
Welcome with a huge coconut hug 🥥⋆｡˚🤗. We are super excited for community contributions of all kinds - whether it's code improvements, documentation updates, issue reports, feature requests, and discussions in our Discord.

Join our community here:

- 🌟 [Star us on GitHub](https://github.com/cocoindex-io/cocoindex)
- 💬 [Start a GitHub Discussion](https://github.com/cocoindex-io/cocoindex/discussions)
- 👋 [Join our Discord community](https://discord.com/invite/zpA9S2DR7s)
- 𝕏 [Follow us on X](https://x.com/cocoindex_io)
- 🐚 [Follow us on LinkedIn](https://www.linkedin.com/company/cocoindex/about/)
- ▶️ [Subscribe to our YouTube channel](https://www.youtube.com/@cocoindex-io)
- 📜 [Read our blog posts](https://cocoindex.io/blogs/)

## License
CocoIndex is Apache 2.0 licensed.
