<p align="center">
    <img src="https://cocoindex.io/images/github.svg" alt="CocoIndex">
</p>

<h2 align="center">Extract, Transform, Index Data. Easy and Fresh. 🌴</h2>

[![License](https://img.shields.io/badge/license-Apache%202.0-5B5BD6?logo=opensourceinitiative&logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.11%20to%203.13-5B5BD6?logo=python&logoColor=white)](https://www.python.org/)
[![PyPI version](https://img.shields.io/pypi/v/cocoindex?color=5B5BD6)](https://pypi.org/project/cocoindex/)
[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)
[![docs](https://github.com/cocoindex-io/cocoindex/actions/workflows/docs.yml/badge.svg?event=push)](https://github.com/cocoindex-io/cocoindex/actions/workflows/docs.yml)
[![Discord](https://img.shields.io/badge/discord-cocoindex-5B5BD6?logo=discord&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-CocoIndex-5B5BD6?logo=linkedin&logoColor=white)](https://www.linkedin.com/company/cocoindex)
[![X (Twitter)](https://img.shields.io/twitter/follow/cocoindex_io)](https://twitter.com/intent/follow?screen_name=cocoindex_io)

CocoIndex is the world's first open-source engine that supports both custom transformation logic and incremental updates specialized for data indexing.
<p align="center">
    <img src="https://cocoindex.io/images/venn.svg" alt="CocoIndex">
</p>
With CocoIndex, users declare the transformation, CocoIndex creates & maintains an index, and keeps the derived index up to date based on source update, with minimal computation and changes.


## Quick Start:
If you're new to CocoIndex 🤗, we recommend checking out the [Documentation](https://cocoindex.io/docs) or following the [Quick Start Guide](https://cocoindex.io/docs/getting_started/quickstart).

### Setup 
1. Install CocoIndex Python library

```bash
pip install cocoindex
```

2. Setup Postgres with pgvector extension; or bring up a Postgres database using docker compose:

    - Make sure Docker Compose is installed: [docs](https://docs.docker.com/compose/install/)
    - Start a Postgres SQL database for cocoindex using our docker compose config:

    ```bash
    docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/postgres.yaml) up -d
    ```

### Start your first indexing flow!
Follow [Quick Start Guide](https://cocoindex.io/docs/getting_started/quickstart) to define your first indexing flow.
A common indexing flow looks like:

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
            cocoindex.functions.SplitRecursively(
                language="markdown", chunk_size=300, chunk_overlap=100))

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
        vector_index=[("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

It defines a index flow like this:
![Flow diagram](docs/docs/core/flow_example.svg)

### Play with existing example and demo
Go to the [examples directory](examples) to try out with any of the examples, following instructions under specific example directory.

| Example | Description |
|---------|-------------|
| [Text Embedding](examples/text_embedding) | Index text documents with embeddings for semantic search |
| [Code Embedding](examples/code_embedding) | Index code embeddings for semantic search |
| [PDF Embedding](examples/pdf_embedding) | Parse PDF and index text embeddings for semantic search |

More coming and stay tuned! If there's any specific examples you would like to see, please let us know in our [Discord community](https://discord.com/invite/zpA9S2DR7s) 🌱.

## 📖 Documentation
For detailed documentation, visit [Cocoindex Documentation](https://cocoindex.io/docs), including a [Quickstart guide](https://cocoindex.io/docs/getting_started/quickstart).

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