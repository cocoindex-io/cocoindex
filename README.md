<p align="center">
    <img src="https://cocoindex.io/images/github.svg" alt="CocoIndex">
</p>

<h1 align="center">Data transformation for AI</h1>

<div align="center">

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
[![Documentation](https://img.shields.io/badge/Documentation-394e79?logo=readthedocs&logoColor=00B9FF)](https://cocoindex.io/docs/getting_started/quickstart)
[![License](https://img.shields.io/badge/license-Apache%202.0-5B5BD6?logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://img.shields.io/pypi/v/cocoindex?color=5B5BD6)](https://pypi.org/project/cocoindex/)
<!--[![PyPI - Downloads](https://img.shields.io/pypi/dm/cocoindex)](https://pypistats.org/packages/cocoindex) -->
[![PyPI Downloads](https://static.pepy.tech/badge/cocoindex/month)](https://pepy.tech/projects/cocoindex)
[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)
[![Discord](https://img.shields.io/discord/1314801574169673738?logo=discord&color=5B5BD6&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)

</div>

<div align="center">
    <a href="https://trendshift.io/repositories/13939" target="_blank"><img src="https://trendshift.io/api/badge/repositories/13939" alt="cocoindex-io%2Fcocoindex | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>


Ultra performant data transformation framework for AI, with core engine written in Rust. Support incremental processing and data lineage out-of-box.  Exceptional developer velocity. Production-ready at day 0.

⭐ Drop a star to help us grow!

<div align="center">

<!-- Keep these links. Translations will automatically update with the README. -->
[Deutsch](https://readme-i18n.com/cocoindex-io/cocoindex?lang=de) |
[English](https://readme-i18n.com/cocoindex-io/cocoindex?lang=en) |
[Español](https://readme-i18n.com/cocoindex-io/cocoindex?lang=es) |
[français](https://readme-i18n.com/cocoindex-io/cocoindex?lang=fr) |
[日本語](https://readme-i18n.com/cocoindex-io/cocoindex?lang=ja) |
[한국어](https://readme-i18n.com/cocoindex-io/cocoindex?lang=ko) |
[Português](https://readme-i18n.com/cocoindex-io/cocoindex?lang=pt) |
[Русский](https://readme-i18n.com/cocoindex-io/cocoindex?lang=ru) |
[中文](https://readme-i18n.com/cocoindex-io/cocoindex?lang=zh)

</div>

</br>

<p align="center">
    <img src="https://cocoindex.io/images/transformation.svg" alt="CocoIndex Transformation">
</p>

</br>

CocoIndex makes it effortless to transform data with AI, and keep source data and target in sync. Whether you’re building a vector index for RAG, creating knowledge graphs, or performing any custom data transformations — goes beyond SQL.

</br>

<p align="center">
<img alt="CocoIndex Features" src="https://cocoindex.io/images/venn2.svg" />
</p>

</br>



## Exceptional velocity
Just declare transformation in dataflow with ~100 lines of python

```python
# import
data['content'] = flow_builder.add_source(...)

# transform
data['out'] = data['content']
    .transform(...)
    .transform(...)

# collect data
collector.collect(...)

# export to db, vector db, graph db ...
collector.export(...)
```

CocoIndex follows the idea of [Dataflow](https://en.wikipedia.org/wiki/Dataflow_programming) programming model. Each transformation creates a new field solely based on input fields, without hidden states and value mutation. All data before/after each transformation is observable, with lineage out of the box.

**Particularly**, developers don't explicitly mutate data by creating, updating and deleting. They just need to define transformation/formula for a set of source data.

## Plug-and-Play Building Blocks
Native builtins for different source, targets and transformations. Standardize interface, make it 1-line code switch between different components - as easy as assembling building blocks.

<p align="center">
    <img src="https://cocoindex.io/images/components.svg" alt="CocoIndex Features">
</p>

## Data Freshness
CocoIndex keep source data and target in sync effortlessly.

<p align="center">
    <img src="https://github.com/user-attachments/assets/f4eb29b3-84ee-4fa0-a1e2-80eedeeabde6" alt="Incremental Processing" width="700">
</p>

It has out-of-box support for incremental indexing:
- minimal recomputation on source or logic change.
- (re-)processing necessary portions; reuse cache when possible

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


## Define data flow

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
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

It defines an index flow like this:

<p align="center">
    <img width="400" alt="Data Flow" src="https://github.com/user-attachments/assets/2ea7be6d-3d94-42b1-b2bd-22515577e463" />
</p>

## 🚀 Examples and demo

| Example | Description |
|---------|-------------|
| [Text Embedding](examples/text_embedding) | Index text documents with embeddings for semantic search |
| [Code Embedding](examples/code_embedding) | Index code embeddings for semantic search |
| [PDF Embedding](examples/pdf_embedding) | Parse PDF and index text embeddings for semantic search |
| [Manuals LLM Extraction](examples/manuals_llm_extraction) | Extract structured information from a manual using LLM |
| [Amazon S3 Embedding](examples/amazon_s3_embedding) | Index text documents from Amazon S3 |
| [Azure Blob Storage Embedding](examples/azure_blob_embedding) | Index text documents from Azure Blob Storage |
| [Google Drive Text Embedding](examples/gdrive_text_embedding) | Index text documents from Google Drive |
| [Docs to Knowledge Graph](examples/docs_to_knowledge_graph) | Extract relationships from Markdown documents and build a knowledge graph |
| [Embeddings to Qdrant](examples/text_embedding_qdrant) | Index documents in a Qdrant collection for semantic search |
| [Embeddings to LanceDB](examples/text_embedding_lancedb) | Index documents in a LanceDB collection for semantic search |
| [FastAPI Server with Docker](examples/fastapi_server_docker) | Run the semantic search server in a Dockerized FastAPI setup |
| [Product Recommendation](examples/product_recommendation) | Build real-time product recommendations with LLM and graph database|
| [Image Search with Vision API](examples/image_search) | Generates detailed captions for images using a vision model, embeds them, enables live-updating semantic search via FastAPI and served on a React frontend|
| [Face Recognition](examples/face_recognition) | Recognize faces in images and build embedding index |
| [Paper Metadata](examples/paper_metadata) | Index papers in PDF files, and build metadata tables for each paper |
| [Multi Format Indexing](examples/multi_format_indexing) | Build visual document index from PDFs and images with ColPali for semantic search |
| [Custom Output Files](examples/custom_output_files) | Convert markdown files to HTML files and save them to a local directory, using *CocoIndex Custom Targets* |
| [Patient intake form extraction](examples/patient_intake_extraction) | Use LLM to extract structured data from patient intake forms with different formats |


More coming and stay tuned 👀!

## 📖 Documentation
For detailed documentation, visit [CocoIndex Documentation](https://cocoindex.io/docs), including a [Quickstart guide](https://cocoindex.io/docs/getting_started/quickstart).

## 🤝 Contributing
We love contributions from our community ❤️. For details on contributing or running the project for development, check out our [contributing guide](https://cocoindex.io/docs/about/contributing).

## 👥 Community
Welcome with a huge coconut hug 🥥⋆｡˚🤗. We are super excited for community contributions of all kinds - whether it's code improvements, documentation updates, issue reports, feature requests, and discussions in our Discord.

Join our community here:

- 🌟 [Star us on GitHub](https://github.com/cocoindex-io/cocoindex)
- 👋 [Join our Discord community](https://discord.com/invite/zpA9S2DR7s)
- ▶️ [Subscribe to our YouTube channel](https://www.youtube.com/@cocoindex-io)
- 📜 [Read our blog posts](https://cocoindex.io/blogs/)

## Support us:
We are constantly improving, and more features and examples are coming soon. If you love this project, please drop us a star ⭐ at GitHub repo [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex) to stay tuned and help us grow.

## License
CocoIndex is Apache 2.0 licensed.
