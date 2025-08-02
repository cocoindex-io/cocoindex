# Build visual document index from PDFs and images with ColPali
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)


In this example, we build a visual document indexing flow using ColPali for embedding PDFs and images. and query the index with natural language.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps
### Indexing Flow

1. We ingest a list of PDF files and image files from the `source_files` directory.
2. For each file:
   - **PDF files**: convert each page to a high-resolution image (300 DPI)
   - **Image files**: use the image directly
   - Generate visual embeddings for each page/image using ColPali model
3. We will save the embeddings and metadata in Qdrant vector database.

### Query
We will match against user-provided natural language text using ColPali's text-to-visual embedding capability, enabling semantic search across visual document content.



## Prerequisite
[Install Qdrant](https://qdrant.tech/documentation/guides/installation/) if you don't have one running locally.

You can start Qdrant with Docker:
```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

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

## About ColPali
This example uses [ColPali](https://github.com/illuin-tech/colpali), a state-of-the-art vision-language model that enables:
- Direct visual understanding of document layouts, tables, and figures
- Natural language queries against visual document content
- No need for OCR or text extraction - works directly with document images

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
