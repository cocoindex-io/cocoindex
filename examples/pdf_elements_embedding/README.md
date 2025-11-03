# Extract text and images from PDFs and build multimodal search

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we extract texts and images from PDF pages, embed them with two models, and store them in Qdrant for multimodal search:

- Text: SentenceTransformers `all-MiniLM-L6-v2`
- Images: CLIP `openai/clip-vit-large-patch14` (ViT-L/14, 768-dim)

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow

1. Ingest PDF files from the `source_files` directory.
2. For each PDF page:
   - Extract page text and images using `pypdf`.
   - Skip very small images and create thumbnails up to 512×512 for consistency.
   - Split text into chunks with `SplitRecursively` (language="text", chunk_size=600, chunk_overlap=100).
   - Embed text chunks with SentenceTransformers (`all-MiniLM-L6-v2`).
   - Embed images with CLIP (`openai/clip-vit-large-patch14`).
3. Save embeddings and metadata in Qdrant:
   - Text collection: `PdfElementsEmbeddingText`
   - Image collection: `PdfElementsEmbeddingImage`

## Prerequisite

[Install Qdrant](https://qdrant.tech/documentation/guides/installation/) if you don't have one running locally.

Start Qdrant with Docker (exposes HTTP 6333 and gRPC 6334):

```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

Note: This example connects via gRPC at `http://localhost:6334`.

## Input Data Preparation

Download a few sample PDFs (all are board game manuals) and put them into the `source_files` directory by running:

```bash
./fetch_manual_urls.sh
```

You can also put your favorite PDFs into the `source_files` directory.

## Run

Install dependencies:

```bash
pip install -e .
```

Update index, which will also setup the tables at the first time:

```bash
cocoindex update main
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
