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

**NOTE**: The `pdf2image` requires `poppler` to be installed manually. Please refer to [this document](https://pdf2image.readthedocs.io/en/latest/installation.html#installing-poppler) for the specific installation instructions for your platform.

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

## Data Attribution

The example data files used in this demonstration come from the following sources:

### PDF Documents
- **ArXiv Papers**: Research papers sourced from [ArXiv](https://arxiv.org/), an open-access repository of electronic preprints covering various scientific disciplines.

### Image Documents
- **Healthcare Industry Dataset**: Images from the [vidore/syntheticDocQA_healthcare_industry_test](https://huggingface.co/datasets/vidore/syntheticDocQA_healthcare_industry_test) dataset on Hugging Face, which contains synthetic document question-answering data for healthcare industry documents.
- **ESG Reports Dataset**: Images from the [vidore/esg_reports_eng_v2](https://huggingface.co/datasets/vidore/esg_reports_eng_v2) dataset on Hugging Face, containing Environmental, Social, and Governance (ESG) reports.

We thank the creators and maintainers of these datasets for making their data available for research and development purposes.

## About ColPali
This example uses [ColPali](https://github.com/illuin-tech/colpali), a state-of-the-art vision-language model that enables:
- Direct visual understanding of document layouts, tables, and figures
- Natural language queries against visual document content
- No need for OCR or text extraction - works directly with document images

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
