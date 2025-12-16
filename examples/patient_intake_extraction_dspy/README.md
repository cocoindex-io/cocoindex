# Extract structured data from patient intake forms with DSPy

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example shows how to use [DSPy](https://github.com/stanfordnlp/dspy) with Gemini 2.5 Flash (vision model) to extract structured data from patient intake PDFs. DSPy provides a programming model for building AI systems using language models as building blocks.

- **Pydantic Models** (`main.py`) - Defines the data structure using Pydantic for type safety
- **DSPy Module** (`main.py`) - Defines the extraction signature and module using DSPy's ChainOfThought with vision support
- **CocoIndex Flow** (`main.py`) - Wraps DSPy in a custom function, provides the flow to process files incrementally

## Key Features

- **Native PDF Support**: Converts PDFs to images and processes directly with vision models
- **DSPy Vision Integration**: Uses DSPy's `Image` type with `ChainOfThought` for visual document understanding
- **Structured Outputs**: Pydantic models ensure type-safe, validated extraction
- **No Text Extraction Required**: Directly processes PDF images without intermediate markdown conversion
- **Incremental Processing**: CocoIndex handles batching and caching automatically
- **PostgreSQL Storage**: Results stored in a structured database table

## Prerequisites

1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2. Install dependencies

   ```sh
   pip install -U cocoindex dspy-ai pydantic pymupdf
   ```

3. Create a `.env` file. You can copy it from `.env.example` first:

   ```sh
   cp .env.example .env
   ```

   Then edit the file to fill in your `GEMINI_API_KEY`.

## Run

Update index:

```sh
cocoindex update main
```

## How It Works

The example demonstrates DSPy vision integration with CocoIndex:

1. **Pydantic Models**: Define the structured schema (Patient, Contact, Address, etc.)
2. **DSPy Signature**: Declares input (`list[dspy.Image]`) and output (Patient model) fields
3. **DSPy Module**: Uses `ChainOfThought` with vision capabilities to reason about extraction from images
4. **Single-Step Extraction**:
   - The extractor receives PDF bytes directly
   - Internally converts PDF pages to DSPy Image objects using PyMuPDF
   - Processes images with vision model
   - Returns Pydantic model directly
5. **CocoIndex Flow**:
   - Loads PDFs from local directory as binary
   - Applies single transform: PDF bytes → Patient data
   - Stores results in PostgreSQL

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
