# Extract structured data from patient intake forms with BAML

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example shows how to use [BAML](https://boundaryml.com/) to extract structured data from patient intake PDFs. BAML provides type-safe structured data extraction with native PDF support.

- **BAML Schema** (`baml_src/patient.baml`) - Defines the data structure and extraction function
- **CocoIndex Flow** (`main.py`) - Wraps BAML in a custom function, provide the flow to and process files incrementally.

## Prerequisites

1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2. Install dependencies

   ```sh
   pip install -U cocoindex baml-py
   ```

3. **Generate BAML client code** (required step!)

   ```sh
   baml generate
   ```

   This generates the `baml_client/` directory with Python code to call your BAML functions.

4. Create a `.env` file. You can copy it from `.env.example` first:

   ```sh
   cp .env.example .env
   ```

   Then edit the file to fill in your `GEMINI_API_KEY`.

## Run

Update index:

```sh
cocoindex update main
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
