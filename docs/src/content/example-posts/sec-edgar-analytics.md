---
title: SEC Filing Hybrid Search
description: 'Index multi-format SEC filings into Apache Doris with CocoIndex V1 — scrub PII, chunk, embed, and tag 10-K text and XBRL company facts into one table with vector and full-text indexes for hybrid (semantic + keyword) retrieval.'
slug: sec-edgar-analytics
image: https://cocoindex.io/blobs/docs-v1/img/examples/sec-edgar-analytics/cover.png
tags: [structured-data, full-text-search, hybrid-search]
---

![Index multi-format SEC filings into Apache Doris with vector + full-text indexes for hybrid search](https://cocoindex.io/blobs/docs-v1/img/examples/sec-edgar-analytics/cover.png)

SEC filings come in many shapes — narrative 10-K risk factors as text, structured financials as XBRL JSON, exhibits as PDF. In this tutorial we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that pulls these formats into a *single* searchable index in [Apache Doris](https://doris.apache.org/), with both a **vector index** for semantic search and a **full-text index** for keyword search — the foundation for hybrid retrieval. Along the way each document is scrubbed of PII, chunked, embedded, and tagged with risk/topic labels.

The whole pipeline is ordinary `async` Python. Embedding runs on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/), and the Rust engine handles [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) — add a filing and only its chunks are embedded and loaded.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/sec_edgar_analytics)

## Flow overview

![CocoIndex flow: walk text filings and JSON facts, scrub PII, chunk, embed, tag topics, and load one row per chunk into Apache Doris with a vector index and a full-text index](https://cocoindex.io/blobs/docs-v1/img/examples/sec-edgar-analytics/flow-v1.png)

Two source formats fan into one chunk table:

1. **Sources** — `*.txt` 10-K filings and `*.json` XBRL company facts (the JSON is rendered to searchable text first).
2. **Scrub & chunk** — strip SSNs / phones / emails *before* indexing, then split into overlapping chunks.
3. **Embed & tag** — a sentence-transformer embeds each chunk; a keyword pass tags `RISK:*` / `TOPIC:*` labels.
4. **Load into Doris** — one row per chunk, into a table with a vector (ANN) index and a full-text (inverted) index.

## One table, two index types

The row type is a plain dataclass. The magic is in `mount_table_target`: the same table gets a **vector index** (for `l2_distance` semantic search) and an **inverted index** (for `MATCH_ANY` keyword search):

```python title="main.py"
@dataclass
class FilingChunk:
    chunk_id: str          # primary key
    source_type: str       # "filing" | "facts"
    doc_filename: str
    cik: str
    filing_date: str
    form_type: str
    text: str
    topics: list[str]
    embedding: Annotated[NDArray, EMBEDDER]


table = await doris.mount_table_target(
    DORIS_DB, TABLE,
    await doris.TableSchema.from_class(FilingChunk, primary_key=["chunk_id"]),
    vector_indexes=[doris.VectorIndexDef(field_name="embedding", metric_type="l2_distance")],
    inverted_indexes=[doris.InvertedIndexDef(field_name="text", parser="unicode")],
)
```

## Scrub PII, then chunk, embed, and tag

PII is redacted *before* chunking, so it never enters the index. Each format gets a thin per-file entry point (`process_filing`, `process_facts`) that funnels into one shared path — scrub, chunk, embed, tag, declare a row per chunk:

```python title="main.py"
async def _index_text(text, source_type, filename, cik, filing_date, form_type, table):
    embedder = coco.use_context(EMBEDDER)
    for chunk in _splitter.split(_scrub_pii(text), chunk_size=1000, chunk_overlap=200,
                                 language="markdown"):
        table.declare_row(row=FilingChunk(
            chunk_id=_chunk_id(filename, chunk.start.char_offset, chunk.end.char_offset),
            source_type=source_type, doc_filename=filename, cik=cik,
            filing_date=filing_date, form_type=form_type,
            text=chunk.text, topics=_extract_topics(chunk.text),
            embedding=await embedder.embed(chunk.text),
        ))
```

Both sources `declare_row` into the *same* Doris table — `chunk_id` is a stable `uuid5` of the file and chunk offsets, so re-running reconciles cleanly instead of duplicating.

> **A note on the port.** The original v0 example also ingested PDF exhibits via docling; this v1 port focuses on the text and XBRL-JSON sources (the PDF path is identical to the [Manuals to Structured Data](https://cocoindex.io/docs/examples/manuals-llm-extraction/) example — `docling` bytes → Markdown, then the same `_index_text`). It needs **Apache Doris 4.0+** for vector index support; a ready `docker-compose.yml` is included.

## Run the pipeline

```sh
docker compose up -d fe be       # Apache Doris 4.0 (FE + BE)
python download.py               # synthetic 10-K filings + XBRL company facts
cp .env.example .env             # Doris host/ports
pip install -e .
cocoindex update main
```

On the sample data this loads 4 chunks (2 filings + 2 company-facts) into Doris, creating both `idx_vec_embedding` (ANN) and `idx_inv_text` (INVERTED). Topic tags come out as you'd expect — Apple's filing tagged `RISK:CYBER, RISK:CLIMATE, RISK:SUPPLY, RISK:REGULATORY, TOPIC:AI`, Microsoft's `RISK:CYBER, RISK:REGULATORY, TOPIC:AI, TOPIC:CLOUD`.

## Hybrid search with RRF

The payoff is hybrid retrieval — fuse the vector ranking and the keyword ranking with [Reciprocal Rank Fusion](https://en.wikipedia.org/wiki/Learning_to_rank). `search.py` does both in one SQL query:

```python title="search.py (shape)"
WITH semantic AS (
    SELECT chunk_id, ROW_NUMBER() OVER (ORDER BY l2_distance(embedding, {q})) AS rk
    FROM filing_chunks
),
lexical AS (
    SELECT chunk_id, ROW_NUMBER() OVER (
        ORDER BY CASE WHEN text MATCH_ANY '{keywords}' THEN 0 ELSE 1 END) AS rk
    FROM filing_chunks
)
SELECT s.doc_filename, 1.0/(60 + s.rk) + 1.0/(60 + l.rk) AS rrf
FROM semantic s JOIN lexical l USING (chunk_id) ORDER BY rrf DESC
```

```sh
python search.py "cloud computing and AI risk"
```

On the sample data that ranks **Microsoft's** cloud-and-AI filing first (it carries both `TOPIC:CLOUD` and `TOPIC:AI`), Apple's second, and the company-facts rows below — semantic relevance and keyword presence combined, not either alone.

## Incremental updates

- **Add a filing** — only its chunks are scrubbed, embedded, tagged, and stream-loaded into Doris.
- **Edit a filing** — chunks reconcile by `chunk_id`; unchanged chunks are untouched.
- **Delete a filing** — its chunks are removed from the table.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/sec_edgar_analytics](https://github.com/cocoindex-io/cocoindex/tree/main/examples/sec_edgar_analytics). For the PDF-extraction side, see [Manuals to Structured Data](https://cocoindex.io/docs/examples/manuals-llm-extraction/); for a pure-vector setup, see [Text Embedding](https://cocoindex.io/docs/examples/text-embedding/).

Indexing your own filing archive? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
