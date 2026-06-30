"""
SEC EDGAR Analytics (v1) — CocoIndex pipeline example, Apache Doris.

Ingest SEC filings from multiple formats — 10-K text and XBRL company-facts JSON —
into one unified, searchable index in Apache Doris. Each document is scrubbed of
PII, chunked, embedded, and tagged with risk/topic labels, then written to Doris
with both a vector index (for semantic search) and a full-text index (for keyword
search) — the foundation for hybrid retrieval.

Generate the sample data, then run:
    python download.py            # writes data/filings/*.txt + data/company_facts/*.json
    cocoindex update main

Then hybrid-search the filings:
    python search.py "cybersecurity risk"
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import doris, localfs
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

TABLE = "filing_chunks"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

DORIS_DB = coco.ContextKey[doris.ManagedConnection]("sec_doris")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


# ---------------------------------------------------------------------------
# Doris row
# ---------------------------------------------------------------------------


@dataclass
class FilingChunk:
    chunk_id: str  # primary key — uuid5 of (filename, chunk offsets)
    source_type: str  # "filing" | "facts"
    doc_filename: str
    cik: str
    filing_date: str
    form_type: str
    text: str
    topics: list[str]
    embedding: Annotated[NDArray, EMBEDDER]


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


def _scrub_pii(text: str) -> str:
    text = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "[SSN REDACTED]", text)
    text = re.sub(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", "[PHONE REDACTED]", text)
    text = re.sub(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[EMAIL REDACTED]", text
    )
    return text


_TOPIC_KEYWORDS = {
    "RISK:CYBER": [
        "cybersecurity",
        "data breach",
        "ransomware",
        "hacking",
        "cyber attack",
    ],
    "RISK:CLIMATE": [
        "climate change",
        "carbon",
        "sustainability",
        "emissions",
        "environmental",
    ],
    "RISK:SUPPLY": ["supply chain", "logistics", "shortage", "disruption"],
    "RISK:REGULATORY": ["compliance", "regulation", "sec", "enforcement"],
    "TOPIC:AI": ["artificial intelligence", "machine learning", "neural network"],
    "TOPIC:CLOUD": ["cloud computing", "aws", "azure", "saas"],
    "TOPIC:FINANCIAL": ["revenue", "net income", "assets", "liabilities", "cash flow"],
}


def _extract_topics(text: str) -> list[str]:
    low = text.lower()
    return [t for t, kws in _TOPIC_KEYWORDS.items() if any(k in low for k in kws)]


def _company_facts_to_text(content: str) -> str:
    """Render XBRL company-facts JSON as searchable natural language."""
    data = json.loads(content)
    lines = [
        f"# Company Financial Facts: {data.get('entityName', 'Unknown')}",
        f"CIK: {data.get('cik', 'Unknown')}\n",
        "## US-GAAP Financial Metrics\n",
    ]
    us_gaap = data.get("facts", {}).get("us-gaap", {})
    for metric in [
        "Revenues",
        "NetIncomeLoss",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "ResearchAndDevelopmentExpense",
    ]:
        md = us_gaap.get(metric)
        if not md:
            continue
        vals = []
        for unit_data in md.get("units", {}).values():
            for e in sorted(unit_data, key=lambda x: x.get("end", ""), reverse=True)[
                :3
            ]:
                v, end = e.get("val"), e.get("end")
                if v and end:
                    fv = (
                        f"${v / 1e9:.1f}B"
                        if abs(v) >= 1e9
                        else f"${v / 1e6:.1f}M"
                        if abs(v) >= 1e6
                        else f"${v:,.0f}"
                    )
                    vals.append(f"{end}: {fv}")
        if vals:
            lines.append(f"### {md.get('label', metric)}")
            lines.append(f"Recent values: {', '.join(vals[:3])}\n")
    return "\n".join(lines)


def _chunk_id(filename: str, start: int, end: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{filename}|{start}|{end}"))


async def _index_text(
    text: str,
    source_type: str,
    filename: str,
    cik: str,
    filing_date: str,
    form_type: str,
    table: doris.DorisTableTarget[FilingChunk],
) -> None:
    """Common path: scrub PII, chunk, embed, tag, declare one row per chunk."""
    embedder = coco.use_context(EMBEDDER)
    for chunk in _splitter.split(
        _scrub_pii(text), chunk_size=1000, chunk_overlap=200, language="markdown"
    ):
        table.declare_row(
            row=FilingChunk(
                chunk_id=_chunk_id(
                    filename, chunk.start.char_offset, chunk.end.char_offset
                ),
                source_type=source_type,
                doc_filename=filename,
                cik=cik,
                filing_date=filing_date,
                form_type=form_type,
                text=chunk.text,
                topics=_extract_topics(chunk.text),
                embedding=await embedder.embed(chunk.text),
            )
        )


@coco.fn(memo=True)
async def process_filing(
    file: FileLike, table: doris.DorisTableTarget[FilingChunk]
) -> None:
    """10-K text filing: metadata from the {CIK}_{date}_{form}.txt filename."""
    name = file.file_path.path.name
    parts = name.rsplit(".", 1)[0].split("_")
    cik = parts[0] if parts else "unknown"
    filing_date = parts[1] if len(parts) > 1 else "2024-01-01"
    form_type = parts[2] if len(parts) > 2 else "10-K"
    await _index_text(
        await file.read_text(), "filing", name, cik, filing_date, form_type, table
    )


@coco.fn(memo=True)
async def process_facts(
    file: FileLike, table: doris.DorisTableTarget[FilingChunk]
) -> None:
    """XBRL company-facts JSON: render to text, then index."""
    name = file.file_path.path.name
    content = await file.read_text()
    cik = name.replace("CIK", "").replace(".json", "")
    filing_date = (
        json.loads(content).get("filingDate", "2024-01-01") if content else "2024-01-01"
    )
    await _index_text(
        _company_facts_to_text(content), "facts", name, cik, filing_date, "FACTS", table
    )


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(
        DORIS_DB,
        doris.connect(
            doris.DorisConnectionConfig(
                fe_host=os.environ.get("DORIS_FE_HOST", "localhost"),
                fe_http_port=int(os.environ.get("DORIS_HTTP_PORT", "8030")),
                query_port=int(os.environ.get("DORIS_QUERY_PORT", "9030")),
                username=os.environ.get("DORIS_USERNAME", "root"),
                password=os.environ.get("DORIS_PASSWORD", ""),
                database=os.environ.get("DORIS_DATABASE", "sec_analytics"),
                # Stream Load redirects FE → BE; when the BE advertises an
                # unreachable address (e.g. a container IP), rewrite the host.
                be_load_host=os.environ.get("DORIS_BE_LOAD_HOST") or None,
            )
        ),
    )
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield


@coco.fn
async def app_main() -> None:
    table = await doris.mount_table_target(
        DORIS_DB,
        TABLE,
        await doris.TableSchema.from_class(FilingChunk, primary_key=["chunk_id"]),
        vector_indexes=[
            doris.VectorIndexDef(field_name="embedding", metric_type="l2_distance")
        ],
        inverted_indexes=[doris.InvertedIndexDef(field_name="text", parser="unicode")],
    )

    txt = localfs.walk_dir(
        localfs.FilePath(path="./data/filings"),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
    )
    await coco.mount_each(process_filing, txt.items(), table)

    facts = localfs.walk_dir(
        localfs.FilePath(path="./data/company_facts"),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.json"]),
    )
    await coco.mount_each(process_facts, facts.items(), table)


app = coco.App(coco.AppConfig(name="SECFilingAnalytics"), app_main)
