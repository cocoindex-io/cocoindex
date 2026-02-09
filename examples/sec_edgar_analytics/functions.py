"""
SEC EDGAR Analytics - CocoIndex Functions

This module contains all CocoIndex function definitions. By keeping them in a
separate file, we avoid the "Function factory with name already exists" error
when re-running Jupyter notebook cells.

Functions are registered with CocoIndex once when this module is first imported.
"""

import io
import re
import json
from dataclasses import dataclass

import cocoindex
from docling.datamodel.base_models import DocumentStream
from docling.document_converter import DocumentConverter


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class FilingMetadata:
    """Structured metadata from SEC filing filename."""

    cik: str  # Company identifier (e.g., 0000320193 = Apple)
    filing_date: str  # ISO date
    form_type: str  # 10-K, 10-Q, 8-K
    fiscal_year: int | None
    source_type: str  # "filing" — for multi-source filtering


@dataclass
class CompanyFactsMetadata:
    """Metadata extracted from JSON company facts."""

    cik: str
    entity_name: str
    filing_date: str  # Most recent data date
    form_type: str  # Will be "FACTS" for JSON data
    fiscal_year: int | None
    source_type: str  # "facts" — for multi-source filtering


@dataclass
class PdfMetadata:
    """Metadata from PDF exhibit filename."""

    cik: str
    filing_date: str
    exhibit_type: str  # ex21 (subsidiaries), ex31 (certifications), etc.
    form_type: str  # Will be "EXHIBIT"
    fiscal_year: int | None
    source_type: str  # "exhibit" — for multi-source filtering


# =============================================================================
# COCOINDEX FUNCTIONS
# =============================================================================


@cocoindex.op.function(cache=True, behavior_version=1)
def extract_filing_metadata(filename: str) -> FilingMetadata:
    """
    Parse metadata from filename: {CIK}_{date}_{form}.txt

    Example: 0000320193_2024-11-01_10-K.txt
    """
    base_name = filename.rsplit(".", 1)[0] if "." in filename else filename
    parts = base_name.split("_")

    cik = parts[0] if len(parts) > 0 else "unknown"
    filing_date = parts[1] if len(parts) > 1 else "2024-01-01"
    form_type = parts[2] if len(parts) > 2 else "10-K"

    try:
        fiscal_year = int(filing_date[:4])
    except (ValueError, IndexError):
        fiscal_year = None

    return FilingMetadata(
        cik=cik,
        filing_date=filing_date,
        form_type=form_type,
        fiscal_year=fiscal_year,
        source_type="filing",
    )


@cocoindex.op.function(cache=True, behavior_version=1)
def extract_json_metadata(filename: str, content: str) -> CompanyFactsMetadata:
    """
    Extract metadata from JSON company facts file.

    Handles both naming conventions:
    - 0000320193.json (from download_sec_data)
    - CIK0000320193.json (legacy format)
    """
    # Parse CIK from filename (handle both formats)
    base_name = filename.replace(".json", "")
    if base_name.startswith("CIK"):
        cik = base_name.replace("CIK", "")
    else:
        cik = base_name  # Already just the CIK number

    # Parse JSON for entity name and latest date
    try:
        data = json.loads(content)
        entity_name = data.get("entityName", "Unknown Company")

        # Find most recent filing date from the data
        latest_date = "2024-01-01"
        if "facts" in data and "us-gaap" in data["facts"]:
            for metric_data in data["facts"]["us-gaap"].values():
                if "units" in metric_data:
                    for unit_data in metric_data["units"].values():
                        for entry in unit_data:
                            if "filed" in entry and entry["filed"] > latest_date:
                                latest_date = entry["filed"]
    except (json.JSONDecodeError, KeyError):
        entity_name = "Unknown Company"
        latest_date = "2024-01-01"

    try:
        fiscal_year = int(latest_date[:4])
    except (ValueError, IndexError):
        fiscal_year = None

    return CompanyFactsMetadata(
        cik=cik,
        entity_name=entity_name,
        filing_date=latest_date,
        form_type="FACTS",
        fiscal_year=fiscal_year,
        source_type="facts",
    )


@cocoindex.op.function(cache=True, behavior_version=1)
def parse_company_facts(content: str) -> str:
    """
    Convert JSON company facts to searchable natural language text.

    This makes structured financial data discoverable via semantic search.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return "Invalid JSON content"

    lines = []

    # Company header
    entity_name = data.get("entityName", "Unknown Company")
    cik = data.get("cik", "Unknown")
    lines.append(f"# Company Financial Facts: {entity_name}")
    lines.append(f"CIK: {cik}\n")

    # Process US-GAAP facts
    if "facts" in data and "us-gaap" in data["facts"]:
        lines.append("## US-GAAP Financial Metrics\n")

        # Key metrics to highlight
        key_metrics = [
            "Revenues",
            "NetIncomeLoss",
            "Assets",
            "Liabilities",
            "StockholdersEquity",
            "CashAndCashEquivalentsAtCarryingValue",
            "ResearchAndDevelopmentExpense",
            "OperatingIncomeLoss",
        ]

        us_gaap = data["facts"]["us-gaap"]

        for metric_name in key_metrics:
            if metric_name in us_gaap:
                metric_data = us_gaap[metric_name]
                label = metric_data.get("label", metric_name)
                description = metric_data.get("description", "")

                # Get most recent values
                recent_values = []
                if "units" in metric_data:
                    for unit_name, unit_data in metric_data["units"].items():
                        # Sort by date descending, take top 3
                        sorted_data = sorted(
                            unit_data, key=lambda x: x.get("end", ""), reverse=True
                        )[:3]
                        for entry in sorted_data:
                            val = entry.get("val", 0)
                            end = entry.get("end", "")
                            if val and end:
                                # Format large numbers
                                if abs(val) >= 1_000_000_000:
                                    formatted = f"${val / 1_000_000_000:.1f}B"
                                elif abs(val) >= 1_000_000:
                                    formatted = f"${val / 1_000_000:.1f}M"
                                else:
                                    formatted = f"${val:,.0f}"
                                recent_values.append(f"{end}: {formatted}")

                if recent_values:
                    lines.append(f"### {label}")
                    if description:
                        lines.append(f"{description[:200]}")
                    lines.append(f"Recent values: {', '.join(recent_values[:3])}\n")

    return "\n".join(lines) if lines else "No financial data available"


@cocoindex.op.function(cache=True, behavior_version=1)
def extract_pdf_metadata(filename: str) -> PdfMetadata:
    """
    Parse metadata from PDF filename.

    Handles formats like:
    - {CIK}_{date}_{form}_{description}.pdf
    - 0000320193_2026-01-08_DEF 14A_aapl_courtesy-pdf.pdf
    """
    base_name = filename.rsplit(".", 1)[0] if "." in filename else filename
    parts = base_name.split("_")

    cik = parts[0] if len(parts) > 0 else "unknown"
    filing_date = parts[1] if len(parts) > 1 else "2024-01-01"
    form_type = parts[2] if len(parts) > 2 else "EXHIBIT"

    try:
        fiscal_year = int(filing_date[:4])
    except (ValueError, IndexError):
        fiscal_year = None

    return PdfMetadata(
        cik=cik,
        filing_date=filing_date,
        exhibit_type=form_type,
        form_type="EXHIBIT",
        fiscal_year=fiscal_year,
        source_type="exhibit",
    )


@cocoindex.op.function(cache=True, behavior_version=1)
def scrub_pii(text: str) -> str:
    """
    Remove personally identifiable information.

    Runs BEFORE chunking so PII never enters the index.
    """
    # SSN pattern
    text = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "[SSN REDACTED]", text)

    # Phone patterns
    text = re.sub(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", "[PHONE REDACTED]", text)

    # Email pattern
    text = re.sub(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "[EMAIL REDACTED]", text
    )

    return text


@cocoindex.op.function(cache=True, behavior_version=1)
def extract_topics(text: str) -> list[str]:
    """
    Extract risk and topic categories from text.

    Returns an array for Doris array filtering:
    - array_contains(topics, 'RISK:CYBER')
    - arrays_overlap(topics, ['RISK:CYBER', 'RISK:CLIMATE'])
    """
    topics: list[str] = []

    topic_keywords = {
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
        "TOPIC:FINANCIAL": [
            "revenue",
            "net income",
            "assets",
            "liabilities",
            "cash flow",
        ],
    }

    text_lower = text.lower()
    for topic, keywords in topic_keywords.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)

    return topics


# =============================================================================
# PDF CONVERSION
# =============================================================================


@cocoindex.op.function(cache=True, behavior_version=1)
def pdf_to_markdown(content: bytes) -> str:
    """Convert PDF bytes to markdown text using docling."""
    converter = DocumentConverter()
    source = DocumentStream(name="input.pdf", stream=io.BytesIO(content))
    result = converter.convert(source)
    return result.document.export_to_markdown()


# =============================================================================
# TRANSFORM FLOW
# =============================================================================


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[float]]:
    """
    Convert text to embedding vector.

    Model loads once, reused for all embeddings.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )
