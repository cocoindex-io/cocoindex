# SEC EDGAR Financial Analytics

[![GitHub](https://img.shields.io/github/stars/cocoindex/cocoindex?style=social)](https://github.com/cocoindex/cocoindex)

Financial document analytics with **transparent ETL** and **hybrid search** using CocoIndex + Apache Doris.

This example demonstrates how to build a production-ready document search system with:
- **Vector search** for semantic similarity
- **Full-text search** for keyword matching
- **Hybrid search** combining both with temporal scoring
- **Array field filtering** (like healthcare codes[])
- **Multi-entity aggregation** (like patient cohorts)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Compliance Search Tool                           │
├─────────────────────────────────────────────────────────────────────────┤
│  Query: "cybersecurity risks"                                           │
│  Filters: time_gate=365 days, topics=[RISK:CYBER], source=filing        │
│                                                                         │
│  Results:                                                               │
│  [0.032] Apple 10-K 2024 → "We face significant cybersecurity..."       │
│  [0.029] Microsoft 10-K 2024 → "Cyber threats continue to evolve..."    │
│  [0.025] JPMorgan 10-K 2024 → "We invest $700M in cybersecurity..."     │
└─────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ Hybrid Search
┌─────────────────────────────────────────────────────────────────────────┐
│                          Apache Doris                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ HNSW Vector  │  │  Inverted    │  │   Array      │                   │
│  │    Index     │  │   Index      │  │  Columns     │                   │
│  │ (semantic)   │  │ (keywords)   │  │ (topics[])   │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ Incremental ETL
┌─────────────────────────────────────────────────────────────────────────┐
│                     CocoIndex Multi-Source Pipeline                     │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                 │
│  │ TXT Filings  │   │ JSON Facts   │   │ PDF Exhibits │                 │
│  │ (10-K/10-Q)  │   │ (API Data)   │   │ (Documents)  │                 │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                 │
│         │                  │                  │                         │
│         ▼                  ▼                  ▼                         │
│  ┌─────────────────────────────────────────────────────┐                │
│  │              Unified Chunk Collector                │                │
│  │   Scrub PII → Chunk → Embed → Extract Topics        │                │
│  └─────────────────────────────────────────────────────┘                │
│                                                                         │
│  ✓ Cached (only reprocess changed files)                                │
│  ✓ Incremental (daily updates in seconds)                               │
│  ✓ Traceable (full lineage from chunk to source)                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Features Demonstrated

| Feature | Implementation | Healthcare Mapping |
|---------|---------------|-------------------|
| **ETL Caching** | `@cocoindex.op.function(cache=True)` | Same for FHIR processing |
| **Hybrid Search** | Doris `MATCH_ANY` + `l2_distance` | Same for clinical notes |
| **Array Filtering** | `array_contains(topics, 'RISK:CYBER')` | `array_contains(codes, 'LOINC:2093-3')` |
| **Temporal Scoring** | `EXP(-ln(2) * days / half_life)` | Prioritize recent encounters |
| **Multi-Entity Aggregation** | `ROW_NUMBER() OVER PARTITION BY cik` | Patient cohort queries |
| **JSON Querying** | `JSON_EXTRACT(raw_metadata, '$.path')` | FHIR VARIANT columns |
| **Lineage Tracking** | CocoInsight visualization | Same for compliance |

## Prerequisites

- Python 3.11+
- Docker and Docker Compose

> **Want to try it interactively?** Open [tutorial.ipynb](tutorial.ipynb) for a step-by-step walkthrough.

## Quick Start

### 1. Install Dependencies

```bash
cd examples/sec_edgar_analytics
pip install -e .
```

### 2. Start Doris and PostgreSQL

```bash
docker compose up -d

# Wait for Doris to be ready (~90 seconds)
docker compose logs -f doris-fe
# Look for: "Doris FE started successfully"

# Verify it's running
mysql -h127.0.0.1 -P9030 -uroot
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env if needed (defaults work for local Docker setup)
```

### 4. Run the Tutorial

Open the Jupyter notebook and run all cells:

```bash
jupyter notebook tutorial.ipynb
```

The notebook will:
1. Create sample data (Apple, Microsoft, JPMorgan)
2. Set up database tables
3. Build the search index
4. Run example queries

## Example Queries

### Basic Hybrid Search

```
Enter search query: cybersecurity risks in cloud infrastructure
```

Returns chunks ranked by:
- Semantic similarity to query (70%)
- Keyword match (20%)
- Recency of filing (10%)

### Topic Filtering

```
Enter search query: topics:RISK:CYBER,RISK:REGULATORY compliance requirements
```

Filters to chunks containing any of the specified topics, then ranks by similarity.

### Portfolio Search

```
Enter search query: portfolio:0000320193,0000789019,0001018724 AI investments
```

Returns top results per company (Apple, Microsoft, Amazon) for cross-company analysis.

## Visualize Data Lineage with CocoInsight

CocoIndex automatically tracks how data flows through your pipeline.

```bash
cocoindex server -ci main
```

Open [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight) to see:
- Source files → Transformations → Target tables
- Which PDF page each chunk came from
- Full transformation graph for debugging

## Direct Doris SQL Queries

Connect via MySQL protocol:

```bash
mysql -h localhost -P 9030 -u root
```

### Hybrid Search

```sql
USE sec_analytics;

-- Vector + keyword hybrid search
SELECT
    doc_filename,
    chunk_start,
    text,
    l2_distance(embedding, [0.1, 0.2, ...]) as distance
FROM filing_chunks
WHERE text MATCH_ANY 'cybersecurity breach'
  AND filing_date >= '2024-01-01'
ORDER BY distance ASC
LIMIT 10;
```

### Array Field Filtering

```sql
-- Filter by topics (like healthcare codes[])
SELECT doc_filename, text, topics
FROM filing_chunks
WHERE array_contains(topics, 'RISK:CYBER');

-- OR matching across topics
SELECT doc_filename, text
FROM filing_chunks
WHERE arrays_overlap(topics, ['RISK:CYBER', 'RISK:CLIMATE']);
```

### Portfolio/Cohort Aggregation

```sql
-- Top 3 relevant chunks per company
WITH ranked AS (
    SELECT
        cik,
        doc_filename,
        text,
        l2_distance(embedding, [...]) AS score,
        ROW_NUMBER() OVER (PARTITION BY cik ORDER BY score ASC) AS rank
    FROM filing_chunks
    WHERE cik IN ('0000320193', '0000789019')
)
SELECT * FROM ranked WHERE rank <= 3;
```

### Temporal Trend Analysis

```sql
-- Cybersecurity mentions by year
SELECT
    fiscal_year,
    COUNT(DISTINCT cik) AS num_companies,
    COUNT(*) AS total_mentions
FROM filing_chunks
WHERE text MATCH_ANY 'cybersecurity risk'
GROUP BY fiscal_year
ORDER BY fiscal_year DESC;
```

## Custom Functions

The example includes several custom transformation functions demonstrating CocoIndex patterns:

### 1. Metadata Extraction (1:1)

```python
@cocoindex.op.function(cache=True, behavior_version=1)
def extract_filing_metadata(filename: str) -> FilingMetadata:
    """Extract CIK, date, form type from filename."""
```

### 2. PII Scrubbing (1:1)

```python
@cocoindex.op.function(cache=True, behavior_version=1)
def scrub_pii(text: str) -> str:
    """Remove SSN, phone, email patterns."""
```

### 3. Topic Extraction (Array Output)

```python
@cocoindex.op.function(cache=True, behavior_version=1)
def extract_topics(text: str) -> list[str]:
    """Extract topic codes like RISK:CYBER, TOPIC:AI."""
```

## Technique Coverage for Healthcare

This demo validates techniques needed for healthcare document search:

| Healthcare Requirement | Finance Demo Equivalent | Status |
|-----------------------|------------------------|--------|
| FHIR codes[] array filtering | topics[] array filtering | ✅ Tested |
| VARIANT/JSON column querying | raw_metadata JSON column | ✅ Tested |
| Patient cohort aggregation | Portfolio multi-entity search | ✅ Tested |
| Temporal relevance (encounters) | Filing date recency scoring | ✅ Tested |
| PHI de-identification | PII scrubbing | ✅ Implemented |
| Audit lineage | CocoInsight visualization | ✅ Built-in |

## Project Structure

```
sec_edgar_analytics/
├── tutorial.ipynb          # Interactive tutorial (pipeline + queries)
├── functions.py            # Transformation functions (PII scrub, topics)
├── search.py               # Doris query helpers
├── download.py             # Sample data generator
├── pyproject.toml          # Dependencies
├── docker-compose.yml      # Doris + PostgreSQL
├── .env.example            # Configuration template
└── data/                   # Working data directory (gitignored)
    ├── filings/            # TXT 10-K filings
    ├── company_facts/      # JSON XBRL data
    └── exhibits_pdf/       # PDF exhibit documents
```

## Troubleshooting

### Doris Not Starting

```bash
# Check container logs
docker compose logs doris

# Restart containers
docker compose down && docker compose up -d
```

### Connection Refused

Wait for Doris to fully initialize (~60 seconds). Check health:

```bash
curl http://localhost:8030/api/bootstrap
```

### Index Build Errors

Ensure the database exists:

```sql
CREATE DATABASE IF NOT EXISTS sec_analytics;
```

## Related Resources

- [CocoIndex Documentation](https://cocoindex.io/docs)
- [Apache Doris Vector Search](https://doris.apache.org/docs/dev/data-table/data-model)
- [SEC EDGAR API](https://www.sec.gov/developer)
