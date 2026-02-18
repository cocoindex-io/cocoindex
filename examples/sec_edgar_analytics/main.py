"""
SEC EDGAR Financial Analytics - Main Entry Point

Run with:
    cocoindex setup main.py    # Create tables
    cocoindex update main.py   # Process data
    cocoindex server -ci main.py  # Launch with CocoInsight UI

Or run directly for interactive search:
    python main.py
"""

import os
from datetime import timedelta

from dotenv import load_dotenv

import cocoindex
import cocoindex.targets.doris as coco_doris

from functions import (
    extract_filing_metadata,
    extract_json_metadata,
    parse_company_facts,
    extract_pdf_metadata,
    pdf_to_markdown,
    scrub_pii,
    extract_topics,
    text_to_embedding,
)
from search import (
    extract_keywords,
    format_embedding,
    format_list,
    build_where,
    doris_query,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()

DORIS_FE_HOST: str = os.environ.get("DORIS_FE_HOST", "localhost")
DORIS_FE_HTTP_PORT: int = int(os.environ.get("DORIS_HTTP_PORT", "8030"))
DORIS_BE_LOAD_HOST: str | None = os.environ.get("DORIS_BE_LOAD_HOST", None)
DORIS_QUERY_PORT: int = int(os.environ.get("DORIS_QUERY_PORT", "9030"))
DORIS_USERNAME: str = os.environ.get("DORIS_USERNAME", "root")
DORIS_PASSWORD: str = os.environ.get("DORIS_PASSWORD", "")
DORIS_DATABASE: str = os.environ.get("DORIS_DATABASE", "sec_analytics")

TABLE_CHUNKS = "filing_chunks"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def process_and_collect(
    doc: cocoindex.DataScope,
    text_field: str,
    metadata: cocoindex.DataSlice,
    collector: cocoindex.flow.DataCollector,
) -> None:
    """
    Common chunk processing for all source types.

    1. Scrub PII from text
    2. Split into chunks (1000 chars, 200 overlap)
    3. Generate embeddings
    4. Extract topic tags
    5. Collect into unified index
    """
    doc["scrubbed"] = doc[text_field].transform(scrub_pii)

    doc["chunks"] = doc["scrubbed"].transform(
        cocoindex.functions.SplitRecursively(),
        language="markdown",
        chunk_size=1000,
        chunk_overlap=200,
    )

    with doc["chunks"].row() as chunk:
        chunk["embedding"] = text_to_embedding(chunk["text"])
        chunk["topics"] = chunk["text"].transform(extract_topics)

        collector.collect(
            chunk_id=cocoindex.GeneratedField.UUID,
            source_type=metadata["source_type"],
            doc_filename=doc["filename"],
            location=chunk["location"],
            cik=metadata["cik"],
            filing_date=metadata["filing_date"],
            form_type=metadata["form_type"],
            fiscal_year=metadata["fiscal_year"],
            text=chunk["text"],
            embedding=chunk["embedding"],
            topics=chunk["topics"],
        )


# =============================================================================
# FLOW DEFINITION
# =============================================================================


@cocoindex.flow_def(name="SECFilingAnalytics")
def sec_filing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    SEC Filing Analytics Pipeline - Multi-Source

    Ingests TXT, JSON, and PDF into a unified searchable index.
    """

    # =========================================================================
    # SOURCES
    # =========================================================================
    data_scope["txt_filings"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data/filings", included_patterns=["*.txt"]),
        refresh_interval=timedelta(hours=1),
    )
    data_scope["json_facts"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="data/company_facts", included_patterns=["*.json"]
        ),
        refresh_interval=timedelta(hours=1),
    )
    data_scope["pdf_exhibits"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="data/exhibits_pdf", included_patterns=["*.pdf"], binary=True
        ),
        refresh_interval=timedelta(hours=1),
    )

    # =========================================================================
    # UNIFIED COLLECTOR
    # =========================================================================
    chunk_collector = data_scope.add_collector()

    # =========================================================================
    # PROCESS EACH SOURCE
    # =========================================================================

    # TXT Filings
    with data_scope["txt_filings"].row() as filing:
        filing["metadata"] = filing["filename"].transform(extract_filing_metadata)
        process_and_collect(filing, "content", filing["metadata"], chunk_collector)

    # JSON Facts
    with data_scope["json_facts"].row() as facts:
        facts["metadata"] = facts["filename"].transform(
            extract_json_metadata, content=facts["content"]
        )
        facts["parsed"] = facts["content"].transform(parse_company_facts)
        process_and_collect(facts, "parsed", facts["metadata"], chunk_collector)

    # PDF Exhibits
    with data_scope["pdf_exhibits"].row() as pdf:
        pdf["metadata"] = pdf["filename"].transform(extract_pdf_metadata)
        pdf["markdown"] = pdf["content"].transform(pdf_to_markdown)
        process_and_collect(pdf, "markdown", pdf["metadata"], chunk_collector)

    # =========================================================================
    # EXPORT TO DORIS
    # =========================================================================
    chunk_collector.export(
        "filing_chunks",
        coco_doris.DorisTarget(
            fe_host=DORIS_FE_HOST,
            fe_http_port=DORIS_FE_HTTP_PORT,
            be_load_host=DORIS_BE_LOAD_HOST,
            query_port=DORIS_QUERY_PORT,
            username=DORIS_USERNAME,
            password=DORIS_PASSWORD,
            database=DORIS_DATABASE,
            table=TABLE_CHUNKS,
        ),
        primary_key_fields=["chunk_id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
            )
        ],
        fts_indexes=[
            cocoindex.FtsIndexDef(field_name="text", parameters={"parser": "unicode"})
        ],
    )


# =============================================================================
# SEARCH FUNCTIONS
# =============================================================================


async def search(
    query: str,
    time_gate_days: int | None = None,
    source_types: list[str] | None = None,
    limit: int = 10,
) -> list[dict]:
    """
    Hybrid search using RRF (Reciprocal Rank Fusion).
    Combines semantic similarity and keyword matching.
    """
    table = f"{DORIS_DATABASE}.{TABLE_CHUNKS}"
    embedding = format_embedding(await text_to_embedding.eval_async(query))
    keywords = extract_keywords(query)

    conditions = []
    if time_gate_days:
        conditions.append(
            f"filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {time_gate_days} DAY)"
        )
    if source_types:
        conditions.append(f"source_type IN ({format_list(source_types)})")
    where = build_where(conditions)

    sql = f"""
    WITH
    semantic AS (
        SELECT chunk_id, doc_filename, cik, filing_date, source_type, text, topics,
               ROW_NUMBER() OVER (ORDER BY l2_distance(embedding, {embedding})) AS rank
        FROM {table} WHERE {where}
    ),
    lexical AS (
        SELECT chunk_id,
               ROW_NUMBER() OVER (ORDER BY CASE WHEN text MATCH_ANY '{keywords}' THEN 0 ELSE 1 END) AS rank
        FROM {table} WHERE {where}
    )
    SELECT s.*, l.rank AS lex_rank,
           1.0/(60 + s.rank) + 1.0/(60 + l.rank) AS score
    FROM semantic s JOIN lexical l USING (chunk_id)
    ORDER BY score DESC LIMIT {limit}
    """

    return [
        {
            "doc_filename": r[1],
            "cik": r[2],
            "filing_date": str(r[3]) if r[3] else None,
            "source_type": r[4],
            "text": r[5],
            "topics": r[6] or [],
            "sem_rank": r[7],
            "lex_rank": r[8],
            "rrf_score": float(r[9]),
        }
        for r in await doris_query(
            {
                "fe_host": DORIS_FE_HOST,
                "query_port": DORIS_QUERY_PORT,
                "username": DORIS_USERNAME,
                "password": DORIS_PASSWORD,
                "database": DORIS_DATABASE,
            },
            sql,
        )
    ]


# =============================================================================
# MAIN
# =============================================================================


async def _main() -> None:
    """Interactive search loop."""
    print("SEC EDGAR Financial Analytics")
    print("=" * 40)
    print("Enter a search query to find relevant SEC filings.")
    print("Press Enter with no input to quit.\n")

    while True:
        query = input("Search: ").strip()
        if not query:
            break

        print("\nSearching...\n")
        results = await search(query, limit=5)

        if not results:
            print("No results found.\n")
            continue

        for i, r in enumerate(results, 1):
            print(f"{i}. [{r['rrf_score']:.4f}] {r['doc_filename']}")
            print(
                f"   CIK: {r['cik']} | Source: {r['source_type']} | Topics: {r['topics']}"
            )
            print(f"   {r['text'][:150]}...")
            print()


if __name__ == "__main__":
    import asyncio

    cocoindex.init()
    asyncio.run(_main())
