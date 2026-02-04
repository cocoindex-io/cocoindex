"""
SEC EDGAR Analytics - SQL Helpers

Low-level Doris query utilities shared by notebook search functions.
"""

import re

import cocoindex.targets.doris as coco_doris


def extract_keywords(query: str) -> str:
    """Extract meaningful keywords, removing stop words."""
    stop_words = {
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "to",
        "of",
        "in",
        "for",
        "on",
        "with",
        "at",
        "by",
        "from",
        "that",
        "this",
        "it",
        "and",
        "or",
        "but",
        "not",
    }
    words = re.findall(r"\b\w+\b", query.lower())
    return " ".join(w for w in words if w not in stop_words and len(w) > 2)


def format_embedding(vec: list[float]) -> str:
    """Format embedding vector for SQL."""
    return f"[{','.join(str(v) for v in vec)}]"


def format_list(items: list[str]) -> str:
    """Format list for SQL IN clause."""
    return ",".join(repr(s) for s in items)


def build_where(conditions: list[str]) -> str:
    """Build WHERE clause from conditions."""
    return " AND ".join(conditions) if conditions else "1=1"


async def doris_query(config: dict, sql: str) -> list[tuple]:
    """Execute SQL and return rows."""
    conn = await coco_doris.connect_async(
        fe_host=config["fe_host"],
        query_port=config["query_port"],
        username=config["username"],
        password=config["password"],
        database=config["database"],
    )
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql)
            return await cursor.fetchall()
    finally:
        conn.close()
        await conn.ensure_closed()
