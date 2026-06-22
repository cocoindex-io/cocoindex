"""
Hybrid search over the SEC filing index in Apache Doris.

Combines semantic similarity (vector / l2_distance) with keyword matching
(full-text MATCH_ANY on the inverted index) using Reciprocal Rank Fusion (RRF).

    python search.py "cybersecurity risk"
    python search.py "cloud revenue" --source facts
"""

from __future__ import annotations

import asyncio
import os
import re
import sys

import pymysql
from dotenv import load_dotenv

from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

load_dotenv()
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
_embedder = SentenceTransformerEmbedder(EMBED_MODEL)


def _conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.environ.get("DORIS_FE_HOST", "localhost"),
        port=int(os.environ.get("DORIS_QUERY_PORT", "9030")),
        user=os.environ.get("DORIS_USERNAME", "root"),
        password=os.environ.get("DORIS_PASSWORD", ""),
        database=os.environ.get("DORIS_DATABASE", "sec_analytics"),
        autocommit=True,
    )


def search(query: str, source_type: str | None = None, limit: int = 5) -> None:
    vec = asyncio.run(_embedder.embed(query))
    vstr = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
    keywords = " ".join(re.findall(r"[A-Za-z]{3,}", query.lower()))
    where = f"source_type = '{source_type}'" if source_type else "1 = 1"

    sql = f"""
    WITH semantic AS (
        SELECT chunk_id, doc_filename, source_type, topics, text,
               ROW_NUMBER() OVER (ORDER BY l2_distance(embedding, {vstr})) AS rk
        FROM filing_chunks WHERE {where}
    ),
    lexical AS (
        SELECT chunk_id,
               ROW_NUMBER() OVER (ORDER BY CASE WHEN text MATCH_ANY '{keywords}'
                                  THEN 0 ELSE 1 END) AS rk
        FROM filing_chunks WHERE {where}
    )
    SELECT s.doc_filename, s.source_type, s.topics, s.text,
           1.0/(60 + s.rk) + 1.0/(60 + l.rk) AS rrf
    FROM semantic s JOIN lexical l USING (chunk_id)
    ORDER BY rrf DESC LIMIT {limit}
    """
    c = _conn()
    cur = c.cursor()
    cur.execute(sql)
    print(
        f'\nHybrid search: "{query}"'
        + (f" [source={source_type}]" if source_type else "")
    )
    for fn, st, topics, text, rrf in cur.fetchall():
        print(f"\n[{float(rrf):.4f}] {fn} ({st})  topics={topics}")
        print(f"    {text.strip()[:160]}")
    c.close()


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    src = None
    if "--source" in sys.argv:
        src = sys.argv[sys.argv.index("--source") + 1]
    if not args:
        print(__doc__)
    else:
        search(" ".join(args), source_type=src)
