"""
Hybrid query over the zvec scene collection.

Runs straight against zvec (querying stays outside CocoIndex, which only writes).
Combines three signals in one call:

  - dense: CLIP text embedding vs the keyframe vector
  - fts: full-text match over the transcript
  - scalar: a boolean filter over start / end / video_path

A reranker (RRF by default) fuses the dense and fts sub-queries.

Examples (against the downloaded sample corpus):

    python query.py "two people talking on a bridge" --fts "robotics"
    python query.py "a red-haired girl" --mode dense
    python query.py "" --mode fts --fts "robot hand"
    python query.py "two people talking on a bridge" --filter "start < 15"
"""

from __future__ import annotations

import argparse

import zvec  # the zvec package: Query / Fts / RrfReRanker
from cocoindex.connectors import zvec as coco_zvec  # the connector: managed_connection

import main  # CLIP helpers + collection config


def _run(
    text: str,
    fts: str | None,
    scalar_filter: str | None,
    topk: int,
    mode: str,
) -> None:
    queries: list[zvec.Query] = []
    if mode in ("hybrid", "dense") and text:
        # Loads the CLIP model on the first dense query, which can take a while
        # (model download on the very first run, then load from cache).
        print("Loading the CLIP model to embed the query...", flush=True)
        queries.append(
            zvec.Query(field_name="embedding", vector=main.embed_query(text))
        )
    if mode in ("hybrid", "fts"):
        match = fts if fts is not None else text
        if match:
            queries.append(
                zvec.Query(field_name="transcript", fts=zvec.Fts(match_string=match))
            )

    if not queries:
        raise SystemExit("Nothing to query: provide query text and/or --fts.")

    # A reranker only applies when fusing more than one sub-query.
    reranker = zvec.RrfReRanker() if len(queries) > 1 else None

    if not (main.ZVEC_BASE_PATH / main.COLLECTION_NAME).exists():
        raise SystemExit(
            f"No collection at {main.ZVEC_BASE_PATH / main.COLLECTION_NAME}. "
            "Index the videos first with: cocoindex update main.py"
        )

    print("Opening the zvec collection...", flush=True)
    with coco_zvec.managed_connection(main.ZVEC_BASE_PATH) as conn:
        col = conn.open_existing(main.COLLECTION_NAME)
        results = col.query(
            queries=queries,
            topk=topk,
            filter=scalar_filter,
            reranker=reranker,
            output_fields=["video_path", "start", "end", "transcript"],
        )

    if not results:
        print("No matches.")
        return

    for i, doc in enumerate(results, 1):
        f = doc.fields or {}
        start = f.get("start")
        end = f.get("end")
        span = f"{start:.1f}-{end:.1f}s" if start is not None else "?"
        transcript = (f.get("transcript") or "").strip()
        snippet = transcript[:100] + ("..." if len(transcript) > 100 else "")
        print(f"{i}. [{doc.score:.4f}] {f.get('video_path')} @ {span}")
        if snippet:
            print(f"     {snippet}")


def main_cli() -> None:
    p = argparse.ArgumentParser(
        description="Hybrid search over the zvec scene collection."
    )
    p.add_argument(
        "query", help="Natural-language query (dense side). Use '' for fts-only."
    )
    p.add_argument(
        "--fts", default=None, help="FTS match string. Defaults to the query text."
    )
    p.add_argument(
        "--filter",
        dest="scalar_filter",
        default=None,
        help='zvec scalar filter, e.g. "start >= 10 and end < 60".',
    )
    p.add_argument(
        "--topk", type=int, default=10, help="Number of results (default 10)."
    )
    p.add_argument(
        "--mode",
        choices=["hybrid", "dense", "fts"],
        default="hybrid",
        help="Which signals to use (default hybrid).",
    )
    args = p.parse_args()
    _run(args.query, args.fts, args.scalar_filter, args.topk, args.mode)


if __name__ == "__main__":
    main_cli()
