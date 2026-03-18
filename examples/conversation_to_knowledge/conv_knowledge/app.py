"""
Conversation to Knowledge — CocoIndex pipeline example.

Convert podcast sessions (from YouTube) into a structured knowledge graph
stored in SurrealDB, with entity resolution for persons, techs, and orgs.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import re
from collections.abc import AsyncIterator
from typing import Any

import cocoindex as coco
from cocoindex.connectors import localfs, surrealdb
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import PatternFilePathMatcher

from .extract import extract_session
from .fetch import fetch_transcript
from .models import (
    LLM_MODEL,
    Org,
    Person,
    RawStatement,
    Session,
    SessionRawEntities,
    Statement,
    Tech,
    make_id,
    resolve_canonical,
)
from .resolve import EMBEDDER, resolve_entities

# ---------------------------------------------------------------------------
# Context keys
# ---------------------------------------------------------------------------

SURREAL_DB = coco.ContextKey[surrealdb.ConnectionFactory]("surreal_db", tracked=False)

# ---------------------------------------------------------------------------
# YouTube URL parsing
# ---------------------------------------------------------------------------

_YOUTUBE_URL_RE = re.compile(
    r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})"
)


def extract_video_id(url: str) -> str:
    m = _YOUTUBE_URL_RE.search(url)
    if m is None:
        raise ValueError(f"Cannot extract YouTube video ID from: {url}")
    return m.group(1)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.provide(
        SURREAL_DB,
        surrealdb.ConnectionFactory(
            url=os.environ.get("SURREALDB_URL", "ws://localhost:8787/rpc"),
            namespace=os.environ.get("SURREALDB_NS", "cocoindex"),
            database=os.environ.get("SURREALDB_DB", "yt_conversations"),
            credentials={
                "username": os.environ.get("SURREALDB_USER", "root"),
                "password": os.environ.get("SURREALDB_PASS", "root"),
            },
        ),
    )
    builder.provide(
        EMBEDDER,
        SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2"),
    )
    builder.provide(
        LLM_MODEL,
        os.environ.get("LLM_MODEL", "openai/gpt-5.4-mini"),
    )
    yield


# ---------------------------------------------------------------------------
# Phase 1: Per-session processing
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_session(
    youtube_id: str,
    session_table: surrealdb.TableTarget[Any],
    statement_table: surrealdb.TableTarget[Any],
    session_statement_rel: surrealdb.RelationTarget[Any],
) -> SessionRawEntities:
    """Process a single session: fetch, extract, declare session + statements."""
    transcript = await fetch_transcript(youtube_id)
    extraction = await extract_session(transcript)

    # Declare session node
    session = Session(
        id=youtube_id,
        name=extraction.name or transcript.yt_title,
        description=extraction.description,
        transcript=transcript.transcript,
        date=extraction.date or transcript.yt_upload_date,
    )
    session_table.declare_record(row=session)

    # Declare statements + session_statement edges
    raw_statements: list[RawStatement] = []
    for stmt in extraction.statements:
        stmt_id = make_id(youtube_id, stmt.statement)
        statement_table.declare_record(
            row=Statement(id=stmt_id, statement=stmt.statement)
        )
        session_statement_rel.declare_relation(from_id=youtube_id, to_id=stmt_id)
        raw_statements.append(stmt)

    return SessionRawEntities(
        session_id=youtube_id,
        persons=list(extraction.persons_attending),
        statements=raw_statements,
    )


# ---------------------------------------------------------------------------
# Phase 3: Knowledge base creation
# ---------------------------------------------------------------------------


@coco.fn
async def create_knowledge_base(
    all_session_raw: list[SessionRawEntities],
    person_dedup: dict[str, str | None],
    tech_dedup: dict[str, str | None],
    org_dedup: dict[str, str | None],
    person_table: surrealdb.TableTarget[Any],
    tech_table: surrealdb.TableTarget[Any],
    org_table: surrealdb.TableTarget[Any],
    person_session_rel: surrealdb.RelationTarget[Any],
    person_statement_rel: surrealdb.RelationTarget[Any],
    statement_involves_rel: surrealdb.RelationTarget[Any],
) -> None:
    """Declare canonical entity nodes and all relationships."""
    # Declare canonical person nodes
    for name, upstream in person_dedup.items():
        if upstream is None:
            person_table.declare_record(row=Person(id=make_id(name), name=name))
    # Declare canonical tech nodes
    for name, upstream in tech_dedup.items():
        if upstream is None:
            tech_table.declare_record(row=Tech(id=make_id(name), name=name))
    # Declare canonical org nodes
    for name, upstream in org_dedup.items():
        if upstream is None:
            org_table.declare_record(row=Org(id=make_id(name), name=name))

    # Declare relationships
    for session_raw in all_session_raw:
        # person_session: person attended session
        for person_name in session_raw.persons:
            canonical = resolve_canonical(person_name, person_dedup)
            person_session_rel.declare_relation(
                from_id=make_id(canonical),
                to_id=session_raw.session_id,
            )

        # person_statement + statement_involves
        for stmt in session_raw.statements:
            stmt_id = make_id(session_raw.session_id, stmt.statement)
            # person_statement: person made the statement
            seen_speakers: set[str] = set()
            for speaker in stmt.speakers:
                canonical = resolve_canonical(speaker, person_dedup)
                cid = make_id(canonical)
                if cid not in seen_speakers:
                    seen_speakers.add(cid)
                    person_statement_rel.declare_relation(from_id=cid, to_id=stmt_id)
            # statement_involves: deduplicate after resolution
            seen_involves: set[str] = set()
            for p in stmt.involved_persons:
                canonical = resolve_canonical(p, person_dedup)
                cid = make_id(canonical)
                key = f"person:{cid}"
                if key not in seen_involves:
                    seen_involves.add(key)
                    statement_involves_rel.declare_relation(
                        from_id=stmt_id,
                        to_id=cid,
                        to_table=person_table,
                    )
            for t in stmt.involved_techs:
                canonical = resolve_canonical(t, tech_dedup)
                cid = make_id(canonical)
                key = f"tech:{cid}"
                if key not in seen_involves:
                    seen_involves.add(key)
                    statement_involves_rel.declare_relation(
                        from_id=stmt_id,
                        to_id=cid,
                        to_table=tech_table,
                    )
            for o in stmt.involved_orgs:
                canonical = resolve_canonical(o, org_dedup)
                cid = make_id(canonical)
                key = f"org:{cid}"
                if key not in seen_involves:
                    seen_involves.add(key)
                    statement_involves_rel.declare_relation(
                        from_id=stmt_id,
                        to_id=cid,
                        to_table=org_table,
                    )


# ---------------------------------------------------------------------------
# Helpers for collecting raw entities
# ---------------------------------------------------------------------------


def _collect_all_raw(
    all_session_raw: list[SessionRawEntities],
    entity_type: str,
) -> set[str]:
    """Collect all raw entity names of a given type across sessions."""
    result: set[str] = set()
    for session_raw in all_session_raw:
        if entity_type == "persons":
            result.update(session_raw.persons)
            for stmt in session_raw.statements:
                result.update(stmt.speakers)
                result.update(stmt.involved_persons)
        elif entity_type == "techs":
            for stmt in session_raw.statements:
                result.update(stmt.involved_techs)
        elif entity_type == "orgs":
            for stmt in session_raw.statements:
                result.update(stmt.involved_orgs)
    return result


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main() -> None:
    # --- Setup table targets ---
    session_table = await surrealdb.mount_table_target(
        SURREAL_DB, "session", await surrealdb.TableSchema.from_class(Session)
    )
    statement_table = await surrealdb.mount_table_target(
        SURREAL_DB, "statement", await surrealdb.TableSchema.from_class(Statement)
    )
    person_table = await surrealdb.mount_table_target(
        SURREAL_DB, "person", await surrealdb.TableSchema.from_class(Person)
    )
    tech_table = await surrealdb.mount_table_target(
        SURREAL_DB, "tech", await surrealdb.TableSchema.from_class(Tech)
    )
    org_table = await surrealdb.mount_table_target(
        SURREAL_DB, "org", await surrealdb.TableSchema.from_class(Org)
    )

    # --- Setup relation targets ---
    session_statement_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "session_statement", session_table, statement_table
    )
    person_session_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "person_session", person_table, session_table
    )
    person_statement_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "person_statement", person_table, statement_table
    )
    statement_involves_rel = await surrealdb.mount_relation_target(
        SURREAL_DB,
        "statement_involves",
        statement_table,
        [person_table, tech_table, org_table],  # polymorphic TO
    )

    # --- Phase 1: Per-session processing ---
    files = localfs.walk_dir(
        pathlib.Path(os.environ.get("INPUT_DIR", "./input")),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
    )

    all_session_raw: list[SessionRawEntities] = []
    async for _key, file in files.items():
        text = await file.read_text()
        for line in text.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            youtube_id = extract_video_id(line)
            raw = await coco.use_mount(
                coco.component_subpath("session", youtube_id),
                process_session,
                youtube_id,
                session_table,
                statement_table,
                session_statement_rel,
            )
            all_session_raw.append(raw)

    # --- Phase 2: Entity resolution ---
    all_raw_persons = _collect_all_raw(all_session_raw, "persons")
    all_raw_techs = _collect_all_raw(all_session_raw, "techs")
    all_raw_orgs = _collect_all_raw(all_session_raw, "orgs")

    person_dedup, tech_dedup, org_dedup = await asyncio.gather(
        coco.use_mount(
            coco.component_subpath("resolve", "person"),
            resolve_entities,
            all_raw_persons,
        ),
        coco.use_mount(
            coco.component_subpath("resolve", "tech"),
            resolve_entities,
            all_raw_techs,
        ),
        coco.use_mount(
            coco.component_subpath("resolve", "org"),
            resolve_entities,
            all_raw_orgs,
        ),
    )

    # --- Phase 3: Declare knowledge base ---
    await coco.mount(
        coco.component_subpath("knowledge_base"),
        create_knowledge_base,
        all_session_raw=all_session_raw,
        person_dedup=person_dedup,
        tech_dedup=tech_dedup,
        org_dedup=org_dedup,
        person_table=person_table,
        tech_table=tech_table,
        org_table=org_table,
        person_session_rel=person_session_rel,
        person_statement_rel=person_statement_rel,
        statement_involves_rel=statement_involves_rel,
    )


app = coco.App(
    coco.AppConfig(name="ConversationToKnowledge"),
    app_main,
)
