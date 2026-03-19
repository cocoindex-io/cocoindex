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
from cocoindex.resources.id import IdGenerator

from .extract import extract_metadata, extract_statements, format_transcript
from .fetch import fetch_transcript
from .models import (
    ENTITY_TYPES,
    LLM_MODEL,
    RESOLUTION_LLM_MODEL,
    PERSON_ENTITY_NAME,
    Entity,
    IdentifiedStatement,
    Session,
    SessionRawEntities,
    Statement,
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
        SentenceTransformerEmbedder("Snowflake/snowflake-arctic-embed-xs"),
    )
    builder.provide(
        LLM_MODEL,
        os.environ.get("LLM_MODEL", "openai/gpt-5.4"),
    )
    builder.provide(
        RESOLUTION_LLM_MODEL,
        os.environ.get("RESOLUTION_LLM_MODEL", "openai/gpt-5-mini"),
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
    """Process a single session: fetch, extract (2-step), declare session + statements."""
    transcript = await fetch_transcript(youtube_id)

    # Step 1: format with empty map (no names known yet), then extract metadata
    step1_text = format_transcript(transcript.utterances, {})
    metadata = await extract_metadata(step1_text, transcript)

    # Step 2: format with real names, then extract statements
    speaker_map = {s.label: s.name for s in metadata.speakers}
    step2_text = format_transcript(transcript.utterances, speaker_map)
    stmt_extraction = await extract_statements(step2_text)

    id_gen = IdGenerator(youtube_id)

    # Declare session node (store the fully-resolved transcript)
    session_id = await id_gen.next_id()
    session = Session(
        id=session_id,
        youtube_id=youtube_id,
        name=metadata.name or transcript.yt_title,
        description=metadata.description,
        transcript=step2_text,
        date=metadata.date or transcript.yt_upload_date,
    )
    session_table.declare_record(row=session)

    # Declare statements + session_statement edges
    identified_stmts: list[IdentifiedStatement] = []
    for stmt in stmt_extraction.statements:
        stmt_id = await id_gen.next_id(stmt.statement)
        statement_table.declare_record(
            row=Statement(id=stmt_id, statement=stmt.statement)
        )
        session_statement_rel.declare_relation(from_id=session_id, to_id=stmt_id)
        identified_stmts.append(IdentifiedStatement(id=stmt_id, raw=stmt))

    # Only identified speakers form the person_session relationship.
    identified_persons = [s.name for s in metadata.speakers]

    return SessionRawEntities(
        session_id=session_id,
        raw_entities={PERSON_ENTITY_NAME: identified_persons},
        statements=identified_stmts,
    )


# ---------------------------------------------------------------------------
# Phase 3: Knowledge base creation
# ---------------------------------------------------------------------------


@coco.fn
async def create_knowledge_base(
    all_session_raw: list[SessionRawEntities],
    entity_dedup: dict[str, dict[str, str | None]],
    entity_tables: dict[str, surrealdb.TableTarget[Any]],
    person_session_rel: surrealdb.RelationTarget[Any],
    person_statement_rel: surrealdb.RelationTarget[Any],
    statement_mentions_rel: surrealdb.RelationTarget[Any],
) -> None:
    """Declare canonical entity nodes and all relationships."""
    # Declare canonical nodes for each entity type (name is the ID)
    for cfg in ENTITY_TYPES:
        dedup = entity_dedup[cfg.name]
        table = entity_tables[cfg.name]
        for name, upstream in dedup.items():
            if upstream is None:
                table.declare_record(row=Entity(id=name, name=name))

    person_dedup = entity_dedup[PERSON_ENTITY_NAME]

    # Declare relationships
    for session_raw in all_session_raw:
        # person_session: person attended session
        for person_name in session_raw.raw_entities.get(PERSON_ENTITY_NAME, []):
            canonical = resolve_canonical(person_name, person_dedup)
            person_session_rel.declare_relation(
                from_id=canonical,
                to_id=session_raw.session_id,
            )

        # person_statement + statement_mentions
        for identified in session_raw.statements:
            stmt = identified.raw
            stmt_id = identified.id
            # person_statement: person made the statement
            seen_speakers: set[str] = set()
            for speaker in stmt.speakers:
                canonical = resolve_canonical(speaker, person_dedup)
                if canonical not in seen_speakers:
                    seen_speakers.add(canonical)
                    person_statement_rel.declare_relation(
                        from_id=canonical, to_id=stmt_id
                    )
            # statement_mentions: deduplicate after resolution
            for cfg in ENTITY_TYPES:
                dedup = entity_dedup[cfg.name]
                table = entity_tables[cfg.name]
                for canonical in {
                    resolve_canonical(e, dedup)
                    for e in getattr(stmt, f"mentioned_{cfg.name}")
                }:
                    statement_mentions_rel.declare_relation(
                        from_id=stmt_id,
                        to_id=canonical,
                        to_table=table,
                    )


# ---------------------------------------------------------------------------
# Helpers for collecting raw entities
# ---------------------------------------------------------------------------


def _collect_all_raw(
    all_session_raw: list[SessionRawEntities],
    cfg_name: str,
) -> set[str]:
    """Collect all raw entity names of a given type across sessions."""
    result: set[str] = set()
    for session_raw in all_session_raw:
        result.update(session_raw.raw_entities.get(cfg_name, []))
        for identified in session_raw.statements:
            stmt = identified.raw
            if cfg_name == PERSON_ENTITY_NAME:
                result.update(stmt.speakers)
            result.update(getattr(stmt, f"mentioned_{cfg_name}"))
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
    entity_schema = await surrealdb.TableSchema.from_class(Entity)
    entity_tables: dict[str, surrealdb.TableTarget[Any]] = {
        cfg.name: await surrealdb.mount_table_target(
            SURREAL_DB, cfg.name, entity_schema
        )
        for cfg in ENTITY_TYPES
    }

    # --- Setup relation targets ---
    session_statement_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "session_statement", session_table, statement_table
    )
    person_table = entity_tables[PERSON_ENTITY_NAME]
    person_session_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "person_session", person_table, session_table
    )
    person_statement_rel = await surrealdb.mount_relation_target(
        SURREAL_DB, "person_statement", person_table, statement_table
    )
    statement_mentions_rel = await surrealdb.mount_relation_target(
        SURREAL_DB,
        "statement_mentions",
        statement_table,
        [entity_tables[cfg.name] for cfg in ENTITY_TYPES],  # polymorphic TO
    )

    # --- Phase 1: Per-session processing ---
    files = localfs.walk_dir(
        pathlib.Path(os.environ.get("INPUT_DIR", "./input")),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
    )

    session_coros = []
    async for _key, file in files.items():
        text = await file.read_text()
        for line in text.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            youtube_id = extract_video_id(line)
            session_coros.append(
                coco.use_mount(
                    coco.component_subpath("session", youtube_id),
                    process_session,
                    youtube_id,
                    session_table,
                    statement_table,
                    session_statement_rel,
                )
            )
    all_session_raw = list(await asyncio.gather(*session_coros))

    # --- Phase 2: Entity resolution (one mount per entity type) ---
    entity_dedup: dict[str, dict[str, str | None]] = dict(
        zip(
            [cfg.name for cfg in ENTITY_TYPES],
            await asyncio.gather(
                *(
                    coco.use_mount(
                        coco.component_subpath("resolve", cfg.name),
                        resolve_entities,
                        _collect_all_raw(all_session_raw, cfg.name),
                    )
                    for cfg in ENTITY_TYPES
                )
            ),
        )
    )

    # --- Phase 3: Declare knowledge base ---
    await coco.mount(
        coco.component_subpath("knowledge_base"),
        create_knowledge_base,
        all_session_raw=all_session_raw,
        entity_dedup=entity_dedup,
        entity_tables=entity_tables,
        person_session_rel=person_session_rel,
        person_statement_rel=person_statement_rel,
        statement_mentions_rel=statement_mentions_rel,
    )


app = coco.App(
    coco.AppConfig(name="ConversationToKnowledge"),
    app_main,
)
