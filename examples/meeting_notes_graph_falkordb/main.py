"""
Meeting Notes Graph (v1) - CocoIndex pipeline example, FalkorDB flavor.
"""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import cocoindex as coco
from cocoindex.connectors import falkordb, google_drive
from cocoindex.ops.entity_resolution import ResolvedEntities, resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

sys.path.append(str(Path(__file__).resolve().parents[2]))
from examples.meeting_notes_graph_common import (  # noqa: E402
    ExtractedMeeting,
    Meeting,
    MeetingExtraction,
    Person,
    Task,
    collect_meeting_extractions,
    declare_person_relations,
    extract_meeting_with_model,
    process_file_common,
    raw_person_names,
)

KG_DB = coco.ContextKey[falkordb.ConnectionFactory]("kg_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)
RESOLUTION_LLM_MODEL = coco.ContextKey[str]("resolution_llm_model", detect_change=True)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.provide(
        KG_DB,
        falkordb.ConnectionFactory(
            uri=os.environ.get("FALKORDB_URI", "falkor://localhost:6379"),
            graph=os.environ.get("FALKORDB_GRAPH", "meeting_notes"),
        ),
    )
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-5.4"))
    builder.provide(
        RESOLUTION_LLM_MODEL,
        os.environ.get("RESOLUTION_LLM_MODEL", "openai/gpt-5-mini"),
    )
    builder.provide(
        EMBEDDER,
        SentenceTransformerEmbedder("Snowflake/snowflake-arctic-embed-xs"),
    )
    yield


@coco.fn(memo=True)
async def extract_meeting(section_text: str) -> ExtractedMeeting:
    return await extract_meeting_with_model(section_text, coco.use_context(LLM_MODEL))


@coco.fn(memo=True)
async def process_file(
    file: google_drive.DriveFile,
    meeting_table: falkordb.TableTarget[Meeting],
    task_table: falkordb.TableTarget[Task],
    decided_rel: falkordb.RelationTarget[Any],
) -> list[MeetingExtraction]:
    return await process_file_common(
        file, meeting_table, task_table, decided_rel, extract_meeting
    )


@coco.fn(memo=True)
async def _resolve_persons(raw_persons: set[str]) -> ResolvedEntities:
    return await resolve_entities(
        entities=raw_persons,
        embedder=coco.use_context(EMBEDDER),
        resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)),
    )


@coco.fn
async def create_person_relations(
    meetings: list[MeetingExtraction],
    persons: ResolvedEntities,
    person_table: falkordb.TableTarget[Person],
    attended_rel: falkordb.RelationTarget[Any],
    assigned_rel: falkordb.RelationTarget[Any],
) -> None:
    declare_person_relations(
        meetings, persons, person_table, attended_rel, assigned_rel
    )


@coco.fn
async def app_main() -> None:
    meeting_table = await falkordb.mount_table_target(
        KG_DB,
        "Meeting",
        await falkordb.TableSchema.from_class(Meeting, primary_key="id"),
        primary_key="id",
    )
    person_table = await falkordb.mount_table_target(
        KG_DB,
        "Person",
        await falkordb.TableSchema.from_class(Person, primary_key="name"),
        primary_key="name",
    )
    task_table = await falkordb.mount_table_target(
        KG_DB,
        "Task",
        await falkordb.TableSchema.from_class(Task, primary_key="description"),
        primary_key="description",
    )

    attended_rel = await falkordb.mount_relation_target(
        KG_DB, "ATTENDED", person_table, meeting_table
    )
    decided_rel = await falkordb.mount_relation_target(
        KG_DB, "DECIDED", meeting_table, task_table
    )
    assigned_rel = await falkordb.mount_relation_target(
        KG_DB, "ASSIGNED_TO", person_table, task_table
    )

    all_meetings = await collect_meeting_extractions(
        process_file, meeting_table, task_table, decided_rel
    )

    persons = await coco.use_mount(
        coco.component_subpath("resolve_persons"),
        _resolve_persons,
        raw_person_names(all_meetings),
    )

    await coco.mount(
        coco.component_subpath("person_relations"),
        create_person_relations,
        all_meetings,
        persons,
        person_table,
        attended_rel,
        assigned_rel,
    )


app = coco.App(
    coco.AppConfig(name="MeetingNotesGraphFalkorDB"),
    app_main,
)
