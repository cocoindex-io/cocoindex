"""
Meeting Notes Graph (v1) — CocoIndex pipeline example.

Ingest Markdown meeting notes from Google Drive, split each note into
per-meeting sections at heading boundaries, extract structured information
with LiteLLM + instructor, deduplicate person names with embedding-based
entity resolution, and build a knowledge graph in FalkorDB:

  Meeting nodes — one per meeting section
  Person  nodes — canonical organizers, participants, and task assignees
  Task    nodes — tasks decided in meetings

  ATTENDED     Person -> Meeting (with is_organizer flag)
  DECIDED      Meeting -> Task
  ASSIGNED_TO  Person -> Task

The pipeline runs in three phases:
  1. Per-file extraction declares Meeting and Task nodes plus DECIDED edges,
     and emits raw (un-resolved) person names for downstream resolution.
  2. Person entity resolution maps raw names to canonical names.
  3. A final pass declares canonical Person nodes and the person-touching
     edges (ATTENDED, ASSIGNED_TO) using resolved names.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import instructor
import litellm
import pydantic

import cocoindex as coco
from cocoindex.connectors import falkordb, google_drive
from cocoindex.ops.entity_resolution import ResolvedEntities, resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.id import IdGenerator

litellm.drop_params = True


# ---------------------------------------------------------------------------
# Context keys
# ---------------------------------------------------------------------------

KG_DB = coco.ContextKey[falkordb.ConnectionFactory]("kg_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)
RESOLUTION_LLM_MODEL = coco.ContextKey[str]("resolution_llm_model", detect_change=True)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# FalkorDB row schemas (dataclasses for declare_record / declare_relation)
# ---------------------------------------------------------------------------


@dataclass
class Meeting:
    id: int  # Generated via generate_id((note_file, time_iso))
    note_file: str
    time: datetime.date
    note: str


@dataclass
class Person:
    name: str  # canonical


@dataclass
class Task:
    description: str


@dataclass
class AttendedRel:
    """ATTENDED edge payload. The relation PK is auto-derived from
    (from_id=person, to_id=meeting_id) by the FalkorDB connector — we mount
    this relation without a TableSchema so the connector's endpoint-based
    fallback supplies the PK, giving exactly one edge per (person, meeting).
    """

    is_organizer: bool


# DECIDED and ASSIGNED_TO carry no payload — declared without schema or
# record, with the connector deriving PKs from (from_id, to_id).


# ---------------------------------------------------------------------------
# LLM extraction schemas (Pydantic, for instructor)
# ---------------------------------------------------------------------------


class ExtractedPerson(pydantic.BaseModel):
    name: str = pydantic.Field(
        description="Full name of the person, as written in the note."
    )


class ExtractedTask(pydantic.BaseModel):
    description: str = pydantic.Field(
        description="Concise, standalone description of the task or action item."
    )
    assigned_to: list[ExtractedPerson] = pydantic.Field(
        default_factory=list,
        description="People the task is assigned to.",
    )


class ExtractedMeeting(pydantic.BaseModel):
    time: datetime.date = pydantic.Field(
        description="Date of the meeting in ISO format (YYYY-MM-DD)."
    )
    note: str = pydantic.Field(
        description="A brief summary or notes from the meeting section.",
    )
    organizer: ExtractedPerson = pydantic.Field(
        description="The person who organized or led the meeting."
    )
    participants: list[ExtractedPerson] = pydantic.Field(
        default_factory=list,
        description=(
            "People who attended the meeting other than the organizer. "
            "Do not include the organizer here."
        ),
    )
    tasks: list[ExtractedTask] = pydantic.Field(
        default_factory=list,
        description="Action items or tasks decided in the meeting.",
    )


EXTRACT_PROMPT = """\
You are an expert at reading meeting notes and extracting structured information.

Given a single meeting section (Markdown), extract:
- The meeting date (look for a date in the heading or body; required).
- A brief note summarizing what the meeting was about.
- The organizer (the person who ran the meeting). If unclear, pick the person
  who appears most central to the meeting.
- Participants other than the organizer.
- Tasks or action items decided, including who they are assigned to.

Return only what is supported by the text. Use full names where available.
"""


# ---------------------------------------------------------------------------
# LLM extraction
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def extract_meeting(section_text: str) -> ExtractedMeeting:
    """Extract a structured Meeting from a Markdown section via LiteLLM + instructor."""
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=ExtractedMeeting,
        messages=[
            {"role": "system", "content": EXTRACT_PROMPT},
            {"role": "user", "content": section_text},
        ],
    )
    # Re-validate to restore class identity for pickling.
    return ExtractedMeeting.model_validate(result.model_dump())


# ---------------------------------------------------------------------------
# Splitting — match v0's `\n\n##? ` heading regex
# ---------------------------------------------------------------------------

_HEADING_RE = re.compile(r"\n\n##?\s+")


def _split_meetings(text: str) -> list[str]:
    parts = _HEADING_RE.split("\n\n" + text)
    return [p.strip() for p in parts if p.strip()]


# ---------------------------------------------------------------------------
# Internal transfer types (Phase 1 → Phase 3)
# ---------------------------------------------------------------------------


@dataclass
class MeetingExtraction:
    """Raw per-meeting data carried forward to entity resolution + relation declaration."""

    meeting_id: int
    organizer: str  # raw name
    participants: list[str]  # raw names
    task_assignees: list[
        tuple[str, list[str]]
    ]  # (task_description, [raw assignee names])


# ---------------------------------------------------------------------------
# Phase 1: per-meeting and per-file processing
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_file(
    file: google_drive.DriveFile,
    meeting_table: falkordb.TableTarget[Meeting],
    task_table: falkordb.TableTarget[Task],
    decided_rel: falkordb.RelationTarget[Any],
) -> list[MeetingExtraction]:
    text = await file.read_text()
    note_file = file.file_path.path.as_posix()
    id_generator = IdGenerator()
    extractions = []
    for section in _split_meetings(text):
        extracted = await extract_meeting(section)
        meeting_id = await id_generator.next_id(extracted.time)

        meeting_table.declare_record(
            row=Meeting(
                id=meeting_id,
                note_file=note_file,
                time=extracted.time,
                note=extracted.note,
            )
        )

        for task in extracted.tasks:
            task_table.declare_record(row=Task(description=task.description))
            decided_rel.declare_relation(from_id=meeting_id, to_id=task.description)

        extractions.append(
            MeetingExtraction(
                meeting_id=meeting_id,
                organizer=extracted.organizer.name,
                participants=[p.name for p in extracted.participants],
                task_assignees=[
                    (t.description, [a.name for a in t.assigned_to])
                    for t in extracted.tasks
                ],
            )
        )
    return extractions


# ---------------------------------------------------------------------------
# Phase 2: Person entity resolution
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def _resolve_persons(raw_persons: set[str]) -> ResolvedEntities:
    return await resolve_entities(
        entities=raw_persons,
        embedder=coco.use_context(EMBEDDER),
        resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)),
    )


# ---------------------------------------------------------------------------
# Phase 3: declare canonical Person nodes + person-touching relations
# ---------------------------------------------------------------------------


@coco.fn
async def create_person_relations(
    meetings: list[MeetingExtraction],
    persons: ResolvedEntities,
    person_table: falkordb.TableTarget[Person],
    attended_rel: falkordb.RelationTarget[Any],
    assigned_rel: falkordb.RelationTarget[Any],
) -> None:
    # Declare canonical Person nodes.
    for canonical_name in persons.canonicals():
        person_table.declare_record(row=Person(name=canonical_name))

    for m in meetings:
        # ATTENDED — aggregate organizer + participants. Organizer flag wins
        # on collision so a person listed as both gets a single edge with
        # is_organizer=true. Resolution happens before aggregation so two
        # raw names that resolve to the same person also collapse.
        attendees: dict[str, bool] = {persons.canonical_of(m.organizer): True}
        for p in m.participants:
            attendees.setdefault(persons.canonical_of(p), False)

        for canonical, is_organizer in attendees.items():
            attended_rel.declare_relation(
                from_id=canonical,
                to_id=m.meeting_id,
                record=AttendedRel(is_organizer=is_organizer),
            )

        # ASSIGNED_TO — dedup per (canonical person, task description).
        for task_desc, assignees in m.task_assignees:
            seen: set[str] = set()
            for raw in assignees:
                canonical = persons.canonical_of(raw)
                if canonical in seen:
                    continue
                seen.add(canonical)
                assigned_rel.declare_relation(from_id=canonical, to_id=task_desc)


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main() -> None:
    # --- Mount node tables ---
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

    # --- Mount relation targets ---
    # ATTENDED carries is_organizer; mounted without a schema so the connector
    # auto-derives the relation PK from (from_id, to_id).
    attended_rel = await falkordb.mount_relation_target(
        KG_DB, "ATTENDED", person_table, meeting_table
    )
    decided_rel = await falkordb.mount_relation_target(
        KG_DB, "DECIDED", meeting_table, task_table
    )
    assigned_rel = await falkordb.mount_relation_target(
        KG_DB, "ASSIGNED_TO", person_table, task_table
    )

    # --- Phase 1: per-file extraction ---
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = [
        folder.strip()
        for folder in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")
        if folder.strip()
    ]
    source = google_drive.GoogleDriveSource(
        service_account_credential_path=credential_path,
        root_folder_ids=root_folder_ids,
    )

    file_coros = []
    async for path_key, file in source.items():
        file_coros.append(
            coco.use_mount(
                coco.component_subpath("file", path_key),
                process_file,
                file,
                meeting_table,
                task_table,
                decided_rel,
            )
        )
    per_file: list[list[MeetingExtraction]] = list(await asyncio.gather(*file_coros))
    all_meetings: list[MeetingExtraction] = [m for ms in per_file for m in ms]

    # --- Phase 2: Person entity resolution ---
    raw_persons: set[str] = set()
    for m in all_meetings:
        raw_persons.add(m.organizer)
        raw_persons.update(m.participants)
        for _task_desc, assignees in m.task_assignees:
            raw_persons.update(assignees)

    persons = await coco.use_mount(
        coco.component_subpath("resolve_persons"),
        _resolve_persons,
        raw_persons,
    )

    # --- Phase 3: declare Person nodes + person-touching relations ---
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
