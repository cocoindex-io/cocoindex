"""
Meeting Notes Graph (v1) — CocoIndex pipeline example.

Ingest Markdown meeting notes from a local folder, split each note into
per-meeting sections at heading boundaries, extract structured information
with LiteLLM + instructor, deduplicate person names with embedding-based
entity resolution, and build a knowledge graph in SurrealDB:

  meeting records — one per meeting section
  person  records — canonical organizers, participants, and task assignees
  task    records — tasks decided in meetings

  attended     person -> meeting (with is_organizer flag)
  decided      meeting -> task
  assigned_to  person -> task

The pipeline runs in three phases:
  1. Per-file extraction declares meeting and task records plus decided edges,
     and publishes raw (un-resolved) person names into an in-memory LiveMap.
  2. Person entity resolution maps raw names to canonical names.
  3. Phases 2+3 run together in a periodically-refreshed component that reads
     the LiveMap snapshot and declares canonical person records and the
     person-touching edges (attended, assigned_to) using resolved names.

Run once (catch-up) or keep it watching the notes folder (live):

  cocoindex update main       # one-shot
  cocoindex update -L main    # live: file edits stream into the graph
"""

from __future__ import annotations

import datetime
import os
import pathlib
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated, Any

import instructor
import litellm
import pydantic

import cocoindex as coco
from cocoindex.connectors import localfs, surrealdb
from cocoindex.ops.entity_resolution import ResolvedEntities, resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator
from cocoindex.resources.live_map import LiveMap

litellm.drop_params = True


# ---------------------------------------------------------------------------
# Context keys
# ---------------------------------------------------------------------------

KG_DB = coco.ContextKey[surrealdb.ConnectionFactory]("kg_db")
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
        surrealdb.ConnectionFactory(
            url=os.environ.get("SURREALDB_URL", "ws://localhost:8000/rpc"),
            namespace=os.environ.get("SURREALDB_NS", "cocoindex"),
            database=os.environ.get("SURREALDB_DB", "meeting_notes"),
            credentials={
                "username": os.environ.get("SURREALDB_USER", "root"),
                "password": os.environ.get("SURREALDB_PASS", "root"),
            },
        ),
    )
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-5-mini"))
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
# SurrealDB row schemas (dataclasses for declare_record / declare_relation).
# The `id` field becomes the SurrealDB record id.
# ---------------------------------------------------------------------------


@dataclass
class Meeting:
    id: int  # Generated via generate_id((note_file, time_iso))
    note_file: str
    # Stored as an ISO string: the connector's datetime mapping requires
    # SurrealDB datetime literals, which JSON transport doesn't produce.
    time: Annotated[datetime.date, surrealdb.SurrealType("string")]
    note: str


@dataclass
class Person:
    id: str  # canonical name — traversals read as person:`Alice Chen`


@dataclass
class Task:
    id: str  # task description


@dataclass
class AttendedRel:
    """attended edge payload. The relation record id is auto-derived from
    (from_id=person, to_id=meeting_id) by the SurrealDB connector — we mount
    this relation without a TableSchema so the connector's endpoint-based
    fallback supplies the id, giving exactly one edge per (person, meeting).
    """

    is_organizer: bool


# decided and assigned_to carry no payload — declared without schema or
# record, with the connector deriving record ids from (from_id, to_id).


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


# Not memoized: LiveMap producers must re-run each session to refill the
# in-memory map. The expensive part — extract_meeting — is memoized per section.
@coco.fn
async def process_file(
    file: FileLike,
    meeting_table: surrealdb.TableTarget[Meeting],
    task_table: surrealdb.TableTarget[Task],
    decided_rel: surrealdb.RelationTarget[Any],
    extractions: LiveMap[str, list[MeetingExtraction]],
) -> None:
    text = await file.read_text()
    note_file = file.file_path.path.as_posix()
    id_generator = IdGenerator()
    file_extractions = []
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
            task_table.declare_record(row=Task(id=task.description))
            decided_rel.declare_relation(from_id=meeting_id, to_id=task.description)

        file_extractions.append(
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
    extractions.declare_entry(note_file, file_extractions)


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
# Phase 3: declare canonical person records + person-touching relations.
# Runs as an auto-refreshed component: each cycle reads the LiveMap snapshot,
# so edits streamed in by phase 1 reach the graph within one interval.
# ---------------------------------------------------------------------------


async def sync_person_graph(
    extractions: LiveMap[str, list[MeetingExtraction]],
    person_table: surrealdb.TableTarget[Person],
    attended_rel: surrealdb.RelationTarget[Any],
    assigned_rel: surrealdb.RelationTarget[Any],
) -> None:
    meetings: list[MeetingExtraction] = []
    async for _note_file, file_extractions in extractions:
        meetings.extend(file_extractions)

    raw_persons: set[str] = set()
    for m in meetings:
        raw_persons.add(m.organizer)
        raw_persons.update(m.participants)
        for _task_desc, assignees in m.task_assignees:
            raw_persons.update(assignees)

    persons = await _resolve_persons(raw_persons)

    # Declare canonical person records.
    for canonical_name in persons.canonicals():
        person_table.declare_record(row=Person(id=canonical_name))

    for m in meetings:
        # attended — aggregate organizer + participants. Organizer flag wins
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

        # assigned_to — dedup per (canonical person, task description).
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
async def app_main(notesdir: pathlib.Path) -> None:
    # --- Mount node tables ---
    meeting_table = await surrealdb.mount_table_target(
        KG_DB,
        "meeting",
        await surrealdb.TableSchema.from_class(Meeting),
    )
    person_table = await surrealdb.mount_table_target(
        KG_DB,
        "person",
        await surrealdb.TableSchema.from_class(Person),
    )
    task_table = await surrealdb.mount_table_target(
        KG_DB,
        "task",
        await surrealdb.TableSchema.from_class(Task),
    )

    # --- Mount relation targets ---
    # attended carries is_organizer; mounted without a schema so the connector
    # auto-derives the relation record id from (from_id, to_id).
    attended_rel = await surrealdb.mount_relation_target(
        KG_DB, "attended", person_table, meeting_table
    )
    decided_rel = await surrealdb.mount_relation_target(
        KG_DB, "decided", meeting_table, task_table
    )
    assigned_rel = await surrealdb.mount_relation_target(
        KG_DB, "assigned_to", person_table, task_table
    )

    # --- Phase 1: per-file extraction, live-watching the notes folder ---
    extractions: LiveMap[str, list[MeetingExtraction]] = await LiveMap.create()
    source = localfs.walk_dir(
        notesdir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,
    )
    handle = await coco.mount_each(
        process_file,
        source.items(),
        meeting_table,
        task_table,
        decided_rel,
        extractions,
    )
    # Wait for the initial scan so the first aggregation sees every note.
    await handle.ready()

    # --- Phases 2+3: entity resolution + person graph, refreshed periodically ---
    await coco.mount(
        coco.auto_refresh(sync_person_graph, interval=datetime.timedelta(seconds=5)),
        extractions,
        person_table,
        attended_rel,
        assigned_rel,
    )


app = coco.App(
    coco.AppConfig(name="MeetingNotesGraphSurrealDB"),
    app_main,
    notesdir=pathlib.Path("./notes"),
)
