from __future__ import annotations

import asyncio
import datetime
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import instructor
import litellm
import pydantic

import cocoindex as coco
from cocoindex.connectors import google_drive
from cocoindex.ops.entity_resolution import ResolvedEntities
from cocoindex.resources.id import IdGenerator

litellm.drop_params = True


@dataclass
class Meeting:
    id: int
    note_file: str
    time: datetime.date
    note: str


@dataclass
class Person:
    name: str


@dataclass
class Task:
    description: str


@dataclass
class AttendedRel:
    is_organizer: bool


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


async def extract_meeting_with_model(
    section_text: str, model: str
) -> ExtractedMeeting:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=model,
        response_model=ExtractedMeeting,
        messages=[
            {"role": "system", "content": EXTRACT_PROMPT},
            {"role": "user", "content": section_text},
        ],
    )
    return ExtractedMeeting.model_validate(result.model_dump())


_HEADING_RE = re.compile(r"\n\n##?\s+")


def _split_meetings(text: str) -> list[str]:
    parts = _HEADING_RE.split("\n\n" + text)
    return [p.strip() for p in parts if p.strip()]


@dataclass
class MeetingExtraction:
    meeting_id: int
    organizer: str
    participants: list[str]
    task_assignees: list[tuple[str, list[str]]]


ExtractMeetingFn = Callable[[str], Awaitable[ExtractedMeeting]]


async def process_file_common(
    file: google_drive.DriveFile,
    meeting_table: Any,
    task_table: Any,
    decided_rel: Any,
    extract_meeting: ExtractMeetingFn,
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


def raw_person_names(meetings: list[MeetingExtraction]) -> set[str]:
    raw_persons: set[str] = set()
    for meeting in meetings:
        raw_persons.add(meeting.organizer)
        raw_persons.update(meeting.participants)
        for _task_desc, assignees in meeting.task_assignees:
            raw_persons.update(assignees)
    return raw_persons


async def collect_meeting_extractions(
    process_file: Any,
    meeting_table: Any,
    task_table: Any,
    decided_rel: Any,
) -> list[MeetingExtraction]:
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
    return [meeting for meetings in per_file for meeting in meetings]


def declare_person_relations(
    meetings: list[MeetingExtraction],
    persons: ResolvedEntities,
    person_table: Any,
    attended_rel: Any,
    assigned_rel: Any,
) -> None:
    for canonical_name in persons.canonicals():
        person_table.declare_record(row=Person(name=canonical_name))

    for meeting in meetings:
        attendees: dict[str, bool] = {
            persons.canonical_of(meeting.organizer): True
        }
        for participant in meeting.participants:
            attendees.setdefault(persons.canonical_of(participant), False)

        for canonical, is_organizer in attendees.items():
            attended_rel.declare_relation(
                from_id=canonical,
                to_id=meeting.meeting_id,
                record=AttendedRel(is_organizer=is_organizer),
            )

        for task_desc, assignees in meeting.task_assignees:
            seen: set[str] = set()
            for raw in assignees:
                canonical = persons.canonical_of(raw)
                if canonical in seen:
                    continue
                seen.add(canonical)
                assigned_rel.declare_relation(from_id=canonical, to_id=task_desc)
