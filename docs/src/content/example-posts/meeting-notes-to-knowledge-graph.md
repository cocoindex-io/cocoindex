---
title: Turn Meeting Notes into a Knowledge Graph
description: 'Build a self-updating knowledge graph from Google Drive meeting notes with CocoIndex V1 — LLM extraction with instructor + LiteLLM, embedding-based person entity resolution, and a Neo4j graph.'
slug: meeting-notes-to-knowledge-graph
image: https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/cover.png
tags: [knowledge-graph, llm-extraction]
---

![Turn meeting notes into a self-updating knowledge graph with LLM extraction and CocoIndex](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/cover.png)

Meeting notes are a graph pretending to be a folder of documents. Every note records who ran the meeting, who showed up, what got decided, and who owns each task — relationships between people, meetings, and tasks. But they're written as prose, scattered across a shared drive, so you can full-text search them and not much else. You can't ask *"what is Alice on the hook for across every meeting this quarter?"* or *"which meetings did the platform team actually attend?"*

In this tutorial, we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that turns a Google Drive folder of Markdown meeting notes into a queryable knowledge graph in [Neo4j](https://neo4j.com/). An LLM extracts the organizer, participants, and tasks from each meeting; an embedding + LLM **entity-resolution** pass collapses the same person written five different ways into one node; and the result is a property graph you can query in Cypher.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed graph targets — runs in a Rust engine underneath, so editing one note re-extracts only that note, and the graph reconciles itself: no orphaned people, no stale edges, no cleanup scripts.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j)

## What we're building

We're modeling meeting notes as a **property graph** — the data model behind [Neo4j](https://cocoindex.io/docs/connectors/neo4j/) and most graph databases. Two ideas carry it:

- **Nodes** are the things: a `Person`, a `Meeting`, a `Task`. Each node has a *label* (its type) and a bag of *properties* (`name`, `time`, `note`, …).
- **Relationships** are the connections between things — and they're first-class. A relationship is *typed* and *directed* (`Person -[:ATTENDED]-> Meeting`), and it can carry properties of its own. Here, the `ATTENDED` edge holds an `is_organizer` flag, so "ran the meeting" and "showed up" are the same edge with different data.

That second idea is the whole point: in a property graph, "who is on the hook for what" stops being prose buried in a document and becomes an edge you can traverse in a query. The schema here is small — three node types, three relationship types:

![Property graph schema: Person, Meeting, and Task nodes joined by ATTENDED (carrying is_organizer), DECIDED, and ASSIGNED_TO edges, with sample node and edge properties labeled](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/schema.png)

- **`Meeting`** nodes — one per meeting section, keyed by a stable integer id derived from `(note_file, date)`.
- **`Person`** nodes — canonical organizers, participants, and task assignees, deduplicated by an embedding + LLM entity-resolution pass (so "Alice", "Alice Chen", and "alice c." collapse to a single node).
- **`Task`** nodes — tasks decided in meetings, keyed by description.
- **`ATTENDED`** edges — `Person → Meeting`, carrying an `is_organizer` flag.
- **`DECIDED`** edges — `Meeting → Task`.
- **`ASSIGNED_TO`** edges — `Person → Task`.

The source is one or more Google Drive folders shared with a service account. The flow watches for changes and keeps the graph up to date incrementally.

## Why CocoIndex for meeting-note graphs

A knowledge graph over living notes is easy to demo and hard to keep correct. Three things make it tricky, and each maps onto a CocoIndex primitive:

- **LLM extraction is expensive.** [Memoization](https://cocoindex.io/docs/advanced_topics/memoization_keys/) caches every extraction by content — edit one note and only that note hits the LLM again. A no-change re-run makes zero LLM calls.
- **People are written inconsistently.** The same person shows up as "Alice", "Alice Chen", and "alice c." across notes. Names are shared *across* files, so no single note's processing can own a `Person` node — and a graph full of near-duplicate people is useless. CocoIndex ships an [`entity_resolution`](https://cocoindex.io/docs/ops/entity_resolution/) op that collapses them, and a [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) model that owns cross-file nodes in one place.
- **Graphs accumulate garbage.** Delete a note, reassign a task, tighten a prompt — a hand-rolled pipeline leaves orphaned nodes and stale edges behind. In CocoIndex, nodes and edges are [target states](https://cocoindex.io/docs/programming_guide/target_state/): you declare what should exist, and the engine inserts, updates, and deletes the difference.

Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) with your own Pydantic models — swap in any provider, prompt, or schema.

## Pipeline overview

![CocoIndex flow: Google Drive meeting notes through three phases — per-note extraction declaring Meeting and Task nodes, person entity resolution, and a final person-relations pass — landing in a Neo4j property graph](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/flow-v1.png)

The pipeline runs in three phases:

1. **Per-file extraction** — read each note from Google Drive, split it by Markdown headings into meeting sections, and for each section LLM-extract a structured meeting (date, note, organizer, participants, tasks). `Meeting` and `Task` nodes plus `DECIDED` edges are declared here; raw person names are carried forward.
2. **Person entity resolution** — collect every raw person name across all notes and deduplicate them with embedding similarity + LLM confirmation, producing a canonical-name mapping.
3. **Person-touching relations** — declare the canonical `Person` nodes, then wire up the `ATTENDED` and `ASSIGNED_TO` edges using resolved names.

You [declare the transformation](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python; CocoIndex works out what to insert, update, and delete. Think: **target_state = transformation(source_state)**.

## Define the graph schema

Nodes and edges are plain dataclasses. Each becomes a Neo4j label (or relationship type), with one field as the primary key:

```python title="main.py"
@dataclass
class Meeting:
    id: int  # stable id generated from (note_file, date)
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
    """ATTENDED edge payload — just the organizer flag. The relation's identity
    is auto-derived from its (person, meeting) endpoints, so one edge exists per
    (person, meeting) pair."""

    is_organizer: bool
```

`DECIDED` and `ASSIGNED_TO` carry no payload, so they get no schema at all — the connector derives each edge's identity from its endpoints: one edge per `(meeting, task)` or `(person, task)` pair.

These dataclasses are the bridge between plain Python and the property graph. Each record you declare into a node table becomes a **node**, with its fields as properties; each relation you declare becomes a typed, directed **edge** between two nodes, with its payload (like `is_organizer`) as edge properties:

![A meeting record becoming a Meeting node, and an attendance record becoming an ATTENDED edge from a Person node to a Meeting node carrying is_organizer](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/records-to-graph.png)

So a `Meeting(note_file=…, time=…, note=…)` becomes a `Meeting` node carrying those fields, and an `ATTENDED` relation from a person to that meeting becomes a `Person -[:ATTENDED {is_organizer: true}]-> Meeting` edge. You declare the records and relations in Python; the [Neo4j connector](https://cocoindex.io/docs/connectors/neo4j/) creates, updates, and removes the matching nodes and edges to match.

## Shared resources: the lifespan

The [lifespan](https://cocoindex.io/docs/programming_guide/context/) provides what every step needs — the graph connection factory, two LLM model ids, and the embedder — once at startup, via [context keys](https://cocoindex.io/docs/programming_guide/context/):

```python title="main.py"
KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)
RESOLUTION_LLM_MODEL = coco.ContextKey[str]("resolution_llm_model", detect_change=True)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(
        KG_DB,
        neo4j.ConnectionFactory(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.environ.get("NEO4J_USER", "neo4j"),
                  os.environ.get("NEO4J_PASSWORD", "cocoindex")),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        ),
    )
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-5-mini"))
    builder.provide(RESOLUTION_LLM_MODEL, os.environ.get("RESOLUTION_LLM_MODEL", "openai/gpt-5-mini"))
    builder.provide(EMBEDDER, SentenceTransformerEmbedder("Snowflake/snowflake-arctic-embed-xs"))
    yield
```

Two models, on purpose: a stronger model (`LLM_MODEL`) does the structured extraction; a smaller, cheaper one (`RESOLUTION_LLM_MODEL`) confirms entity-resolution pairs. Both are [LiteLLM provider strings](https://docs.litellm.ai/docs/providers), so `LLM_MODEL=ollama/llama3.2` runs extraction locally with no API key.

Note `detect_change=True` on the model ids and the embedder: they participate in change detection. Point `LLM_MODEL` at a different model and CocoIndex knows every memoized extraction is stale — the corpus re-extracts on the next run, with no cache to clear by hand.

## Split notes into meetings

A single note file often holds several meetings, one per Markdown heading. We split on `#` / `##` headings preceded by a blank line, keeping each heading with its section:

```python title="main.py"
_HEADING_RE = re.compile(r"\n\n##?\s+")


def _split_meetings(text: str) -> list[str]:
    parts = _HEADING_RE.split("\n\n" + text)
    return [p.strip() for p in parts if p.strip()]
```

## LLM extraction

Extraction is typed end to end: Pydantic models describe what we want, instructor enforces them, and the field descriptions double as instructions to the model.

```python title="main.py"
class ExtractedPerson(pydantic.BaseModel):
    name: str = pydantic.Field(description="Full name of the person, as written in the note.")


class ExtractedTask(pydantic.BaseModel):
    description: str = pydantic.Field(description="Concise, standalone description of the task.")
    assigned_to: list[ExtractedPerson] = pydantic.Field(default_factory=list)


class ExtractedMeeting(pydantic.BaseModel):
    time: datetime.date = pydantic.Field(description="Date of the meeting (YYYY-MM-DD).")
    note: str = pydantic.Field(description="A brief summary of the meeting section.")
    organizer: ExtractedPerson = pydantic.Field(description="The person who organized or led the meeting.")
    participants: list[ExtractedPerson] = pydantic.Field(default_factory=list)
    tasks: list[ExtractedTask] = pydantic.Field(default_factory=list)
```

One memoized function turns a Markdown section into a typed `ExtractedMeeting`:

```python title="main.py"
@coco.fn(memo=True)
async def extract_meeting(section_text: str) -> ExtractedMeeting:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=ExtractedMeeting,
        messages=[
            {"role": "system", "content": EXTRACT_PROMPT},
            {"role": "user", "content": section_text},
        ],
    )
    return ExtractedMeeting.model_validate(result.model_dump())
```

[`@coco.fn(memo=True)`](https://cocoindex.io/docs/programming_guide/function/) is what makes iteration affordable: the result is cached keyed by the section text (and the function's own code). Unchanged meeting sections never hit the LLM again.

## Phase 1: per-file extraction

![Phase 1 — one process_file component per note: split into meetings, memoized LLM extraction, declare Meeting and Task nodes into Neo4j, and carry MeetingExtraction forward to phases 2 and 3](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/stage-phase1.png)

`process_file` runs once per note. For each meeting section it extracts the structured meeting, declares the `Meeting` node, declares a `Task` node + `DECIDED` edge per task, and returns the raw (un-resolved) person names for phase 2:

```python title="main.py"
@coco.fn(memo=True)
async def process_file(
    file: google_drive.DriveFile,
    meeting_table: neo4j.TableTarget[Meeting],
    task_table: neo4j.TableTarget[Task],
    decided_rel: neo4j.RelationTarget[Any],
) -> list[MeetingExtraction]:
    text = await file.read_text()
    note_file = file.file_path.path.as_posix()
    id_generator = IdGenerator()
    extractions = []
    for section in _split_meetings(text):
        extracted = await extract_meeting(section)
        meeting_id = await id_generator.next_id(extracted.time)

        meeting_table.declare_record(
            row=Meeting(id=meeting_id, note_file=note_file,
                        time=extracted.time, note=extracted.note)
        )
        for task in extracted.tasks:
            task_table.declare_record(row=Task(description=task.description))
            decided_rel.declare_relation(from_id=meeting_id, to_id=task.description)

        extractions.append(MeetingExtraction(
            meeting_id=meeting_id,
            organizer=extracted.organizer.name,
            participants=[p.name for p in extracted.participants],
            task_assignees=[(t.description, [a.name for a in t.assigned_to])
                            for t in extracted.tasks],
        ))
    return extractions
```

The `Meeting` id comes from CocoIndex's [`IdGenerator`](https://cocoindex.io/docs/common_resources/id_generation/): `next_id(content)` folds the content in and is stable, so the same meeting always maps to the same node — re-running never duplicates.

Each note runs as its own [processing component](https://cocoindex.io/docs/programming_guide/processing_component/), mounted in `app_main` and keyed by the file path:

```python title="main.py"
file_coros = []
async for path_key, file in source.items():
    file_coros.append(
        coco.use_mount(
            coco.component_subpath("file", path_key),
            process_file, file, meeting_table, task_table, decided_rel,
        )
    )
per_file = list(await asyncio.gather(*file_coros))
all_meetings = [m for ms in per_file for m in ms]
```

Why a component per file? **Ownership.** The component at `("file", path_key)` owns that note's `Meeting` and `Task` nodes — if the file disappears, so does the component, and CocoIndex deletes its nodes (and the `DECIDED` edges) automatically. [`coco.use_mount`](https://cocoindex.io/docs/programming_guide/app/) returns each file's extractions, and `asyncio.gather` runs all files concurrently. `Person` nodes are deliberately *not* declared here — people are shared across notes, so they wait for phases 2 and 3.

## Phase 2: resolve people

![Phase 2 — a single resolve_persons pass: the set of raw person names from every note is deduplicated by embedding similarity plus LLM confirmation into a canonical-name map](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/stage-phase2.png)

This is the step that separates a useful graph from a messy one. We have a pile of raw names from every note — "Alice", "Alice Chen", "alice c." — and we want one `Person` node per actual person. CocoIndex's [`entity_resolution`](https://cocoindex.io/docs/ops/entity_resolution/) op embeds each name, finds near-matches by vector similarity, and asks an LLM to confirm *only* the close pairs — cheap embeddings filter the field, the expensive model runs only where it's genuinely ambiguous:

```python title="main.py"
@coco.fn(memo=True)
async def _resolve_persons(raw_persons: set[str]) -> ResolvedEntities:
    return await resolve_entities(
        entities=raw_persons,
        embedder=coco.use_context(EMBEDDER),                       # snowflake-arctic-embed-xs
        resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)),
    )
```

It runs as its own component over the deduplicated set of every name seen in phase 1:

```python title="main.py"
raw_persons: set[str] = set()
for m in all_meetings:
    raw_persons.add(m.organizer)
    raw_persons.update(m.participants)
    for _desc, assignees in m.task_assignees:
        raw_persons.update(assignees)

persons = await coco.use_mount(
    coco.component_subpath("resolve_persons"), _resolve_persons, raw_persons,
)
```

Because it's a memoized component keyed by the name set, resolution only re-runs when the set of raw names actually changes — and even then it reuses cached embeddings and only makes fresh LLM calls for genuinely new pairs. The result, `persons`, maps any raw name to its canonical form via `persons.canonical_of(name)`.

## Phase 3: people, attendance, and assignments

![Phase 3 — a single create_person_relations pass: MeetingExtraction from phase 1 and the resolved names from phase 2 are combined to declare Person nodes plus ATTENDED and ASSIGNED_TO edges into Neo4j](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/stage-phase3.png)

With the canonical mapping in hand, one component declares the `Person` nodes and the two person-touching edge types — the cross-file part of the graph that no single note could own:

```python title="main.py"
@coco.fn
async def create_person_relations(
    meetings: list[MeetingExtraction],
    persons: ResolvedEntities,
    person_table: neo4j.TableTarget[Person],
    attended_rel: neo4j.RelationTarget[Any],
    assigned_rel: neo4j.RelationTarget[Any],
) -> None:
    for canonical_name in persons.canonicals():
        person_table.declare_record(row=Person(name=canonical_name))

    for m in meetings:
        # ATTENDED — organizer flag wins on collision, so a person listed as both
        # organizer and participant gets a single edge with is_organizer=true.
        attendees: dict[str, bool] = {persons.canonical_of(m.organizer): True}
        for p in m.participants:
            attendees.setdefault(persons.canonical_of(p), False)
        for canonical, is_organizer in attendees.items():
            attended_rel.declare_relation(
                from_id=canonical, to_id=m.meeting_id,
                record=AttendedRel(is_organizer=is_organizer),
            )

        # ASSIGNED_TO — dedup per (canonical person, task).
        for task_desc, assignees in m.task_assignees:
            for canonical in {persons.canonical_of(a) for a in assignees}:
                assigned_rel.declare_relation(from_id=canonical, to_id=task_desc)
```

Two details carry the correctness here. Resolution happens *before* aggregation, so two raw names that resolve to the same person collapse into one `ATTENDED` edge instead of two. And because the canonical names are the primary keys, re-asserting the same attendance or assignment from another note is a no-op, not a duplicate.

## Wire it up: app_main

`app_main` mounts the targets and runs the three phases. Node tables come first, because relation targets are declared *between* two node tables — that's how the connector knows each edge's endpoint labels and keys:

```python title="main.py"
@coco.fn
async def app_main() -> None:
    meeting_table = await neo4j.mount_table_target(
        KG_DB, "Meeting",
        await neo4j.TableSchema.from_class(Meeting, primary_key="id"), primary_key="id")
    person_table = await neo4j.mount_table_target(
        KG_DB, "Person",
        await neo4j.TableSchema.from_class(Person, primary_key="name"), primary_key="name")
    task_table = await neo4j.mount_table_target(
        KG_DB, "Task",
        await neo4j.TableSchema.from_class(Task, primary_key="description"), primary_key="description")

    # ATTENDED carries is_organizer; DECIDED and ASSIGNED_TO carry no payload, so
    # they mount without a schema and the connector derives PKs from endpoints.
    attended_rel = await neo4j.mount_relation_target(KG_DB, "ATTENDED", person_table, meeting_table)
    decided_rel = await neo4j.mount_relation_target(KG_DB, "DECIDED", meeting_table, task_table)
    assigned_rel = await neo4j.mount_relation_target(KG_DB, "ASSIGNED_TO", person_table, task_table)

    source = google_drive.GoogleDriveSource(
        service_account_credential_path=os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"],
        root_folder_ids=[f.strip() for f in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",") if f.strip()],
    )

    # Phase 1: per-file fan-out (above) → all_meetings
    # Phase 2: persons = resolve_persons(all raw names)
    # Phase 3: declare Person nodes + person edges
    await coco.mount(
        coco.component_subpath("person_relations"),
        create_person_relations, all_meetings, persons, person_table, attended_rel, assigned_rel,
    )


app = coco.App(coco.AppConfig(name="MeetingNotesGraphNeo4j"), app_main)
```

That's the whole pipeline — one file, ~250 lines.

## Run the pipeline

You'll need a Neo4j instance, an LLM key, and a Google Drive service account. Start Neo4j with Docker:

```sh
docker run -d \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/cocoindex \
  --name cocoindex-neo4j \
  neo4j:5.26-community
```

The browser UI is at <http://localhost:7474> (log in with `neo4j` / `cocoindex`).

Set up the environment (copy `.env.example` to `.env` and fill in):

```sh
export OPENAI_API_KEY="your-openai-api-key"          # or set LLM_MODEL=ollama/llama3.2
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL=/absolute/path/to/service_account.json
export GOOGLE_DRIVE_ROOT_FOLDER_IDS=folderId1,folderId2
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=cocoindex
export LLM_MODEL=openai/gpt-5-mini
export RESOLUTION_LLM_MODEL=openai/gpt-5-mini         # smaller model for entity resolution
```

The Google Drive source reads Markdown notes from one or more folders shared with the service account — see [Setting up a service account](https://cocoindex.io/docs/connectors/google_drive/#setting-up-a-service-account) for the folder IDs and sharing steps. Install and build the graph:

```sh
uv pip install -e .
cocoindex update main
```

## Explore the graph

Open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`) and ask the graph questions:

![The resulting graph in Neo4j Browser: Person, Meeting, and Task nodes joined by ATTENDED, DECIDED, and ASSIGNED_TO edges](https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/neo4j-browser.png)

```cypher
// Everything
MATCH p=()-->() RETURN p LIMIT 100

// Who attended which meetings (including organizer; one edge per attendee)
MATCH (p:Person)-[:ATTENDED]->(m:Meeting)
RETURN p.name, m.note_file, m.time

// Tasks decided in meetings
MATCH (m:Meeting)-[:DECIDED]->(t:Task)
RETURN m.note_file, m.time, t.description

// Everything one person is on the hook for
MATCH (p:Person {name: "Alice Chen"})-[:ASSIGNED_TO]->(t:Task)
RETURN t.description

// Meetings someone organized
MATCH (p:Person)-[r:ATTENDED {is_organizer: true}]->(m:Meeting)
RETURN p.name, m.note_file, m.time
```

## Incremental updates

This is where the declarative model pays for itself. You never compute a diff or write cleanup logic — change something, re-run `cocoindex update main`, and CocoIndex works out the minimum set of LLM calls and graph writes.

**Data changes.**

- **Edit one note** — only that note's component re-runs and re-extracts its sections. Its `Meeting` / `Task` nodes are diffed; if it introduced or dropped a person, phase 2 reruns and phase 3 reconciles the edges. Every other note is served from the memo cache.
- **Add a note** — one new component, a handful of extractions, plus the resolution and graph diff.
- **Delete a note** — its component disappears, so its `Meeting` and `Task` nodes and `DECIDED` edges are cleaned up automatically; people only that note introduced fall out of the canonical set on the next resolution pass.
- **Nothing changed** — the run completes in a fraction of a second with zero LLM calls.

**Logic changes** are reconciled the same way:

- **Tighten the extraction prompt** — the function's code changed, so all sections re-extract; the graph then diffs against what's in the database and applies only the difference.
- **Swap the LLM** — `LLM_MODEL` has `detect_change=True`, so changing the env var invalidates every memoized extraction. No cache to clear, no manual rebuild.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/meeting_notes_graph_neo4j](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j).

This pipeline is the [docs knowledge graph](https://cocoindex.io/docs/examples/docs-to-knowledge-graph) plus an entity-resolution pass — the natural next step when the LLM names the same thing two ways. For a bigger end-to-end build (transcription, multi-entity schemas, polymorphic edges), see [Turn Podcasts into a Knowledge Graph](https://cocoindex.io/docs/examples/podcast-to-knowledge-graph).

Got a shared drive full of meeting notes, standup logs, or design docs you want to turn into a graph? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
