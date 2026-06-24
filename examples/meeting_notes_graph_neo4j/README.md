<p align="center">
  <a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/" title="Turn Google Drive meeting notes into a self-updating knowledge graph with CocoIndex and Neo4j — LLM extraction, embedding-based person resolution, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/meeting-notes-to-knowledge-graph/cover.svg" alt="Turn a Google Drive folder of meeting notes into a self-updating knowledge graph with CocoIndex and Neo4j — an LLM extracts the organizer, participants, and tasks per meeting, an embedding plus LLM pass collapses the same person written five ways into one node, and the result is a Person / Meeting / Task graph in Cypher" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn meeting notes into a <em>self-updating</em> knowledge graph.</h1>

<p align="center">
  <b>An LLM pulls the organizer, participants, and tasks out of each meeting; an embedding + LLM pass collapses "Alice", "Alice Chen", and "alice c." into <em>one</em> Person node — in plain async Python.</b><br/>
  Point it at a Drive folder of Markdown notes, and it re-extracts only the note you edited, then reconciles the graph.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Meeting notes are a graph pretending to be a folder of documents — every note records who ran the meeting, who showed up, what got decided, and who owns each task. But it's prose, scattered across a shared drive, so you can full-text search it and not much else. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so editing one note re-extracts one note, and the graph reconciles itself: no orphaned people, no stale edges, no cleanup scripts.

## How it works

Three node types, three relationship types, and "who is on the hook for what" becomes an edge you traverse:

- **`Meeting`** nodes — one per meeting section, keyed by a stable integer id derived from `(note_file, date)`.
- **`Person`** nodes — canonical organizers, participants, and assignees, deduplicated by an embedding + LLM entity-resolution pass.
- **`Task`** nodes — tasks decided in meetings, keyed by description.
- **`ATTENDED`** edges — `Person → Meeting`, carrying an `is_organizer` flag. **`DECIDED`** edges — `Meeting → Task`. **`ASSIGNED_TO`** edges — `Person → Task`.

Because people are shared across notes, the pipeline runs in three phases — read it top-to-bottom in [`main.py`](main.py):

```python
@coco.fn(memo=True)  # Phase 1 — per note: split into meetings, declare Meeting/Task + DECIDED, carry raw names forward
async def process_file(file, meeting_table, task_table, decided_rel) -> list[MeetingExtraction]:
    for section in _split_meetings(await file.read_text()):
        extracted = await extract_meeting(section)
        meeting_id = await id_generator.next_id(extracted.time)
        meeting_table.declare_record(row=Meeting(id=meeting_id, ...))
        for task in extracted.tasks:
            task_table.declare_record(row=Task(description=task.description))
            decided_rel.declare_relation(from_id=meeting_id, to_id=task.description)
        ...

@coco.fn(memo=True)  # Phase 2 — collapse "Alice" / "Alice Chen" / "alice c." into canonical names
async def _resolve_persons(raw_persons: set[str]) -> ResolvedEntities:
    return await resolve_entities(entities=raw_persons, embedder=coco.use_context(EMBEDDER),
                                  resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)))

@coco.fn              # Phase 3 — declare canonical Person nodes + ATTENDED / ASSIGNED_TO using resolved names
async def create_person_relations(meetings, persons, person_table, attended_rel, assigned_rel) -> None:
    for canonical_name in persons.canonicals():  person_table.declare_record(row=Person(name=canonical_name))
    ...
```

Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) with your own Pydantic models; `DECIDED` and `ASSIGNED_TO` carry no payload, so the Neo4j connector derives their identity from the endpoints — one edge per pair.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the property-graph schema, the three-phase flow, entity resolution, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Entity resolution built in.** CocoIndex's [`entity_resolution`](https://cocoindex.io/docs/ops/entity_resolution/) op embeds every raw name, filters by vector similarity, and asks the LLM to confirm *only* the close pairs — so the same person written five ways collapses to one node, cheaply.
- **Cross-file nodes, owned in one place.** People are shared across notes, so no single note's component can own a `Person` node. The two cross-file phases own the canonical set and the person-touching edges, exactly once.
- **Incremental by default.** `@coco.fn(memo=True)` caches each extraction by content; edit one note and only that note re-extracts, then resolution and the graph diff. A no-change re-run makes zero LLM calls.
- **Two models on purpose.** A stronger `LLM_MODEL` does the structured extraction; a cheaper `RESOLUTION_LLM_MODEL` confirms resolution pairs — both are [LiteLLM provider strings](https://docs.litellm.ai/docs/providers) you can swap.
- **Honest cache busting.** The model ids and embedder are declared with `detect_change=True`, so swapping any of them re-extracts against it with no cache to clear by hand.

## Run it

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure & install** — this source reads notes from one or more Google Drive folders shared with a service account (see [Setting up a service account](https://cocoindex.io/docs/connectors/google_drive/#setting-up-a-service-account)):

```sh
cp .env.example .env     # set OPENAI_API_KEY, GOOGLE_SERVICE_ACCOUNT_CREDENTIAL, GOOGLE_DRIVE_ROOT_FOLDER_IDS
pip install -e .
```

**3. Build the graph:**

```sh
cocoindex update main
```

> First run downloads the `snowflake-arctic-embed-xs` model (~90 MB) from Hugging Face for the entity-resolution step and caches it locally, so the initial invocation needs network access. The LLM extraction additionally calls your configured `LLM_MODEL` (OpenAI by default), which does require an API key.

**4. Explore the graph** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`) and ask:

```cypher
-- Who attended which meetings (including organizer; one edge per attendee)
MATCH (p:Person)-[:ATTENDED]->(m:Meeting)
RETURN p.name, m.note_file, m.time

-- Everything one person is on the hook for
MATCH (p:Person {name: "Alice Chen"})-[:ASSIGNED_TO]->(t:Task)
RETURN t.description

-- Meetings someone organized
MATCH (p:Person)-[r:ATTENDED {is_organizer: true}]->(m:Meeting)
RETURN p.name, m.note_file, m.time
```

This pipeline is the [docs knowledge graph](https://cocoindex.io/docs/examples/docs-to-knowledge-graph/) plus an entity-resolution pass — the natural next step when the LLM names the same thing two ways.

---

<p align="center">
  If this turned your shared drive into a graph, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/meeting_notes_graph_neo4j" alt="" width="1" height="1" />
