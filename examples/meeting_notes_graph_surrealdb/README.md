<h1 align="center">Turn meeting notes into a <em>self-updating</em> graph in SurrealDB.</h1>

<p align="center">
  <b>An LLM pulls the organizer, participants, and tasks out of each meeting; an embedding + LLM pass collapses "Alice", "Alice Chen", and "alice c." into <em>one</em> person record — into SurrealDB, in plain async Python.</b><br/>
  Point it at a folder of Markdown notes, and it re-extracts only the note you edited, then reconciles the graph.
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

This is the meeting-notes knowledge graph, targeting [SurrealDB](https://surrealdb.com/) instead of Neo4j — a multi-model database whose `RELATE` edges you traverse in SurrealQL. Meeting notes are a graph pretending to be a folder of documents: every note records who ran the meeting, who showed up, what got decided, and who owns each task. But it's prose, scattered across a shared drive, so you can full-text search it and not much else. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so editing one note re-extracts one note, and the graph reconciles itself: no orphaned people, no stale edges, no cleanup scripts.

## How it works

Three record tables, three relation tables, and "who is on the hook for what" becomes an edge you traverse:

- **`meeting`** records — one per meeting section, keyed by a stable integer id derived from `(note_file, date)`.
- **`person`** records — canonical organizers, participants, and assignees, deduplicated by an embedding + LLM entity-resolution pass. The record id *is* the canonical name: `person:⟨Alice Chen⟩`.
- **`task`** records — tasks decided in meetings, keyed by description.
- **`attended`** edges — `person → meeting`, carrying an `is_organizer` flag. **`decided`** edges — `meeting → task`. **`assigned_to`** edges — `person → task`.

Because people are shared across notes, the pipeline runs in three phases — read it top-to-bottom in [`main.py`](main.py):

```python
@coco.fn(memo=True)  # Phase 1 — per note: split into meetings, declare meeting/task + decided, carry raw names forward
async def process_file(file, meeting_table, task_table, decided_rel) -> list[MeetingExtraction]:
    for section in _split_meetings(await file.read_text()):
        extracted = await extract_meeting(section)
        meeting_id = await id_generator.next_id(extracted.time)
        meeting_table.declare_record(row=Meeting(id=meeting_id, ...))
        for task in extracted.tasks:
            task_table.declare_record(row=Task(id=task.description))
            decided_rel.declare_relation(from_id=meeting_id, to_id=task.description)
        ...

@coco.fn(memo=True)  # Phase 2 — collapse "Alice" / "Alice Chen" / "alice c." into canonical names
async def _resolve_persons(raw_persons: set[str]) -> ResolvedEntities:
    return await resolve_entities(entities=raw_persons, embedder=coco.use_context(EMBEDDER),
                                  resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)))

@coco.fn              # Phase 3 — declare canonical person records + attended / assigned_to using resolved names
async def create_person_relations(meetings, persons, person_table, attended_rel, assigned_rel) -> None:
    for canonical_name in persons.canonicals():  person_table.declare_record(row=Person(id=canonical_name))
    ...
```

Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) with your own Pydantic models; `decided` and `assigned_to` carry no payload, so the SurrealDB connector derives their record ids from the endpoints — one edge per pair.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/">Full Tutorial →</a></b><br/>
  The closest walkthrough is the Neo4j version — same extraction, resolution, and three-phase flow; only the graph store differs. Step-by-step coverage of the property-graph schema, entity resolution, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Entity resolution built in.** CocoIndex's [`entity_resolution`](https://cocoindex.io/docs/ops/entity_resolution/) op embeds every raw name, filters by vector similarity, and asks the LLM to confirm *only* the close pairs — so the same person written five ways collapses to one record, cheaply.
- **Cross-file records, owned in one place.** People are shared across notes, so no single note's component can own a `person` record. The two cross-file phases own the canonical set and the person-touching edges, exactly once.
- **Incremental by default.** `@coco.fn(memo=True)` caches each extraction by content; edit one note and only that note re-extracts, then resolution and the graph diff. A no-change re-run makes zero LLM calls.
- **Two models on purpose.** A stronger `LLM_MODEL` does the structured extraction; a cheaper `RESOLUTION_LLM_MODEL` confirms resolution pairs — both are [LiteLLM provider strings](https://docs.litellm.ai/docs/providers) you can swap.
- **Honest cache busting.** The model ids and embedder are declared with `detect_change=True`, so swapping any of them re-extracts against it with no cache to clear by hand.

## Run it

**1. Start SurrealDB** (Docker) — in-memory storage is fine for a demo:

```sh
docker run -d --name cocoindex-surrealdb -p 8000:8000 surrealdb/surrealdb:latest start --user root --pass root
```

**2. Configure & install** — the pipeline reads Markdown notes from the local [`notes/`](notes/) folder; two sample notes are included (edit them, add your own, and re-run to watch the graph reconcile):

```sh
cp .env.example .env     # set OPENAI_API_KEY
pip install -e .
```

Both the extraction and resolution models default to `openai/gpt-5-mini`; override `LLM_MODEL` / `RESOLUTION_LLM_MODEL` for any [LiteLLM provider](https://docs.litellm.ai/docs/providers). The `SURREALDB_*` variables default to `ws://localhost:8000/rpc` with namespace `cocoindex` and database `meeting_notes`.

**3. Build the graph:**

```sh
cocoindex update main
```

**4. Explore the graph** — open [Surrealist](https://surrealist.app/) or `surreal sql --endpoint ws://localhost:8000 -u root -p root --ns cocoindex --db meeting_notes`, and ask:

```surql
-- Who attended which meetings (including organizer; one edge per attendee)
SELECT in AS person, out.note_file AS note_file, out.time AS time FROM attended;

-- Everything one person is on the hook for (task ids are the descriptions)
SELECT ->assigned_to->task AS tasks FROM person:⟨Alice Chen⟩;

-- Meetings someone organized
SELECT in AS person, out.note_file AS note_file FROM attended WHERE is_organizer;
```

This pipeline is the [docs knowledge graph](https://cocoindex.io/docs/examples/docs-to-knowledge-graph/) plus an entity-resolution pass — the natural next step when the LLM names the same thing two ways. Prefer another graph store? See the [Neo4j variant](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j).

---

<p align="center">
  If this turned your shared drive into a graph, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/meeting_notes_graph_surrealdb" alt="" width="1" height="1" />
