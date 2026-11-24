# Meeting Notes Graph — SurrealDB (Rust)

Rust port of the Python
[`meeting_notes_graph_surrealdb`](../../meeting_notes_graph_surrealdb) example.

An LLM pulls the organizer, participants, and tasks out of each meeting; an
embedding + LLM pass collapses "Alice", "Alice Chen", and "alice c." into *one*
person record — into [SurrealDB](https://surrealdb.com/). Point it at a folder
of Markdown notes, and it re-extracts only the note you edited, then reconciles
the graph.

## How it works

Three record tables, three relation tables:

- **`meeting`** records — one per meeting section, keyed by a stable integer id
  derived from the meeting date.
- **`person`** records — canonical organizers, participants, and assignees,
  deduplicated by an embedding + LLM entity-resolution pass. The record id *is*
  the canonical name: `person:⟨Alice Chen⟩`.
- **`task`** records — tasks decided in meetings, keyed by description.
- **`attended`** edges — `person → meeting`, carrying an `is_organizer` flag.
  **`decided`** edges — `meeting → task`. **`assigned_to`** edges — `person → task`.

Because people are shared across notes, the pipeline runs in three phases:

1. Per note (`process_file`, one live child component per file): split into
   meetings, extract each with the LLM (`extract_meeting`, memoized by section
   text), declare `meeting`/`task` records and `decided` edges, and publish the
   raw person names into an in-memory `LiveMap`.
2. `resolve_persons` (memoized) collapses the raw names with
   `cocoindex::resolve_entities` + `LlmPairResolver`.
3. `sync_person_graph`, an `auto_refresh` component, reads the `LiveMap`
   snapshot and declares the canonical `person` records and the `attended` /
   `assigned_to` edges with resolved names.

## Parallel to the Python example

| Concern            | Python                                               | Rust (this example)                                              |
| ------------------ | ---------------------------------------------------- | ---------------------------------------------------------------- |
| LLM extraction     | `instructor` + LiteLLM, Pydantic models              | `ApiChatClient::chat_json` + `serde` structs                     |
| Entity resolution  | `resolve_entities` + `LlmPairResolver`               | `cocoindex::resolve_entities` + `ops::api::LlmPairResolver`      |
| Embeddings         | `SentenceTransformerEmbedder`                        | `ops::sentence_transformers::SentenceTransformerEmbedder`        |
| Source             | `localfs.walk_dir(live=True)` + `coco.mount_each`    | `walk_dir(..).live()` + `ctx.mount_each_live`                    |
| Cross-file phase   | `coco.auto_refresh(sync_person_graph, interval=5s)`  | `ctx.auto_refresh(.., 5s, ..)`                                   |
| Raw-name hand-off  | `LiveMap`                                            | `LiveMap`                                                        |
| Targets            | `surrealdb.mount_table_target` / `mount_relation_target` | `surrealdb::mount_table_target_with_schema` / `mount_relation_target` |
| Row schema         | Dataclass annotations                                | `#[derive(SchemaFields)]` + `TableSchema::from_row`              |
| Cache busting      | `ContextKey(..., detect_change=True)` on model ids   | `context_key!(static LLM_MODEL: String, detect_change)`          |

The LLM goes through an OpenAI-compatible endpoint: `OPENAI_API_KEY`, and
`LLM_BASE_URL` for another provider (e.g. Ollama). Model ids are plain model
names (`gpt-5-mini`), not LiteLLM provider strings.

## Run

**1. Start SurrealDB** — in-memory storage is fine for a demo:

```sh
docker run -d --name cocoindex-surrealdb -p 8000:8000 surrealdb/surrealdb:latest start --user root --pass root
```

**2. Configure** — the pipeline reads Markdown notes from the local
[`notes/`](notes/) folder; two sample notes are included (edit them, add your
own, and re-run to watch the graph reconcile):

```sh
export OPENAI_API_KEY=...
# optional: LLM_MODEL / RESOLUTION_LLM_MODEL (default gpt-5-mini), LLM_BASE_URL,
# SURREALDB_URL (default localhost:8000), SURREALDB_NS / SURREALDB_DB / SURREALDB_USER / SURREALDB_PASS
```

**3. Build the graph:**

```sh
cargo run                    # one-shot catch-up
cargo run -- --live          # live: file edits stream into the graph (Ctrl-C to stop)
cargo run -- /path/to/notes  # another notes folder
```

**4. Explore the graph** — open [Surrealist](https://surrealist.app/) or
`surreal sql --endpoint ws://localhost:8000 -u root -p root --ns cocoindex --db meeting_notes`, and ask:

```surql
-- Who attended which meetings (including organizer; one edge per attendee)
SELECT in AS person, out.note_file AS note_file, out.time AS time FROM attended;

-- Everything one person is on the hook for (task ids are the descriptions)
SELECT ->assigned_to->task AS tasks FROM person:⟨Alice Chen⟩;

-- Meetings someone organized
SELECT in AS person, out.note_file AS note_file FROM attended WHERE is_organizer;
```

A no-change re-run makes zero LLM calls: each section's extraction and each
resolution decision is memoized by the engine. Prefer another graph store? See
the [Neo4j variant](../meeting_notes_graph_neo4j).
