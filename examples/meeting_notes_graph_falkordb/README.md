# Build Meeting Notes Knowledge Graph from Google Drive — FalkorDB (CocoIndex v1)

Extract structured information from meeting notes stored in Google Drive and
build a knowledge graph in [FalkorDB](https://www.falkordb.com/). The flow
ingests Markdown notes, splits them by headings into per-meeting sections,
uses an LLM (via [LiteLLM](https://docs.litellm.ai/) +
[instructor](https://python.useinstructor.com/)) to parse participants,
organizer, time, and tasks, and writes nodes and relationships into the graph.


Please drop [CocoIndex on Github](https://github.com/cocoindex-io/cocoindex) a
star to support us and stay tuned for more updates. Thank you so much 🥥🤗.
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## What this builds

- `Meeting` nodes — one per meeting section, keyed by a stable integer id
  derived from `(note_file, date)`
- `Person` nodes — canonical organizers, participants, and task assignees,
  deduplicated by an embedding + LLM entity-resolution pass (so "Alice",
  "Alice Chen", and "alice c." collapse to a single node)
- `Task` nodes — tasks decided in meetings (keyed by description)
- Relationships:
  - `ATTENDED` — `Person → Meeting` (with `is_organizer` flag)
  - `DECIDED` — `Meeting → Task`
  - `ASSIGNED_TO` — `Person → Task`

The source is one or more Google Drive folders shared with a service account.
The flow watches for changes and keeps the graph up to date incrementally.

## How it works

The pipeline runs in three phases:

1. **Per-file extraction.** Read each file from Google Drive, split it by
   Markdown headings (`#` / `##`) into meeting sections, and for each section
   extract a structured `Meeting` via LiteLLM + instructor (date, note,
   organizer, participants, tasks with assignees). `Meeting` and `Task` nodes
   plus `DECIDED` edges are declared in this phase. Raw person names are
   carried forward.
2. **Person entity resolution.** All raw person names from all files are
   deduplicated using sentence-transformer embeddings and an LLM pair resolver
   to produce a canonical-name mapping.
3. **Person-touching relations.** Canonical `Person` nodes are declared, then
   `ATTENDED` and `ASSIGNED_TO` edges are wired up using resolved names.

CocoIndex reconciles changes incrementally — re-running after editing one note
only re-processes the affected sections, and the resolution phase only re-runs
when the set of raw names changes.

## Prerequisites

- A running FalkorDB instance:
  ```sh
  docker run -d -p 6379:6379 -p 3000:3000 falkordb/falkordb:latest
  ```
  The browser UI is at <http://localhost:3000>.
- An LLM key (defaults to OpenAI; configure via `LLM_MODEL` for other
  providers — see [LiteLLM providers](https://docs.litellm.ai/docs/providers)).
- A Google Cloud service account with read access to the source folders, and
  the folder IDs you want to ingest. See
  [Setup for Google Drive](https://cocoindex.io/docs/sources/googledrive#setup-for-google-drive).

## Environment

Set the following variables (copy `.env.example` to `.env` and fill in):

```sh
export OPENAI_API_KEY=sk-...
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL=/absolute/path/to/service_account.json
export GOOGLE_DRIVE_ROOT_FOLDER_IDS=folderId1,folderId2
export FALKORDB_URI=falkor://localhost:6379
export FALKORDB_GRAPH=meeting_notes
export LLM_MODEL=openai/gpt-5.4
export RESOLUTION_LLM_MODEL=openai/gpt-5-mini   # used for entity resolution
```

Then:

```sh
set -a && source .env && set +a
```

## Run

Install dependencies:

```sh
uv pip install -e .
```

Build/update the graph:

```sh
cocoindex update main
```

## Browse the knowledge graph

Open the FalkorDB browser at <http://localhost:3000>, select the
`meeting_notes` graph, and run Cypher queries.

```cypher
// All relationships
MATCH p=()-->() RETURN p

// Who attended which meetings (including organizer; one edge per attendee)
MATCH (p:Person)-[:ATTENDED]->(m:Meeting)
RETURN p.name, m.note_file, m.time, m.id

// Tasks decided in meetings
MATCH (m:Meeting)-[:DECIDED]->(t:Task)
RETURN m.note_file, m.time, t.description

// Task assignments
MATCH (p:Person)-[:ASSIGNED_TO]->(t:Task)
RETURN p.name, t.description

// Meetings someone organized
MATCH (p:Person)-[r:ATTENDED {is_organizer: true}]->(m:Meeting)
RETURN p.name, m.note_file, m.time
```

You can also use `redis-cli`:

```sh
redis-cli GRAPH.QUERY meeting_notes \
  "MATCH (p:Person)-[:ATTENDED]->(m:Meeting) RETURN p.name, m.note_file, m.time"
```
