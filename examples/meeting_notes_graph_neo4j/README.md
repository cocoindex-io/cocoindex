# Build Meeting Notes Knowledge Graph from Google Drive — Neo4j (CocoIndex v1)

Extract structured information from meeting notes stored in Google Drive and
build a knowledge graph in [Neo4j](https://neo4j.com/). The flow ingests
Markdown notes, splits them by headings into per-meeting sections, uses an
LLM (via [LiteLLM](https://docs.litellm.ai/) +
[instructor](https://python.useinstructor.com/)) to parse participants,
organizer, time, and tasks, and writes nodes and relationships into the graph.

> **Looking for the FalkorDB flavor?** See
> [`examples/meeting_notes_graph_falkordb`](../meeting_notes_graph_falkordb/) — same
> flow shape, swaps the Neo4j connector for the FalkorDB connector.

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

- A running Neo4j 5.18+ instance:
  ```sh
  docker run -d \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/cocoindex \
    --name cocoindex-neo4j \
    neo4j:5.26-community
  ```
  The browser UI is at <http://localhost:7474>; log in with `neo4j` /
  `cocoindex`.

  > **Why 5.18+?** Vector index DDL (`CREATE VECTOR INDEX … OPTIONS { indexConfig: {...} }`)
  > shipped in 5.18. Older Neo4j 5 servers need the
  > `db.index.vector.createNodeIndex` procedure, which this connector
  > doesn't emit. The flow itself doesn't use vector indexes, but the
  > connector requires 5.18+ for parity.

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
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=cocoindex
export NEO4J_DATABASE=neo4j
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

Open Neo4j Browser at <http://localhost:7474>, log in, and run Cypher queries:

```cypher
// All relationships
MATCH p=()-->() RETURN p LIMIT 100

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

To wipe the graph between runs:

```cypher
MATCH (n) DETACH DELETE n
```

You can also use `cypher-shell` from the command line:

```sh
docker exec -it cocoindex-neo4j cypher-shell -u neo4j -p cocoindex \
  "MATCH (p:Person)-[:ATTENDED]->(m:Meeting) RETURN p.name, m.note_file, m.time"
```

## CocoInsight

Use [CocoInsight](https://cocoindex.io/cocoinsight) to inspect data lineage and
debug the pipeline. It connects to your local CocoIndex server with zero
pipeline data retention.

```sh
cocoindex server -ci main
```

Then open <https://cocoindex.io/cocoinsight>.
