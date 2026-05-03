# Moved

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

## Get Started
- Neo4j version → [`examples/meeting_notes_graph_neo4j`](../meeting_notes_graph_neo4j/)

- FalkorDB version → [`examples/meeting_notes_graph_falkordb`](../meeting_notes_graph_falkordb/)
