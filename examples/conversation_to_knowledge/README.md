<p align="center">
  <a href="https://cocoindex.io/docs/examples/podcast-to-knowledge-graph/" title="Turn podcasts into a knowledge graph with LLM and CocoIndex — transcription, LLM extraction, entity resolution, and a SurrealDB graph">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/podcast-to-knowledge-graph/cover.svg" alt="Turn podcasts into a knowledge graph with LLM and CocoIndex — podcast episodes transcribed, extracted, and resolved into a graph of people, statements, technologies, and organizations" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn podcasts into a <em>knowledge graph</em>.</h1>

<p align="center">
  <b>YouTube episodes → a queryable graph of who said what about which technologies — in plain async Python.</b><br/>
  Transcribe with speaker diarization, extract statements & entities with an LLM, resolve duplicates with embeddings, and sync it all into SurrealDB.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/podcast-to-knowledge-graph/" title="Read the full tutorial"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="Read the CocoIndex tutorial" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

You declare the graph in native Python and your own types — `target_state = transformation(source_state)`. The heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so adding one episode processes one episode, not the whole corpus.

## How it works

Read YouTube URLs → fetch & transcribe (yt-dlp + AssemblyAI diarization) → extract speakers, statements, and mentioned entities with an LLM → resolve duplicate people/techs/orgs with embeddings + LLM → declare nodes and relationships into SurrealDB.

The whole graph is declared as **target states** — read it in [`conv_knowledge/app.py`](conv_knowledge/app.py):

```python
# Phase 1 — one memoized component per episode: transcribe, extract, declare nodes + edges
@coco.fn(memo=True)
async def process_session(youtube_id, session_table, statement_table, session_statement_rel):
    transcript = await fetch_transcript(youtube_id)          # yt-dlp + AssemblyAI diarization
    metadata   = await extract_metadata(step1_text, transcript)   # LLM → who is speaking
    stmts      = await extract_statements(step2_text)             # LLM → claims + mentioned entities

    session_table.declare_record(row=Session(id=session_id, ...))     # graph node
    for stmt in stmts.statements:
        statement_table.declare_record(row=Statement(id=..., statement=stmt.statement))
        session_statement_rel.declare_relation(from_id=session_id, to_id=stmt_id)  # edge

# Phase 2 — collapse "GPT-4" / "gpt4" / "ChatGPT-4" into one canonical node
entity_dedup = await resolve_entities(
    entities=raw_names, embedder=coco.use_context(EMBEDDER),
    resolve_pair=LlmPairResolver(model=coco.use_context(RESOLUTION_LLM_MODEL)),
)

# Polymorphic edge: a statement can mention a person, a tech, or an org
statement_mentions_rel = await surrealdb.mount_relation_target(
    SURREAL_DB, "statement_mentions", statement_table,
    [entity_tables[c.name] for c in ENTITY_TYPES],
)
```

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/podcast-to-knowledge-graph/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough: the two-step LLM extraction, the data models, entity resolution, the graph schema, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Structured LLM extraction.** OpenAI (via LiteLLM) + Pydantic models pull speakers, thematic statements, and mentioned entities as *typed data* — not freeform text you have to re-parse.
- **Entity resolution, built in.** [`resolve_entities`](https://cocoindex.io/docs/ops/entity_resolution/) collapses near-duplicate people, techs, and orgs using embedding similarity + LLM confirmation, so the graph has one canonical node per real-world thing.
- **Incremental, per episode.** `@coco.fn(memo=True)` with one component per YouTube ID means adding an episode processes only that episode; unchanged sessions are skipped.
- **A real graph, declaratively.** Nodes and polymorphic relationships are declared as target states; CocoIndex syncs them into [SurrealDB](https://cocoindex.io/docs/connectors/) and cleans up what's gone — no migration scripts.
- **Plain async Python, swappable parts.** Transcriber, LLM, embedder, and graph store are all yours to change.

## Run it

**1. Start SurrealDB** (Docker):

```sh
docker run -d --name surrealdb --user root -p 8787:8000 \
  -v surrealdb-data:/data surrealdb/surrealdb:latest \
  start --user root --pass root surrealkv:/data/database
```

**2. Set keys** — transcription + extraction:

```sh
export ASSEMBLYAI_API_KEY="..."   # speaker-diarized transcription
export OPENAI_API_KEY="sk-..."    # LLM extraction via LiteLLM

# Optional (shown with defaults)
export SURREALDB_URL="ws://localhost:8787/rpc"
export SURREALDB_NS="cocoindex"
export SURREALDB_DB="yt_conversations"
export SURREALDB_USER="root"
export SURREALDB_PASS="root"
export INPUT_DIR="./input"
export LLM_MODEL="openai/gpt-5-mini"
export RESOLUTION_LLM_MODEL="openai/gpt-5-mini"
```

**3. Install deps:**

```sh
pip install -e .
```

**4. Add YouTube URLs** — one per line in `input/sample.txt` (`#` for comments):

```
https://www.youtube.com/watch?v=VIDEO_ID_1
https://www.youtube.com/watch?v=VIDEO_ID_2
```

**5. Build the graph** (incremental — re-running skips unchanged sessions):

```sh
cocoindex update conv_knowledge.app
```

## Explore the graph

SurrealDB ships [Surrealist](https://surrealdb.com/surrealist), a built-in UI for browsing and querying. For example — *which technologies are mentioned by the most distinct people?*

```surql
SELECT name,
  array::len(array::distinct(
    <-statement_mentions<-statement<-person_statement<-person.id
  )) AS person_count
FROM tech ORDER BY person_count DESC LIMIT 10;
```

The graph is small and expressive — `session`, `statement`, `person`, `tech`, `org` nodes, joined by `session_statement`, `person_session`, `person_statement`, and the polymorphic `statement_mentions`.

## More graph examples

Building graphs from other sources? See [meeting notes → Neo4j](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j) and [→ FalkorDB](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_falkordb), or [browse all examples](https://github.com/cocoindex-io/cocoindex/tree/main/examples).

---

<p align="center">
  If this turned hours of podcasts into something you can actually query, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/podcast-to-knowledge-graph/">Tutorial</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/conversation_to_knowledge" alt="" width="1" height="1" />
