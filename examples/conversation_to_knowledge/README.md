# Turn Podcasts into a Knowledge Graph with LLM and CocoIndex

<p align="center">
<img width="3840" height="2174" alt="cocoindex-surrealdb-meetup" src="https://github.com/user-attachments/assets/6b0a4c09-eeaa-4020-ab24-35a997613576" />
Continously Converts YouTube podcast/interview sessions into a structured knowledge graph in SurrealDB.

</p>


<div align="center">

[![Discord](https://img.shields.io/discord/1314801574169673738?logo=discord&color=5B5BD6&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
[![Documentation](https://img.shields.io/badge/Documentation-394e79?logo=readthedocs&logoColor=00B9FF)](https://cocoindex.io/docs/getting_started/quickstart)
[![License](https://img.shields.io/badge/license-Apache%202.0-5B5BD6?logoColor=white)](https://opensource.org/licenses/Apache-2.0)
<!--[![PyPI - Downloads](https://img.shields.io/pypi/dm/cocoindex)](https://pypistats.org/packages/cocoindex) -->
[![PyPI Downloads](https://static.pepy.tech/badge/cocoindex/month)](https://pepy.tech/projects/cocoindex)
[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)


🌟 Please help star [CocoIndex](https://github.com/cocoindex-io/cocoindex) if you like this project!

</div>

**Pipeline:**
1. Read YouTube URLs from plain text files
2. Download audio via `yt-dlp`, transcribe with speaker diarization via AssemblyAI
3. Extract metadata, speakers, and thematic statements via LLM (`openai/gpt-5.4-mini`)
4. Resolve duplicate entities (persons, techs, orgs) using embedding similarity (faiss) + LLM confirmation
5. Store the knowledge graph in SurrealDB: sessions, statements, persons, techs, orgs, and their relationships

## Prerequisites

- Python 3.11+
- [FFmpeg](https://ffmpeg.org/) installed (required by `yt-dlp` for audio extraction)
- Docker (for SurrealDB)
- An [AssemblyAI API key](https://www.assemblyai.com/) (for transcription with speaker diarization)
- An OpenAI API key (for LLM extraction via litellm)

## Setup

### 1. Start SurrealDB

Run SurrealDB with persistent storage via Docker:

```sh
docker run -d \
  --name surrealdb \
  --user root \
  -p 8787:8000 \
  -v surrealdb-data:/data \
  surrealdb/surrealdb:latest \
  start --user root --pass root surrealkv:/data/database
```

This persists data in a Docker volume (`surrealdb-data`) across container restarts.

### 2. Set environment variables

```sh
# Required
export ASSEMBLYAI_API_KEY="..."
export OPENAI_API_KEY="sk-..."

# Optional (shown with defaults)
export SURREALDB_URL="ws://localhost:8787/rpc"
export SURREALDB_NS="cocoindex"
export SURREALDB_DB="yt_conversations"
export SURREALDB_USER="root"
export SURREALDB_PASS="root"
export INPUT_DIR="./input"
export LLM_MODEL="openai/gpt-5.4-mini"
```

### 3. Install dependencies

```sh
pip install -e .
```

### 4. Add YouTube URLs

Edit `input/sample.txt` (or create new `.txt` files under `input/`). One URL per line, `#` for comments:

```
# AI podcasts
https://www.youtube.com/watch?v=VIDEO_ID_1
https://www.youtube.com/watch?v=VIDEO_ID_2
```

## Run

Build/update the knowledge graph:

```sh
cocoindex update conv_knowledge.app
```

This is incremental — re-running skips sessions that haven't changed.

## Visualize the Knowledge Graph

SurrealDB includes a built-in web UI called **Surrealist** for exploring and visualizing data.

### Option A: Surrealist Cloud

1. Go to [app.surrealdb.com](https://app.surrealdb.com)
2. Connect to your local instance:
   - Endpoint: `ws://localhost:8787`
   - Namespace: `cocoindex`
   - Database: `yt_conversations`
   - Username: `root` / Password: `root`
3. Use the **Explorer** tab to browse tables and relations
4. Use the **Query** tab to run SurrealQL queries (see below)

### Option B: Surrealist Desktop

Download from [surrealdb.com/surrealist](https://surrealdb.com/surrealist) for a native app with the same features.

### Example queries

See all relationships:

```surql
SELECT * FROM statement_involves, session_statement, person_session;
```

Browse all sessions and their statements:

```surql
SELECT id, name, description, date FROM session;
```

Find all statements a person made:

```surql
SELECT
  <-person_statement<-person.name AS speaker,
  statement
FROM statement;
```

Explore the full graph around a person:

```surql
SELECT
  name,
  ->person_session->session.name AS sessions,
  ->person_statement->statement.statement AS statements
FROM person;
```

Find all entities involved in a statement:

```surql
SELECT
  statement,
  ->statement_involves->person.name AS persons,
  ->statement_involves->tech.name AS techs,
  ->statement_involves->org.name AS orgs
FROM statement;
```

Top N techs mentioned by the most people:

```surql
SELECT
  name,
  array::distinct(
    <-statement_involves<-statement<-person_statement<-person.name
  ) AS persons,
  array::len(array::distinct(
    <-statement_involves<-statement<-person_statement<-person.id
  )) AS person_count
FROM tech
ORDER BY person_count DESC
LIMIT 10;
```

## Schema

```
Nodes:  session, statement, person, tech, org
Edges:  session_statement  (session -> statement)
        person_session     (person -> session)
        person_statement   (person -> statement)
        statement_involves (statement -> person | tech | org)
```
