# Build Meeting Notes Knowledge Graph from Google Drive

We will extract structured information from meeting notes stored in Google Drive and build a knowledge graph in Neo4j. The flow ingests Markdown notes, splits them by headings into meetings, uses an LLM to parse participants, organizer, time, and tasks, and then writes nodes and relationships into a graph database.

Please drop [CocoIndex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us and stay tuned for more updates. Thank you so much 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## What this builds

The pipeline defines:

- Meeting nodes: one per meeting section, keyed by source note file and meeting time
- Person nodes: people who organized or attended meetings
- Task nodes: tasks decided in meetings
- Relationships:
  - `ATTENDED` Person → Meeting (organizer included, marked in flow when collected)
  - `DECIDED` Meeting → Task
  - `ASSIGNED_TO` Person → Task

The source is Google Drive folders shared with a service account. The flow watches for recent changes and keeps the graph up to date.

## How it works

1. Ingest files from Google Drive (service account + root folder IDs)
2. Split each note by Markdown headings into meeting sections
3. Use an LLM to extract a structured `Meeting` object: time, note, organizer, participants, and tasks (with assignees)
4. Collect nodes and relationships in-memory
5. Export to Neo4j:
   - Nodes: `Meeting` (explicit export), `Person` and `Task` (declared with primary keys)
   - Relationships: `ATTENDED`, `DECIDED`, `ASSIGNED_TO`

## Prerequisite

- Install [Neo4j](https://cocoindex.io/docs/targets/neo4j) and start it locally
  - Default local browser: <http://localhost:7474>
  - Default credentials used in this example: username `neo4j`, password `cocoindex`
- [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai)
- Prepare Google Drive:
  - Create a Google Cloud service account and download its JSON credential
  - Share the source folders with the service account email
  - Collect the root folder IDs you want to ingest
  - See [Setup for Google Drive](https://cocoindex.io/docs/sources/googledrive#setup-for-google-drive) for details

## Environment

Set the following environment variables:

```sh
export OPENAI_API_KEY=sk-...
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL=/absolute/path/to/service_account.json
export GOOGLE_DRIVE_ROOT_FOLDER_IDS=folderId1,folderId2
```

Alternatively, fill in your values in `.env.example` and source it:

```sh
set -a && source .env.example && set +a
```

Notes:

- `GOOGLE_DRIVE_ROOT_FOLDER_IDS` accepts a comma-separated list of folder IDs
- The flow polls recent changes and refreshes periodically

## Run

### Build/update the graph

Install dependencies:

```sh
pip install -e .
```

Update the index (run the flow once to build/update the graph):

```sh
cocoindex update main
```

### Browse the knowledge graph

Open Neo4j Browser at <http://localhost:7474>.

Sample Cypher queries:

```cypher
// All relationships
MATCH p=()-->() RETURN p

// Who attended which meetings (including organizer)
MATCH (p:Person)-[:ATTENDED]->(m:Meeting)
RETURN p, m

// Tasks decided in meetings
MATCH (m:Meeting)-[:DECIDED]->(t:Task)
RETURN m, t

// Task assignments
MATCH (p:Person)-[:ASSIGNED_TO]->(t:Task)
RETURN p, t
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. It just connects to your local CocoIndex server, with Zero pipeline data retention.

Start CocoInsight:

```sh
cocoindex server -ci main
```

Then open the UI at <https://cocoindex.io/cocoinsight>.
