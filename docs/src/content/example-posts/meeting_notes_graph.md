---
image: https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/cover.png
---

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/v0/examples/meeting_notes_graph)

![Meeting Notes Graph](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/cover.png)


Meeting notes capture decisions, action items, participant information, and the relationships between people and tasks.

With a knowledge graph, you can run queries like: *"Who attended meetings where the topic was 'budget planning'?"* or *"What tasks did Sarah get assigned across all meetings?"*

Build a meeting knowledge graph from Google Drive Markdown notes using LLM extraction and Neo4j, with automatic continuous updates.


![Neo4j Property Graph](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/neo4j.png)

## The Problem: Unstructured Meeting Data at Enterprise Scale

Meeting notes change after publication: participants correct details, reassign tasks, and revise decisions. A knowledge graph needs to reflect those edits and remove data deleted from the source. Reprocessing the full collection on each update repeats extraction work for unchanged notes.

## Architecture Overview

The pipeline follows a clear data flow with incremental processing built in at every stage:

```
Google Drive (Documents - with change tracking)
  → Identify changed documents
  → Split into meetings
  → Extract structured data with LLM (only for changed documents)
  → Collect nodes and relationships
  → Export to Neo4j (with upsert logic)
```

**Prerequisites**

- Install [Neo4j](https://cocoindex.io/docs-v0/targets/neo4j) and start it locally
    - Default local browser: [http://localhost:7474](http://localhost:7474/)
    - Default credentials used in this example: username `neo4j`, password `cocoindex`
    [→ Neo4j Target](https://cocoindex.io/docs-v0/targets/neo4j)

- [Configure your OpenAI API key](https://cocoindex.io/docs-v0/ai/llm#openai)
- Prepare Google Drive:
    - Create a Google Cloud service account and download its JSON credential
    - Share the source folders with the service account email
    - Collect the root folder IDs you want to ingest
    - See [Setup for Google Drive](https://cocoindex.io/docs-v0/sources/googledrive#setup-for-google-drive) for details
    [→ GoogleDrive Source Setup](https://cocoindex.io/docs-v0/sources/googledrive)

**Environment**

Set the following environment variables:

```sh
export OPENAI_API_KEY=sk-...
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL=/absolute/path/to/service_account.json
export GOOGLE_DRIVE_ROOT_FOLDER_IDS=folderId1,folderId2
```

:::info
- `GOOGLE_DRIVE_ROOT_FOLDER_IDS` accepts a comma-separated list of folder IDs
- The flow polls recent changes and refreshes periodically
:::


## Flow Definition

### Overview

![Overview](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/flow.png)

### Add source and collector

```python
@cocoindex.flow_def(name="MeetingNotesGraph")
def meeting_notes_graph_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that extracts triples from files and builds knowledge graph.
    """
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.GoogleDrive(
            service_account_credential_path=credential_path,
            root_folder_ids=root_folder_ids,
            recent_changes_poll_interval=datetime.timedelta(seconds=10),
        ),
        refresh_interval=datetime.timedelta(minutes=1),
    )
```

The pipeline starts by connecting to Google Drive using a service account. CocoIndex's built-in source connector handles authentication and provides **incremental change detection**. The **`recent_changes_poll_interval`** parameter means the source checks for new or modified files every 10 seconds, while the **`refresh_interval`** determines when the entire flow re-runs (every minute).

[→ GoogleDrive Source](https://cocoindex.io/docs-v0/sources/googledrive)

![Ingest documents](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/ingest.png)

The Google Drive source connector tracks file changes:

1. Lists files from Google Drive with last modified time
2. Identifies only the files that have been added or modified since the last successful run
3. Skips unchanged files entirely
4. Passes only changed documents downstream

When the source data and processing logic are unchanged, CocoIndex reuses cached results instead of repeating LLM extraction. It still checks the source for changes.

Check out How live updates work in CocoIndex:

[→ How live updates work in CocoIndex](https://cocoindex.io/docs-v0/tutorials/live_updates)


### Add collector

```python
meeting_nodes = data_scope.add_collector()
attended_rels = data_scope.add_collector()
decided_tasks_rels = data_scope.add_collector()
assigned_rels = data_scope.add_collector()
```

[→ Collectors](https://cocoindex.io/docs-v0/core/flow_def#data-collector)

The pipeline then collects data into specialized collectors for different entity types and relationships:

- **Meeting Nodes** - Store the meeting itself with its date and notes
- **Attendance Relationships** - Capture who attended meetings and whether they were the organizer
- **Task Decision Relationships** - Link meetings to decisions (tasks that were decided upon)
- **Task Assignment Relationships** - Assign specific tasks to people

## Process each document

### Extract meetings

```python
with data_scope["documents"].row() as document:
    document["meetings"] = document["content"].transform(
        cocoindex.functions.SplitBySeparators(
            separators_regex=[r"\n\n##?\ "], keep_separator="RIGHT"
        )
    )
```

Meeting documents often contain multiple meetings in a single file. This step splits documents on Markdown headers (## or #) preceded by blank lines, treating each section as a separate meeting. The **`keep_separator="RIGHT"`** means the separator (header) is kept with the right segment, preserving context.

![Extract meetings](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/meetings.png)

## Extract meeting

### Define Meeting schema

```python
@dataclass
class Person:
    name: str

@dataclass
class Task:
    description: str
    assigned_to: list[Person]

@dataclass
class Meeting:
    time: datetime.date
    note: str
    organizer: Person
    participants: list[Person]
    tasks: list[Task]
```

The dataclass schema specifies the fields and Python types that the LLM should return. CocoIndex parses the response into this structure for the graph-building steps.

### Extract and collect relationship

```python
with document["meetings"].row() as meeting:
    parsed = meeting["parsed"] = meeting["text"].transform(
        cocoindex.functions.ExtractByLlm(
            llm_spec=cocoindex.LlmSpec(
                api_type=cocoindex.LlmApiType.OPENAI, model="gpt-5"
            ),
            output_type=Meeting,
        )
    )
```

CocoIndex caches `ExtractByLlm` output. It reuses the result while the input text, model, and output type definition remain unchanged.

[→ ExtractByLlm](https://cocoindex.io/docs-v0/ops/functions/#extractbyllm)

![Extract metadata](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/metadata.png)


## Collect relationship

```python
meeting_key = {"note_file": document["filename"], "time": parsed["time"]}
meeting_nodes.collect(**meeting_key, note=parsed["note"])

attended_rels.collect(
    id=cocoindex.GeneratedField.UUID,
    **meeting_key,
    person=parsed["organizer"]["name"],
    is_organizer=True,
)

with parsed["participants"].row() as participant:
    attended_rels.collect(
        id=cocoindex.GeneratedField.UUID,
        **meeting_key,
        person=participant["name"],
    )

with parsed["tasks"].row() as task:
    decided_tasks_rels.collect(
        id=cocoindex.GeneratedField.UUID,
        **meeting_key,
        description=task["description"],
    )
    with task["assigned_to"].row() as assigned_to:
        assigned_rels.collect(
            id=cocoindex.GeneratedField.UUID,
            **meeting_key,
            task=task["description"],
            person=assigned_to["name"],
        )
```

**Collectors** in CocoIndex act like in‑memory buffers: you declare collectors for different categories (meeting nodes, attendance, tasks, assignments), then as you process each document you "collect" relevant entries.

This block **collects nodes and relationships** from parsed meeting notes to build a knowledge graph in Neo4j using CocoIndex:

- **Person → Meeting (ATTENDED)**

    Links participants (including organizers) to the meetings they attended.

- **Meeting → Task (DECIDED)**

    Links meetings to tasks or decisions that were made.

- **Person → Task (ASSIGNED_TO)**

    Links tasks back to the people responsible for them.


## Map to graph database

### Overview

We will be creating a property graph with following nodes and relationships:
![Graph](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/graph.png)

To learn more about property graph, please refer to CocoIndex's [Property Graph Targets](https://cocoindex.io/docs-v0/targets#property-graph-targets) documentation.

[→ Neo4j Target](https://cocoindex.io/docs-v0/targets/neo4j)

[→ Property Graph Targets](https://cocoindex.io/docs-v0/targets/#property-graph-targets)
### Map Meeting Nodes

```python
meeting_nodes.export(
    "meeting_nodes",
    cocoindex.targets.Neo4j(
        connection=conn_spec, mapping=cocoindex.targets.Nodes(label="Meeting")
    ),
    primary_key_fields=["note_file", "time"],
)
```

- This uses CocoIndex's **Neo4j target** to export data to a graph database.
- The `mapping=cocoindex.targets.Nodes(label="Meeting")` part tells CocoIndex: "Take each row collected in `meeting_nodes` and map it to a **node** in the Neo4j graph, with label `Meeting`."
- `primary_key_fields=["note_file", "time"]` instructs CocoIndex which fields uniquely identify a node. That way, if the same meeting (same `note_file` and `time`) appears in different runs/updates, it will map to the same node — avoiding duplicates.

### What "node export" means in CocoIndex → Neo4j context

| Collector rows | Graph entities |
| --- | --- |
| Each collected row (meeting with its fields) | One node in Neo4j with label `Meeting` |
| Fields of that row | Properties of the node (e.g. `note_file`, `time`, `note`) |

### Declare Person and Task Nodes

```python
flow_builder.declare(
    cocoindex.targets.Neo4jDeclaration(
        connection=conn_spec,
        nodes_label="Person",
        primary_key_fields=["name"],
    )
)
flow_builder.declare(
    cocoindex.targets.Neo4jDeclaration(
        connection=conn_spec,
        nodes_label="Task",
        primary_key_fields=["description"],
    )
)
```

- The `declare(...)` [method](https://cocoindex.io/docs-v0/core/flow_def) on `flow_builder` lets you **pre‐declare** node labels that may appear as source or target nodes in relationships — even if you don't have an explicit collector exporting them as standalone node rows.
- `Neo4jDeclaration` is the specification for such declared nodes: you give it the connection, the node label (type), and the `primary_key_fields` that uniquely identify instances of that node

For example, for the `Person` Declaration,

- You tell CocoIndex: "We expect `Person`‑labeled nodes to exist in the graph. They will be referenced in relationships (e.g. a meeting's organizer or attendees, task assignee), but we don't have a dedicated collector exporting Person rows."
- By declaring `Person`, CocoIndex will handle deduplication: multiple relationships referencing the same `name` will map to the same `Person` node in Neo4j (because `name` is the primary key).

#### How declaration works with relationships & export logic

- When you later export relationship collectors (e.g. ATTENDED, DECIDED, ASSIGNED_TO), those relationships will reference nodes of type `Person` or `Task`. CocoIndex needs to know how to treat those node labels so it can create or match the corresponding nodes properly. `declare(...)` gives CocoIndex that knowledge.
- CocoIndex handles **matching & deduplication** of nodes by checking primary‑key fields. If a node with the same primary key already exists, it reuses it rather than creating a duplicate.

### Map ATTENDED Relationship

**ATTENDED relationships**

```python
attended_rels.export(
    "attended_rels",
    cocoindex.targets.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.targets.Relationships(
            rel_type="ATTENDED",
            source=cocoindex.targets.NodeFromFields(
                label="Person",
                fields=[
                    cocoindex.targets.TargetFieldMapping(
                        source="person", target="name"
                    )
                ],
            ),
            target=cocoindex.targets.NodeFromFields(
                label="Meeting",
                fields=[
                    cocoindex.targets.TargetFieldMapping("note_file"),
                    cocoindex.targets.TargetFieldMapping("time"),
                ],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

- This call ensures that **ATTENDED relationships** — i.e. "Person → Meeting" (organizer or participant → the meeting) — are explicitly encoded as edges in the Neo4j graph.
- It links `Person` nodes with `Meeting` nodes via `ATTENDED` relationships, enabling queries like "which meetings did Alice attend?" or "who attended meeting X?".
- By mapping `Person` and `Meeting` nodes correctly and consistently (using unique keys), it ensures a clean graph with no duplicate persons or meetings.
- Because relationships get unique IDs and are exported with consistent keys, the graph remains stable across incremental updates: re-runs won't duplicate edges or nodes.

![Map ATTENDED relationship](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/export.png)

### Map DECIDED Relationship

**DECIDED relationships**

```python
decided_tasks_rels.export(
    "decided_tasks_rels",
    cocoindex.targets.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.targets.Relationships(
            rel_type="DECIDED",
            source=cocoindex.targets.NodeFromFields(
                label="Meeting",
                fields=[
                    cocoindex.targets.TargetFieldMapping("note_file"),
                    cocoindex.targets.TargetFieldMapping("time"),
                ],
            ),
            target=cocoindex.targets.NodeFromFields(
                label="Task",
                fields=[
                    cocoindex.targets.TargetFieldMapping("description"),
                ],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

- Encodes **DECIDED** edges: links `Meeting` → `Task` in the graph.
- Enables queries like: "Tasks decided in Meeting X?" or "Which meeting decided Task Y?"
- Consistent mapping avoids duplicate nodes; unique IDs keep the graph deduped on re-runs.

### Map ASSIGNED_TO Relationship

**ASSIGNED_TO relationships**

```python
assigned_rels.export(
    "assigned_rels",
    cocoindex.targets.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.targets.Relationships(
            rel_type="ASSIGNED_TO",
            source=cocoindex.targets.NodeFromFields(
                label="Person",
                fields=[
                    cocoindex.targets.TargetFieldMapping(
                        source="person", target="name"
                    ),
                ],
            ),
            target=cocoindex.targets.NodeFromFields(
                label="Task",
                fields=[
                    cocoindex.targets.TargetFieldMapping(
                        source="task", target="description"
                    ),
                ],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

It takes all the **task assignment data** you collected (`assigned_rels`) — i.e., which person is responsible for which task.

- This explicitly encodes **task ownership** in the graph, linking people to the tasks they are responsible for.
- It enables queries like:
    - "Which tasks is Alice assigned to?"
    - "Who is responsible for Task X?"
- By using consistent node mappings (`name` for `Person`, `description` for `Task`), it prevents duplicate person or task nodes.
- Unique IDs on relationships ensure the graph remains stable across incremental updates — re-running the flow won't create duplicate edges.

## The Resulting Graph

After running this pipeline, query these nodes and relationships in Neo4j:

![Resulting Graph](https://cocoindex.io/blobs/docs/img/examples/meeting_notes_graph/neo4j.png)

**Nodes:**
- `Meeting`: Individual meetings (date, notes)
- `Person`: Participants
- `Task`: Action items

**Relationships:**
- `ATTENDED`: Person attended Meeting
- `DECIDED`: Meeting decided Task
- `ASSIGNED_TO`: Person assigned to Task

CocoIndex exports to Neo4j incrementally—only changed nodes or relationships are updated, avoiding duplicates and minimizing unnecessary writes.

## Run

**Build/update the graph**

Install dependencies:

```sh
pip install -e .
```

Update the index (run the flow once to build/update the graph):

```sh
cocoindex update main
```

**Browse the knowledge graph**

Open Neo4j Browser at [http://localhost:7474](http://localhost:7474/).

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


**CocoInsight**

 CocoInsight (Free beta now) is a tool to troubleshoot the index generation and understand the data lineage of the pipeline. It connects to your local CocoIndex server, with Zero pipeline data retention.

Start CocoInsight:

```sh
cocoindex server -ci main
```


## Key CocoIndex Features Demonstrated

The pipeline uses these CocoIndex features:

### 1. Incremental Processing with Change Detection

When meeting-note files change, CocoIndex reprocesses those files and reuses the work for unchanged files. This avoids repeating LLM extraction for the entire collection.

### 2. Data Lineage and Observability

CocoIndex records data lineage through LLM extraction, collection, and mapping. Use it to trace a graph field back to the source data and processing steps that produced it.

### 3. Declarative Data Flow

The pipeline is defined in Python. CocoIndex handles scheduling, error recovery, state management, and change tracking.

### 4. Schema Management and Idempotency

CocoIndex creates Neo4j nodes and relationships from the declared mappings. Primary key fields identify existing records during incremental updates, so changes can update or remove those records.

### 5. Real-time Update Capability

In live mode, the pipeline polls Google Drive for changes and updates the graph after processing them. Update latency depends on the polling interval and processing time.

## Support CocoIndex

If this example was helpful, the easiest way to support CocoIndex is to [give the project a ⭐ on GitHub](https://github.com/cocoindex-io/cocoindex).

Share the example or [open an issue](https://github.com/cocoindex-io/cocoindex/issues) if you find a problem.
