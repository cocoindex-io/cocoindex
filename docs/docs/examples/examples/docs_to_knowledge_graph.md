---
title: Real-Time Knowledge Graph for Documents with LLM
description: CocoIndex now supports knowledge graph with incremental processing. Build live knowledge for agents is super easy with CocoIndex.
sidebar_class_name: hidden
slug: /examples/knowledge-graph-for-docs
canonicalUrl: '/examples/knowledge-graph-for-docs'
sidebar_custom_props:
  image: /img/examples/docs_to_knowledge_graph/cover.png
  tags: [knowledge-graph, structured-data-extraction]
image: /img/examples/docs_to_knowledge_graph/cover.png
tags: [knowledge-graph, structured-data-extraction]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph" margin="0 0 24px 0" />
<YouTubeButton url="https://youtu.be/2KVkpUGRtnk?si=MRalDweWrid-IFje" margin="0 0 24px 0" />

![Knowledge Graph for Docs](/img/examples/docs_to_knowledge_graph/cover.png)

## Overview
[CocoIndex](https://github.com/cocoindex-io/cocoindex) makes it easy to build and maintain knowledge graphs with continuous source updates. In this tutorial, we will use LLM to extract relationships between the concepts in each document, and generate two kinds of relationships:
1. Relationships between subjects and objects. E.g., "CocoIndex supports Incremental Processing"
2. Mentions of entities in a document. E.g., "core/basics.mdx" mentions `CocoIndex` and `Incremental Processing`.

and then build a knowledge graph.

![Relationship between subjects and objects](/img/examples/docs_to_knowledge_graph/relationship.png)


## Flow Overview
![Flow overview](/img/examples/docs_to_knowledge_graph/flow.png)
- Add documents as source.
- For each document, extract the title and summary, and collects to `Document` nodes.
- For each document, use LLM to extract relationships — `subject`, `predicate`, `object`, and collect different kinds of relationships.
- CocoIndex can direct map the collected data to Neo4j nodes and relationships.

## Setup
*   [Install PostgreSQL](https://cocoindex.io/docs/getting_started/installation#-install-postgres). CocoIndex uses PostgreSQL internally for incremental processing.
*   [Install Neo4j](https://cocoindex.io/docs/ops/targets#neo4j-dev-instance), a graph database.
*   [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai).  Alternatively, we have native support for Gemini, Ollama, LiteLLM. You can choose your favorite LLM provider and work completely on-premises.

    <DocumentationButton url="https://cocoindex.io/docs/ai/llm" text="LLM" margin="0 0 16px 0" />


## Documentation
<DocumentationButton url="https://cocoindex.io/docs/ops/targets#property-graph-targets" text="Property Graph Targets" margin="0 0 16px 0" />


## Data flow to build knowledge graph

### Add documents as source

We will process CocoIndex documentation markdown files (`.md`, `.mdx`) from the `docs/core` directory ([markdown files](https://github.com/cocoindex-io/cocoindex/tree/main/docs/docs/core), [deployed docs](https://cocoindex.io/docs/core/basics)).

```python
@cocoindex.flow_def(name="DocsToKG")
def docs_to_kg_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="../../docs/docs/core",
                                    included_patterns=["*.md", "*.mdx"]))
```

Here `flow_builder.add_source` creates a [KTable](https://cocoindex.io/docs/core/data_types#KTable).
`filename` is the key of the KTable.

<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" margin="0 0 16px 0" />


### Add data collectors

Add collectors at the root scope:

```python
document_node = data_scope.add_collector()
entity_relationship = data_scope.add_collector()
entity_mention = data_scope.add_collector()
```

-   `document_node` collects documents. E.g., [`core/basics.mdx`](https://cocoindex.io/docs/core/basics) is a document.
-   `entity_relationship` collects relationships. E.g., "CocoIndex supports Incremental Processing" indicates a relationship between `CocoIndex` and `Incremental Processing`.
-   `entity_mention` collects mentions of entities in a document. E.g., [`core/basics.mdx`](https://cocoindex.io/docs/core/basics) mentions `CocoIndex` and `Incremental Processing`.

### Process each document and extract summary
Define a `DocumentSummary` data class to extract the summary of a document.

```python
@dataclasses.dataclass
class DocumentSummary:
    title: str
    summary: str
```

Within the flow, use [`cocoindex.functions.ExtractByLlm`](https://cocoindex.io/docs/ops/functions#extractbyllm) for structured output.

```python
with data_scope["documents"].row() as doc:
    doc["summary"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                output_type=DocumentSummary,
                instruction="Please summarize the content of the document."))

    document_node.collect(
        filename=doc["filename"], title=doc["summary"]["title"],
        summary=doc["summary"]["summary"])
```

`doc["summary"]` adds a new column to the KTable `data_scope["documents"]`.

![Document summary](/img/examples/docs_to_knowledge_graph/summary.png)

### Extract relationships from the document using LLM

Define a data class to represent relationship for the LLM extraction.

```python
@dataclasses.dataclass
class Relationship:
    """
    Describe a relationship between two entities.
    Subject and object should be Core CocoIndex concepts only, should be nouns. For example, `CocoIndex`, `Incremental Processing`, `ETL`,  `Data` etc.
    """
    subject: str
    predicate: str
    object: str
```
The Data class defines a knowledge graph relationship. We recommend putting detailed instructions in the class-level docstring to help the LLM extract relationships correctly.

- `subject`: Represents the entity the statement is about (e.g., 'CocoIndex').
- `predicate`: Describes the type of relationship or property connecting the subject and object (e.g., 'supports').
- `object`: Represents the entity or value that the subject is related to via the predicate (e.g., 'Incremental Processing').

This structure represents facts like "CocoIndex supports Incremental Processing". Its graph representation is:


Next, we will use `cocoindex.functions.ExtractByLlm` to extract the relationships from the document.

```python
doc["relationships"] = doc["content"].transform(
    cocoindex.functions.ExtractByLlm(
        llm_spec=cocoindex.LlmSpec(
            api_type=cocoindex.LlmApiType.OPENAI,
            model="gpt-4o"
        ),
        output_type=list[Relationship],
        instruction=(
            "Please extract relationships from CocoIndex documents. "
            "Focus on concepts and ignore examples and code. "
        )
    )
)
```

`doc["relationships"]` adds a new field `relationships` to each document. `output_type=list[Relationship]` specifies that the output of the transformation is a [LTable](https://cocoindex.io/docs/core/data_types#LTable).

![Extract Relationships](/img/examples/docs_to_knowledge_graph/extract_relationship.png)

### Collect relationships

```python
with doc["relationships"].row() as relationship:
    # relationship between two entities
    entity_relationship.collect(
        id=cocoindex.GeneratedField.UUID,
        subject=relationship["subject"],
        object=relationship["object"],
        predicate=relationship["predicate"],
    )
    # mention of an entity in a document, for subject
    entity_mention.collect(
        id=cocoindex.GeneratedField.UUID, entity=relationship["subject"],
        filename=doc["filename"],
    )
    # mention of an entity in a document, for object
    entity_mention.collect(
        id=cocoindex.GeneratedField.UUID, entity=relationship["object"],
        filename=doc["filename"],
    )
```

- `entity_relationship` collects relationships between subjects and objects.
- `entity_mention` collects mentions of entities (as subjects or objects) in the document separately. For example, `core/basics.mdx` has a sentence `CocoIndex supports Incremental Processing`. We want to collect:
    - `core/basics.mdx` mentions `CocoIndex`.
    - `core/basics.mdx` mentions `Incremental Processing`.


### Build knowledge graph

#### Basic concepts
All nodes for Neo4j need two things:
1. Label: The type of the node. E.g., `Document`, `Entity`.
2. Primary key field: The field that uniquely identifies the node. E.g., `filename` for `Document` nodes.

CocoIndex uses the primary key field to match the nodes and deduplicate them. If you have multiple nodes with the same primary key, CocoIndex keeps only one of them.

![Deduplication](/img/examples/docs_to_knowledge_graph/dedupe.png)

There are two ways to map nodes:
1. When you have a collector just for the node, you can directly export it to Neo4j.
2. When you have a collector for relationships connecting to the node, you can map nodes from selected fields in the relationship collector. You must declare a node label and primary key field.


#### Configure Neo4j connection:

```python
conn_spec = cocoindex.add_auth_entry(
    "Neo4jConnection",
    cocoindex.storages.Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="cocoindex",
))
```

#### Export `Document` nodes to Neo4j

![Document nodes](/img/examples/docs_to_knowledge_graph/export_document.png)

```python
document_node.export(
    "document_node",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Nodes(label="Document")),
    primary_key_fields=["filename"],
)
```

This exports Neo4j nodes with label `Document` from the `document_node` collector.
- It declares Neo4j node label `Document`. It specifies `filename` as the primary key field.
- It carries all the fields from `document_node` collector to Neo4j nodes with label `Document`.



#### Export `RELATIONSHIP` and `Entity` nodes to Neo4j

We don't have explicit collector for `Entity` nodes.
They are part of the `entity_relationship` collector and fields are collected during the relationship extraction.

To export them as Neo4j nodes, we need to first declare `Entity` nodes.

```python
flow_builder.declare(
    cocoindex.storages.Neo4jDeclaration(
        connection=conn_spec,
        nodes_label="Entity",
        primary_key_fields=["value"],
    )
)
```

Next, export the `entity_relationship` to Neo4j.

![Export relationship](/img/examples/docs_to_knowledge_graph/export_relationship.png)

```python
entity_relationship.export(
    "entity_relationship",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="RELATIONSHIP",
            source=cocoindex.storages.NodeFromFields(
                label="Entity",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="subject", target="value"),
                ]
            ),
            target=cocoindex.storages.NodeFromFields(
                label="Entity",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="object", target="value"),
                ]
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

The `cocoindex.storages.Relationships` declares how to map relationships in Neo4j.

In a relationship, there's:
1.  A source node and a target node.
2.  A relationship connecting the source and target.
Note that different relationships may share the same source and target nodes.

`NodeFromFields` takes the fields from the `entity_relationship` collector and creates `Entity` nodes.

#### Export the `entity_mention` to Neo4j.

![Export Entity Mention](/img/examples/docs_to_knowledge_graph/relationship.png)

```python
entity_mention.export(
    "entity_mention",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="MENTION",
            source=cocoindex.storages.NodesFromFields(
                label="Document",
                fields=[cocoindex.storages.TargetFieldMapping("filename")],
            ),
            target=cocoindex.storages.NodesFromFields(
                label="Entity",
                fields=[cocoindex.storages.TargetFieldMapping(
                    source="entity", target="value")],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

Similarly here, we export `entity_mention` to Neo4j Relationships using `cocoindex.storages.Relationships`.
It creates relationships by:
- Creating `Document` nodes and `Entity` nodes from the `entity_mention` collector.
- Connecting `Document` nodes and `Entity` nodes with relationship `MENTION`.


## Query and test your index
1.  Install the dependencies:

    ```sh
    pip install -e .
    ```

2.  Run following commands to setup and update the index.
    ```sh
    cocoindex update --setup main.py
    ```

    You'll see the index updates state in the terminal. For example,

    ```
    documents: 7 added, 0 removed, 0 updated
    ```

## CocoInsight

I used CocoInsight to troubleshoot the index generation and understand the data lineage of the pipeline.  It is in free beta now, you can give it a try.

```sh
cocoindex server -ci main
```

And then open the url `https://cocoindex.io/cocoinsight`.  It just connects to your local CocoIndex server, with zero pipeline data retention.


## Browse the knowledge graph
After the knowledge graph is built, you can explore the knowledge graph you built in Neo4j Browser.

For the dev environment, you can connect to Neo4j browser using credentials:
- username: `Neo4j`
- password: `cocoindex`
which is pre-configured in our docker compose [config.yaml](https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/Neo4j.yaml).

You can open it at [http://localhost:7474](http://localhost:7474), and run the following Cypher query to get all relationships:

```cypher
MATCH p=()-->() RETURN p
```

## Kuzu
Cocoindex natively supports Kuzu - a high performant, embedded open source graph database.

<DocumentationButton url="https://cocoindex.io/docs/ops/targets#kuzu" text="Kuzu" margin="0 0 16px 0" />

The GraphDB interface in CocoIndex is standardized, you just need to **switch the configuration** without any additional code changes. CocoIndex supports exporting to Kuzu through its API server. You can bring up a Kuzu API server locally by running:

``` sh
KUZU_DB_DIR=$HOME/.kuzudb
KUZU_PORT=8123
docker run -d --name kuzu -p ${KUZU_PORT}:8000 -v ${KUZU_DB_DIR}:/database kuzudb/api-server:latest
```

In your CocoIndex flow, you need to add the Kuzu connection spec to your flow.

```python
kuzu_conn_spec = cocoindex.add_auth_entry(
    "KuzuConnection",
    cocoindex.storages.KuzuConnection(
        api_server_url="http://localhost:8123",
    ),
)
```

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/blob/30761f8ab674903d742c8ab2e18d4c588df6d46f/examples/docs_to_knowledge_graph/main.py#L33-L37"  margin="0 0 16px 0" />
