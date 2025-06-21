# Build Real-Time Knowledge Graph For Documents with LLM

We will process a list of documents and use LLM to extract relationships between the concepts in each document.
We will generate two kinds of relationships:

1. Relationships between subjects and objects. E.g., "CocoIndex supports Incremental Processing"
2. Mentions of entities in a document. E.g., "core/basics.mdx" mentions `CocoIndex` and `Incremental Processing`.

You can find a step by step blog for this project [here](https://cocoindex.io/blogs/knowledge-graph-for-docs)

Please drop [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

![example-explanation](https://github.com/user-attachments/assets/07ddbd60-106f-427f-b7cc-16b73b142d27)

## Prerequisite
*   [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
*   Install [Neo4j](https://cocoindex.io/docs/ops/targets#neo4j-dev-instance) or [Kuzu](https://cocoindex.io/docs/ops/targets#kuzu-dev-instance) if you don't have one.
    *   The example uses Neo4j by default for now. If you want to use Kuzu, find out the "SELECT ONE GRAPH DATABASE TO USE" section and switch the active branch.
*   [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai).

## Documentation
You can read the official CocoIndex Documentation for Property Graph Targets [here](https://cocoindex.io/docs/ops/targets#property-graph-targets).

## Run

### Build the index

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
cocoindex setup main.py
```

Update index:

```bash
cocoindex update main.py
```

### Browse the knowledge graph

After the knowledge graph is built, you can explore the knowledge graph.

* If you're using Neo4j, you can open the explorer at [http://localhost:7474](http://localhost:7474), with username `neo4j` and password `cocoindex`.
* If you're using Kuzu, you can start a Kuzu explorer locally. See [Kuzu dev instance](https://cocoindex.io/docs/ops/targets#kuzu-dev-instance) for more details.

You can run the following Cypher query to get all relationships:

```cypher
MATCH p=()-->() RETURN p
```

<img width="1366" alt="neo4j-for-coco-docs" src="https://github.com/user-attachments/assets/3c8b6329-6fee-4533-9480-571399b57e57" />

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci main.py
```

And then open the url https://cocoindex.io/cocoinsight.

<img width="1430" alt="cocoinsight" src="https://github.com/user-attachments/assets/d5ada581-cceb-42bf-a949-132df674f3dd" />
