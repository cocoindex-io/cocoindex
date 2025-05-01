# Build Real-Time Knowledge Graph For Documents with LLM and Neo4j

We will process a list of documents and use LLM to extract relationships between the concepts in each document. 
We will generate two kinds of relationships:

1. Relationships between subjects and objects. E.g., "CocoIndex supports Incremental Processing"
2. Mentions of entities in a document. E.g., "core/basics.mdx" mentions `CocoIndex` and `Incremental Processing`.

You can find a step by step blog for this project [here](https://cocoindex.io/blogs/knowledge-graph-for-docs)

Please drop [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite
*   [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
*   [Install Neo4j](https://cocoindex.io/docs/ops/storages#neo4j) if you don't have one.
*   [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai).

## Documentation
You can read the official CocoIndex Documentation for Property Graph Targets [here](https://cocoindex.io/docs/ops/storages#property-graph-targets).

## Run

### Build the index

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
python main.py cocoindex setup
```

Update index:

```bash
python main.py cocoindex update
```

### Browse the knowledge graph

After the knowledge graph is build, you can explore the knowledge graph you built in Neo4j Browser.

For the dev enviroment, you can connect neo4j browser using credentials:
- username: `neo4j`
- password: `cocoindex`
which is pre-configured in the our docker compose [config.yaml](https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/neo4j.yaml).

You can open it at [http://localhost:7474](http://localhost:7474), and run the following Cypher query to get all relationships:

```cypher
MATCH p=()-->() RETURN p
```

## CocoInsight 
CocoInsight is a tool to help you understand your data pipeline and data index. CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
python main.py cocoindex server -c https://cocoindex.io
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight). It connects to your local CocoIndex server with zero data retention.

You can view the pipeline flow and the data preview in the CocoInsight UI:
![CocoInsight UI](https://cocoindex.io/blogs/assets/images/cocoinsight-edd71690dcc35b6c5cf1cb31b51b6f6f.png)
