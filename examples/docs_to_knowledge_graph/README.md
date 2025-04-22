# Build Knowledge Graph from Markdown Documents, with OpenAI, Neo4j and CocoIndex

In this example, we

*   Extract relationships from Markdown documents.
*   Build a knowledge graph from the relationships.

Please give [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite

Before running the example, you need to:

*   [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
*   [Install Neo4j](https://cocoindex.io/docs/ops/storages#neo4j) if you don't have one.
*   Install / configure LLM API. In this example we use OpenAI. You need to [configure OpenAI API key](https://cocoindex.io/docs/ai/llm#openai) before running the example. Alternatively, you can also follow the comments in source code to switch to Ollama, which runs LLM model locally, and get it ready following [this guide](https://cocoindex.io/docs/ai/llm#ollama).

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
