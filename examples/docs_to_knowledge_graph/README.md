# Build Real-Time Knowledge Graph For Documents with LLM

We will process a list of documents and use LLM to extract relationships between the concepts in each document.
We will generate two kinds of relationships:

1. Relationships between subjects and objects. E.g., "CocoIndex supports Incremental Processing"
2. Mentions of entities in a document. E.g., "core/basics.mdx" mentions `CocoIndex` and `Incremental Processing`.

You can find a step by step blog for this project [here](https://cocoindex.io/blogs/knowledge-graph-for-docs)

Please drop [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

![example-explanation](https://github.com/user-attachments/assets/07ddbd60-106f-427f-b7cc-16b73b142d27)

## Prerequisite

* [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
* Install [Neo4j](https://cocoindex.io/docs/targets/neo4j).
* Install / configure LLM API. In this example we use Ollama, which runs LLM model locally. You need to get it ready following [this guide](https://cocoindex.io/docs/ai/llm#ollama). Alternatively, you can also follow the comments in source code to switch to OpenAI, and [configure OpenAI API key](https://cocoindex.io/docs/ai/llm#openai) before running the example.

## Documentation

You can read the official CocoIndex Documentation for Property Graph Targets [here](https://cocoindex.io/docs/targets#property-graph-targets).

## Run

### Build the index

Install dependencies:

```bash
pip install -e .
```

Update index:

```bash
cocoindex update main
```

### Browse the knowledge graph

After the knowledge graph is built, you can explore the knowledge graph.

* If you're using Neo4j, you can open the explorer at [http://localhost:7474](http://localhost:7474), with username `neo4j` and password `cocoindex`.
You can run the following Cypher query to get all relationships:

```cypher
MATCH p=()-->() RETURN p
```

<img width="1366" alt="neo4j-for-coco-docs" src="https://github.com/user-attachments/assets/3c8b6329-6fee-4533-9480-571399b57e57" />

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci main
```

And then open the url <https://cocoindex.io/cocoinsight>.

<img width="1430" alt="cocoinsight" src="https://github.com/user-attachments/assets/d5ada581-cceb-42bf-a949-132df674f3dd" />
