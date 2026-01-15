# Build Real-Time Knowledge Graph For Documents with LLM using FalkorDB

This example is similar to [docs_to_knowledge_graph](../docs_to_knowledge_graph) but uses **FalkorDB** as the graph database instead of Neo4j.

We will process a list of documents and use LLM to extract relationships between the concepts in each document.
We will generate two kinds of relationships:

1. Relationships between subjects and objects. E.g., "CocoIndex supports Incremental Processing"
2. Mentions of entities in a document. E.g., "core/basics.mdx" mentions `CocoIndex` and `Incremental Processing`.

Please drop [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite

* [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
* Install [FalkorDB](#install-falkordb).
* Install / configure LLM API. In this example we use OpenAI. You need to [configure OpenAI API key](https://cocoindex.io/docs/ai/llm#openai) before running the example.

### Install FalkorDB

FalkorDB is a high-performance graph database that uses Redis as its storage backend. You can run it using Docker:

```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```

This will start FalkorDB on the default port 6379.

## Documentation

You can read the official CocoIndex Documentation for Property Graph Targets [here](https://cocoindex.io/docs/targets#property-graph-targets).

## Run

### Build the index

Install dependencies:

```sh
pip install -e .
```

Update index:

```sh
cocoindex update main
```

### Browse the knowledge graph

After the knowledge graph is built, you can explore it using the FalkorDB CLI or any Redis client that supports the FalkorDB module.

You can connect using `redis-cli`:

```sh
redis-cli -p 6379
```

Then run Cypher queries using FalkorDB's GRAPH.QUERY command:

```
GRAPH.QUERY knowledge_graph "MATCH p=()-->() RETURN p LIMIT 25"
```

Or get all nodes:

```
GRAPH.QUERY knowledge_graph "MATCH (n) RETURN n LIMIT 25"
```

### Using FalkorDB Browser (Optional)

You can also use the FalkorDB Browser for a visual interface. Run:

```sh
docker run -p 3000:3000 -e FALKORDB_HOST=host.docker.internal -e FALKORDB_PORT=6379 falkordb/falkordb-browser:latest
```

Then open [http://localhost:3000](http://localhost:3000) in your browser.

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

And then open the url <https://cocoindex.io/cocoinsight>.
