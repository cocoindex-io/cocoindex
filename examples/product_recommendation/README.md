# Build Real-Time Recommendation Engine with LLM and Graph Database

We will build a real-time product recommendation engine with LLM and graph database. In particular, we will use LLM to understand the category (taxonomy) of a product. In addition, we will use LLM to enumerate the complementary products - users are likely to buy together with the current product (pencil and notebook).

We will use Graph to explore the relationships between products that can be further used for product recommendations or labeling.

Please drop [CocoIndex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us and stay tuned for more updates. Thank you so much 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite

* [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres)
* Install [Neo4j](https://cocoindex.io/docs/targets/neo4j)
* [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai).

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

![Neo4j Browser Screenshot](img/neo4j.png)

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci main
```

And then open the url <https://cocoindex.io/cocoinsight>.

![CocoInsight Screenshot](img/cocoinsight.png)
