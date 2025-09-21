---
title:  Real-Time Product Recommendation Engine with LLM and Graph Database
description: Build a real-time product recommendation engine with LLM and graph database, from the aspect of product category (taxonomy) understanding.
sidebar_class_name: hidden
slug: /examples/product_recommendation
canonicalUrl: '/examples/product_recommendation'
sidebar_custom_props:
  image: /img/examples/product_recommendation/cover.png
  tags: [knowledge-graph ]
image: /img/examples/product_recommendation/cover.png
tags: [knowledge-graph]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/product_recommendation" margin="0 0 24px 0" />

![Product Recommendation](/img/examples/product_recommendation/cover.png)

## Overview

We will build a real-time product recommendation engine with LLM and graph database. In particular, we will:
- Use LLM to understand the category (taxonomy) of a product.
- Use LLM to enumerate the complementary products - users are likely to buy together with the current product (pencil and notebook).
- Use Graph to explore the relationships between products that can be further used for product recommendations or labeling.


Product taxonomy is a way to organize product catalogs in a logical and hierarchical structure; a great detailed explanation can be found [here](https://help.shopify.com/en/manual/products/details/product-category). In practice, it is a complicated problem: a product can be part of multiple categories, and a category can have multiple parents.


## Prerequisites
*   [Install PostgreSQL](https://cocoindex.io/docs/getting_started/installation#-install-postgres). CocoIndex uses PostgreSQL internally for incremental processing.
*   [Install Neo4j](https://cocoindex.io/docs/ops/storages#Neo4j), a graph database.
*   -  [Configure your OpenAI API key](https://cocoindex.io/docs/ai/llm#openai). Create a `.env` file from `.env.example`, and fill `OPENAI_API_KEY`.

Alternatively, we have native support for Gemini, Ollama, LiteLLM. You can choose your favorite LLM provider and work completely on-premises.

<DocumentationButton url="https://cocoindex.io/docs/ai/llm" text="LLM" margin="0 0 16px 0" />


## Documentation
<DocumentationButton url="https://cocoindex.io/docs/ops/targets#property-graph-targets" text="Property Graph Targets" margin="0 0 16px 0" />

## Flow Overview

The core flow is about [~100 lines of python code](https://github.com/cocoindex-io/cocoindex/blob/1d42ab31692c73743425f7712c9af395ef98c80e/examples/product_taxonomy_knowledge_graph/main.py#L75-L177)

We are going to declare a data flow
1.  ingest products (in JSON)
2.  for each product,
    - parse JSON
    - map & clean up data
    - extract taxonomy from the mapped data
3.  collect data
4.  export data to neo4j


## Add source

```python
@cocoindex.flow_def(name="StoreProduct")
def store_product_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["products"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="products",
                                    included_patterns=["*.json"]),
        refresh_interval=datetime.timedelta(seconds=5))
```

Here `flow_builder.add_source` creates a [KTable](https://cocoindex.io/docs/core/data_types#KTable).
`filename` is the key of the KTable.



## Add data collectors

Add collectors at the root scope to collect the product, taxonomy and complementary taxonomy.

```python
product_node = data_scope.add_collector()
product_taxonomy = data_scope.add_collector()
product_complementary_taxonomy = data_scope.add_collector()
```

## Process each product

We will parse the JSON file for each product, and transform the data to the format that we need for downstream processing.

### Data mapping

```python
@cocoindex.op.function(behavior_version=2)
def extract_product_info(product: cocoindex.typing.Json, filename: str) -> ProductInfo:
    return ProductInfo(
        id=f"{filename.removesuffix('.json')}",
        url=product["source"],
        title=product["title"],
        price=float(product["price"].lstrip("$").replace(",", "")),
        detail=Template(PRODUCT_TEMPLATE).render(**product),
    )
```

Here we define a function for data mapping, e.g.,
- clean up the `id` field
- map `title` -> `title`
- clean up the `price` field
- generate a markdown string for the product detail based on all the fields (for LLM to extract taxonomy and complementary taxonomy, we find that markdown works best as context for LLM).

### Process product JSON in the flow

Within the flow, we plug in the data mapping transformation to process each product JSON.

```python
with data_scope["products"].row() as product:
    data = (product["content"]
            .transform(cocoindex.functions.ParseJson(), language="json")
            .transform(extract_product_info, filename=product["filename"]))
    product_node.collect(id=data["id"], url=data["url"], title=data["title"], price=data["price"])
```

It performs the following transformations:

1. The first `transform()` parses the JSON file.

    <DocumentationButton url="https://cocoindex.io/docs/ops/functions#parsejson" text="ParseJson" margin="0 0 16px 0" />
    ![ParseJson](/img/examples/product_recommendation/parse_json.png)

2.  The second `transform()` performs the defined data mapping.
    ![Extract product info and data mapping](/img/examples/product_recommendation/extract_product.png)

3. We collect the fields we need for the product node in Neo4j.



## Extract taxonomy and complementary taxonomy

![Product Taxonomy Info](/img/examples/product_recommendation/taxonomy.png)

### Product Taxonomy Definition

Since we are using LLM to extract product taxonomy, we need to provide a detailed instruction at the class-level docstring.

```python
@dataclasses.dataclass
class ProductTaxonomy:
    """
    Taxonomy for the product.

    A taxonomy is a concise noun (or short noun phrase), based on its core functionality, without specific details such as branding, style, etc.

    Always use the most common words in US English.

    Use lowercase without punctuation, unless it's a proper noun or acronym.

    A product may have multiple taxonomies. Avoid large categories like "office supplies" or "electronics". Use specific ones, like "pen" or "printer".
    """
    name: str
```

### Define Product Taxonomy Info

Basically we want to extract all possible taxonomies for a product, and think about what other products are likely to be bought together with the current product.

```python
@dataclasses.dataclass
class ProductTaxonomyInfo:
    """
    Taxonomy information for the product.

    Fields:
    - taxonomies: Taxonomies for the current product.
    - complementary_taxonomies: Think about when customers buy this product, what else they might need as complementary products. Put labels for these complentary products.
    """
    taxonomies: list[ProductTaxonomy]
    complementary_taxonomies: list[ProductTaxonomy]
```


For each product, we want some insight about its taxonomy and complementary taxonomy and we could use that as bridge to find related product using knowledge graph.



### LLM Extraction

Finally, we will use `cocoindex.functions.ExtractByLlm` to extract the taxonomy and complementary taxonomy from the product detail.

```python
taxonomy = data["detail"].transform(cocoindex.functions.ExtractByLlm(
            llm_spec=cocoindex.LlmSpec(
                api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4.1"),
                output_type=ProductTaxonomyInfo))
```

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#extractbyllm" text="ExtractByLlm" margin="0 0 16px 0" />


For example, LLM takes the description of the *gel pen*, and extracts taxonomy to be *gel pen*.
Meanwhile, it suggests that when people buy *gel pen*, they may also be interested in *notebook* etc as complimentary taxonomy.

![Extract taxonomy and complementary taxonomy](/img/examples/product_recommendation/extract_taxonomy.png)

### Collect taxonomy and complementary taxonomy

And then we will collect the taxonomy and complementary taxonomy to the collector.
```python
with taxonomy['taxonomies'].row() as t:
    product_taxonomy.collect(id=cocoindex.GeneratedField.UUID, product_id=data["id"], taxonomy=t["name"])
with taxonomy['complementary_taxonomies'].row() as t:
    product_complementary_taxonomy.collect(id=cocoindex.GeneratedField.UUID, product_id=data["id"], taxonomy=t["name"])
```


## Build knowledge graph

### Basic concepts
All nodes for Neo4j need two things:
1. Label: The type of the node. E.g., `Product`, `Taxonomy`.
2. Primary key field: The field that uniquely identifies the node. E.g., `id` for `Product` nodes.

CocoIndex uses the primary key field to match the nodes and deduplicate them. If you have multiple nodes with the same primary key, CocoIndex keeps only one of them.

![Deduplication](/img/examples/product_recommendation/dedupe.png)

There are two ways to map nodes:
1. When you have a collector just for the node, you can directly export it to Neo4j. For example `Product`. We've collected each product explicitly.
2. When you have a collector for relationships connecting to the node, you can map nodes from selected fields in the relationship collector. You must declare a node label and primary key field.

For example,

```python
product_taxonomy.collect(id=cocoindex.GeneratedField.UUID, product_id=data["id"], taxonomy=t["name"])
```

Collects a relationship, and taxonomy node is created from the relationship.


### Configure Neo4j connection

```python
conn_spec = cocoindex.add_auth_entry(
    "Neo4jConnection",
    cocoindex.storages.Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="cocoindex",
))
```

### Export `Product` nodes to Neo4j

```python
product_node.export(
    "product_node",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Nodes(label="Product")
    ),
    primary_key_fields=["id"],
)
```
![Export Product](/img/examples/product_recommendation/export_product.png)


This exports Neo4j nodes with label `Product` from the `product_node` collector.
- It declares Neo4j node label `Product`. It specifies `id` as the primary key field.
- It carries all the fields from `product_node` collector to Neo4j nodes with label `Product`.


### Export `Taxonomy` nodes to Neo4j

We don't have explicit collector for `Taxonomy` nodes.
They are part of the `product_taxonomy` and `product_complementary_taxonomy` collectors and fields are collected during the taxonomy extraction.

To export them as Neo4j nodes, we need to first declare `Taxonomy` nodes.

```python
flow_builder.declare(
    cocoindex.storages.Neo4jDeclaration(
        connection=conn_spec,
        nodes_label="Taxonomy",
        primary_key_fields=["value"],
    )
)
```


Next, export the `product_taxonomy` as relationship to Neo4j.

```python
product_taxonomy.export(
    "product_taxonomy",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="PRODUCT_TAXONOMY",
            source=cocoindex.storages.NodeFromFields(
                label="Product",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="product_id", target="id"),
                ]
            ),
            target=cocoindex.storages.NodeFromFields(
                label="Taxonomy",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="taxonomy", target="value"),
                ]
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

![Export Taxonomy](/img/examples/product_recommendation/export_taxonomy.png)



Similarly, we can export the `product_complementary_taxonomy` as relationship to Neo4j.
```python
product_complementary_taxonomy.export(
    "product_complementary_taxonomy",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="PRODUCT_COMPLEMENTARY_TAXONOMY",
            source=cocoindex.storages.NodeFromFields(
                label="Product",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="product_id", target="id"),
                ]
            ),
            target=cocoindex.storages.NodeFromFields(
                label="Taxonomy",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="taxonomy", target="value"),
                ]
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```
![Export Complementary Taxonomy](/img/examples/product_recommendation/export_all.png)

The `cocoindex.storages.Relationships` declares how to map relationships in Neo4j.

In a relationship, there's:
1.  A source node and a target node.
2.  A relationship connecting the source and target.
Note that different relationships may share the same source and target nodes.

`NodeFromFields` takes the fields from the `entity_relationship` collector and creates `Taxonomy` nodes.


## Run the flow
1.  Install the dependencies:

    ```
    pip install -e .
    ```

2.  Run the following command to setup and update the index.
    ```sh
    cocoindex update --setup main.py
    ```

    You'll see the index updates state in the terminal. For example, you'll see the following output:

    ```
    documents: 9 added, 0 removed, 0 updated
    ```

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

![Neo4j Browser](/img/examples/product_recommendation/neo4j.png)

## CocoInsight
I used CocoInsight to troubleshoot the index generation and understand the data lineage of the pipeline. It is in free beta now, you can give it a try. Run following command to start CocoInsight:

```
cocoindex server -ci main
```

And then open the url `https://cocoindex.io/cocoinsight`.  It just connects to your local CocoIndex server, with Zero pipeline data retention.
