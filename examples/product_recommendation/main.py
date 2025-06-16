"""
This example shows how to extract relationships from Markdown documents and build a knowledge graph.
"""

import dataclasses
import datetime
import cocoindex
from jinja2 import Template

neo4j_conn_spec = cocoindex.add_auth_entry(
    "Neo4jConnection",
    cocoindex.targets.Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="cocoindex",
    ),
)
kuzu_conn_spec = cocoindex.add_auth_entry(
    "KuzuConnection",
    cocoindex.targets.KuzuConnection(
        api_server_url="http://localhost:8123",
    ),
)

# SELECT ONE GRAPH DATABASE TO USE
# This example can use either Neo4j or Kuzu as the graph database.
# Please make sure only one branch is live and others are commented out.

# Use Neo4j
GraphDbSpec = cocoindex.targets.Neo4j
GraphDbConnection = cocoindex.targets.Neo4jConnection
GraphDbDeclaration = cocoindex.targets.Neo4jDeclaration
conn_spec = neo4j_conn_spec

# Use Kuzu
#  GraphDbSpec = cocoindex.targets.Kuzu
#  GraphDbConnection = cocoindex.targets.KuzuConnection
#  GraphDbDeclaration = cocoindex.targets.KuzuDeclaration
#  conn_spec = kuzu_conn_spec


# Template for rendering product information as markdown to provide information to LLMs
PRODUCT_TEMPLATE = """
# {{ title }}

## Highlights
{% for highlight in highlights %}
- {{ highlight }}
{% endfor %}

## Description
{{ description.header | default('') }}
{{ description.paragraph | default('') }}
{% for bullet in description.bullets %}

- {{ bullet }}
{% endfor %}

 """


@dataclasses.dataclass
class ProductInfo:
    id: str
    title: str
    price: float
    detail: str


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


@cocoindex.op.function(behavior_version=2)
def extract_product_info(product: cocoindex.Json, filename: str) -> ProductInfo:
    # Print  markdown for LLM to extract the taxonomy and complimentary taxonomy
    return ProductInfo(
        id=f"{filename.removesuffix('.json')}",
        title=product["title"],
        price=float(product["price"].lstrip("$").replace(",", "")),
        detail=Template(PRODUCT_TEMPLATE).render(**product),
    )


@cocoindex.flow_def(name="StoreProduct")
def store_product_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that extracts triples from files and build knowledge graph.
    """
    data_scope["products"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="products", included_patterns=["*.json"]),
        refresh_interval=datetime.timedelta(seconds=5),
    )

    product_node = data_scope.add_collector()
    product_taxonomy = data_scope.add_collector()
    product_complementary_taxonomy = data_scope.add_collector()

    with data_scope["products"].row() as product:
        data = (
            product["content"]
            .transform(cocoindex.functions.ParseJson(), language="json")
            .transform(extract_product_info, filename=product["filename"])
        )
        taxonomy = data["detail"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4.1"
                ),
                output_type=ProductTaxonomyInfo,
            )
        )

        product_node.collect(id=data["id"], title=data["title"], price=data["price"])
        with taxonomy["taxonomies"].row() as t:
            product_taxonomy.collect(
                id=cocoindex.GeneratedField.UUID,
                product_id=data["id"],
                taxonomy=t["name"],
            )
        with taxonomy["complementary_taxonomies"].row() as t:
            product_complementary_taxonomy.collect(
                id=cocoindex.GeneratedField.UUID,
                product_id=data["id"],
                taxonomy=t["name"],
            )

    product_node.export(
        "product_node",
        GraphDbSpec(
            connection=conn_spec, mapping=cocoindex.targets.Nodes(label="Product")
        ),
        primary_key_fields=["id"],
    )

    flow_builder.declare(
        GraphDbDeclaration(
            connection=conn_spec,
            nodes_label="Taxonomy",
            primary_key_fields=["value"],
        )
    )

    product_taxonomy.export(
        "product_taxonomy",
        GraphDbSpec(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="PRODUCT_TAXONOMY",
                source=cocoindex.targets.NodeFromFields(
                    label="Product",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="product_id", target="id"
                        ),
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Taxonomy",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="taxonomy", target="value"
                        ),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
    product_complementary_taxonomy.export(
        "product_complementary_taxonomy",
        GraphDbSpec(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="PRODUCT_COMPLEMENTARY_TAXONOMY",
                source=cocoindex.targets.NodeFromFields(
                    label="Product",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="product_id", target="id"
                        ),
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Taxonomy",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="taxonomy", target="value"
                        ),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
