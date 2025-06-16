"""
This example shows how to extract relationships from documents and build a knowledge graph.
"""

import dataclasses
import cocoindex

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


@dataclasses.dataclass
class DocumentSummary:
    """Describe a summary of a document."""

    title: str
    summary: str


@dataclasses.dataclass
class Relationship:
    """
    Describe a relationship between two entities.
    Subject and object should be Core CocoIndex concepts only, should be nouns. For example, `CocoIndex`, `Incremental Processing`, `ETL`,  `Data` etc.
    """

    subject: str
    predicate: str
    object: str


@cocoindex.flow_def(name="DocsToKG")
def docs_to_kg_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that extracts relationship from files and build knowledge graph.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="../../docs/docs/core", included_patterns=["*.md", "*.mdx"]
        )
    )

    document_node = data_scope.add_collector()
    entity_relationship = data_scope.add_collector()
    entity_mention = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        # extract summary from document
        doc["summary"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    # Supported LLM: https://cocoindex.io/docs/ai/llm
                    api_type=cocoindex.LlmApiType.OPENAI,
                    model="gpt-4o",
                ),
                output_type=DocumentSummary,
                instruction="Please summarize the content of the document.",
            )
        )
        document_node.collect(
            filename=doc["filename"],
            title=doc["summary"]["title"],
            summary=doc["summary"]["summary"],
        )

        # extract relationships from document
        doc["relationships"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    # Supported LLM: https://cocoindex.io/docs/ai/llm
                    api_type=cocoindex.LlmApiType.OPENAI,
                    model="gpt-4o",
                ),
                output_type=list[Relationship],
                instruction=(
                    "Please extract relationships from CocoIndex documents. "
                    "Focus on concepts and ignore examples and code. "
                ),
            )
        )

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
                id=cocoindex.GeneratedField.UUID,
                entity=relationship["subject"],
                filename=doc["filename"],
            )
            # mention of an entity in a document, for object
            entity_mention.collect(
                id=cocoindex.GeneratedField.UUID,
                entity=relationship["object"],
                filename=doc["filename"],
            )

    # export to neo4j
    document_node.export(
        "document_node",
        GraphDbSpec(
            connection=conn_spec, mapping=cocoindex.targets.Nodes(label="Document")
        ),
        primary_key_fields=["filename"],
    )
    # Declare reference Node to reference entity node in a relationship
    flow_builder.declare(
        GraphDbDeclaration(
            connection=conn_spec,
            nodes_label="Entity",
            primary_key_fields=["value"],
        )
    )
    entity_relationship.export(
        "entity_relationship",
        GraphDbSpec(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="RELATIONSHIP",
                source=cocoindex.targets.NodeFromFields(
                    label="Entity",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="subject", target="value"
                        ),
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Entity",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="object", target="value"
                        ),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
    entity_mention.export(
        "entity_mention",
        GraphDbSpec(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="MENTION",
                source=cocoindex.targets.NodeFromFields(
                    label="Document",
                    fields=[cocoindex.targets.TargetFieldMapping("filename")],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Entity",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="entity", target="value"
                        )
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
