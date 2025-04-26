"""
This example shows how to extract relationships from documents and build a knowledge graph.
"""
import dataclasses
from dotenv import load_dotenv
import cocoindex

@dataclasses.dataclass
class DocumentSummary:
    """Describe a summary of a document."""
    title: str
    summary: str

@dataclasses.dataclass
class Relationship:
    """Describe a relationship between two entities."""
    subject: str
    predicate: str
    object: str

@cocoindex.flow_def(name="DocsToKG")
def docs_to_kg_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that extracts relationship from files and build knowledge graph.
    """
    # configure neo4j connection
    conn_spec = cocoindex.add_auth_entry(
        "Neo4jConnection",
        cocoindex.storages.Neo4jConnection(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="cocoindex",
    ))

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="../../docs/docs/core",
                                    included_patterns=["*.md", "*.mdx"]))

    document_node = data_scope.add_collector()
    entity_relationship = data_scope.add_collector()
    entity_mention = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        # extract summary from document
        doc["summary"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    # Supported LLM: https://cocoindex.io/docs/ai/llm
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                output_type=DocumentSummary,
                instruction="Please summarize the content of the document."))
        document_node.collect(
            filename=doc["filename"], title=doc["summary"]["title"],
            summary=doc["summary"]["summary"])

        # extract relationships from document
        doc["relationships"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    # Supported LLM: https://cocoindex.io/docs/ai/llm
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                    output_type=list[Relationship],
                    instruction=(
                        "Please extract relationships from CocoIndex documents. "
                        "Focus on concepts and ingnore specific examples. "
                        "Each relationship should be a tuple of (subject, predicate, object).")))

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


    # export to neo4j
    document_node.export(
        "document_node",
        cocoindex.storages.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.storages.NodeMapping(label="Document")),
        primary_key_fields=["filename"],
    )
    # Declare reference Node to reference entity node in a relationship
    flow_builder.declare(
        cocoindex.storages.Neo4jDeclarations(
            connection=conn_spec,
            referenced_nodes=[
                cocoindex.storages.ReferencedNode(
                    label="Entity",
                    primary_key_fields=["value"],
                )
            ]
        )
    )
    entity_relationship.export(
        "entity_relationship",
        cocoindex.storages.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.storages.RelationshipMapping(
                rel_type="RELATIONSHIP",
                source=cocoindex.storages.NodeReferenceMapping(
                    label="Entity",
                    fields=[
                        cocoindex.storages.TargetFieldMapping(
                            source="subject", target="value"),
                    ]
                ),
                target=cocoindex.storages.NodeReferenceMapping(
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
    entity_mention.export(
        "entity_mention",
        cocoindex.storages.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.storages.RelationshipMapping(
                rel_type="MENTION",
                source=cocoindex.storages.NodeReferenceMapping(
                    label="Document",
                    fields=[cocoindex.storages.TargetFieldMapping("filename")],
                ),
                target=cocoindex.storages.NodeReferenceMapping(
                    label="Entity",
                    fields=[cocoindex.storages.TargetFieldMapping(
                        source="entity", target="value")],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )

@cocoindex.main_fn()
def _run():
    pass

if __name__ == "__main__":
    load_dotenv(override=True)
    _run()
