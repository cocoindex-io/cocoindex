"""
This example shows how to extract relationships from Markdown documents and build a knowledge graph.
"""
import dataclasses
from dotenv import load_dotenv
import cocoindex


@dataclasses.dataclass
class Relationship:
    """Describe a relationship between two nodes."""
    subject: str
    predicate: str
    object: str

@dataclasses.dataclass
class Relationships:
    """Describe a relationship between two nodes."""
    relationships: list[Relationship]

@cocoindex.flow_def(name="DocsToKG")
def docs_to_kg_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that extracts triples from files and build knowledge graph.
    """

    conn_spec = cocoindex.add_auth_entry(
        "Neo4jConnection",
        cocoindex.storages.Neo4jConnectionSpec(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="cocoindex",
    ))

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="../../docs/docs/core",
                                    included_patterns=["*.md", "*.mdx"]))

    relationships = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown", chunk_size=10000)

        with doc["chunks"].row() as chunk:
            chunk["relationships"] = chunk["text"].transform(
                cocoindex.functions.ExtractByLlm(
                    llm_spec=cocoindex.LlmSpec(
                        api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                    output_type=Relationships,
                    instruction=(
                        "Please extract relationships from CocoIndex documents. "
                        "Focus on concepts and ingnore specific examples. "
                        "Each relationship should be a tuple of (subject, predicate, object).")))

            with chunk["relationships"]["relationships"].row() as relationship:
                relationships.collect(
                    id=cocoindex.GeneratedField.UUID,
                    subject=relationship["subject"],
                    predicate=relationship["predicate"],
                    object=relationship["object"],
                )

    relationships.export(
        "relationships",
        cocoindex.storages.Neo4jRelationship(
            connection=conn_spec,
            rel_type="RELATIONSHIP",
            source=cocoindex.storages.Neo4jRelationshipEndSpec(
                label="Entity",
                fields=[cocoindex.storages.Neo4jFieldMapping(field_name="subject", node_field_name="value")]
            ),
            target=cocoindex.storages.Neo4jRelationshipEndSpec(
                label="Entity",
                fields=[cocoindex.storages.Neo4jFieldMapping(field_name="object", node_field_name="value")]
            ),
            nodes={
                "Entity": cocoindex.storages.Neo4jRelationshipNodeSpec(key_field_name="value"),
            },
        ),
        primary_key_fields=["id"],
    )

@cocoindex.main_fn()
def _run():
    pass

if __name__ == "__main__":
    load_dotenv(override=True)
    _run()
