"""
This example shows how to extract relationships from Markdown documents and build a knowledge graph.
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
    """Describe a relationship between two nodes."""
    subject: str
    predicate: str
    object: str

@cocoindex.flow_def(name="DocsToKG")
def docs_to_kg_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that extracts triples from files and build knowledge graph.
    """

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
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown", chunk_size=10000)

        doc["summary"] = doc["content"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                output_type=DocumentSummary,
                instruction="Please summarize the content of the document."))
        document_node.collect(
            filename=doc["filename"], title=doc["summary"]["title"],
            summary=doc["summary"]["summary"])

        with doc["chunks"].row() as chunk:
            chunk["relationships"] = chunk["text"].transform(
                cocoindex.functions.ExtractByLlm(
                    llm_spec=cocoindex.LlmSpec(
                        api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                    # Replace by this spec below, to use Ollama API instead of OpenAI
                    #   llm_spec=cocoindex.LlmSpec(
                    #       api_type=cocoindex.LlmApiType.OLLAMA, model="llama3.2"),
                    output_type=list[Relationship],
                    instruction=(
                        "Please extract relationships from CocoIndex documents. "
                        "Focus on concepts and ingnore specific examples. "
                        "Each relationship should be a tuple of (subject, predicate, object).")))

            with chunk["relationships"].row() as relationship:
                relationship["subject_embedding"] = relationship["subject"].transform(
                    cocoindex.functions.SentenceTransformerEmbed(
                        model="sentence-transformers/all-MiniLM-L6-v2"))
                relationship["object_embedding"] = relationship["object"].transform(
                    cocoindex.functions.SentenceTransformerEmbed(
                        model="sentence-transformers/all-MiniLM-L6-v2"))
                entity_relationship.collect(
                    id=cocoindex.GeneratedField.UUID,
                    subject=relationship["subject"],
                    subject_embedding=relationship["subject_embedding"],
                    object=relationship["object"],
                    object_embedding=relationship["object_embedding"],
                    predicate=relationship["predicate"],
                )
                entity_mention.collect(
                    id=cocoindex.GeneratedField.UUID, entity=relationship["subject"],
                    filename=doc["filename"], location=chunk["location"],
                )
                entity_mention.collect(
                    id=cocoindex.GeneratedField.UUID, entity=relationship["object"],
                    filename=doc["filename"], location=chunk["location"],
                )
    document_node.export(
        "document_node",
        cocoindex.storages.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.storages.NodeMapping(label="Document")),
        primary_key_fields=["filename"],
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
                        cocoindex.storages.TargetFieldMapping(
                            source="subject_embedding", target="embedding"),
                    ]
                ),
                target=cocoindex.storages.NodeReferenceMapping(
                    label="Entity",
                    fields=[
                        cocoindex.storages.TargetFieldMapping(
                            source="object", target="value"),
                        cocoindex.storages.TargetFieldMapping(
                            source="object_embedding", target="embedding"),
                    ]
                ),
                nodes_storage_spec={
                    "Entity": cocoindex.storages.NodeStorageSpec(
                        primary_key_fields=["value"],
                        vector_indexes=[
                            cocoindex.VectorIndexDef(
                                field_name="embedding",
                                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
                            ),
                        ],
                    ),
                },
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
