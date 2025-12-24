import cocoindex


def test_surrealdb_specs_importable() -> None:
    # Smoke test: specs are present and instantiable.
    _conn = cocoindex.targets.SurrealDBConnection(
        url="ws://localhost:8000",
        namespace="test_ns",
        database="test_db",
        username="root",
        password="root",
    )

    # AuthEntryReference is a runtime wrapper; for spec construction, we can reference an auth key.
    conn_ref = cocoindex.auth_registry.ref_auth_entry("surreal")

    target = cocoindex.targets.SurrealDB(
        connection=conn_ref,
        mapping=cocoindex.targets.Nodes(label="Document"),
    )
    assert target.mapping.label == "Document"

    decl = cocoindex.targets.SurrealDBDeclaration(
        connection=conn_ref,
        nodes_label="Place",
        primary_key_fields=["name"],
        vector_indexes=[cocoindex.VectorIndexDef("embedding")],
    )
    assert decl.nodes_label == "Place"
