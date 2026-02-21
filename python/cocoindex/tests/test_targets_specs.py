from cocoindex.auth_registry import AuthEntryReference, ref_auth_entry
from cocoindex.engine_object import dump_engine_object
from cocoindex.targets import (
    Kuzu,
    KuzuConnection,
    KuzuDeclaration,
    Ladybug,
    LadybugConnection,
    LadybugDeclaration,
    Nodes,
)


def test_ladybug_target_dump() -> None:
    conn_ref: AuthEntryReference[LadybugConnection] = ref_auth_entry("ladybug")
    spec = Ladybug(connection=conn_ref, mapping=Nodes(label="Document"))
    dumped = dump_engine_object(spec)

    assert dumped["connection"]["key"] == "ladybug"
    assert dumped["mapping"]["kind"] == "Node"
    assert dumped["mapping"]["label"] == "Document"


def test_ladybug_declaration_dump() -> None:
    conn_ref: AuthEntryReference[LadybugConnection] = ref_auth_entry("ladybug")
    decl = LadybugDeclaration(
        connection=conn_ref,
        nodes_label="Place",
        primary_key_fields=["name"],
    )
    dumped = dump_engine_object(decl)

    assert dumped["kind"] == "Ladybug"
    assert dumped["nodes_label"] == "Place"
    assert dumped["primary_key_fields"] == ["name"]


def test_kuzu_aliases_dump_as_ladybug() -> None:
    conn_ref: AuthEntryReference[KuzuConnection] = ref_auth_entry("ladybug")
    target = Kuzu(connection=conn_ref, mapping=Nodes(label="Document"))
    target_dumped = dump_engine_object(target)
    assert target_dumped["mapping"]["kind"] == "Node"
    assert target_dumped["mapping"]["label"] == "Document"

    decl = KuzuDeclaration(
        connection=conn_ref,
        nodes_label="Place",
        primary_key_fields=["name"],
    )
    decl_dumped = dump_engine_object(decl)
    assert decl_dumped["kind"] == "Ladybug"
    assert decl_dumped["nodes_label"] == "Place"
    assert decl_dumped["primary_key_fields"] == ["name"]
