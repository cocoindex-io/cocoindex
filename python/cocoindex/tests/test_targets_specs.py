from cocoindex.auth_registry import AuthEntryReference, ref_auth_entry
from cocoindex.engine_object import dump_engine_object
from cocoindex.targets import Ladybug, LadybugConnection, LadybugDeclaration, Nodes


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
