"""Tests for the Neo4j target connector.

Run with:
    uv run pytest python/tests/connectors/test_neo4j_target.py -v

Unit tests run without a server. Integration tests require a running Neo4j
(spun up automatically via testcontainers when ``NEO4J_TEST_SERVER=1``).
"""

from __future__ import annotations

import pytest

# =============================================================================
# Cypher builder unit tests — no DB needed, no driver needed
# =============================================================================


from cocoindex.connectors.neo4j._cypher import (
    build_constraint_create,
    build_constraint_drop,
    build_node_delete,
    build_node_index_create,
    build_node_index_drop,
    build_node_upsert,
    build_relationship_delete,
    build_relationship_index_create,
    build_relationship_index_drop,
    build_relationship_upsert,
    build_vector_index_create,
    build_vector_index_drop,
    constraint_name,
    index_name,
    validate_identifier,
    vector_index_name,
)


class TestValidateIdentifier:
    @pytest.mark.parametrize(
        "name", ["users", "_private", "T1", "a_b_c", "X", "Document", "MENTION"]
    )
    def test_valid(self, name: str) -> None:
        validate_identifier(name, "test")

    @pytest.mark.parametrize(
        "name",
        ["my-table", "123abc", "", "has space", "ba`ck", "semi;colon", "a.b", "X-Y"],
    )
    def test_invalid(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid Neo4j"):
            validate_identifier(name, "test")


class TestNameBuilders:
    def test_index_name_node(self) -> None:
        assert index_name("node", "Document", ["filename"]) == (
            "coco_idx_node_Document__filename"
        )

    def test_index_name_relationship(self) -> None:
        assert index_name("rel", "MENTION", ["id"]) == "coco_idx_rel_MENTION__id"

    def test_index_name_compound_pk(self) -> None:
        assert index_name("node", "X", ["a", "b"]) == "coco_idx_node_X__a__b"

    def test_constraint_name(self) -> None:
        assert constraint_name("Document", ["filename"]) == (
            "coco_uniq_Document__filename"
        )

    def test_vector_index_name(self) -> None:
        assert vector_index_name("Document", "embedding") == (
            "coco_vec_Document__embedding"
        )


class TestNodeUpsertCypher:
    def test_single_pk_with_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], True)
            == "MERGE (n:`Document` {`filename`: $key_0}) SET n += $props"
        )

    def test_single_pk_no_props(self) -> None:
        assert (
            build_node_upsert("Document", ["filename"], False)
            == "MERGE (n:`Document` {`filename`: $key_0})"
        )

    def test_compound_pk(self) -> None:
        # Same shape as FalkorDB; Neo4j MERGE accepts compound keys.
        assert build_node_upsert("X", ["a", "b"], True) == (
            "MERGE (n:`X` {`a`: $key_0, `b`: $key_1}) SET n += $props"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_upsert("X", [], True)


class TestNodeDeleteCypher:
    def test_uses_detach_delete(self) -> None:
        # DETACH DELETE protects against DELETE failing on nodes that still
        # have incident edges another flow owns.
        assert (
            build_node_delete("Document", ["filename"])
            == "MATCH (n:`Document` {`filename`: $key_0}) DETACH DELETE n"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_node_delete("X", [])


class TestRelationshipUpsertCypher:
    def test_three_merges_with_props(self) -> None:
        assert build_relationship_upsert(
            "REL", "Entity", ["value"], "Entity", ["value"], ["id"], True
        ) == (
            "MERGE (s:`Entity` {`value`: $from_key_0}) "
            "MERGE (t:`Entity` {`value`: $to_key_0}) "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t) "
            "SET r += $props"
        )

    def test_no_set_on_endpoints(self) -> None:
        # Endpoints' properties are owned by their own table's _RecordHandler.
        cypher = build_relationship_upsert("REL", "A", ["x"], "B", ["y"], ["id"], True)
        assert "SET s" not in cypher
        assert "SET t" not in cypher
        assert "SET r += $props" in cypher

    def test_no_props(self) -> None:
        assert build_relationship_upsert(
            "REL", "A", ["x"], "B", ["y"], ["id"], False
        ) == (
            "MERGE (s:`A` {`x`: $from_key_0}) "
            "MERGE (t:`B` {`y`: $to_key_0}) "
            "MERGE (s)-[r:`REL` {`id`: $rel_key_0}]->(t)"
        )

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", [], "B", ["y"], ["id"], True)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", [], ["id"], True)
        with pytest.raises(ValueError):
            build_relationship_upsert("REL", "A", ["x"], "B", ["y"], [], True)


class TestRelationshipDeleteCypher:
    def test_does_not_cascade(self) -> None:
        cypher = build_relationship_delete("REL", ["id"])
        assert cypher == "MATCH ()-[r:`REL` {`id`: $key_0}]->() DELETE r"
        assert "DELETE s" not in cypher
        assert "DELETE t" not in cypher

    def test_empty_pk_raises(self) -> None:
        with pytest.raises(ValueError):
            build_relationship_delete("REL", [])


class TestIndexDdlCypher:
    def test_node_index_create_named_with_if_not_exists(self) -> None:
        # Neo4j 5 syntax: CREATE INDEX <name> IF NOT EXISTS FOR (n:L) ON (n.f)
        assert build_node_index_create(
            "coco_idx_node_Document__filename", "Document", ["filename"]
        ) == (
            "CREATE INDEX `coco_idx_node_Document__filename` IF NOT EXISTS "
            "FOR (n:`Document`) ON (n.`filename`)"
        )

    def test_node_index_create_compound(self) -> None:
        assert build_node_index_create("idx_x", "X", ["a", "b"]) == (
            "CREATE INDEX `idx_x` IF NOT EXISTS FOR (n:`X`) ON (n.`a`, n.`b`)"
        )

    def test_node_index_drop_uses_name(self) -> None:
        # Unlike FalkorDB's by-(label,field) drop, Neo4j drops by name.
        assert build_node_index_drop("coco_idx_node_Document__filename") == (
            "DROP INDEX `coco_idx_node_Document__filename` IF EXISTS"
        )

    def test_relationship_index_create(self) -> None:
        assert build_relationship_index_create(
            "coco_idx_rel_REL__id", "REL", ["id"]
        ) == (
            "CREATE INDEX `coco_idx_rel_REL__id` IF NOT EXISTS "
            "FOR ()-[r:`REL`]-() ON (r.`id`)"
        )

    def test_relationship_index_drop_uses_name(self) -> None:
        assert build_relationship_index_drop("coco_idx_rel_REL__id") == (
            "DROP INDEX `coco_idx_rel_REL__id` IF EXISTS"
        )


class TestConstraintDdlCypher:
    def test_single_field_creates_unique_constraint(self) -> None:
        assert build_constraint_create(
            "coco_uniq_Document__filename", "Document", ["filename"]
        ) == (
            "CREATE CONSTRAINT `coco_uniq_Document__filename` IF NOT EXISTS "
            "FOR (n:`Document`) REQUIRE n.`filename` IS UNIQUE"
        )

    def test_compound_creates_node_key(self) -> None:
        # Neo4j 5: REQUIRE (n.a, n.b) IS NODE KEY
        assert build_constraint_create("c", "X", ["a", "b"]) == (
            "CREATE CONSTRAINT `c` IF NOT EXISTS "
            "FOR (n:`X`) REQUIRE (n.`a`, n.`b`) IS NODE KEY"
        )

    def test_drop(self) -> None:
        assert build_constraint_drop("coco_uniq_Document__filename") == (
            "DROP CONSTRAINT `coco_uniq_Document__filename` IF EXISTS"
        )

    def test_empty_fields_raises(self) -> None:
        with pytest.raises(ValueError):
            build_constraint_create("c", "X", [])


class TestVectorIndexCypher:
    def test_create(self) -> None:
        assert build_vector_index_create(
            "coco_vec_Doc__embedding", "Doc", "embedding", 384, "cosine"
        ) == (
            "CREATE VECTOR INDEX `coco_vec_Doc__embedding` IF NOT EXISTS "
            "FOR (n:`Doc`) ON n.`embedding` "
            "OPTIONS { indexConfig: { "
            "`vector.dimensions`: 384, "
            "`vector.similarity_function`: 'cosine' } }"
        )

    def test_drop_uses_name(self) -> None:
        # Neo4j vector indexes share the index namespace, drop by name.
        assert build_vector_index_drop("coco_vec_Doc__embedding") == (
            "DROP INDEX `coco_vec_Doc__embedding` IF EXISTS"
        )

    def test_zero_dimension_rejected(self) -> None:
        with pytest.raises(ValueError):
            build_vector_index_create("c", "Doc", "embedding", 0, "cosine")

    def test_negative_dimension_rejected(self) -> None:
        with pytest.raises(ValueError):
            build_vector_index_create("c", "Doc", "embedding", -1, "cosine")
