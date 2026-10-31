"""Tests for the Omnigraph target connector.

Run with:
    uv run pytest python/tests/connectors/test_omnigraph_target.py -v

Builder unit tests run without a server. Live tests boot `omnigraph-server`
0.14.0 from test/bin/v0.14.0 through `omnigraph_server_fixture` and are gated
on OMNIGRAPH_TEST_SERVER=1; they need the Claude Code sandbox disabled.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import subprocess
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Annotated, Any, cast

import cocoindex as coco
import pytest

pytest.importorskip("omnigraph", reason="the omnigraph SDK is not installed")
import omnigraph as _og  # noqa: E402
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex.connectorkits import statediff
from cocoindex.connectorkits.target import ManagedBy
from cocoindex.connectors import omnigraph
from cocoindex.connectors.omnigraph import _gq as ogq
from cocoindex.connectors.omnigraph import _target as ogt
from cocoindex.connectors.omnigraph._client import ConnectionFactory, _GraphClient
from cocoindex.connectors.omnigraph._cluster import ClusterConfig
from cocoindex.connectors.omnigraph._gq import (
    COCO_KEY,
    Bind,
    PropertyValue,
    Query,
    Statement,
    build_edge_delete,
    build_edge_insert,
    build_endpoint_stub,
    build_node_delete,
    build_node_upsert,
    merge_type_into_schema,
    ownership_marker,
    ownership_property,
    release_ownership,
    remove_type_from_schema,
    render_edge_type,
    render_node_type,
    render_property,
    render_query,
    validate_identifier,
    validate_pg_type,
)
from cocoindex.connectors.omnigraph._target import (
    EdgeSchema,
    NodeSchema,
    OmnigraphType,
    PropertyDef,
    _EdgeHandler,
    _EdgeTypeHandler,
    _EdgeValue,
    _NodeHandler,
    _NodeTypeHandler,
    _NodeValue,
    _type_tracking_record_from_spec,
    _TypeKey,
    _TypeSpec,
    derive_coco_key,
    plan_commits,
)

from tests import common

from .omnigraph_server_fixture import (
    DATA,
    SCHEMA,
    LossyProxy,
    OmnigraphServer,
    live_server,
)

coco_env = common.create_test_env(__file__)


@pytest.fixture(autouse=True)
def _omnigraph_home(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """The connector runs the omnigraph CLI with the caller's environment,
    and the CLI keeps authentication state in `OMNIGRAPH_HOME`; point it at
    a fresh directory so tests never write into the developer's own."""
    home = tmp_path_factory.mktemp("omnigraph-home")
    monkeypatch.setenv("OMNIGRAPH_HOME", str(home))
    return home


#: A cluster configuration nothing ever deploys through: the unit tests below
#: mock the transport, but a system-managed type needs one to be declared.
_FAKE_CLUSTER = ClusterConfig(
    config_dir="/nonexistent/cluster", target="srv", schema_file="g.pg"
)


def _fake_conn(branch: str = "main") -> ConnectionFactory:
    """A factory for unit tests: no server behind it."""
    return ConnectionFactory(
        "http://og.invalid", graph_id="g", branch=branch, cluster=_FAKE_CLUSTER
    )


def _engine_error(message: str) -> _og.BadRequestError:
    """The SDK error a refused request raises, carrying the engine's text."""
    return _og.BadRequestError(message, status=400, method="POST", url="test")


@dataclass
class _Doc:
    slug: str
    title: str
    words: int
    ratio: float
    published: bool
    on: datetime.date
    at: datetime.datetime
    note: str | None


class TestValidateIdentifier:
    @pytest.mark.parametrize("name", ["Person", "_private", "T1", "a_b_c", "WorksAt"])
    def test_valid(self, name: str) -> None:
        validate_identifier(name, "node type")

    @pytest.mark.parametrize(
        "name",
        [
            "my-type",
            "123abc",
            "",
            "has space",
            "semi;colon",
            "a.b",
            "back`tick",
            "foo\n",
            "foo\r",
            "foo\t",
            "foo\x00",
            "foo\nbar",
        ],
    )
    def test_invalid(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid Omnigraph node type"):
            validate_identifier(name, "node type")


class TestRenderProperty:
    def test_plain(self) -> None:
        assert render_property("title", "String", is_key=False) == "title: String"

    def test_key(self) -> None:
        assert render_property("slug", "String", is_key=True) == "slug: String @key"

    def test_nullable_is_passed_through(self) -> None:
        assert render_property("age", "I32?", is_key=False) == "age: I32?"

    def test_pg_type_cannot_inject_another_schema_block(self) -> None:
        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            render_property(
                "title",
                "String\n}\nnode Surprise {\n  slug: String @key",
                is_key=False,
            )


class TestRenderNodeType:
    def test_injects_coco_key_and_the_ownership_marker(self) -> None:
        """Every block this connector renders carries `coco_key` and a
        nullable, never-written `coco_managed_by_<app>`. The latter is what
        lets the schema sink tell this app's types from a user's and from
        another app's — a user-managed type must declare `coco_key` too, so
        that property alone cannot — and it is a property rather than a
        comment because the engine only stores an applied source when
        something structural changed, so a comment could never be released
        on handoff."""
        assert render_node_type(
            "Source",
            [("slug", "String"), ("title", "String")],
            key=("slug",),
            owner="og",
        ) == (
            "node Source {\n  slug: String @key\n  title: String\n"
            "  coco_key: String\n  coco_managed_by_og: Bool?\n}"
        )

    def test_zero_properties_still_gets_coco_key(self) -> None:
        assert render_node_type("Empty", [], key=(), owner="og") == (
            "node Empty {\n  coco_key: String\n  coco_managed_by_og: Bool?\n}"
        )

    def test_composite_key_raises(self) -> None:
        """Omnigraph allows exactly one `@key` per node type: a two-`@key`
        block is rejected outright at `init` with "node type Reading has
        multiple @key constraints; only one is supported" (verified against
        the binary). Rendering it anyway would emit a `.pg` the engine can
        never accept, so it's refused here."""
        with pytest.raises(ValueError, match="exactly one @key property"):
            render_node_type(
                "Reading",
                [("sensor", "String"), ("at", "DateTime"), ("v", "F64")],
                key=("sensor", "at"),
                owner="og",
            )

    def test_underscore_names_are_reserved_but_id_is_not(self) -> None:
        """Since 0.11 the engine's identity is the system field `@id`, so a
        plain `id` is an ordinary property (verified on 0.14, even as the
        `@key`), and every name starting with `_` is reserved instead."""
        assert "id: I64" in render_node_type(
            "X", [("slug", "String"), ("id", "I64")], key=("slug",), owner="og"
        )
        with pytest.raises(ValueError, match=r"\['_x'\] is reserved by Omnigraph"):
            render_node_type(
                "X", [("slug", "String"), ("_x", "I64")], key=("slug",), owner="og"
            )

    def test_key_property_must_exist(self) -> None:
        with pytest.raises(ValueError, match="key property 'missing' is not declared"):
            render_node_type("X", [("a", "String")], key=("missing",), owner="og")

    def test_key_must_not_be_nullable(self) -> None:
        with pytest.raises(ValueError, match="key property 'a' must not be nullable"):
            render_node_type("X", [("a", "String?")], key=("a",), owner="og")

    def test_caller_may_not_shadow_coco_key(self) -> None:
        with pytest.raises(ValueError, match="reserved"):
            render_node_type(
                "X", [("a", "String"), (COCO_KEY, "String")], key=("a",), owner="og"
            )


class TestRenderEdgeType:
    def test_no_properties_still_gets_coco_key(self) -> None:
        assert render_edge_type("Supports", "Source", "Claim", [], owner="og") == (
            "edge Supports: Source -> Claim {\n"
            "  coco_key: String\n  coco_managed_by_og: Bool?\n  @key(@src, @dst)\n}"
        )

    def test_with_properties(self) -> None:
        assert render_edge_type(
            "WorksAt", "Person", "Company", [("role", "String")], owner="og"
        ) == (
            "edge WorksAt: Person -> Company {\n  role: String\n"
            "  coco_key: String\n  coco_managed_by_og: Bool?\n  @key(@src, @dst)\n}"
        )


class TestOwnershipProperty:
    def test_an_identifier_app_name_is_used_as_is(self) -> None:
        assert ownership_property("meeting_notes") == "coco_managed_by_meeting_notes"

    def test_other_names_are_made_identifiers_without_colliding(self) -> None:
        dashed, underscored = ownership_property("my-app"), ownership_property("my_app")
        assert dashed != underscored
        assert dashed.startswith("coco_managed_by_my_app_")
        validate_identifier(dashed, "property name")


class TestMergeTypeIntoSchema:
    """Omnigraph's schema is applied whole-graph, not per type — applying
    one type's fragment alone silently drops every other type (verified
    against the engine). This merge is what makes reconciling each type
    independently safe; it is fiddly text handling and deserves direct
    tests rather than only a live end-to-end check."""

    def test_appends_a_new_type(self) -> None:
        existing = (
            render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
            + "\n"
        )
        b = render_node_type("B", [("slug", "String")], key=("slug",), owner="og")
        merged = merge_type_into_schema(existing, "node", "B", b)
        assert "node A {" in merged
        assert "node B {" in merged
        assert merged.index("node A {") < merged.index("node B {")

    def test_replaces_an_existing_type(self) -> None:
        a1 = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        a2 = render_node_type(
            "A", [("slug", "String"), ("title", "String")], key=("slug",), owner="og"
        )
        merged = merge_type_into_schema(a1 + "\n", "node", "A", a2)
        assert merged == a2 + "\n"
        assert "title" in merged

    def test_leaves_unrelated_types_untouched(self) -> None:
        a = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        b = render_node_type("B", [("slug", "String")], key=("slug",), owner="og")
        existing = f"{a}\n\n{b}\n"
        b2 = render_node_type(
            "B", [("slug", "String"), ("title", "String")], key=("slug",), owner="og"
        )
        merged = merge_type_into_schema(existing, "node", "B", b2)
        assert a in merged
        assert b2 in merged
        assert b not in merged

    def test_replaces_the_brace_less_edge_form(self) -> None:
        """A property-less edge is legal and renders brace-less (`edge E: A
        -> B`, no `{ }` at all) — verified against the engine, which both
        accepts this form and reproduces it via `schema show`. This
        connector's own builders never emit it (they always add
        `coco_key`), but the schema being merged into can legitimately
        contain one from elsewhere, and the merge must still find its
        exact extent — ending at the line, not spilling into whatever
        follows."""
        existing = (
            "node A {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "node B {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "edge E: A -> B\n"
        )
        e2 = render_edge_type("E", "A", "B", [("weight", "I64")], owner="og")
        merged = merge_type_into_schema(existing, "edge", "E", e2)
        assert "node A {\n  slug: String @key\n  coco_key: String\n}" in merged
        assert "node B {\n  slug: String @key\n  coco_key: String\n}" in merged
        assert e2 in merged
        # The old brace-less form must be gone -- not just shadowed by the
        # new braced one, which also starts with "edge E: A -> B".
        assert "edge E: A -> B\n" not in merged

    def test_an_edge_fragment_never_touches_a_same_named_node_block(self) -> None:
        """A `.pg` may legally hold `node Link` and `edge Link` side by side
        — the engine accepts it (verified against the binary). Matching on
        the name alone wrote the edge's fragment over the NODE's block,
        silently destroying the node type and leaving TWO `edge Link`
        blocks behind: the same whole-graph destruction class as applying a
        single type's fragment alone, keyed on a name collision instead of
        an omission."""
        existing = (
            "node Link {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "edge Link: A -> B {\n  coco_key: String\n}\n"
        )
        frag = render_edge_type("Link", "A", "B", [("weight", "I64?")], owner="og")
        merged = merge_type_into_schema(existing, "edge", "Link", frag)
        assert "node Link {\n  slug: String @key\n  coco_key: String\n}" in merged
        assert merged.count("edge Link") == 1
        assert "weight: I64?" in merged

    def test_a_node_fragment_never_touches_a_same_named_edge_block(self) -> None:
        existing = (
            "node Link {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "edge Link: A -> B {\n  coco_key: String\n}\n"
        )
        frag = render_node_type(
            "Link",
            [("slug", "String"), ("title", "String?")],
            key=("slug",),
            owner="og",
        )
        merged = merge_type_into_schema(existing, "node", "Link", frag)
        assert "edge Link: A -> B {\n  coco_key: String\n}" in merged
        assert merged.count("node Link") == 1
        assert "title: String?" in merged

    def test_unbalanced_braces_raise_instead_of_corrupting(self) -> None:
        """With no closing brace the splice point used to stay at the
        OPENING brace, so the fragment landed mid-declaration and the
        corrupt result went straight to `schema apply`. Input is engine
        output today, so this is defense in depth on the whole-graph write
        path — but a bad edit there costs the entire schema."""
        broken = "node A {\n  slug: String @key\n  coco_key: String\n"
        with pytest.raises(ValueError, match="unbalanced braces"):
            merge_type_into_schema(broken, "node", "A", "node A {\n}")

    def test_duplicate_blocks_raise_instead_of_editing_only_the_first(self) -> None:
        """Only the first block was ever found, so a merge rewrote one and
        left the other as a stale duplicate, and a removal deleted one and
        left the other behind — exactly the state the kind-blind match used
        to create."""
        a = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        with pytest.raises(ValueError, match="declares node 'A' 2 times"):
            merge_type_into_schema(a + "\n\n" + a + "\n", "node", "A", a)

    def test_rejects_an_unknown_kind(self) -> None:
        with pytest.raises(ValueError, match="must be 'node' or 'edge'"):
            merge_type_into_schema("", "relation", "A", "node A {\n}")

    # `schema show` returns the source exactly as it was written (verified
    # against the engine), so a schema a person wrote or edited by hand keeps
    # its formatting — and a merger that only recognised `node X {` at the
    # start of a line, ended blocks at the first `}` it saw, and appended
    # what it failed to find, produced schemas the engine then refused
    # ("duplicate node name", "expected EOI or schema_decl").

    def test_finds_an_indented_block(self) -> None:
        existing = "  node Person {\n    slug: String @key\n    coco_key: String\n  }\n"
        frag = render_node_type(
            "Person", [("slug", "String"), ("age", "I64?")], key=("slug",), owner="og"
        )
        merged = merge_type_into_schema(existing, "node", "Person", frag)
        assert merged.count("node Person") == 1
        assert frag in merged
        assert "    slug: String @key" not in merged

    def test_finds_the_second_of_two_declarations_on_one_line(self) -> None:
        person = "node Person { slug: String @key coco_key: String }"
        company = "node Company { slug: String @key coco_key: String }"
        existing = f"{person} {company}\n"
        frag = render_node_type(
            "Company",
            [("slug", "String"), ("name", "String?")],
            key=("slug",),
            owner="og",
        )
        merged = merge_type_into_schema(existing, "node", "Company", frag)
        assert merged.count("node Company") == 1
        assert person in merged
        assert frag in merged

    def test_a_brace_inside_a_comment_does_not_end_the_block(self) -> None:
        existing = (
            "node Person {\n  slug: String @key // closes } here\n  coco_key: String\n}\n\n"
            "node Company {\n  slug: String @key\n  coco_key: String\n}\n"
        )
        frag = render_node_type(
            "Person", [("slug", "String"), ("age", "I64?")], key=("slug",), owner="og"
        )
        merged = merge_type_into_schema(existing, "node", "Person", frag)
        assert merged.count("node Person") == 1
        assert "} here" not in merged
        assert "node Company {\n  slug: String @key\n  coco_key: String\n}" in merged

    def test_a_declaration_inside_a_comment_is_not_a_block(self) -> None:
        existing = (
            "// node Ghost { slug: String @key }\n"
            "node A {\n  slug: String @key\n  coco_key: String\n}\n"
        )
        assert remove_type_from_schema(existing, "node", "Ghost") == existing
        frag = render_node_type(
            "A", [("slug", "String"), ("t", "String?")], key=("slug",), owner="og"
        )
        merged = merge_type_into_schema(existing, "node", "A", frag)
        assert merged.startswith("// node Ghost { slug: String @key }\n")
        assert merged.count("node A") == 1

    def test_a_property_named_like_a_keyword_is_not_a_block(self) -> None:
        existing = (
            "node A {\n  slug: String @key\n  edge: String?\n  node: String?\n"
            "  coco_key: String\n}\n"
        )
        assert remove_type_from_schema(existing, "edge", "String") == existing
        frag = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        assert merge_type_into_schema(existing, "node", "A", frag) == frag + "\n"

    def test_edge_endpoints_are_read_from_an_indented_declaration(self) -> None:
        from cocoindex.connectors.omnigraph._gq import edge_types_referencing

        existing = (
            "node A {\n  slug: String @key\n  coco_key: String\n}\n"
            "  edge E: A -> A {\n    coco_key: String\n  }\n"
            "// edge Ghost: A -> A\n"
        )
        assert edge_types_referencing(existing, "A") == ["E"]


class TestRemoveTypeFromSchema:
    """The first half of the two-step `@key`-change rebuild: the engine
    rejects an in-place key change but accepts drop-then-re-add as two
    separate `schema apply` calls (verified against the engine)."""

    def test_removes_the_middle_block_without_doubling_the_separator(self) -> None:
        a = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        b = render_node_type("B", [("slug", "String")], key=("slug",), owner="og")
        c = render_node_type("C", [("slug", "String")], key=("slug",), owner="og")
        existing = f"{a}\n\n{b}\n\n{c}\n"
        removed = remove_type_from_schema(existing, "node", "B")
        assert a in removed
        assert c in removed
        assert "node B {" not in removed
        assert "\n\n\n" not in removed

    def test_removes_the_only_block(self) -> None:
        a = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        removed = remove_type_from_schema(a + "\n", "node", "A")
        assert "node A" not in removed

    def test_absent_type_is_a_no_op(self) -> None:
        a = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        existing = a + "\n"
        assert remove_type_from_schema(existing, "node", "Z") == existing

    def test_round_trips_with_merge_for_a_key_change(self) -> None:
        """The exact rebuild sequence _apply_type_schema drives: remove the
        old block, then merge the new definition back in — proving the two
        functions compose into a schema with the type's key actually
        changed and every other type still present."""
        a1 = render_node_type("A", [("slug", "String")], key=("slug",), owner="og")
        b = render_node_type("B", [("slug", "String")], key=("slug",), owner="og")
        existing = f"{a1}\n\n{b}\n"
        without_a = remove_type_from_schema(existing, "node", "A")
        a2 = render_node_type(
            "A", [("slug", "String"), ("title", "String")], key=("title",), owner="og"
        )
        rebuilt = merge_type_into_schema(without_a, "node", "A", a2)
        assert a2 in rebuilt
        assert b in rebuilt
        assert a1 not in rebuilt  # A's old (slug-keyed) definition is gone


class TestIncidentEdges:
    SCHEMA = (
        "node Person {\n  slug: String @key\n  coco_key: String\n}\n"
        "node Company {\n  cid: I64 @key\n  coco_key: String\n}\n"
        "edge WorksAt: Person -> Company {\n  coco_key: String\n}\n"
        "edge Knows: Person -> Person {\n  coco_key: String\n}\n"
    )

    def test_patterns_cover_both_ends_and_self_loops(self) -> None:
        assert ogq.incident_edge_patterns(self.SCHEMA, "Person") == [
            ogq.IncidentEdge("WorksAt", "from"),
            ogq.IncidentEdge("Knows", "from"),
            ogq.IncidentEdge("Knows", "to"),
        ]
        assert ogq.incident_edge_patterns(self.SCHEMA, "Company") == [
            ogq.IncidentEdge("WorksAt", "to")
        ]
        assert ogq.incident_edge_patterns(self.SCHEMA, "Nope") == []

    def test_query_matches_by_coco_key_in_traversal_spelling(self) -> None:
        """The read side names an edge type by its traversal spelling, which
        starts lowercase (`worksAt` for `edge WorksAt`, verified against
        the engine, which looks it up case-insensitively). The node is
        found by `coco_key`, bound as a parameter like every value."""
        out = render_query(
            [
                ogq.build_incident_edges_query(
                    "Person", "k1", ogq.IncidentEdge("WorksAt", "from")
                )
            ]
        )
        assert out.expr == (
            "query m($s0_p_coco_key: String) { match { "
            "$n: Person { coco_key: $s0_p_coco_key } $n $e:worksAt $o } "
            "return { $e.coco_key as edge_key } }"
        )
        assert out.params == {"s0_p_coco_key": "k1"}
        incoming = render_query(
            [
                ogq.build_incident_edges_query(
                    "Company", "k7", ogq.IncidentEdge("WorksAt", "to")
                )
            ]
        )
        assert "$o $e:worksAt $n }" in incoming.expr


class TestOwnershipDetection:
    """`coco_managed_by_<app>` is a declared property. Ownership detection
    searched the block's raw text, comments included, so a user-owned block
    whose comment merely mentioned the property counted as the connector's
    — and an app drop deleted it. Detection and release must look at
    declarations only."""

    USER_EDGE = (
        "edge E: A -> B {\n"
        "  // coco_managed_by_og: Bool? was deliberately omitted\n"
        "  coco_key: String\n"
        "}\n"
    )
    OWNED_EDGE = (
        "edge E: A -> B {\n"
        "  coco_key: String // no coco_managed_by_og here, only below\n"
        "  coco_managed_by_og: Bool?\n"
        "}\n"
    )
    ANOTHER_APPS_EDGE = OWNED_EDGE.replace(
        "coco_managed_by_og: Bool?", "coco_managed_by_other: Bool?"
    )

    def test_a_comment_does_not_confer_ownership(self) -> None:
        assert ownership_marker(self.USER_EDGE, "edge", "E") is None
        assert (
            release_ownership(self.USER_EDGE, "edge", "E", "coco_managed_by_og")
            == self.USER_EDGE
        )

    def test_a_declaration_confers_ownership_whatever_the_comments_say(self) -> None:
        assert ownership_marker(self.OWNED_EDGE, "edge", "E") == "coco_managed_by_og"
        assert release_ownership(
            self.OWNED_EDGE, "edge", "E", "coco_managed_by_og"
        ) == (
            "edge E: A -> B {\n"
            "  coco_key: String // no coco_managed_by_og here, only below\n"
            "}\n"
        )

    def test_the_marker_names_its_app(self) -> None:
        """A release only ever drops this app's own marker: another app's
        stays, so its drop still finds its type marked as its own."""
        assert (
            ownership_marker(self.ANOTHER_APPS_EDGE, "edge", "E")
            == "coco_managed_by_other"
        )
        assert (
            release_ownership(self.ANOTHER_APPS_EDGE, "edge", "E", "coco_managed_by_og")
            == self.ANOTHER_APPS_EDGE
        )


class TestNodeMutations:
    def test_upsert_is_a_statement_with_positional_slots(self) -> None:
        """A builder returns a statement, not query text: its body holds a
        `$?` slot per bound value, and the binds carry each slot's label,
        type and value. Names are allocated only when a query is rendered,
        so the value never touches the text and no renaming is ever needed
        to combine statements."""
        m = build_node_upsert(
            "Person",
            [
                PropertyValue("email", "String", "ada@x.com"),
                PropertyValue("display_name", "String", "Ada"),
            ],
            coco_key="k1",
        )
        assert m == Statement(
            "insert Person { email: $?, display_name: $?, coco_key: $? }",
            (
                Bind("p_email", "String", "ada@x.com"),
                Bind("p_display_name", "String", "Ada"),
                Bind("p_coco_key", "String", "k1"),
            ),
        )
        rendered = render_query([m])
        assert rendered.expr == (
            "query m($s0_p_email: String, $s0_p_display_name: String, "
            "$s0_p_coco_key: String) { insert Person { email: $s0_p_email, "
            "display_name: $s0_p_display_name, coco_key: $s0_p_coco_key } }"
        )
        assert rendered.params == {
            "s0_p_email": "ada@x.com",
            "s0_p_display_name": "Ada",
            "s0_p_coco_key": "k1",
        }

    def test_delete_uses_coco_key_single_predicate(self) -> None:
        m = render_query([build_node_delete("Person", coco_key="k1")])
        assert m.expr == (
            "query m($s0_p_coco_key: String) "
            "{ delete Person where coco_key = $s0_p_coco_key }"
        )
        assert m.params == {"s0_p_coco_key": "k1"}

    def test_endpoint_stub_is_key_plus_coco_key(self) -> None:
        m = render_query(
            [
                build_endpoint_stub(
                    "Person", [PropertyValue("email", "String", "ada@x.com")], "k1"
                )
            ]
        )
        assert m.expr == (
            "query m($s0_p_email: String, $s0_p_coco_key: String) "
            "{ insert Person { email: $s0_p_email, coco_key: $s0_p_coco_key } }"
        )

    def test_value_is_never_interpolated(self) -> None:
        # A value containing GQ-syntax characters must not change the query's
        # shape at all — asserting the full string, not just `not in`, is
        # what actually proves the value never reaches the query text.
        nasty = '", role: "pwned'
        m = render_query(
            [
                build_node_upsert(
                    "Person", [PropertyValue("email", "String", nasty)], coco_key="k1"
                )
            ]
        )
        assert m.expr == (
            "query m($s0_p_email: String, $s0_p_coco_key: String) "
            "{ insert Person { email: $s0_p_email, coco_key: $s0_p_coco_key } }"
        )
        assert nasty not in m.expr
        assert m.params == {"s0_p_email": nasty, "s0_p_coco_key": "k1"}

    def test_type_is_never_interpolated(self) -> None:
        # Unlike `value`, `pg_type` lands directly in the signature text — it
        # must be validated, not merely passed through unexamined.
        evil = (
            "String) { delete Person where coco_key = $p_coco_key } query z($x: String"
        )
        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            build_node_upsert(
                "Person", [PropertyValue("email", evil, "v")], coco_key="k1"
            )

    def test_empty_pg_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            build_node_upsert(
                "Person", [PropertyValue("email", "", "v")], coco_key="k1"
            )

    def test_bad_property_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid Omnigraph property name"):
            build_node_upsert(
                "Person", [PropertyValue("a b", "String", 1)], coco_key="k1"
            )

    def test_coco_key_named_prop_rejected(self) -> None:
        with pytest.raises(ValueError, match="reserved"):
            build_node_upsert(
                "Person", [PropertyValue(COCO_KEY, "String", "x")], coco_key="k1"
            )

    def test_duplicate_property_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="Duplicate"):
            build_node_upsert(
                "Person",
                [
                    PropertyValue("email", "String", "a"),
                    PropertyValue("email", "String", "b"),
                ],
                coco_key="k1",
            )


class TestEdgeMutations:
    def test_insert_carries_coco_key_not_id(self) -> None:
        m = render_query(
            [
                build_edge_insert(
                    "WorksAt",
                    PropertyValue("from", "String", "ada@x.com"),
                    PropertyValue("to", "String", "acme"),
                    [PropertyValue("role", "String", "Eng")],
                    coco_key="e1",
                )
            ]
        )
        assert m.expr == (
            "query m($s0_e_from: String, $s0_e_to: String, $s0_p_role: String, "
            "$s0_p_coco_key: String) { insert WorksAt { from: $s0_e_from, "
            "to: $s0_e_to, role: $s0_p_role, coco_key: $s0_p_coco_key } }"
        )
        assert "id:" not in m.expr
        assert m.params == {
            "s0_e_from": "ada@x.com",
            "s0_e_to": "acme",
            "s0_p_role": "Eng",
            "s0_p_coco_key": "e1",
        }

    def test_insert_without_properties(self) -> None:
        m = render_query(
            [
                build_edge_insert(
                    "Supports",
                    PropertyValue("from", "String", "load-test"),
                    PropertyValue("to", "String", "lower-latency"),
                    [],
                    coco_key="e2",
                )
            ]
        )
        assert m.expr == (
            "query m($s0_e_from: String, $s0_e_to: String, $s0_p_coco_key: String) "
            "{ insert Supports { from: $s0_e_from, to: $s0_e_to, "
            "coco_key: $s0_p_coco_key } }"
        )

    def test_delete_uses_coco_key(self) -> None:
        m = render_query([build_edge_delete("WorksAt", coco_key="e1")])
        assert m.expr == (
            "query m($s0_p_coco_key: String) "
            "{ delete WorksAt where coco_key = $s0_p_coco_key }"
        )

    def test_no_edge_update_builder_exists(self) -> None:
        import cocoindex.connectors.omnigraph._gq as gq

        assert not hasattr(gq, "build_edge_update")

    def test_endpoint_type_is_never_interpolated(self) -> None:
        evil = (
            "String) { delete WorksAt where coco_key = $p_coco_key } query z($x: String"
        )
        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            build_edge_insert(
                "WorksAt",
                PropertyValue("from", evil, "ada@x.com"),
                PropertyValue("to", "String", "acme"),
                [],
                coco_key="e1",
            )

    def test_coco_key_named_prop_rejected(self) -> None:
        with pytest.raises(ValueError, match="reserved"):
            build_edge_insert(
                "WorksAt",
                PropertyValue("from", "String", "a"),
                PropertyValue("to", "String", "b"),
                [PropertyValue(COCO_KEY, "String", "x")],
                coco_key="e1",
            )

    def test_from_named_prop_rejected(self) -> None:
        with pytest.raises(ValueError, match="reserved"):
            build_edge_insert(
                "WorksAt",
                PropertyValue("from", "String", "a"),
                PropertyValue("to", "String", "b"),
                [PropertyValue("from", "String", "x")],
                coco_key="e1",
            )

    def test_to_named_prop_rejected(self) -> None:
        with pytest.raises(ValueError, match="reserved"):
            build_edge_insert(
                "WorksAt",
                PropertyValue("from", "String", "a"),
                PropertyValue("to", "String", "b"),
                [PropertyValue("to", "String", "x")],
                coco_key="e1",
            )

    def test_duplicate_property_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="Duplicate"):
            build_edge_insert(
                "WorksAt",
                PropertyValue("from", "String", "a"),
                PropertyValue("to", "String", "b"),
                [
                    PropertyValue("role", "String", "x"),
                    PropertyValue("role", "String", "y"),
                ],
                coco_key="e1",
            )


class TestValidatePgType:
    @pytest.mark.parametrize(
        "pg_type",
        [
            "String",
            "Bool",
            "I32",
            "I64",
            "U32",
            "U64",
            "F32",
            "F64",
            "Date",
            "DateTime",
            "Blob",
            "String?",
            "I32?",
            "DateTime?",
            "Vector(768)",
            "Vector(3)?",
            "[String]",
            "[I32]?",
            "enum(a, b, c)",
            "enum(a,b,c)?",
            "enum(only_one)",
        ],
    )
    def test_valid(self, pg_type: str) -> None:
        validate_pg_type(pg_type)  # must not raise

    @pytest.mark.parametrize(
        "pg_type",
        [
            "",
            "String) { delete Person where coco_key = $p_coco_key } query z($x: String",
            "Strin g",
            "Vector()",
            "Vector(abc)",
            "Vector(0)",
            "[Bogus]",
            "[String",
            "enum()",
            "enum(1abc)",
            "Foo",
            "string",
            "String??",
        ],
    )
    def test_invalid(self, pg_type: str) -> None:
        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            validate_pg_type(pg_type)


class TestDeriveCocoKey:
    def test_deterministic(self) -> None:
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        assert derive_coco_key(("a", "b")) == derive_coco_key(("a", "b"))

    def test_order_matters(self) -> None:
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        assert derive_coco_key(("a", "b")) != derive_coco_key(("b", "a"))

    def test_no_underscore_collision(self) -> None:
        """The neo4j connector's f"{a}_{b}_{c}_{d}" form collides here; a
        fingerprint over the tuple must not. See spec decision 4."""
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        assert derive_coco_key(("x", "y_z")) != derive_coco_key(("x_y", "z"))

    def test_type_is_significant(self) -> None:
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        assert derive_coco_key((1, 2)) != derive_coco_key(("1", "2"))

    def test_single_part_key(self) -> None:
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        assert derive_coco_key(("slug-1",)) != derive_coco_key(("slug-2",))

    def test_is_hex(self) -> None:
        from cocoindex.connectors.omnigraph._target import derive_coco_key

        k = derive_coco_key(("a", "b"))
        int(k, 16)
        assert k.islower()


class TestTypeMapping:
    @pytest.mark.asyncio
    async def test_scalar_mapping(self) -> None:
        schema = await NodeSchema.from_class(_Doc, key="slug")
        assert {n: p.pg_type for n, p in schema.properties.items()} == {
            "slug": "String",
            "title": "String",
            "words": "I64",
            "ratio": "F64",
            "published": "Bool",
            "on": "Date",
            "at": "DateTime",
            "note": "String?",
        }

    @pytest.mark.asyncio
    async def test_date_and_datetime_get_isoformat_encoders(self) -> None:
        """`PropertyDef.encoder` is what lets a `date`/`datetime` field reach
        the transport layer without crashing `json.dumps`. Verified against
        the 0.14 engine: `Date` takes `"2026-01-01"`, and `DateTime` refuses
        non-zero digits past the third fractional one, so a datetime is cut
        to milliseconds."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        on = datetime.date(2026, 1, 1)
        at = datetime.datetime(2026, 1, 1, 12, 30)  # noqa: DTZ001
        assert schema.properties["on"].encoder is not None
        assert schema.properties["on"].encoder(on) == on.isoformat()
        assert schema.properties["at"].encoder is not None
        assert schema.properties["at"].encoder(at) == "2026-01-01T12:30:00.000"
        micro = datetime.datetime(2026, 1, 1, 12, 30, 0, 123456)  # noqa: DTZ001
        assert schema.properties["at"].encoder(micro) == "2026-01-01T12:30:00.123"
        assert schema.properties["slug"].encoder is None
        assert schema.properties["note"].encoder is None

    def test_explicit_date_and_datetime_definitions_encode_by_type(self) -> None:
        """A hand-built `PropertyDef("at", "DateTime")` has no encoder set,
        and encoding was keyed on that field: a `datetime` then reached
        `json.dumps` raw, and a `date` key reached the engine's StableKey
        raw. The built-in ISO encoding follows the `.pg` type, whether the
        definition came from a dataclass or was written out by hand."""
        at = datetime.datetime(2026, 1, 1, 12, 30, tzinfo=datetime.UTC)
        assert ogt._encode_property(PropertyDef("at", "DateTime"), at).value == (
            "2026-01-01T12:30:00.000+00:00"
        )
        assert (
            ogt._encode_property(
                PropertyDef("on", "Date?"), datetime.date(2026, 1, 5)
            ).value
            == "2026-01-05"
        )
        assert ogt._encode_property(
            PropertyDef("days", "[Date]"), [datetime.date(2026, 1, 5)]
        ).value == ["2026-01-05"]
        assert (
            ogt._tracking_key_value(
                PropertyDef("on", "Date"), datetime.date(2026, 1, 5)
            )
            == "2026-01-05"
        )

    def test_isoformat_given_explicitly_counts_as_the_built_in_key_encoder(
        self,
    ) -> None:
        """The obvious thing to write by hand is the same thing the connector
        applies by default; it must not be refused as a custom key encoder."""
        for pg_type, encoder in (
            ("DateTime", datetime.datetime.isoformat),
            ("Date", datetime.date.isoformat),
        ):
            schema = NodeSchema(
                properties={"k": PropertyDef("k", pg_type, encoder)}, key=("k",)
            )

            def mount(schema: NodeSchema = schema) -> None:
                omnigraph.node_target(_fresh_db("iso"), "T", schema)

            _in_component(mount)

    @pytest.mark.asyncio
    async def test_list_of_dates_encodes_every_element(self) -> None:
        """`list[datetime.date]` maps to `[Date]`, which the schema accepts —
        but `json.dumps` cannot serialize the elements, so the encoder has
        to reach into the list. A nullable list stays `None` when absent."""

        @dataclass
        class _L:
            slug: str
            days: list[datetime.date]
            maybe: list[datetime.datetime] | None

        schema = await NodeSchema.from_class(_L, key="slug")
        assert schema.properties["days"].pg_type == "[Date]"
        assert schema.properties["maybe"].pg_type == "[DateTime]?"
        encoded = ogt._encode_properties(
            {"slug": "a", "days": [datetime.date(2026, 1, 1)], "maybe": None},
            schema.properties,
        )
        by_name = {p.name: p.value for p in encoded}
        assert by_name["days"] == ["2026-01-01"]
        assert by_name["maybe"] is None
        json.dumps(by_name)

    @pytest.mark.asyncio
    async def test_key_normalised_to_tuple(self) -> None:
        schema = await NodeSchema.from_class(_Doc, key="slug")
        assert schema.key == ("slug",)

    @pytest.mark.asyncio
    async def test_composite_key_raises(self) -> None:
        """Same engine limit `_gq.render_node_type` enforces (one `@key` per
        node type), caught here so the error names the dataclass rather than
        surfacing only later out of `.render()`."""

        @dataclass
        class _R:
            sensor: str
            at: datetime.datetime
            v: float

        with pytest.raises(ValueError, match="composite key"):
            await NodeSchema.from_class(_R, key=["sensor", "at"])

    @pytest.mark.asyncio
    async def test_id_field_raises(self) -> None:
        """`id` is reserved by Omnigraph whether the schema ends up
        describing a node or an edge, and it's the likeliest collision in a
        real dataclass — so it's rejected here, naming the class."""

        @dataclass
        class _WithId:
            id: str
            title: str

        with pytest.raises(ValueError, match=r"_WithId\.id: 'id' is reserved"):
            await NodeSchema.from_class(_WithId, key="id")

    @pytest.mark.asyncio
    async def test_empty_key_raises(self) -> None:
        """An unkeyed node `insert` is a strict insert, not an upsert — every
        re-run would duplicate every node. Verified against the live engine:
        inserting the same unkeyed node twice yields two rows."""

        with pytest.raises(ValueError, match="at least one key property"):
            await NodeSchema.from_class(_Doc, key=[])

    @pytest.mark.asyncio
    async def test_nullable_key_raises_at_from_class(self) -> None:
        """Same rule `_gq.render_node_type` enforces, but caught here so the
        error names the dataclass and field the user actually got wrong,
        rather than surfacing only later out of `.render()`."""

        with pytest.raises(
            ValueError, match=r"key property 'note' of _Doc must not be nullable"
        ):
            await NodeSchema.from_class(_Doc, key="note")

    @pytest.mark.asyncio
    async def test_annotation_override(self) -> None:
        @dataclass
        class _S:
            slug: str
            small: Annotated[int, OmnigraphType("I32")]

        schema = await NodeSchema.from_class(_S, key="slug")
        assert schema.properties["small"].pg_type == "I32"

    @pytest.mark.asyncio
    async def test_omnigraph_type_override_is_validated(self) -> None:
        """`OmnigraphType` is a security boundary: its `pg_type` is spliced
        directly into generated query text downstream, so a crafted value
        must be rejected here rather than silently accepted into a schema."""

        @dataclass
        class _Evil:
            slug: str
            small: Annotated[
                str,
                OmnigraphType("String) { delete X where y = $z } query w($a: String"),
            ]

        with pytest.raises(ValueError, match="Invalid Omnigraph property type"):
            await NodeSchema.from_class(_Evil, key="slug")

    @pytest.mark.asyncio
    async def test_nullable_list_element_raises(self) -> None:
        """`[I64?]` is rejected by the engine with "expected core_type"."""

        @dataclass
        class _N:
            slug: str
            vals: list[int | None]

        with pytest.raises(TypeError, match="non-nullable scalars"):
            await NodeSchema.from_class(_N, key="slug")

    @pytest.mark.asyncio
    async def test_nested_list_raises(self) -> None:
        """`[[String]]` is rejected by the engine with "expected base_type"."""

        @dataclass
        class _NN:
            slug: str
            vals: list[list[str]]

        with pytest.raises(TypeError, match="non-nullable scalars"):
            await NodeSchema.from_class(_NN, key="slug")

    @pytest.mark.asyncio
    async def test_unmapped_type_raises(self) -> None:
        @dataclass
        class _U:
            slug: str
            weird: complex

        with pytest.raises(TypeError, match="no Omnigraph type mapping for"):
            await NodeSchema.from_class(_U, key="slug")

    @pytest.mark.asyncio
    async def test_bytes_has_no_mapping(self) -> None:
        """Omnigraph's `Blob` is an external URI reference (the engine fetches
        a `file://` value), not inline bytes — a Python `bytes` value can
        never be a `Blob`, so it must fall through to the generic
        no-mapping error rather than silently being declared as one."""

        @dataclass
        class _B:
            slug: str
            payload: bytes

        with pytest.raises(TypeError, match="no Omnigraph type mapping for"):
            await NodeSchema.from_class(_B, key="slug")

    @pytest.mark.asyncio
    async def test_render(self) -> None:
        @dataclass
        class _Src:
            slug: str
            title: str

        schema = await NodeSchema.from_class(_Src, key="slug")
        assert schema.render("Source", owner="og") == (
            "node Source {\n  slug: String @key\n  title: String\n"
            "  coco_key: String\n  coco_managed_by_og: Bool?\n}"
        )


def _spec(schema: NodeSchema, managed_by: ManagedBy = ManagedBy.SYSTEM) -> _TypeSpec:
    return _TypeSpec(
        schema=schema,
        key=schema.key,
        from_type=None,
        to_type=None,
        managed_by=managed_by,
        owner="og",
    )


class TestEdgeSchema:
    @pytest.mark.asyncio
    async def test_from_class_needs_no_key(self) -> None:
        """An edge's identity is always `(from_id, to_id)`; its own properties
        never form a key. Building edge properties through
        `NodeSchema.from_class` forced a meaningless `key=` onto every edge
        dataclass, which the example then had to explain away."""
        schema = await EdgeSchema.from_class(_AttendedRel)
        assert schema.properties == {
            "is_organizer": PropertyDef("is_organizer", "Bool")
        }
        assert not hasattr(schema, "key")

    @pytest.mark.asyncio
    async def test_from_class_rejects_id(self) -> None:
        @dataclass
        class _WithId:
            id: str

        with pytest.raises(ValueError, match="'id' is reserved"):
            await EdgeSchema.from_class(_WithId)

    def test_a_node_schema_is_not_an_edge_schema(self) -> None:
        """Keyed schemas describe node types only; handing one to an edge
        type is refused at mount, where the mistake is."""
        node_schema = NodeSchema(
            properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
        )

        def mount_with_a_node_schema() -> None:
            omnigraph.edge_target(
                _fresh_db("node_schema"),
                "E",
                _bare_node_target(node_schema, "A"),
                _bare_node_target(node_schema, "B"),
                node_schema,  # type: ignore[arg-type]
            )

        with pytest.raises(TypeError, match="EdgeSchema"):
            _in_component(mount_with_a_node_schema)


class TestNodeTypeReconcile:
    @pytest.mark.asyncio
    async def test_absent_creates(self) -> None:
        schema = await NodeSchema.from_class(_Doc, key="slug")
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema), [], True
        )
        assert out is not None
        assert out.action.main_action == "insert"
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_unchanged_is_noop(self) -> None:
        """A root provider that owns children must never return bare
        `None` for "no change needed" — the engine only refreshes a
        child's handler when reconcile()'s own output carries a fresh
        ChildTargetDef, so bare None here would starve this type's own
        node/edge children on the very next unchanged run."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        prev = _type_tracking_record_from_spec(_spec(schema))
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema), [prev], False
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_property_added_is_additive(self) -> None:
        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str | None

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"))
        )
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_V2, key="slug")),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action is None
        assert out.action.property_actions == {"prop:title": "insert"}
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_non_nullable_property_added_raises(self) -> None:
        """Engine rejects this via schema apply (OG-MF-103); fail in Python instead."""

        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str  # non-nullable addition

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"))
        )
        with pytest.raises(ValueError, match="non-nullable"):
            _NodeTypeHandler().reconcile(
                _TypeKey("og", "node", "Doc"),
                _spec(await NodeSchema.from_class(_V2, key="slug")),
                [prev],
                False,
            )

    @pytest.mark.asyncio
    async def test_nullable_property_added_is_additive(self) -> None:
        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str | None

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"))
        )
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_V2, key="slug")),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action is None
        assert out.action.property_actions == {"prop:title": "insert"}
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_property_dropped_is_lossy(self) -> None:
        @dataclass
        class _V1:
            slug: str
            title: str

        @dataclass
        class _V2:
            slug: str

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"))
        )
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_V2, key="slug")),
            [prev],
            False,
        )
        assert out is not None
        assert out.child_invalidation == "lossy"

    def test_encoder_change_is_lossy(self) -> None:
        """A changed encoder rewrites what the graph holds for every row, but
        the rendered `.pg` is byte-identical, so the only way a memoized
        declaring component ever learns of it is through the type's own
        tracking record: the change has to invalidate the children like a
        retyped property does. A hand-built definition without an encoder
        and one carrying the built-in encoder for its type are the same
        encoding and must not count as a change."""

        def schema(encoder: ogt.ValueEncoder | None) -> NodeSchema:
            return NodeSchema(
                properties={
                    "slug": PropertyDef("slug", "String"),
                    "name": PropertyDef("name", "String", encoder),
                    "at": PropertyDef("at", "DateTime", ogt._ENCODERS["DateTime"]),
                },
                key=("slug",),
            )

        prev = _type_tracking_record_from_spec(_spec(schema(str.lower)))
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema(str.upper)), [prev], False
        )
        assert out is not None
        assert out.action.main_action is None
        assert out.child_invalidation == "lossy"

        by_hand = NodeSchema(
            properties={
                "slug": PropertyDef("slug", "String"),
                "name": PropertyDef("name", "String", str.lower),
                "at": PropertyDef("at", "DateTime"),
            },
            key=("slug",),
        )
        assert (
            _type_tracking_record_from_spec(_spec(by_hand)).tracking_record.sub
            == prev.tracking_record.sub
        )

    @pytest.mark.asyncio
    async def test_key_change_is_destructive(self) -> None:
        @dataclass
        class _V:
            slug: str
            title: str

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V, key="slug"))
        )
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_V, key="title")),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action == "replace"
        assert out.child_invalidation == "destructive"

    @pytest.mark.asyncio
    async def test_user_managed_schema_drift_is_tracked_not_rejected(self) -> None:
        """User-managed means the schema is the user's: they migrate it with
        `omnigraph schema apply`, then declare the wider dataclass. The
        connector used to compare that declaration against what it had
        tracked and refuse — advising exactly that migration, which it then
        rejected again on every run, because the check read tracking
        history and never the live schema. No validation, no schema write:
        the new declaration is simply tracked from here on. That holds even
        for a non-nullable addition — whether the user's migration was legal
        is the engine's call, made when they applied it."""

        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str  # non-nullable addition

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"), ManagedBy.USER)
        )
        spec_v2 = _spec(await NodeSchema.from_class(_V2, key="slug"), ManagedBy.USER)
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), spec_v2, [prev], False
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions
        assert out.tracking_record == _type_tracking_record_from_spec(spec_v2)

    @pytest.mark.asyncio
    async def test_handing_a_type_to_the_user_releases_ownership(
        self,
    ) -> None:
        """A block the connector rendered declares `coco_managed_by_<app>`. Once the
        app declares the type `managed_by=user` that is stale — and a later
        drop of a node type read it as current ownership, taking the user's
        edge type (and its edges) along. The handoff has to write once to
        drop the property; after that the block is the user's."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        prev = _type_tracking_record_from_spec(_spec(schema, ManagedBy.SYSTEM))
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema, ManagedBy.USER), [prev], False
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions
        assert out.action.release_ownership is True

    @pytest.mark.asyncio
    async def test_taking_a_type_back_from_the_user_rewrites_its_block(self) -> None:
        """The reverse handoff. A system-managed declaration after a
        user-managed record wrote nothing when the schema was otherwise
        unchanged, so the ownership property stayed absent and the next drop of a
        node type it referenced treated it as the user's. Reclaiming must
        re-render the block even with nothing else to change."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        prev = _type_tracking_record_from_spec(_spec(schema, ManagedBy.USER))
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(schema, ManagedBy.SYSTEM),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action == "upsert"
        assert out.action.release_ownership is False

    @pytest.mark.asyncio
    async def test_a_type_already_user_managed_releases_nothing(self) -> None:
        schema = await NodeSchema.from_class(_Doc, key="slug")
        prev = _type_tracking_record_from_spec(_spec(schema, ManagedBy.USER))
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema, ManagedBy.USER), [prev], False
        )
        assert out is not None
        assert out.action.release_ownership is False

    @pytest.mark.asyncio
    async def test_user_managed_first_run_adopts(self) -> None:
        """The first run of a managed_by=user type has NO tracking records,
        so the tracked diff is empty and there is nothing to disagree with.
        This used to raise, making the mode unusable on run one even against
        a perfectly matching graph, and with an error text ("differs from the
        tracked one") that misdescribed the cause. Nothing is tracked yet;
        there is nothing to differ from."""
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_Doc, key="slug"), ManagedBy.USER),
            [],
            True,
        )
        assert out is not None
        # No schema write, but a child handler all the same -- node/edge
        # upserts must still flow into a user-managed type.
        assert out.action.main_action is None and not out.action.property_actions
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_user_managed_first_run_adopts_with_a_plain_string(self) -> None:
        """`ManagedBy` is a StrEnum, so `managed_by="user"` compares equal but
        is NOT identical. Under an `is` check it fell through to full SYSTEM
        management — the connector would rewrite a schema the user declared
        they own. Same expectation as the enum case above."""
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(
                await NodeSchema.from_class(_Doc, key="slug"), cast(ManagedBy, "user")
            ),
            [],
            True,
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions

    @pytest.mark.asyncio
    async def test_user_managed_unchanged_is_noop(self) -> None:
        """The second run: tracked, unchanged, still no schema write. This is
        the second of the two bare-`None` sites `_noop_output` replaced, and
        it had no coverage."""
        spec = _spec(await NodeSchema.from_class(_Doc, key="slug"), ManagedBy.USER)
        prev = _type_tracking_record_from_spec(spec)
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), spec, [prev], False
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions

    @pytest.mark.asyncio
    async def test_undeclaring_a_user_managed_type_does_not_drop_it(self) -> None:
        """Un-declaring a `managed_by=user` type must be a no-op, not a drop.

        The connector did not create the type and does not own it, so pulling
        the mount out of the app is a statement about the app, not about the
        graph. Dropping the block here deletes the user's type AND every row
        in it -- the one irreversible thing this connector can do to data it
        was explicitly told it does not manage. `omnigraph.mdx` promises
        exactly this ("Removing a `managed_by="user"` type from your app is
        also never destructive") and nothing enforced it: the drop path never
        consulted ownership, and structurally could not, because the desired
        state is NON_EXISTENCE and the tracking record did not persist
        `managed_by` at all.
        """
        spec = _spec(await NodeSchema.from_class(_Doc, key="slug"), ManagedBy.USER)
        prev = _type_tracking_record_from_spec(spec)
        assert (
            _NodeTypeHandler().reconcile(
                _TypeKey("og", "node", "Doc"), coco.NON_EXISTENCE, [prev], False
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_multiple_divergent_prev_records_forces_action(self) -> None:
        """`prev_possible_records` can hold more than one candidate after an
        interrupted update. Picking one arbitrarily (as opposed to requiring
        every candidate to agree with `desired` before treating state as
        converged) can silently skip reconciliation: here `desired` matches
        the first candidate exactly but diverges from the second, so an
        action must still be emitted."""

        @dataclass
        class _Narrow:
            slug: str

        @dataclass
        class _Wide:
            slug: str
            title: str

        narrow_schema = await NodeSchema.from_class(_Narrow, key="slug")
        wide_schema = await NodeSchema.from_class(_Wide, key="slug")
        matching_prev = _type_tracking_record_from_spec(_spec(narrow_schema))
        divergent_prev = _type_tracking_record_from_spec(_spec(wide_schema))

        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(narrow_schema),
            [matching_prev, divergent_prev],
            False,
        )
        assert out is not None
        assert out.action.main_action is None
        assert out.action.property_actions == {"prop:title": "delete"}
        assert out.child_invalidation == "lossy"


def _edge_spec(
    schema: NodeSchema,
    from_type: str = "Source",
    to_type: str = "Claim",
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> _TypeSpec:
    return _TypeSpec(
        schema=schema,
        key=(),
        from_type=from_type,
        to_type=to_type,
        managed_by=managed_by,
        owner="og",
    )


class TestEdgeTypeReconcile:
    @pytest.mark.asyncio
    async def test_absent_creates(self) -> None:
        @dataclass
        class _Props:
            role: str

        schema = await NodeSchema.from_class(_Props, key="role")
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"), _edge_spec(schema), [], True
        )
        assert out is not None
        assert out.action.main_action == "insert"
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_unchanged_is_noop(self) -> None:
        """See TestNodeTypeReconcile.test_unchanged_is_noop: bare None
        here would starve this edge type's own child entities."""

        @dataclass
        class _Props:
            role: str

        schema = await NodeSchema.from_class(_Props, key="role")
        prev = _type_tracking_record_from_spec(_edge_spec(schema))
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"), _edge_spec(schema), [prev], False
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_property_added_is_additive(self) -> None:
        @dataclass
        class _V1:
            role: str

        @dataclass
        class _V2:
            role: str
            since: str | None

        prev = _type_tracking_record_from_spec(
            _edge_spec(await NodeSchema.from_class(_V1, key="role"))
        )
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"),
            _edge_spec(await NodeSchema.from_class(_V2, key="role")),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action is None
        assert out.action.property_actions == {"prop:since": "insert"}
        assert out.child_invalidation is None

    @pytest.mark.asyncio
    async def test_non_nullable_property_added_raises(self) -> None:
        @dataclass
        class _V1:
            role: str

        @dataclass
        class _V2:
            role: str
            since: str  # non-nullable addition

        prev = _type_tracking_record_from_spec(
            _edge_spec(await NodeSchema.from_class(_V1, key="role"))
        )
        with pytest.raises(ValueError, match="non-nullable"):
            _EdgeTypeHandler().reconcile(
                _TypeKey("og", "edge", "WorksAt"),
                _edge_spec(await NodeSchema.from_class(_V2, key="role")),
                [prev],
                False,
            )

    @pytest.mark.asyncio
    async def test_property_dropped_is_lossy(self) -> None:
        @dataclass
        class _V1:
            role: str
            since: str

        @dataclass
        class _V2:
            role: str

        prev = _type_tracking_record_from_spec(
            _edge_spec(await NodeSchema.from_class(_V1, key="role"))
        )
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"),
            _edge_spec(await NodeSchema.from_class(_V2, key="role")),
            [prev],
            False,
        )
        assert out is not None
        assert out.child_invalidation == "lossy"

    @pytest.mark.asyncio
    async def test_from_type_change_is_destructive(self) -> None:
        @dataclass
        class _Props:
            role: str

        schema = await NodeSchema.from_class(_Props, key="role")
        prev = _type_tracking_record_from_spec(
            _edge_spec(schema, from_type="Source", to_type="Claim")
        )
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"),
            _edge_spec(schema, from_type="Person", to_type="Claim"),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action == "replace"
        assert out.child_invalidation == "destructive"

    @pytest.mark.asyncio
    async def test_to_type_change_is_destructive(self) -> None:
        @dataclass
        class _Props:
            role: str

        schema = await NodeSchema.from_class(_Props, key="role")
        prev = _type_tracking_record_from_spec(
            _edge_spec(schema, from_type="Source", to_type="Claim")
        )
        out = _EdgeTypeHandler().reconcile(
            _TypeKey("og", "edge", "WorksAt"),
            _edge_spec(schema, from_type="Source", to_type="Report"),
            [prev],
            False,
        )
        assert out is not None
        assert out.action.main_action == "replace"
        assert out.child_invalidation == "destructive"


class TestTypeOwnershipMatrix:
    """Every combination of tracked ownership and declared ownership.

    Twelve cases, small enough to be exhaustive: the previous state is
    nothing / system-managed / user-managed / one of each (an interrupted
    update leaves several candidates behind), crossed with a declaration that
    is system-managed, user-managed, or absent. None of this was answerable
    before `managed_by` started riding on the persisted tracking record.
    """

    @staticmethod
    async def _prev(
        ownerships: list[ManagedBy],
    ) -> list[ogt._TypeTrackingRecord]:
        """Previous records that differ ONLY in ownership, so each case below
        isolates the ownership decision from any schema difference."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        return [_type_tracking_record_from_spec(_spec(schema, m)) for m in ownerships]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("prev_ownerships", "writes"),
        [
            ([], True),
            ([ManagedBy.SYSTEM], False),
            ([ManagedBy.USER], True),
            ([ManagedBy.SYSTEM, ManagedBy.USER], True),
        ],
        ids=["no-prev", "system", "user", "mixed"],
    )
    async def test_declared_system_managed(
        self, prev_ownerships: list[ManagedBy], writes: bool
    ) -> None:
        """A system-managed declaration writes DDL when the graph might not
        already match it: on a first run, where nothing is tracked and the
        engine reports `prev_may_be_missing`, and whenever any tracked record
        was user-managed — the block then lacks its ownership property, and only a
        re-render puts it back. A system-managed record already carrying this
        exact schema means there is nothing to apply."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(schema, ManagedBy.SYSTEM),
            await self._prev(prev_ownerships),
            not prev_ownerships,
        )
        assert out is not None
        assert bool(out.action.main_action or out.action.property_actions) is writes

    @pytest.mark.asyncio
    async def test_user_to_system_handoff_applies_a_schema_change(self) -> None:
        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str | None

        old_schema = await NodeSchema.from_class(_V1, key="slug")
        new_schema = await NodeSchema.from_class(_V2, key="slug")
        prev = _type_tracking_record_from_spec(_spec(old_schema, ManagedBy.USER))

        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(new_schema, ManagedBy.SYSTEM),
            [prev],
            False,
        )

        assert out is not None
        assert out.action.main_action is None
        assert out.action.property_actions == {"prop:title": "insert"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prev_ownerships",
        [[], [ManagedBy.SYSTEM], [ManagedBy.USER], [ManagedBy.SYSTEM, ManagedBy.USER]],
        ids=["no-prev", "system", "user", "mixed"],
    )
    async def test_declared_user_managed(
        self, prev_ownerships: list[ManagedBy]
    ) -> None:
        """A user-managed declaration never writes DDL, whatever is tracked --
        including a type this connector used to manage itself. Handing a type
        over is allowed; what is not allowed is touching `.pg` afterwards."""
        schema = await NodeSchema.from_class(_Doc, key="slug")
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(schema, ManagedBy.USER),
            await self._prev(prev_ownerships),
            not prev_ownerships,
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("prev_ownerships", "drops"),
        [
            ([], False),
            ([ManagedBy.SYSTEM], True),
            ([ManagedBy.USER], False),
            ([ManagedBy.SYSTEM, ManagedBy.USER], False),
        ],
        ids=["no-prev", "system", "user", "mixed"],
    )
    async def test_undeclared(
        self, prev_ownerships: list[ManagedBy], drops: bool
    ) -> None:
        """Removal is the case that matters, because it is the irreversible
        one. The connector drops only what it owns outright, and a single
        user-managed candidate vetoes the drop: after an interrupted update we
        do not get to pick which candidate is the engine's real state, and
        guessing wrong deletes rows the connector never created."""
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            coco.NON_EXISTENCE,
            await self._prev(prev_ownerships),
            False,
        )
        if not drops:
            assert out is None
            return
        assert out is not None
        assert coco.is_non_existence(out.action.spec)
        assert out.action.main_action == "delete"


class TestGuardsUnderPrevMayBeMissing:
    """The non-nullable-addition guard reads a diff computed with
    `prev_may_be_missing=False`, and these tests are what pin that.

    Collapsing the two diffs into one looks like a simplification and
    silently disables the guard: with `prev_may_be_missing` set (a
    `--reprocess`, or internal state lost while the graph persists) the main
    action becomes "upsert", which empties `property_actions` and leaves it
    nothing to fire on. The failure mode is not a wrong error message -- it is
    the raw `OG-MF-103` engine error coming back.
    """

    @pytest.mark.asyncio
    async def test_user_managed_drift_writes_nothing_even_when_prev_may_be_missing(
        self,
    ) -> None:
        """`prev_may_be_missing` turns the write path's main action into an
        "upsert" for a system-managed type. A user-managed one must still
        emit no schema action at all, and must not be rejected either."""

        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str | None

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"), ManagedBy.USER)
        )
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(await NodeSchema.from_class(_V2, key="slug"), ManagedBy.USER),
            [prev],
            True,
        )
        assert out is not None
        assert out.action.main_action is None and not out.action.property_actions

    @pytest.mark.asyncio
    async def test_non_nullable_addition_still_raises(self) -> None:
        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str  # non-nullable addition

        prev = _type_tracking_record_from_spec(
            _spec(await NodeSchema.from_class(_V1, key="slug"))
        )
        with pytest.raises(ValueError, match="non-nullable"):
            _NodeTypeHandler().reconcile(
                _TypeKey("og", "node", "Doc"),
                _spec(await NodeSchema.from_class(_V2, key="slug")),
                [prev],
                True,
            )

    @pytest.mark.asyncio
    async def test_property_on_only_some_prev_records_is_lossy(self) -> None:
        """An accepted behaviour change from the statediff port.

        A nullable property that only SOME candidate previous states carry
        used to be classified as a plain additive alter; it is lossy now.
        More conservative, matches neo4j and falkordb, and reachable only
        after an interrupted update left several candidates behind -- exactly
        the moment when guessing "additive" is least safe.
        """

        @dataclass
        class _V1:
            slug: str

        @dataclass
        class _V2:
            slug: str
            title: str | None

        narrow = await NodeSchema.from_class(_V1, key="slug")
        wide = await NodeSchema.from_class(_V2, key="slug")
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"),
            _spec(wide),
            [
                _type_tracking_record_from_spec(_spec(narrow)),
                _type_tracking_record_from_spec(_spec(wide)),
            ],
            False,
        )
        assert out is not None
        assert out.action.property_actions == {"prop:title": "upsert"}
        assert out.child_invalidation == "lossy"


class TestTypeTrackingRecordRoundTrip:
    @pytest.mark.asyncio
    async def test_managed_by_survives_the_engine_decoder(self) -> None:
        """`managed_by` is only worth persisting if it comes back.

        The engine stores whatever `reconcile` returns and hands it back as
        `prev_possible_records` on the next run, decoded against this
        handler's own annotation. Refusing to drop a user-managed type is
        decided entirely from that restored flag, so a record that
        round-trips without it silently reinstates the drop.
        """
        from cocoindex._internal import serde

        schema = await NodeSchema.from_class(_Doc, key="slug")
        out = _NodeTypeHandler().reconcile(
            _TypeKey("og", "node", "Doc"), _spec(schema, ManagedBy.USER), [], True
        )
        assert out is not None
        assert not coco.is_non_existence(out.tracking_record)

        hint = serde.unwrap_element_type(
            serde.get_param_annotation(_NodeTypeHandler().reconcile, 2)
        )
        restored = serde.make_deserialize_fn(hint)(serde.serialize(out.tracking_record))
        assert restored == out.tracking_record
        assert restored.managed_by == ManagedBy.USER

        # And the restored record really is enough to veto the drop.
        assert (
            _NodeTypeHandler().reconcile(
                _TypeKey("og", "node", "Doc"), coco.NON_EXISTENCE, [restored], False
            )
            is None
        )


_NK = _TypeKey("og", "node", "Source")
_EK = _TypeKey("og", "edge", "Supports")


_SOURCE_PROPS = {
    "slug": PropertyDef("slug", "String"),
    "title": PropertyDef("title", "String"),
}
_SUPPORTS_PROPS = {"w": PropertyDef("w", "I64")}


def _node_handler(title_encoder: ogt.ValueEncoder | None = None) -> _NodeHandler:
    props = dict(_SOURCE_PROPS)
    if title_encoder is not None:
        props["title"] = PropertyDef("title", "String", title_encoder)
    return _NodeHandler("Source", ("slug",), _NK, props)


def _edge_handler(w_encoder: ogt.ValueEncoder | None = None) -> _EdgeHandler:
    props = dict(_SUPPORTS_PROPS)
    if w_encoder is not None:
        props["w"] = PropertyDef("w", "I64", w_encoder)
    return _EdgeHandler(
        "Supports",
        _EK,
        "Source",
        "Claim",
        _SOURCE_PROPS["slug"],
        _SOURCE_PROPS["slug"],
        props,
    )


class TestNodeReconcile:
    def test_new_node_upserts(self) -> None:
        out = _node_handler().reconcile(
            "s1", _NodeValue({"slug": "s1", "title": "T"}), [], False
        )
        assert out is not None and out.action.op == "upsert"

    def test_encoder_change_forces_a_write(self) -> None:
        """Change detection has to see what the engine will be sent, not the
        raw Python value: swapping a property's encoder changes every stored
        value while leaving every raw value alone, so a fingerprint taken
        before encoding scheduled no write at all."""
        v = _NodeValue({"slug": "s1", "title": "Title"})
        first = _node_handler(str.lower).reconcile("s1", v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = _node_handler(str.upper).reconcile(
            "s1", v, [first.tracking_record], False
        )
        assert out is not None and out.action.op == "upsert"
        assert {p.name: p.value for p in out.action.properties}["title"] == "TITLE"

    def test_unchanged_is_noop(self) -> None:
        h, v = _node_handler(), _NodeValue({"slug": "s1", "title": "T"})
        first = h.reconcile("s1", v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        assert h.reconcile("s1", v, [first.tracking_record], False) is None

    def test_changed_reupserts(self) -> None:
        h = _node_handler()
        first = h.reconcile("s1", _NodeValue({"slug": "s1", "title": "T"}), [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile(
            "s1",
            _NodeValue({"slug": "s1", "title": "T2"}),
            [first.tracking_record],
            False,
        )
        assert out is not None and out.action.op == "upsert"

    def test_prev_may_be_missing_forces_write(self) -> None:
        h, v = _node_handler(), _NodeValue({"slug": "s1", "title": "T"})
        first = h.reconcile("s1", v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile("s1", v, [first.tracking_record], True)
        assert out is not None and out.action.op == "upsert"

    def test_undeclared_deletes(self) -> None:
        h = _node_handler()
        first = h.reconcile("s1", _NodeValue({"slug": "s1", "title": "T"}), [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile("s1", coco.NON_EXISTENCE, [first.tracking_record], False)
        assert out is not None and out.action.op == "delete"
        # The sink turns this delete into a key-only stub if an edge still
        # references the node, so the key travels with it.
        assert out.action.key_properties == (PropertyValue("slug", "String", "s1"),)

    def test_undeclared_and_never_written_is_noop(self) -> None:
        assert _node_handler().reconcile("s1", coco.NON_EXISTENCE, [], False) is None

    def test_undeclared_with_uncertain_prev_still_deletes(self) -> None:
        """`prev_possible_records=[]` with `prev_may_be_missing` does NOT mean
        the entity was never written — it means we lost track of it.

        The engine reports that state after a `--reprocess`, after internal
        state is lost while the graph persists, and when a prior delete's
        tracking record was dropped before the delete itself landed. Treating
        it as "never existed" orphans the row in the graph forever: nothing
        else ever deletes it, and nothing tracks it any more.

        Deleting by `coco_key` is a documented no-op when the entity isn't
        there, so emitting the delete is safe even if it turns out to be
        unnecessary. This is the guard every sibling connector uses, and it is
        the same reasoning the insert path already applies one method below —
        which is why the two must not disagree.
        """
        out = _node_handler().reconcile("s1", coco.NON_EXISTENCE, [], True)
        assert out is not None and out.action.op == "delete"

    def test_coco_key_is_derived_from_the_key_tuple(self) -> None:
        out = _node_handler().reconcile("s1", _NodeValue({"slug": "s1"}), [], False)
        assert out is not None
        assert out.action.coco_key == derive_coco_key(("s1",))

    def test_scalar_key_equals_singleton_tuple_key(self) -> None:
        """A single-field key may arrive as a bare scalar or as a 1-tuple;
        both must zip identically against `key_fields` and yield the same
        `coco_key`."""
        h = _node_handler()
        scalar_out = h.reconcile("s1", _NodeValue({"slug": "s1"}), [], False)
        tuple_out = h.reconcile(("s1",), _NodeValue({"slug": "s1"}), [], False)
        assert scalar_out is not None and tuple_out is not None
        assert scalar_out.action.coco_key == tuple_out.action.coco_key


class TestEdgeReconcile:
    def test_new_edge_inserts(self) -> None:
        out = _edge_handler().reconcile(("a", "b"), _EdgeValue("a", "b", {}), [], False)
        assert out is not None and out.action.op == "insert"

    def test_encoder_change_forces_a_replace(self) -> None:
        """Edge counterpart of the node case: the fingerprint must cover the
        encoded value, or an encoder change never reaches the graph."""
        v = _EdgeValue("a", "b", {"w": 2})
        first = _edge_handler(lambda w: w * 10).reconcile(("a", "b"), v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = _edge_handler(lambda w: w * 100).reconcile(
            ("a", "b"), v, [first.tracking_record], False
        )
        assert out is not None and out.action.op == "replace"
        assert {p.name: p.value for p in out.action.properties}["w"] == 200

    def test_unchanged_is_noop(self) -> None:
        h, v = _edge_handler(), _EdgeValue("a", "b", {"w": 1})
        first = h.reconcile(("a", "b"), v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        assert h.reconcile(("a", "b"), v, [first.tracking_record], False) is None

    def test_changed_edge_is_replace_not_insert(self) -> None:
        """A strict insert would duplicate; there is no edge update. Must be replace."""
        h = _edge_handler()
        first = h.reconcile(("a", "b"), _EdgeValue("a", "b", {"w": 1}), [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile(
            ("a", "b"), _EdgeValue("a", "b", {"w": 2}), [first.tracking_record], False
        )
        assert out is not None and out.action.op == "replace"

    def test_prev_may_be_missing_forces_replace(self) -> None:
        h, v = _edge_handler(), _EdgeValue("a", "b", {})
        first = h.reconcile(("a", "b"), v, [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile(("a", "b"), v, [first.tracking_record], True)
        assert out is not None and out.action.op == "replace"

    def test_undeclared_deletes(self) -> None:
        h = _edge_handler()
        first = h.reconcile(("a", "b"), _EdgeValue("a", "b", {}), [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        out = h.reconcile(
            ("a", "b"), coco.NON_EXISTENCE, [first.tracking_record], False
        )
        assert out is not None and out.action.op == "delete"

    def test_undeclared_with_uncertain_prev_still_deletes(self) -> None:
        """Edge counterpart of the node case: an edge whose tracking was lost
        must still be deleted, or it survives in the graph untracked forever.

        `_EdgeHandler.reconcile` already refuses to trust `(prev=[],
        prev_may_be_missing=True)` on the insert side — it forces "replace"
        there precisely because the edge might still be live. The delete side
        must read that same signal the same way.
        """
        out = _edge_handler().reconcile(("a", "b"), coco.NON_EXISTENCE, [], True)
        assert out is not None and out.action.op == "delete"

    def test_repointing_changes_coco_key(self) -> None:
        h = _edge_handler()
        a = h.reconcile(("s1", "c1"), _EdgeValue("s1", "c1", {}), [], False)
        b = h.reconcile(("s1", "c2"), _EdgeValue("s1", "c2", {}), [], False)
        assert a is not None and b is not None
        assert a.action.coco_key != b.action.coco_key

    def test_endpoint_metadata_is_carried_for_stubs(self) -> None:
        """The sink needs each endpoint's full key definition for stubs."""
        out = _edge_handler().reconcile(("a", "b"), _EdgeValue("a", "b", {}), [], False)
        assert out is not None
        assert out.action.from_type == "Source" and out.action.to_type == "Claim"
        assert out.action.from_key_property == PropertyDef("slug", "String")

    def test_empty_and_prev_may_be_missing_is_replace_not_insert(self) -> None:
        """Empty `prev_possible_records` does not mean the edge is absent: the
        engine reports (empty, prev_may_be_missing=True) both when a prior
        delete's tracking record was dropped before the delete itself landed,
        and when internal state was lost outright while the target persists
        (e.g. db_path repointed). A bare insert in either case would silently
        duplicate a still-live edge. Must fall through to replace — deleting
        a nonexistent edge by coco_key is a no-op, so replace is safe even if
        the edge turns out not to have existed."""
        out = _edge_handler().reconcile(("a", "b"), _EdgeValue("a", "b", {}), [], True)
        assert out is not None and out.action.op == "replace"


def _client() -> _GraphClient:
    return _GraphClient(_fake_conn())


#: Stands in wherever `_apply_type_schema` needs a spec that merely exists.
_PLACEHOLDER_SPEC = _TypeSpec(
    schema=None,
    key=(),
    from_type=None,
    to_type=None,
    managed_by=ManagedBy.SYSTEM,
    owner="og",
)


def _type_action(
    main_action: statediff.DiffAction | None,
    type_name: str,
    fragment: str | None,
    *,
    type_kind: str = "node",
    property_actions: dict[str, statediff.DiffAction] | None = None,
    release_ownership: bool = False,
    owner: str = "og",
) -> ogt._TypeAction:
    """A `_TypeAction` carrying only what `_apply_type_schema` reads — whether
    anything has to be written at all (`main_action`/`property_actions`),
    whether the type is being removed (`spec`), the type name, and the
    fragment.

    `spec`'s *contents* are what the sink needs to build a child handler and
    are never consulted on the schema path, so a placeholder stands in for
    every action but a removal — which the path detects with
    `coco.is_non_existence(spec)`, and which `_reconcile_removal` is the only
    producer of, always paired with `main_action="delete"`.
    """
    return ogt._TypeAction(
        key=_TypeKey("og", type_kind, type_name),
        spec=coco.NON_EXISTENCE if main_action == "delete" else _PLACEHOLDER_SPEC,
        pg_fragment=fragment,
        main_action=main_action,
        property_actions=property_actions or {},
        release_ownership=release_ownership,
        owner=owner,
    )


def _node(op: str, slug: str, name: str = "Source") -> ogt._NodeAction:
    return ogt._NodeAction(
        op,
        _TypeKey("og", "node", name),
        name,
        (PropertyValue("slug", "String", slug),),
        derive_coco_key((slug,)),
    )


def _edge(op: str, a: str, b: str) -> ogt._EdgeAction:
    return ogt._EdgeAction(
        op,
        _TypeKey("og", "edge", "Supports"),
        "Supports",
        derive_coco_key((a, b)),
        a,
        b,
        (),
        "Source",
        "Claim",
        PropertyDef("slug", "String"),
        PropertyDef("slug", "String"),
    )


class TestRenderQuery:
    def test_single_statement_gets_the_first_prefix(self) -> None:
        m = render_query(
            [Statement("insert P { a: $? }", (Bind("p_a", "String", "1"),))]
        )
        assert m.params == {"s0_p_a": "1"}
        assert m.expr == "query m($s0_p_a: String) { insert P { a: $s0_p_a } }"

    def test_statements_with_the_same_labels_never_collide(self) -> None:
        """Every builder uses the same labels (`p_coco_key`, ...). Naming
        happens per statement at render time, so two of them share one
        query without any rewriting of the statement text."""
        one = Statement(
            "insert P { coco_key: $? }", (Bind("p_coco_key", "String", "A"),)
        )
        two = Statement(
            "insert P { coco_key: $? }", (Bind("p_coco_key", "String", "B"),)
        )
        m = render_query([one, two])
        assert m.params == {"s0_p_coco_key": "A", "s1_p_coco_key": "B"}
        assert m.expr == (
            "query m($s0_p_coco_key: String, $s1_p_coco_key: String) "
            "{ insert P { coco_key: $s0_p_coco_key } insert P { coco_key: $s1_p_coco_key } }"
        )

    def test_a_label_that_is_a_prefix_of_another_is_rendered_whole(self) -> None:
        m = render_query(
            [
                Statement(
                    "insert P { a: $?, ab: $? }",
                    (Bind("p_a", "String", "1"), Bind("p_ab", "String", "2")),
                )
            ]
        )
        assert m.expr == (
            "query m($s0_p_a: String, $s0_p_ab: String) "
            "{ insert P { a: $s0_p_a, ab: $s0_p_ab } }"
        )

    def test_slot_and_bind_counts_must_agree(self) -> None:
        with pytest.raises(ValueError, match="2 slots but 1 bind"):
            render_query(
                [Statement("insert P { a: $?, b: $? }", (Bind("p_a", "String", 1),))]
            )

    def test_duplicate_labels_in_one_statement_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="Duplicate parameter label"):
            render_query(
                [
                    Statement(
                        "insert P { a: $?, b: $? }",
                        (Bind("p_x", "String", 1), Bind("p_x", "String", 2)),
                    )
                ]
            )

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError):
            render_query([])


class TestPlanCommits:
    def test_upsert_only_is_one_commit(self) -> None:
        assert len(plan_commits([_node("upsert", f"s{i}") for i in range(5)])) == 1

    def test_upserts_and_deletes_never_share_a_commit(self) -> None:
        commits = plan_commits([_node("upsert", "a"), _node("delete", "gone")])
        assert len(commits) == 2
        for c in commits:
            assert not ("insert" in c.expr and "delete" in c.expr)

    def test_replace_deletes_before_it_inserts(self) -> None:
        """Delete selects on coco_key and the replacement reuses it — insert-first
        would delete the new edge too. Verified against the engine."""
        commits = plan_commits([_edge("replace", "a", "b")])
        exprs = [c.expr for c in commits]
        del_i = next(i for i, e in enumerate(exprs) if "delete Supports" in e)
        ins_i = next(i for i, e in enumerate(exprs) if "insert Supports" in e)
        assert del_i < ins_i

    def test_three_phases_when_replace_and_removal_coexist(self) -> None:
        commits = plan_commits([_edge("replace", "a", "b"), _node("delete", "gone")])
        assert len(commits) == 3

    def test_edge_insert_has_no_endpoint_stubs(self) -> None:
        """Endpoint stubs are no longer emitted unconditionally by
        plan_commits — a keyed insert is a full-record replace in Omnigraph
        (verified against the engine), so stubbing every edge's endpoints
        would silently null out a node's other nullable properties. The
        sink builds a stub reactively, only after an insert fails with a
        "not found" error for that specific endpoint — see
        TestEndpointRetryLive."""
        (commit,) = plan_commits([_edge("insert", "a", "b")])
        assert "insert Source" not in commit.expr
        assert "insert Claim" not in commit.expr
        assert "insert Supports" in commit.expr

    def test_node_upserts_precede_edge_inserts_whatever_the_action_order(
        self,
    ) -> None:
        """Actions reach `plan_commits` in reconcile order, which is not the
        commit order they need: here the edge is listed first, and both its
        endpoints' upserts are listed after it. Appending in arrival order
        would emit the edge insert ahead of the nodes it references, inside
        the same commit, and the engine would refuse it."""
        commits = plan_commits(
            [
                _edge("insert", "a", "b"),
                _node("upsert", "a"),
                _node("upsert", "b", name="Claim"),
            ]
        )
        (phase_b,) = commits
        assert phase_b.expr.index("insert Source") < phase_b.expr.index(
            "insert Supports"
        )
        assert phase_b.expr.index("insert Claim") < phase_b.expr.index(
            "insert Supports"
        )

    def test_endpoint_refs_are_always_string(self) -> None:
        """An edge's `from`/`to` holds the endpoint's node `id`, which is a
        String whatever the key's declared type is: passing an `I64` for an
        `I64`-keyed endpoint is refused outright ("cannot assign/compare I64
        with String for property `to`", verified against the engine). The
        pg_type used to be inferred from the value's Python type, so any
        non-string-keyed endpoint — an int-keyed Meeting, say — failed on
        every edge insert."""
        action = ogt._EdgeAction(
            "insert",
            _TypeKey("og", "edge", "Attended"),
            "Attended",
            derive_coco_key(("Ada", 7)),
            "Ada",
            7,
            (),
            "Person",
            "Meeting",
            PropertyDef("name", "String"),
            PropertyDef("meeting_id", "I64"),
        )
        (commit,) = plan_commits([action])
        assert "$s0_e_to: String" in commit.expr
        assert "$s0_e_to: I64" not in commit.expr
        assert commit.params["s0_e_to"] == "7"
        assert commit.params["s0_e_from"] == "Ada"
        # ...but the coco_key still derives from the ORIGINAL values, so it
        # matches what `_EdgeHandler` tracked and what a later delete selects.
        assert commit.params["s0_p_coco_key"] == derive_coco_key(("Ada", 7))

    def test_edge_deletes_precede_node_deletes(self) -> None:
        commits = plan_commits([_edge("delete", "a", "b"), _node("delete", "a")])
        joined = " || ".join(c.expr for c in commits)
        assert joined.index("delete Supports") < joined.index("delete Source")

    def test_chunks_when_over_the_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ogt, "_MAX_ENTITIES_PER_TYPE", 4)
        assert len(plan_commits([_node("upsert", f"s{i}") for i in range(10)])) == 3

    def test_cap_is_per_type_not_global(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ogt, "_MAX_ENTITIES_PER_TYPE", 4)
        actions = [_node("upsert", f"s{i}") for i in range(3)]
        actions += [_node("upsert", f"c{i}", name="Claim") for i in range(3)]
        assert len(plan_commits(actions)) == 1

    def test_a_total_cap_bounds_the_commit_across_types(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The per-type cap is deliberate, but on its own it bounded nothing:
        N types each at the cap produced ONE commit of N x cap entities
        (10 types measured at ~9.5 MB of GQ), and
        `_mutate_with_endpoint_retry` re-sends the whole commit on every
        endpoint-stub retry.
        """
        monkeypatch.setattr(ogt, "_MAX_ENTITIES_PER_TYPE", 4)
        monkeypatch.setattr(ogt, "_MAX_ENTITIES_PER_COMMIT", 5)
        actions = [_node("upsert", f"s{i}") for i in range(3)]
        actions += [_node("upsert", f"c{i}", name="Claim") for i in range(3)]
        commits = plan_commits(actions)
        assert len(commits) > 1
        assert max(c.expr.count("insert ") for c in commits) <= 5

    def test_mixed_workload_respects_all_phase_invariants(self) -> None:
        """One call exercising all four action kinds together — a node
        upsert, an edge replace, an edge removal, and a node removal — the
        exact combination the three-phase design exists to keep safe. The
        commit sequence must never delete something it just wrote, and
        never write something it is about to delete; verified directly via
        each mutation's own bound coco_key, not just by argument."""
        replace_coco_key = derive_coco_key(("r1", "r2"))
        removed_edge_coco_key = derive_coco_key(("d1", "d2"))
        removed_node_coco_key = derive_coco_key(("gone",))

        actions = [
            _node("upsert", "keep"),
            _edge("replace", "r1", "r2"),
            _edge("delete", "d1", "d2"),
            _node("delete", "gone"),
        ]
        commits = plan_commits(actions)
        assert len(commits) == 3
        phase_a, phase_b, phase_c = commits

        # Phase A: only the replace's own delete — nothing written yet.
        assert "delete Supports" in phase_a.expr
        assert "insert" not in phase_a.expr
        assert replace_coco_key in phase_a.params.values()

        # Phase B: the kept node's upsert and the replace's re-insert,
        # under the SAME coco_key phase A just deleted — never landing
        # without that delete having already applied ahead of it.
        assert "insert Source" in phase_b.expr
        assert "insert Supports" in phase_b.expr
        assert "delete" not in phase_b.expr
        assert replace_coco_key in phase_b.params.values()
        # ...and the node comes FIRST. Statements in a combined query
        # execute in the order written, so an edge insert ahead of its own
        # endpoint's upsert would fail "not found" and force the sink's
        # stub-and-retry recovery on every single run.
        assert phase_b.expr.index("insert Source") < phase_b.expr.index(
            "insert Supports"
        )

        # Phase C: the two removals, edge before node, under coco_keys that
        # never appear in phase B — nothing here was just written.
        assert phase_c.expr.index("delete Supports") < phase_c.expr.index(
            "delete Source"
        )
        assert removed_edge_coco_key in phase_c.params.values()
        assert removed_node_coco_key in phase_c.params.values()
        assert replace_coco_key not in phase_c.params.values()
        assert not set(phase_c.params.values()) & set(phase_b.params.values())


class TestPlanCommitsByteBudget:
    def test_commits_stay_under_the_servers_request_limit(self) -> None:
        key = _TypeKey("og", "node", "Source")
        actions = [
            ogt._NodeAction(
                "upsert",
                key,
                "Source",
                (
                    PropertyValue("slug", "String", f"s{i:06d}"),
                    PropertyValue("title", "String?", "x" * 40 + "\u00e9" * 20),
                ),
                derive_coco_key((f"s{i:06d}",)),
            )
            for i in range(ogt._MAX_ENTITIES_PER_TYPE)
        ]
        commits = plan_commits(actions)
        assert len(commits) > 1
        assert sum(len(c.params) for c in commits) == 3 * len(actions)
        for commit in commits:
            body = {"query": commit.expr, "params": commit.params, "branch": "main"}
            assert len(json.dumps(body).encode()) < 1_048_576


class TestPlanCommitsEncoding:
    @pytest.mark.asyncio
    async def test_date_and_datetime_properties_survive_json_dumps(self) -> None:
        """A dataclass with date/datetime/optional fields must round-trip
        through plan_commits into a Query whose params survive
        json.dumps — the SDK sends them as a JSON body."""

        @dataclass
        class _Event:
            slug: str
            on: datetime.date
            at: datetime.datetime
            note: str | None

        schema = await NodeSchema.from_class(_Event, key="slug")
        handler = _NodeHandler(
            "Event", ("slug",), _TypeKey("og", "node", "Event"), schema.properties
        )
        out = handler.reconcile(
            "e1",
            _NodeValue(
                {
                    "slug": "e1",
                    "on": datetime.date(2026, 1, 1),
                    "at": datetime.datetime(2026, 1, 1, 12, 0),  # noqa: DTZ001
                    "note": None,
                }
            ),
            [],
            False,
        )
        assert out is not None
        (commit,) = plan_commits([out.action])
        json.dumps(commit.params)  # must not raise
        # plan_commits always renders a chunk with render_query, even a
        # single one — so params carry the `s0_` commit prefix.
        assert commit.params["s0_p_on"] == "2026-01-01"
        assert commit.params["s0_p_at"] == "2026-01-01T12:00:00.000"
        assert commit.params["s0_p_note"] is None


class TestParseMissingEndpoint:
    @pytest.mark.parametrize(
        ("role", "key", "type_name"),
        [
            ("src", "O'Brien", "Person"),
            ("dst", "a'b'c", "Claim"),
            ("src", "", "EmptyKey"),
        ],
    )
    def test_preserves_apostrophes_in_key(
        self, role: str, key: str, type_name: str
    ) -> None:
        error = _engine_error(f"__{role} '{key}' not found in {type_name}")

        assert ogt._parse_missing_endpoint(error) == (role, key, type_name)


class TestBuildEndpointStub:
    def test_uses_the_endpoint_schemas_declared_key_type(self) -> None:
        action = ogt._EdgeAction(
            "insert",
            _TypeKey("og", "edge", "Supports"),
            "Supports",
            derive_coco_key((7, "c1")),
            7,
            "c1",
            (),
            "Source",
            "Claim",
            PropertyDef("source_id", "I32"),
            PropertyDef("slug", "String"),
        )

        stub = ogt._build_endpoint_stub("src", "7", "Source", [action])

        assert stub is not None
        rendered = render_query([stub])
        assert "$s0_p_source_id: I32" in rendered.expr
        assert "$s0_p_source_id: I64" not in rendered.expr
        assert rendered.params["s0_p_source_id"] == 7

    def test_delete_actions_are_not_stub_candidates(self) -> None:
        """A delete `_EdgeAction` carries `from_id=None`/`to_id=None`, so
        `str(action.from_id)` is the literal `"None"`.

        A node whose key value is the string `"None"` therefore matched a
        delete action and built a meaningless endpoint stub. A delete needs no
        endpoints anyway: deleting by `coco_key` never touches them.
        """
        h = _edge_handler()
        first = h.reconcile(("a", "b"), _EdgeValue("a", "b", {}), [], False)
        assert first is not None
        assert not isinstance(first.tracking_record, coco.NonExistenceType)
        deleted = h.reconcile(
            ("a", "b"), coco.NON_EXISTENCE, [first.tracking_record], False
        )
        assert deleted is not None and deleted.action.op == "delete"

        assert (
            ogt._build_endpoint_stub("src", "None", "Source", [deleted.action]) is None
        )


class TestApplyEntityActions:
    @pytest.mark.asyncio
    async def test_branch_create_failure_propagates_original_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cleanup must never mask the original failure: if branch_create
        itself fails, branch_delete(scratch) has nothing to delete and
        raises too — that second error must be suppressed so the original
        branch_create failure is what actually propagates."""

        async def fake_branch_create(self: object, name: str, *, frm: str) -> None:
            raise _engine_error("branch create failed: disk full")

        async def fake_branch_delete(self: object, name: str) -> None:
            raise _engine_error(f"branch {name!r} does not exist")

        monkeypatch.setattr(_GraphClient, "branch_create", fake_branch_create)
        monkeypatch.setattr(_GraphClient, "branch_delete", fake_branch_delete)
        # The delete below makes the sink read the schema first, to learn
        # whether some edge type could still reference the node.
        monkeypatch.setattr(_GraphClient, "read_schema", _noop_read_schema)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())

        key = _TypeKey(db.key, "node", "Source")
        actions: list[ogt._NodeAction | ogt._EdgeAction] = [
            ogt._NodeAction(
                "upsert",
                key,
                "Source",
                (PropertyValue("slug", "String", "a"),),
                derive_coco_key(("a",)),
            ),
            ogt._NodeAction(
                "delete",
                key,
                "Source",
                (),
                derive_coco_key(("gone",)),
            ),
        ]
        with pytest.raises(_og.OmnigraphError, match="branch create failed"):
            await ogt._apply_entity_actions(cp, actions)

    @pytest.mark.asyncio
    async def test_branch_cleanup_failure_is_reported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def noop(self: object, *args: object, **kwargs: object) -> None:
            return None

        async def merged_but_not_deleted(self: object, name: str, *, into: str) -> None:
            raise RuntimeError(
                f"merged scratch branch {name!r} but could not delete it"
            )

        async def fail_delete(self: object, name: str) -> None:
            raise _engine_error(f"failed to delete {name}")

        monkeypatch.setattr(_GraphClient, "mutate", noop)
        monkeypatch.setattr(_GraphClient, "branch_create", noop)
        monkeypatch.setattr(_GraphClient, "branch_merge", merged_but_not_deleted)
        monkeypatch.setattr(_GraphClient, "branch_delete", fail_delete)
        monkeypatch.setattr(_GraphClient, "read_schema", _noop_read_schema)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())
        key = _TypeKey(db.key, "node", "Source")
        actions: list[ogt._NodeAction | ogt._EdgeAction] = [
            ogt._NodeAction(
                "upsert",
                key,
                "Source",
                (PropertyValue("slug", "String", "a"),),
                derive_coco_key(("a",)),
            ),
            ogt._NodeAction("delete", key, "Source", (), derive_coco_key(("gone",))),
        ]

        with pytest.raises(RuntimeError, match="could not delete it") as info:
            await ogt._apply_entity_actions(cp, actions)
        # The cleanup attempt failed too; it is noted, not lost, and does not
        # replace the original error.
        assert any("failed to delete" in note for note in info.value.__notes__)

    @pytest.mark.asyncio
    async def test_single_commit_skips_the_scratch_branch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A batch that plans down to exactly one commit must go straight to
        `mutate` on the real branch — no branch_create/branch_merge at all."""
        calls: list[str] = []

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            calls.append(f"mutate:{branch}")

        async def fake_branch_create(self: object, name: str, *, frm: str) -> None:
            calls.append("branch_create")

        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        monkeypatch.setattr(_GraphClient, "branch_create", fake_branch_create)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())

        key = _TypeKey(db.key, "node", "Source")
        actions: list[ogt._NodeAction | ogt._EdgeAction] = [
            ogt._NodeAction(
                "upsert",
                key,
                "Source",
                (PropertyValue("slug", "String", "a"),),
                derive_coco_key(("a",)),
            ),
        ]
        await ogt._apply_entity_actions(cp, actions)
        assert calls == ["mutate:main"]

    @pytest.mark.asyncio
    async def test_a_node_an_edge_still_references_becomes_a_stub(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting a node cascades to its edges in the graph, while the
        edges' own tracking, in whatever component declared them, still
        says they exist — so they were never re-inserted. A node that an
        edge outside this batch still references is reduced to a key-only
        stub instead; one whose only edges this batch deletes too is
        deleted for real."""
        schema = TestOrphanedEdgeEndpoints.SCHEMA
        queried: list[tuple[str, dict[str, object]]] = []
        commits: list[Query] = []
        edge_ab = derive_coco_key(("a", "co"))
        edge_bb = derive_coco_key(("b", "co"))

        async def fake_read_schema(self: object) -> str | None:
            return schema

        async def fake_query(
            self: object, query: Query, *, branch: str
        ) -> list[dict[str, object]]:
            queried.append((query.expr, query.params))
            (coco_key,) = query.params.values()
            return [
                {
                    "edge_key": edge_ab
                    if coco_key == derive_coco_key(("a",))
                    else edge_bb
                }
            ]

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            commits.append(mutation)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "query", fake_query)
        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        _patch_cli_branch_calls(monkeypatch)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())

        def delete(slug: str) -> ogt._NodeAction:
            return ogt._NodeAction(
                "delete",
                _TypeKey(db.key, "node", "Person"),
                "Person",
                (),
                derive_coco_key((slug,)),
                key_properties=(PropertyValue("slug", "String", slug),),
            )

        await ogt._apply_entity_actions(
            cp,
            [
                delete("a"),
                delete("b"),
                ogt._EdgeAction(
                    "delete",
                    _TypeKey(db.key, "edge", "WorksAt"),
                    "WorksAt",
                    edge_bb,
                    None,
                    None,
                    (),
                    "Person",
                    "Company",
                    PropertyDef("slug", "String"),
                    PropertyDef("cid", "I64"),
                ),
            ],
        )

        # One query per referencing edge type and side: Person is only ever
        # a `from` of WorksAt here.
        assert [params for _, params in queried] == [
            {"s0_p_coco_key": derive_coco_key(("a",))},
            {"s0_p_coco_key": derive_coco_key(("b",))},
        ]
        assert all("$n $e:worksAt $o" in expr for expr, _ in queried)
        # `a` is still referenced: a key-only upsert, in the upsert phase.
        # `b`'s only edge goes in this batch: a real delete, after the edge.
        assert [c.expr for c in commits] == [
            (
                "query m($s0_p_slug: String, $s0_p_coco_key: String) "
                "{ insert Person { slug: $s0_p_slug, coco_key: $s0_p_coco_key } }"
            ),
            (
                "query m($s0_p_coco_key: String, $s1_p_coco_key: String) "
                "{ delete WorksAt where coco_key = $s0_p_coco_key "
                "delete Person where coco_key = $s1_p_coco_key }"
            ),
        ]
        assert commits[0].params == {
            "s0_p_slug": "a",
            "s0_p_coco_key": derive_coco_key(("a",)),
        }

    @pytest.mark.asyncio
    async def test_missing_endpoint_escalates_a_single_commit_to_a_branch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovering a missing endpoint takes TWO commits — the stub, then
        the retried commit — so doing it on the live branch would leave an
        orphan stub node behind, untracked and visible to readers, if the
        retry then failed. The single-commit fast path therefore escalates
        to the scratch branch the moment it sees a "not found", rather than
        stubbing in place. Nothing is lost by the failed first attempt: a
        failed mutation applies nothing."""
        calls: list[str] = []

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            calls.append(f"mutate:{branch}")
            if branch == "main":
                raise _engine_error("__dst 'c1' not found in Claim")

        async def fake_branch_create(self: object, name: str, *, frm: str) -> None:
            calls.append("branch_create")

        async def fake_branch_merge(self: object, name: str, *, into: str) -> None:
            calls.append("branch_merge")

        async def fake_branch_delete(self: object, name: str) -> None:
            calls.append("branch_delete")

        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        monkeypatch.setattr(_GraphClient, "branch_create", fake_branch_create)
        monkeypatch.setattr(_GraphClient, "branch_merge", fake_branch_merge)
        monkeypatch.setattr(_GraphClient, "branch_delete", fake_branch_delete)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())

        key = _TypeKey(db.key, "edge", "Supports")
        actions: list[ogt._NodeAction | ogt._EdgeAction] = [
            ogt._EdgeAction(
                "insert",
                key,
                "Supports",
                derive_coco_key(("s1", "c1")),
                "s1",
                "c1",
                (),
                "Source",
                "Claim",
                PropertyDef("slug", "String"),
                PropertyDef("slug", "String"),
            ),
        ]
        await ogt._apply_entity_actions(cp, actions)

        # The failed live attempt, then the whole thing redone on scratch.
        assert calls[0] == "mutate:main"
        assert calls[1] == "branch_create"
        assert "mutate:main" not in calls[1:]  # no stub written to the live branch
        # The merge deletes the branch in the same call.
        assert calls[-1] == "branch_merge" and "branch_delete" not in calls

    @staticmethod
    def _two_edge_actions(db_key: str) -> list[ogt._NodeAction | ogt._EdgeAction]:
        key = _TypeKey(db_key, "edge", "Supports")
        return [
            ogt._EdgeAction(
                "insert",
                key,
                "Supports",
                derive_coco_key((s, c)),
                s,
                c,
                (),
                "Source",
                "Claim",
                PropertyDef("slug", "String"),
                PropertyDef("slug", "String"),
            )
            for s, c in (("s1", "c1"), ("s2", "c2"))
        ]

    @pytest.mark.asyncio
    async def test_stubs_every_distinct_missing_endpoint_in_the_batch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two edges whose four endpoints are all absent need four stub
        rounds — the engine names only the first missing endpoint per
        attempt — and every one of them must be taken. A fixed two-round
        budget failed this batch on its third attempt."""
        present: set[tuple[str, str]] = set()

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            if "insert Supports" in mutation.expr:
                for i in range(2):
                    frm, to = (
                        mutation.params[f"s{i}_e_from"],
                        mutation.params[f"s{i}_e_to"],
                    )
                    if ("Source", frm) not in present:
                        raise _engine_error(f"__src '{frm}' not found in Source")
                    if ("Claim", to) not in present:
                        raise _engine_error(f"__dst '{to}' not found in Claim")
                return
            # Anything else is a key-only endpoint stub.
            type_name = mutation.expr.split("insert ")[1].split(" ")[0]
            present.add((type_name, str(mutation.params["s0_p_slug"])))

        async def noop(self: object, *args: object, **kwargs: object) -> None:
            return None

        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        monkeypatch.setattr(_GraphClient, "branch_create", noop)
        monkeypatch.setattr(_GraphClient, "branch_merge", noop)
        monkeypatch.setattr(_GraphClient, "branch_delete", noop)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())
        await ogt._apply_entity_actions(cp, self._two_edge_actions(db.key))
        assert present == {
            ("Source", "s1"),
            ("Claim", "c1"),
            ("Source", "s2"),
            ("Claim", "c2"),
        }

    @pytest.mark.asyncio
    async def test_endpoint_still_missing_after_its_stub_fails_loudly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A "not found" that names an endpoint already stubbed in this
        batch means the stub did not take; that must propagate, not spin."""
        attempts = 0

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            nonlocal attempts
            if "insert Supports" in mutation.expr:
                attempts += 1
                raise _engine_error("__src 's1' not found in Source")

        async def noop(self: object, *args: object, **kwargs: object) -> None:
            return None

        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        monkeypatch.setattr(_GraphClient, "branch_create", noop)
        monkeypatch.setattr(_GraphClient, "branch_merge", noop)
        monkeypatch.setattr(_GraphClient, "branch_delete", noop)

        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())
        with pytest.raises(_og.OmnigraphError, match="not found"):
            await ogt._apply_entity_actions(cp, self._two_edge_actions(db.key))
        # The live attempt, the first scratch attempt, and exactly one retry
        # after the stub — never a second retry for the same endpoint.
        assert attempts == 3

    @pytest.mark.asyncio
    async def test_scratch_branch_blocks_schema_changes_until_deleted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        branch_created = asyncio.Event()
        allow_mutations = asyncio.Event()
        schema_started = asyncio.Event()
        schema_read = asyncio.Event()
        calls: list[str] = []

        async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
            calls.append(f"mutate:{branch}")
            await allow_mutations.wait()

        async def fake_branch_create(self: object, name: str, *, frm: str) -> None:
            calls.append("branch_create")
            branch_created.set()

        async def fake_branch_merge(self: object, name: str, *, into: str) -> None:
            calls.append("branch_merge")

        async def fake_branch_delete(self: object, name: str) -> None:
            calls.append("branch_delete")

        async def fake_read_schema(self: object) -> str | None:
            calls.append("schema_read")
            schema_read.set()
            return "node Existing {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fake_apply_schema(self: object, schema_pg: str, **_: object) -> None:
            calls.append("schema_apply")

        monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
        monkeypatch.setattr(_GraphClient, "branch_create", fake_branch_create)
        monkeypatch.setattr(_GraphClient, "branch_merge", fake_branch_merge)
        monkeypatch.setattr(_GraphClient, "branch_delete", fake_branch_delete)
        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        conn = _fake_conn()
        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, conn)
        edge_action = ogt._EdgeAction(
            "replace",
            _TypeKey(db.key, "edge", "Supports"),
            "Supports",
            derive_coco_key(("s1", "c1")),
            "s1",
            "c1",
            (),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        schema_action = _type_action(
            "insert",
            "Added",
            "node Added {\n  slug: String @key\n  coco_key: String\n}",
        )

        async def reconcile_schema() -> None:
            schema_started.set()
            await ogt._apply_type_schema(_GraphClient(conn), [schema_action])

        entity_task = asyncio.create_task(ogt._apply_entity_actions(cp, [edge_action]))
        await branch_created.wait()
        schema_task = asyncio.create_task(reconcile_schema())
        await schema_started.wait()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(schema_read.wait(), timeout=0.05)
        finally:
            allow_mutations.set()
        await asyncio.gather(entity_task, schema_task)

        assert calls.index("branch_merge") < calls.index("schema_read")


class TestScratchBranches:
    """Scratch branches against schema deployments, without a server."""

    @pytest.mark.asyncio
    async def test_only_expired_scratch_branches_are_reaped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = datetime.datetime.now(datetime.UTC)
        old = ogt._scratch_branch_name(now - datetime.timedelta(hours=2))
        fresh = ogt._scratch_branch_name(now - datetime.timedelta(minutes=5))
        mine = ogt._scratch_branch_name(now - datetime.timedelta(hours=3))
        branches = ["main", "staging", "coco_scratch_deadbeef", old, fresh, mine]
        deleted: list[str] = []

        async def fake_branch_list(self: object) -> list[str]:
            return list(branches)

        async def fake_branch_delete(self: object, name: str) -> None:
            deleted.append(name)

        monkeypatch.setattr(_GraphClient, "branch_list", fake_branch_list)
        monkeypatch.setattr(_GraphClient, "branch_delete", fake_branch_delete)
        monkeypatch.setattr(ogt, "_live_scratch_branches", {mine})
        assert await ogt._reap_abandoned_scratch_branches(_client()) == [old]
        assert deleted == [old]

    def test_scratch_branch_names_carry_their_creation_time(self) -> None:
        at = datetime.datetime(2026, 10, 30, 12, 0, 1, tzinfo=datetime.UTC)
        name = ogt._scratch_branch_name(at)
        assert name.startswith("coco_scratch_20261030T120001Z_")
        assert ogt._scratch_branch_created_at(name) == at
        assert ogt._scratch_branch_created_at("coco_scratch_deadbeef") is None

    @pytest.mark.asyncio
    async def test_a_schema_deployment_waits_for_scratch_work_and_holds_new_work(
        self,
    ) -> None:
        gate = ogt._SchemaGate()
        order: list[str] = []
        first_in = asyncio.Event()
        release_first = asyncio.Event()

        async def scratch(name: str, hold: asyncio.Event | None) -> None:
            async with gate.shared():
                order.append(f"{name}:start")
                if hold is not None:
                    first_in.set()
                    await hold.wait()
                order.append(f"{name}:end")

        async def deploy() -> None:
            async with gate.exclusive():
                order.append("deploy")

        first = asyncio.create_task(scratch("first", release_first))
        await first_in.wait()
        deployer = asyncio.create_task(deploy())
        await asyncio.sleep(0)
        second = asyncio.create_task(scratch("second", None))
        await asyncio.sleep(0)
        release_first.set()
        await asyncio.gather(first, deployer, second)
        assert order == [
            "first:start",
            "first:end",
            "deploy",
            "second:start",
            "second:end",
        ]


def test_a_system_managed_type_without_a_cluster_is_refused_at_declaration() -> None:
    db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
    coco_env.context_provider.provide(
        db, ConnectionFactory("http://og.invalid", graph_id="g")
    )

    def declare() -> None:
        schema = NodeSchema({"slug": PropertyDef("slug", "String")}, key=("slug",))
        omnigraph.node_target(db, "Doc", schema)

    with pytest.raises(ValueError, match="has no `cluster`"):
        _in_component(declare)

    def declare_user_managed() -> None:
        schema = NodeSchema({"slug": PropertyDef("slug", "String")}, key=("slug",))
        omnigraph.node_target(db, "Doc", schema, managed_by=ManagedBy.USER)

    _in_component(declare_user_managed)


def test_the_factory_keeps_its_token_out_of_repr_and_memo_key() -> None:
    conn = ConnectionFactory("https://h/", graph_id="g", token="s3cret")
    assert "s3cret" not in repr(conn)
    assert conn.__coco_memo_key__() == ("https://h", "g", "main")


def _sdk_error(
    cls: type[_og.OmnigraphError], status: int, effects: str
) -> _og.OmnigraphError:
    body: Any = {
        "error": f"{cls.__name__}",
        "code": "x",
        "disposition": {"effects": effects, "action": "refresh"},
    }
    return cls(cls.__name__, status=status, method="POST", url="u", body=body)


class _FakeSchema:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    async def get(self) -> Any:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return {"schema_source": outcome}


class _FakeSdk:
    def __init__(self, schema: _FakeSchema) -> None:
        self.schema = schema
        self.mutations = 0

    async def mutate(self, *args: Any, **kwargs: Any) -> Any:
        self.mutations += 1
        raise _sdk_error(_og.ServiceUnavailableError, 503, "none")


class TestFailureMatrix:
    @pytest.mark.asyncio
    async def test_a_busy_server_is_retried_for_reads_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "cocoindex.connectors.omnigraph._client._READ_BACKOFF_SECONDS", 0
        )
        schema = _FakeSchema(
            [_sdk_error(_og.ServiceUnavailableError, 503, "none"), "node A {}"]
        )
        sdk = _FakeSdk(schema)
        conn = _fake_conn()

        async def acquire() -> Any:
            return sdk

        monkeypatch.setattr(conn, "acquire", acquire)
        assert await _GraphClient(conn).read_schema() == "node A {}"
        assert schema.calls == 2
        with pytest.raises(_og.ServiceUnavailableError):
            await _GraphClient(conn).mutate(Query("query m() {}", {}), branch="main")
        assert sdk.mutations == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error", "status", "effects"),
        [
            (_og.ForbiddenError, 403, "none"),
            (_og.ServiceUnavailableError, 503, "unknown"),
            (_og.TooManyRequestsError, 429, "partial"),
        ],
    )
    async def test_reads_that_may_have_run_or_will_not_clear_are_not_retried(
        self,
        monkeypatch: pytest.MonkeyPatch,
        error: type[_og.OmnigraphError],
        status: int,
        effects: str,
    ) -> None:
        schema = _FakeSchema([_sdk_error(error, status, effects), "unused"])
        conn = _fake_conn()

        async def acquire() -> Any:
            return _FakeSdk(schema)

        monkeypatch.setattr(conn, "acquire", acquire)
        with pytest.raises(error):
            await _GraphClient(conn).read_schema()
        assert schema.calls == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            _sdk_error(_og.UnauthorizedError, 401, "none"),
            _sdk_error(_og.ForbiddenError, 403, "none"),
            _sdk_error(_og.GraphUnavailableError, 503, "none"),
            _og.NetworkError(
                "lost",
                method="POST",
                url="u",
                request_dispatched=True,
                outcome_unknown=True,
            ),
        ],
        ids=lambda e: type(e).__name__,
    )
    async def test_a_failed_write_fails_the_sync_unchanged(
        self, monkeypatch: pytest.MonkeyPatch, error: _og.OmnigraphError
    ) -> None:
        calls: list[str] = []

        async def fail(self: object, mutation: Query, *, branch: str) -> None:
            calls.append(branch)
            raise error

        monkeypatch.setattr(_GraphClient, "mutate", fail)
        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, _fake_conn())
        action = ogt._NodeAction(
            "upsert",
            _TypeKey(db.key, "node", "Source"),
            "Source",
            (PropertyValue("slug", "String", "a"),),
            derive_coco_key(("a",)),
        )
        with pytest.raises(type(error)) as info:
            await ogt._apply_entity_actions(cp, [action])
        assert info.value is error
        assert calls == ["main"]  # never retried


class TestApplyTypeSchema:
    """Unit coverage for _apply_type_schema's read-merge-deploy, mocked so it
    doesn't need a server."""

    @pytest.mark.asyncio
    async def test_releasing_ownership_drops_only_the_ownership_property(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        marked = "  coco_key: String\n  coco_managed_by_og: Bool?\n"
        existing = (
            f"node Person {{\n  slug: String @key\n{marked}}}\n\n"
            f"node Company {{\n  slug: String @key\n{marked}}}\n"
        )
        applied: list[str] = []

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def record_apply(self: object, pg_fragment: str, **_: object) -> None:
            applied.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", record_apply)

        await ogt._apply_type_schema(
            _client(),
            [_type_action(None, "Person", "unused", release_ownership=True)],
        )
        assert applied == [
            (
                "node Person {\n  slug: String @key\n  coco_key: String\n}\n\n"
                f"node Company {{\n  slug: String @key\n{marked}}}\n"
            )
        ]

    @pytest.mark.asyncio
    async def test_an_empty_graph_gets_its_first_type_by_deployment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A graph is created by a cluster deployment, so the first type is
        an ordinary schema change against an empty schema."""
        calls: list[tuple[str, str, set[str]]] = []

        async def fake_read_schema(self: object) -> str:
            return ""

        async def fake_apply_schema(
            self: object, pg_fragment: str, *, served: str, types: set[str], **_: object
        ) -> None:
            calls.append((pg_fragment, served, set(types)))

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        fragment = "node A {\n  slug: String @key\n  coco_key: String\n}"
        await ogt._apply_type_schema(_client(), [_type_action("insert", "A", fragment)])
        assert calls == [(f"{fragment}\n", "", {"A"})]

    @pytest.mark.asyncio
    async def test_an_unserved_graph_fails_naming_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def missing(self: object) -> str:
            raise _og.NotFoundError(
                "graph not found", status=404, method="GET", url="u"
            )

        monkeypatch.setattr(_GraphClient, "read_schema", missing)
        fragment = "node A {\n  slug: String @key\n  coco_key: String\n}"
        with pytest.raises(ogt.SchemaDeployError, match="'g' is not served"):
            await ogt._apply_type_schema(
                _client(), [_type_action("insert", "A", fragment)]
            )

    @pytest.mark.asyncio
    async def test_merges_when_graph_already_exists(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exact bug this fix closes: a second type must be merged into
        the existing schema, not applied alone (which would silently wipe
        every other type — verified live) or re-inited (which fails
        outright on an already-initialized store)."""
        calls: list[str] = []
        existing = "node A {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            calls.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        b_fragment = "node B {\n  slug: String @key\n  coco_key: String\n}"
        await ogt._apply_type_schema(
            _client(), [_type_action("insert", "B", b_fragment)]
        )
        (applied,) = calls
        assert "node A {" in applied
        assert "node B {" in applied

    @pytest.mark.asyncio
    async def test_concurrent_updates_preserve_both_schema_changes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        schema = "node A {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fake_read_schema(self: object) -> str | None:
            await asyncio.sleep(0)
            return schema

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            nonlocal schema
            await asyncio.sleep(0)
            schema = pg_fragment

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        b_fragment = "node B {\n  slug: String @key\n  coco_key: String\n}"
        c_fragment = "node C {\n  slug: String @key\n  coco_key: String\n}"
        await asyncio.gather(
            ogt._apply_type_schema(
                _client(), [_type_action("insert", "B", b_fragment)]
            ),
            ogt._apply_type_schema(
                _client(), [_type_action("insert", "C", c_fragment)]
            ),
        )

        assert "node A {" in schema
        assert "node B {" in schema
        assert "node C {" in schema

    @pytest.mark.asyncio
    async def test_replace_drops_then_re_adds_instead_of_merging_in_place(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `@key` change can't go through the ordinary single-call merge
        — the engine rejects it outright, verified live — so "replace"
        must drive two separate `apply_schema` calls: one with the type's
        block removed, one with it re-added under its new definition.
        Other types must survive both calls untouched."""
        calls: list[str] = []
        a_old = "node A {\n  slug: String @key\n  coco_key: String\n}\n"
        b = "node B {\n  slug: String @key\n  coco_key: String\n}\n"
        existing = a_old + "\n" + b

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            calls.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        a_new = "node A {\n  title: String @key\n  coco_key: String\n}"
        await ogt._apply_type_schema(_client(), [_type_action("replace", "A", a_new)])

        assert len(calls) == 2
        drop_call, readd_call = calls
        assert "node A" not in drop_call
        assert "node B {" in drop_call
        assert a_new in readd_call
        assert "node B {" in readd_call

    @pytest.mark.asyncio
    async def test_removing_an_edge_leaves_a_same_named_node_alone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The removal half of the same collision: dropping the edge used
        to delete the NODE's block and leave the edge in place."""
        calls: list[str] = []
        existing = (
            "node Link {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "edge Link: A -> B {\n  coco_key: String\n}\n"
        )

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            calls.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        await ogt._apply_type_schema(
            _client(), [_type_action("delete", "Link", None, type_kind="edge")]
        )
        (applied,) = calls
        assert "node Link {" in applied
        assert "edge Link" not in applied

    @pytest.mark.asyncio
    async def test_a_name_used_as_both_kinds_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`schema apply` accepts `node Link` and `edge Link` together, but
        a mutation resolves the name to the NODE type alone — `insert Link
        { from: ..., to: ... }` fails with "type `Link` has no property
        `from`" (verified against the binary). The edge type would exist and
        be unwritable, so declaring the second one has to fail loudly."""

        async def fake_read_schema(self: object) -> str | None:
            return "node Link {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fail_apply(self: object, pg_fragment: str, **_: object) -> None:
            raise AssertionError("must not write a schema with a clashing name")

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fail_apply)

        frag = render_edge_type("Link", "A", "B", [], owner="og")
        with pytest.raises(ValueError, match="already used by a node type"):
            await ogt._apply_type_schema(
                _client(), [_type_action("insert", "Link", frag, type_kind="edge")]
            )

    @pytest.mark.asyncio
    async def test_a_name_used_as_both_kinds_in_one_batch_is_refused_on_a_fresh_graph(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The clash check used to sit AFTER the uninitialized-graph early
        return, so on a fresh graph it was never reached at all — the sink
        happily `init`ed a schema containing both `node Link` and `edge Link`,
        creating the permanently-unwritable edge type the check exists to
        prevent."""

        async def fake_read_schema(self: object) -> str | None:
            return None

        async def fail_init(self: object, pg_fragment: str) -> None:
            raise AssertionError("must not init a schema with a clashing name")

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)

        with pytest.raises(ValueError, match="both a node and an edge"):
            await ogt._apply_type_schema(
                _client(),
                [
                    _type_action(
                        "insert", "Link", "node Link {\n  slug: String @key\n}"
                    ),
                    _type_action(
                        "insert",
                        "Link",
                        render_edge_type("Link", "A", "B", [], owner="og"),
                        type_kind="edge",
                    ),
                ],
            )

    @pytest.mark.asyncio
    async def test_a_name_used_as_both_kinds_in_one_batch_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The live-schema check can only see types ALREADY in the graph, so
        a node and an edge of the same name arriving together in one sync slip
        past it — neither is in `existing` yet."""

        async def fake_read_schema(self: object) -> str | None:
            return "node Other {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fail_apply(self: object, pg_fragment: str, **_: object) -> None:
            raise AssertionError("must not write a schema with a clashing name")

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fail_apply)

        with pytest.raises(ValueError, match="both a node and an edge"):
            await ogt._apply_type_schema(
                _client(),
                [
                    _type_action(
                        "insert", "Link", "node Link {\n  slug: String @key\n}"
                    ),
                    _type_action(
                        "insert",
                        "Link",
                        render_edge_type("Link", "A", "B", [], owner="og"),
                        type_kind="edge",
                    ),
                ],
            )

    @pytest.mark.asyncio
    async def test_drop_removes_the_type_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A "drop" used to be a silent no-op: the sink returned no child
        handler and wrote nothing. But when a container target state
        disappears the engine emits no per-child deletes, so this apply is
        the ONLY thing that removes a dropped type's rows — without it, the
        type and every node in it persist forever, untracked and
        unreachable."""
        calls: list[str] = []
        existing = (
            "node A {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "node B {\n  slug: String @key\n  coco_key: String\n}\n"
        )

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            calls.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        await ogt._apply_type_schema(_client(), [_type_action("delete", "A", None)])
        (applied,) = calls
        assert "node A" not in applied
        assert "node B {" in applied

    @pytest.mark.asyncio
    async def test_batch_reads_once_and_writes_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every type action for one graph arrives in a single sink call, so
        the batch costs one read-merge-write, not one per action. Folding
        them in memory is also what makes the result correct: each fragment
        merges into the previous fold rather than into a stale re-read."""
        reads, applies = [0], []
        existing = "node A {\n  slug: String @key\n  coco_key: String\n}\n"

        async def fake_read_schema(self: object) -> str | None:
            reads[0] += 1
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            applies.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        await ogt._apply_type_schema(
            _client(),
            [
                _type_action(
                    "insert",
                    "B",
                    "node B {\n  slug: String @key\n  coco_key: String\n}",
                ),
                _type_action(
                    "insert",
                    "C",
                    "node C {\n  slug: String @key\n  coco_key: String\n}",
                ),
                _type_action("delete", "A", None),
            ],
        )
        assert reads == [1]
        assert len(applies) == 1
        assert "node A" not in applies[0]
        assert "node B {" in applies[0] and "node C {" in applies[0]

    @pytest.mark.asyncio
    async def test_noop_batch_does_not_touch_the_schema(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An all-noop batch must not even read the schema — an unchanged
        run is the common case and should cost zero CLI invocations."""

        async def fail(self: object, *args: object) -> None:
            raise AssertionError("a noop batch must issue no schema calls")

        monkeypatch.setattr(_GraphClient, "read_schema", fail)
        monkeypatch.setattr(_GraphClient, "apply_schema", fail)

        await ogt._apply_type_schema(_client(), [_type_action(None, "A", "node A {}")])

    @pytest.mark.asyncio
    async def test_batch_with_a_replace_lands_the_removal_first(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The one case that still needs two writes: a replaced type must be
        gone from a COMMITTED schema before it comes back under its new
        definition. Types changing alongside it ride along in the second
        apply, not a third."""
        calls: list[str] = []
        existing = (
            "node A {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "node B {\n  slug: String @key\n  coco_key: String\n}\n"
        )

        async def fake_read_schema(self: object) -> str | None:
            return existing

        async def fake_apply_schema(
            self: object, pg_fragment: str, **_: object
        ) -> None:
            calls.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", fake_apply_schema)

        a_new = "node A {\n  title: String @key\n  coco_key: String\n}"
        b_new = "node B {\n  slug: String @key\n  note: String?\n  coco_key: String\n}"
        await ogt._apply_type_schema(
            _client(),
            [
                _type_action("replace", "A", a_new),
                _type_action(None, "B", b_new, property_actions={"prop:x": "insert"}),
            ],
        )

        assert len(calls) == 2
        removal, final = calls
        assert "node A" not in removal
        assert "node B {" in removal
        assert a_new in final
        assert "note: String?" in final


# ---------------------------------------------------------------------------
# Public API surface: NodeTarget/EdgeTarget, the twelve entry points, aliases.
# ---------------------------------------------------------------------------


class TestOrphanedEdgeEndpoints:
    """Removing a node type that an edge type still points at.

    The engine rejects the resulting schema outright (`catalog error: edge
    'WorksAt' has an unresolved endpoint`, verified against the binary).
    Every mounted type is its own processing component, and all of them
    share one sink batcher — so when an app is dropped, the node type's
    removal can run alone while the edge type's removal waits behind it in
    the batcher's queue. Waiting inside the sink can therefore never see
    the edge go: the node's batch has to take the connector's own edge
    types along, and leave everyone else's in place.
    """

    _MARKED = "  coco_key: String\n  coco_managed_by_og: Bool?\n"
    SCHEMA = (
        f"node Person {{\n  slug: String @key\n{_MARKED}}}\n"
        f"node Company {{\n  slug: String @key\n{_MARKED}}}\n"
        f"edge WorksAt: Person -> Company {{\n{_MARKED}}}\n"
    )
    #: The same graph with an edge type the connector did not create: no
    #: ownership property, though it declares `coco_key` the way a
    #: `managed_by=user` type must.
    SCHEMA_WITH_FOREIGN_EDGE = SCHEMA.replace(
        f"edge WorksAt: Person -> Company {{\n{_MARKED}}}\n",
        "edge WorksAt: Person -> Company {\n  coco_key: String\n}\n",
    )
    #: And with an edge type another app of this connector owns.
    SCHEMA_WITH_ANOTHER_APPS_EDGE = SCHEMA.replace(
        f"edge WorksAt: Person -> Company {{\n{_MARKED}}}\n",
        "edge WorksAt: Person -> Company {\n  coco_key: String\n"
        "  coco_managed_by_other: Bool?\n}\n",
    )

    @staticmethod
    def _mock(monkeypatch: pytest.MonkeyPatch, schema: str) -> list[str]:
        """Serve `schema` on every read; return the list every write lands in."""
        applied: list[str] = []

        async def fake_read_schema(self: object) -> str | None:
            return schema

        async def record_apply(self: object, pg_fragment: str, **_: object) -> None:
            applied.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", record_apply)
        return applied

    @pytest.mark.asyncio
    async def test_dropping_a_node_type_takes_its_own_edge_types_along(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dropping Person while WorksAt is still declared: WorksAt declares
        the connector's ownership property, so its own removal is coming (it is queued
        behind this very batch when the app is dropped), and taking it
        along now is exactly what that removal would do. Company stays."""
        applied = self._mock(monkeypatch, self.SCHEMA)
        await ogt._apply_type_schema(
            _client(), [_type_action("delete", "Person", None)]
        )
        (final,) = applied
        assert "node Person" not in final and "edge WorksAt" not in final
        assert "node Company" in final

    @pytest.mark.asyncio
    async def test_an_edge_re_pointed_in_the_same_batch_as_its_old_node_drop_survives(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Person is dropped while WorksAt moves from Person to Person2 in the
        same batch. WorksAt still points at Person in the served schema, but
        its new definition does not: it must be re-added, not taken along."""
        schema = (
            self.SCHEMA + f"node Person2 {{\n  slug: String @key\n{self._MARKED}}}\n"
        )
        applied = self._mock(monkeypatch, schema)
        new_edge = (
            f"edge WorksAt: Person2 -> Company {{\n{self._MARKED}  @key(@src, @dst)\n}}"
        )
        edge = _type_action("replace", "WorksAt", new_edge, type_kind="edge")
        edge = edge._replace(
            spec=replace(_PLACEHOLDER_SPEC, from_type="Person2", to_type="Company")
        )
        await ogt._apply_type_schema(
            _client(), [_type_action("delete", "Person", None), edge]
        )
        final = applied[-1]
        assert "node Person {" not in final
        assert "edge WorksAt: Person2 -> Company" in final

    @pytest.mark.asyncio
    async def test_one_batch_with_two_apps_types_is_applied_per_app(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        applied = self._mock(monkeypatch, self.SCHEMA)
        await ogt._apply_type_schema(
            _client(),
            [
                _type_action(
                    "insert", "A", "node A {\n  slug: String @key\n}", owner="og"
                ),
                _type_action(
                    "insert", "B", "node B {\n  slug: String @key\n}", owner="other"
                ),
            ],
        )
        assert len(applied) == 2
        assert "node A" in applied[0] and "node B" in applied[1]

    @pytest.mark.asyncio
    async def test_dropping_a_node_type_referenced_by_a_foreign_edge_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No ownership property means the edge type is a user's
        (`managed_by=user`, which must declare `coco_key` too) or another
        tool's. The connector
        never removes those, so the drop fails naming it and writes nothing."""
        applied = self._mock(monkeypatch, self.SCHEMA_WITH_FOREIGN_EDGE)
        with pytest.raises(ValueError, match=r"WorksAt.*not managed by this app"):
            await ogt._apply_type_schema(
                _client(), [_type_action("delete", "Person", None)]
            )
        assert applied == []

    @pytest.mark.asyncio
    async def test_dropping_a_node_type_referenced_by_another_apps_edge_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ownership property used to say only that some app of this
        connector wrote the block, and a drop took every such edge type
        along — including one another app was still declaring, whose
        removal was never coming. It names the app now: only this app's
        edge types go, and the refusal names the app that owns the other."""
        applied = self._mock(monkeypatch, self.SCHEMA_WITH_ANOTHER_APPS_EDGE)
        with pytest.raises(ValueError, match=r"WorksAt.*coco_managed_by_other"):
            await ogt._apply_type_schema(
                _client(), [_type_action("delete", "Person", None)]
            )
        assert applied == []

    @pytest.mark.asyncio
    async def test_key_changing_a_referenced_node_type_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `@key` change is a drop-and-recreate: the FIRST of its two
        applies lands the schema with the node type removed, so the dangling
        endpoint appears there even though the final schema would be sound.
        The edge stays declared, owned or not, so it is never taken along."""
        applied = self._mock(monkeypatch, self.SCHEMA)
        with pytest.raises(ValueError, match="WorksAt"):
            await ogt._apply_type_schema(
                _client(),
                [
                    _type_action(
                        "replace",
                        "Person",
                        "node Person {\n  email: String @key\n  coco_key: String\n}",
                    )
                ],
            )
        assert applied == []

    @pytest.mark.asyncio
    async def test_dropping_the_edge_type_too_is_allowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Removing both together leaves nothing dangling, so it must pass."""
        applied: list[str] = []

        async def fake_read_schema(self: object) -> str | None:
            return self_schema

        self_schema = self.SCHEMA

        async def record_apply(self: object, pg_fragment: str, **_: object) -> None:
            applied.append(pg_fragment)

        monkeypatch.setattr(_GraphClient, "read_schema", fake_read_schema)
        monkeypatch.setattr(_GraphClient, "apply_schema", record_apply)

        await ogt._apply_type_schema(
            _client(),
            [
                _type_action("delete", "WorksAt", None, type_kind="edge"),
                _type_action("delete", "Person", None),
            ],
        )
        assert applied and "Person" not in applied[-1]
        assert "WorksAt" not in applied[-1]

    @pytest.mark.asyncio
    async def test_removing_an_edge_type_already_taken_along_writes_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The edge type's own removal, arriving after the node's batch took
        it along, finds nothing to remove and must not spend a `schema
        apply` on an unchanged schema."""
        already_gone = remove_type_from_schema(self.SCHEMA, "edge", "WorksAt")
        applied = self._mock(monkeypatch, already_gone)
        await ogt._apply_type_schema(
            _client(), [_type_action("delete", "WorksAt", None, type_kind="edge")]
        )
        assert applied == []


class TestPublicSurface:
    def test_exports_one_vocabulary(self) -> None:
        """Omnigraph's own schema language says `node` and `edge`, and that is
        the only vocabulary this connector exports. The table/relation/record
        aliases doubled every name for no behaviour of their own, and the
        mount-time `key=` could only agree with the schema or be an error."""
        assert set(omnigraph.__all__) == {
            "ClusterConfig",
            "ConnectionFactory",
            "EdgeSchema",
            "EdgeTarget",
            "NodeSchema",
            "NodeTarget",
            "OmnigraphType",
            "PropertyDef",
            "SchemaDeployError",
            "ValueEncoder",
            "declare_edge_target",
            "declare_node_target",
            "edge_target",
            "mount_edge_target",
            "mount_node_target",
            "node_target",
        }
        for alias in (
            "TableTarget",
            "RelationTarget",
            "TableSchema",
            "ColumnDef",
            "table_target",
            "mount_table_target",
            "declare_table_target",
            "relation_target",
            "mount_relation_target",
            "declare_relation_target",
        ):
            assert not hasattr(omnigraph, alias), alias
        assert not hasattr(omnigraph.NodeTarget, "declare_record")
        assert not hasattr(omnigraph.EdgeTarget, "declare_relation")


# ---------------------------------------------------------------------------
# Endpoint key-definition wiring: the stub-and-retry recovery reads
# `_EdgeAction.from_key_property`/`to_key_property`, sourced from
# `_TypeSpec.from_key_property`/`to_key_property`. Without this wiring the
# recovery mechanism cannot preserve the endpoint schema's declared type and
# encoder. See
# `test_endpoint_metadata_is_carried_for_stubs` above for the handler-level
# half of this same contract.
# ---------------------------------------------------------------------------


def _in_component(fn: Callable[[], object]) -> None:
    """Run `fn` inside a component of a throwaway app: `node_target` and
    `edge_target` read the owning app from the component they run in."""

    async def main() -> None:
        fn()

    coco.App(
        coco.AppConfig(name=f"in_component_{uuid.uuid4().hex}", environment=coco_env),
        main,
    ).update_blocking()


def test_node_target_needs_a_component() -> None:
    """The block a type renders names the app that owns it, and the app is
    only known inside a component."""
    schema = NodeSchema(
        properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
    )
    with pytest.raises(RuntimeError, match="component context"):
        omnigraph.node_target(_fresh_db("outside"), "Node", schema)


def _bare_node_target(schema: NodeSchema, type_name: str) -> omnigraph.NodeTarget[Any]:
    """A NodeTarget with no real provider — valid for tests that only read
    `.schema`/`.type_name` (spec construction, validation) and never declare
    an actual node through it."""
    return omnigraph.NodeTarget(None, schema, type_name)  # type: ignore[arg-type]


class TestNodeTargetKeyValidation:
    def test_a_custom_encoder_on_a_key_property_is_refused(self) -> None:
        """With `str.lower`, a declared "Mixed" was stored as "mixed";
        switching the encoder to `str.upper` then upserted a second node
        "MIXED" under the same `coco_key` and left "mixed" behind — and an
        edge declared with the raw "Mixed" would find neither. The key is
        the identity: normalize it before declaring the node, never in an
        encoder."""
        schema = NodeSchema(
            properties={"slug": PropertyDef("slug", "String", str.lower)},
            key=("slug",),
        )
        with pytest.raises(
            ValueError, match=r"key property 'slug' has a custom encoder"
        ):
            _in_component(
                lambda: omnigraph.node_target(_fresh_db("custom"), "Person", schema)
            )

    def test_a_built_in_encoder_of_another_type_is_refused_on_a_key(self) -> None:
        """`datetime.date.isoformat` was accepted on a DateTime key, and on a
        datetime it drops the time: noon and 14:00 on one date were stored
        as one midnight node while tracked as two instants, and removing
        one declaration deleted the node the other still declared. A
        built-in encoder is built in for the one type it encodes."""
        for pg_type, encoder in (
            ("DateTime", datetime.date.isoformat),
            ("Date", datetime.datetime.isoformat),
            ("String", ogt._ENCODERS["DateTime"]),
        ):
            schema = NodeSchema(
                properties={"k": PropertyDef("k", pg_type, encoder)}, key=("k",)
            )

            def mount(schema: NodeSchema = schema) -> None:
                omnigraph.node_target(_fresh_db("other"), "T", schema)

            with pytest.raises(ValueError, match=r"key property 'k'"):
                _in_component(mount)

    def test_the_built_in_date_encoder_on_a_key_is_allowed(self) -> None:
        """Only the fixed `Date`/`DateTime` encoders may sit on a key: they
        are a pure function of the value and never change, so raw and
        encoded identity cannot drift apart."""
        schema = NodeSchema(
            properties={"on": PropertyDef("on", "Date", ogt._ENCODERS["Date"])},
            key=("on",),
        )
        _in_component(lambda: omnigraph.node_target(_fresh_db("date"), "Doc", schema))

    def test_keyless_schema_is_refused(self) -> None:
        """A hand-built `NodeSchema` with an empty key must be refused at
        mount, not silently collapse every row onto one target state.

        `NodeSchema` is public and in `__all__`; only `from_class` enforces a
        non-empty key, and the optional `key=` kwarg is only cross-checked
        when supplied. Without this guard the type renders with no `@key`,
        every row derives the SAME `coco_key` from the empty key tuple, all
        rows share one StableKey (last write wins in tracking), and an
        unkeyed Omnigraph insert is a strict insert that duplicates on every
        re-run. `_validate_edge_endpoint` already has this guard — but only
        for types used as endpoints.
        """
        schema = NodeSchema(properties={"slug": PropertyDef("slug", "String")}, key=())
        with pytest.raises(ValueError, match="must declare a key"):
            _in_component(
                lambda: omnigraph.node_target(_fresh_db("keyless"), "Node", schema)
            )


class TestRecordToDict:
    """`_record_to_dict` is the seam every declared row passes through, so a
    field name it silently mishandles becomes an opaque engine error."""

    @staticmethod
    def _schema() -> NodeSchema:
        return NodeSchema(
            properties={
                "slug": PropertyDef("slug", "String"),
                "title": PropertyDef("title", "String?"),
                "note": PropertyDef("note", "String"),
            },
            key=("slug",),
        )

    def test_unknown_dict_key_is_refused(self) -> None:
        """A misspelled field used to vanish: every declared property came
        back `None`, the key's `coco_key` was derived from `None`, and the
        engine rejected a null into a non-nullable `@key` column with nothing
        naming the typo."""
        with pytest.raises(ValueError, match="slgu"):
            ogt._record_to_dict({"slgu": "a", "note": "n"}, self._schema())

    def test_missing_non_nullable_is_refused(self) -> None:
        with pytest.raises(ValueError, match="note"):
            ogt._record_to_dict({"slug": "a"}, self._schema())

    def test_missing_nullable_defaults_to_none(self) -> None:
        """Omitting a nullable property is legitimate — that is what nullable
        means — so only non-nullable omissions are an error."""
        assert ogt._record_to_dict({"slug": "a", "note": "n"}, self._schema()) == {
            "slug": "a",
            "title": None,
            "note": "n",
        }


class TestEdgeTargetKeyPropertyWiring:
    def test_edge_target_populates_endpoint_key_properties(self) -> None:
        # Key-only schemas — this test is about key-definition wiring, not the
        # stub-compatibility guard (see test_mount_edge_rejects_unstubbable_
        # endpoint below for that), so neither endpoint may carry a
        # non-nullable non-key property or `_build_edge_spec` would raise
        # before we get there.
        source = _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Source",
        )
        claim = _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Claim",
        )
        # `_build_edge_spec` is the pure seam `edge_target()` uses to build
        # its `_TypeSpec` — asserting on it directly (rather than reaching
        # into `coco.TargetState`'s private value) proves the wiring without
        # depending on coco internals.
        spec = ogt._build_edge_spec(
            EdgeSchema(properties=_SUPPORTS_PROPS),
            source,
            claim,
            ManagedBy.SYSTEM,
            "og",
        )
        assert spec.from_key_property == PropertyDef("slug", "String")
        assert spec.to_key_property == PropertyDef("slug", "String")

        # Reconcile the spec through the real handler chain, exactly as the
        # engine would, to prove the field actually reaches `_EdgeAction` —
        # not just `_TypeSpec`.
        out = _EdgeTypeHandler().reconcile(_EK, spec, [], False)
        assert out is not None
        assert not coco.is_non_existence(out.action.spec)
        assert out.action.spec.from_key_property == PropertyDef("slug", "String")
        assert out.action.spec.to_key_property == PropertyDef("slug", "String")

        handler = _EdgeHandler(
            "Supports",
            _EK,
            "Source",
            "Claim",
            out.action.spec.from_key_property,
            out.action.spec.to_key_property,
            _SUPPORTS_PROPS,
        )
        edge_out = handler.reconcile(
            ("s1", "c1"), _EdgeValue("s1", "c1", {}), [], False
        )
        assert edge_out is not None
        assert edge_out.action.from_key_property == PropertyDef("slug", "String")
        assert edge_out.action.to_key_property == PropertyDef("slug", "String")


# ---------------------------------------------------------------------------
# mount-time validation: an endpoint type with a non-nullable, non-key
# property can never be stubbed, so referencing it as an edge endpoint must
# fail at mount time, not mid-sync from an unrelated component.
# ---------------------------------------------------------------------------


@dataclass
class _Strict:
    slug: str
    required: str  # non-null, outside the key -> stub would be rejected


@dataclass
class _StubbableClaim:
    slug: str


async def _declare_bad_edge(db: coco.ContextKey[ConnectionFactory]) -> None:
    strict = omnigraph.declare_node_target(
        db, "Strict", await omnigraph.NodeSchema.from_class(_Strict, key="slug")
    )
    claims = omnigraph.declare_node_target(
        db, "Claim", await omnigraph.NodeSchema.from_class(_StubbableClaim, key="slug")
    )
    # `strict`/`claims` are PendingS (declared, not yet synced) — fine for
    # this test, since the guard only reads `.schema`/`.type_name`, both
    # already in hand at declare time. `mount_edge_target`'s endpoint params
    # default to ResolvedS, so mypy flags the mismatch even though nothing
    # here depends on the endpoints having actually synced.
    await omnigraph.mount_edge_target(db, "Bad", strict, claims)  # type: ignore[arg-type]


def test_mount_edge_rejects_unstubbable_endpoint() -> None:
    """Endpoint validation must fire before any I/O — `declare_node_target`
    only registers a declaration (no store touched, no CLI invoked), so this
    needs no live binary and no OMNIGRAPH_TEST_STORE gate."""
    db = coco.ContextKey[ConnectionFactory](f"test_unstubbable_{uuid.uuid4().hex}")
    coco_env.context_provider.provide(db, _fake_conn())
    app = coco.App(
        coco.AppConfig(
            name="test_mount_edge_rejects_unstubbable_endpoint", environment=coco_env
        ),
        _declare_bad_edge,
        db,
    )
    with pytest.raises(ValueError, match="cannot be referenced as an edge endpoint"):
        app.update_blocking()


@dataclass
class _DateKeyed:
    day: datetime.date


async def _declare_date_keyed_edge(db: coco.ContextKey[ConnectionFactory]) -> None:
    days = omnigraph.declare_node_target(
        db, "Day", await omnigraph.NodeSchema.from_class(_DateKeyed, key="day")
    )
    claims = omnigraph.declare_node_target(
        db, "Claim", await omnigraph.NodeSchema.from_class(_StubbableClaim, key="slug")
    )
    await omnigraph.mount_edge_target(db, "On", claims, days)  # type: ignore[arg-type]


def test_mount_edge_rejects_an_endpoint_key_it_cannot_render() -> None:
    """An edge addresses an endpoint by the node's `id`, a String rendering
    of the key value — and the engine's rendering of a `Date` key is days
    since the epoch (2026-01-05 -> `"20458"`), which nothing on the Python
    side reproduces. Refuse the type at mount time rather than emit an
    endpoint reference that silently matches no node."""
    db = coco.ContextKey[ConnectionFactory](f"test_datekey_{uuid.uuid4().hex}")
    coco_env.context_provider.provide(db, _fake_conn())
    app = coco.App(
        coco.AppConfig(name="test_mount_edge_date_key", environment=coco_env),
        _declare_date_keyed_edge,
        db,
    )
    with pytest.raises(ValueError, match=r"its key 'day' is Date"):
        app.update_blocking()


# ---------------------------------------------------------------------------
# declare_node/declare_edge behavioral coverage: nothing previously called
# these as instance methods through a real coco.App run — TestPublicSurface
# only checks method identity off the *class*, and every other test in this
# file bypasses them by hand-building `_NodeAction`/`_EdgeAction` directly.
# So `_record_to_dict` (dict-vs-dataclass extraction) and `declare_node`'s
# key-tuple construction from `self._schema.key` had zero coverage. These
# drive the real declarative pipeline (mount -> declare -> reconcile ->
# sink) with the CLI client's I/O methods mocked out, and assert on the
# actual `Query` that would be sent — the concrete, observable form of
# "what reaches the target state" — rather than on any internal method.
# ---------------------------------------------------------------------------


async def _noop_schema_call(self: object, schema_pg: str, **_: object) -> None:
    return None


async def _noop_read_schema(self: object) -> str:
    # An empty graph: irrelevant to what's under test here, which is the
    # declarative layer, not the schema transport.
    return ""


def _patch_cli_schema_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_GraphClient, "apply_schema", _noop_schema_call)
    monkeypatch.setattr(_GraphClient, "read_schema", _noop_read_schema)


def _capture_mutations(monkeypatch: pytest.MonkeyPatch) -> list[Query]:
    captured: list[Query] = []

    async def fake_mutate(self: object, mutation: Query, *, branch: str) -> None:
        captured.append(mutation)

    monkeypatch.setattr(_GraphClient, "mutate", fake_mutate)
    return captured


def _patch_cli_branch_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """A brand-new declaration through a real `coco.App` run reports
    `prev_may_be_missing=True` (there's genuinely no tracked history yet —
    see `test_dicts_data_together_insert` in test_component_target_states.py
    for the same behavior on an unrelated target), so a first-ever edge
    goes through `_EdgeHandler`'s "replace" path (delete-then-insert, two
    commits applied via a scratch branch) rather than a bare insert. These
    no-op the scratch-branch machinery so it doesn't touch a real process;
    `_capture_mutations` still sees both commits."""

    async def noop_branch_create(self: object, name: str, *, frm: str) -> None:
        return None

    async def noop_branch_merge(self: object, name: str, *, into: str) -> None:
        return None

    async def noop_branch_delete(self: object, name: str) -> None:
        return None

    monkeypatch.setattr(_GraphClient, "branch_create", noop_branch_create)
    monkeypatch.setattr(_GraphClient, "branch_merge", noop_branch_merge)
    monkeypatch.setattr(_GraphClient, "branch_delete", noop_branch_delete)


def _fresh_db(label: str) -> coco.ContextKey[ConnectionFactory]:
    db = coco.ContextKey[ConnectionFactory](f"test_{label}_{uuid.uuid4().hex}")
    coco_env.context_provider.provide(db, _fake_conn())
    return db


@dataclass
class _Article:
    slug: str
    title: str
    published: datetime.date


async def _declare_article_from_dataclass(
    db: coco.ContextKey[ConnectionFactory],
) -> None:
    schema = await NodeSchema.from_class(_Article, key="slug")
    target = await omnigraph.mount_node_target(db, "Article", schema)
    target.declare_node(
        node=_Article(slug="a1", title="Hello", published=datetime.date(2026, 1, 1))
    )


def test_declare_node_from_dataclass_reaches_the_target_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercises the encoder path too: `published` is a `datetime.date`,
    which would blow up at `json.dumps` if `_record_to_dict` skipped the
    schema-driven extraction and the raw `date` object reached the mutation
    unencoded."""
    _patch_cli_schema_calls(monkeypatch)
    captured = _capture_mutations(monkeypatch)
    db = _fresh_db("declare_node_dataclass")

    app = coco.App(
        coco.AppConfig(
            name="test_declare_node_from_dataclass_reaches_the_target_state",
            environment=coco_env,
        ),
        _declare_article_from_dataclass,
        db,
    )
    app.update_blocking()

    assert len(captured) == 1
    m = captured[0]
    assert "insert Article" in m.expr
    assert m.params["s0_p_slug"] == "a1"
    assert m.params["s0_p_title"] == "Hello"
    assert m.params["s0_p_published"] == "2026-01-01"  # encoder applied
    assert m.params["s0_p_coco_key"] == derive_coco_key(("a1",))


async def _declare_article_from_dict(db: coco.ContextKey[ConnectionFactory]) -> None:
    schema = await NodeSchema.from_class(_Article, key="slug")
    target = await omnigraph.mount_node_target(db, "Article", schema)
    target.declare_node(
        node={"slug": "a2", "title": "World", "published": datetime.date(2026, 2, 2)}
    )


def test_declare_node_from_dict_reaches_the_target_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same schema, same assertions, a plain dict instead of a dataclass
    instance — `_record_to_dict` must handle both shapes equivalently."""
    _patch_cli_schema_calls(monkeypatch)
    captured = _capture_mutations(monkeypatch)
    db = _fresh_db("declare_node_dict")

    app = coco.App(
        coco.AppConfig(
            name="test_declare_node_from_dict_reaches_the_target_state",
            environment=coco_env,
        ),
        _declare_article_from_dict,
        db,
    )
    app.update_blocking()

    assert len(captured) == 1
    m = captured[0]
    assert "insert Article" in m.expr
    assert m.params["s0_p_slug"] == "a2"
    assert m.params["s0_p_title"] == "World"
    assert m.params["s0_p_published"] == "2026-02-02"
    assert m.params["s0_p_coco_key"] == derive_coco_key(("a2",))


@dataclass
class _KeyOnlyNode:
    slug: str


@dataclass
class _EdgeProps:
    weight: int


async def _declare_supports_edge(db: coco.ContextKey[ConnectionFactory]) -> None:
    node_schema = await NodeSchema.from_class(_KeyOnlyNode, key="slug")
    source = await omnigraph.mount_node_target(db, "Source", node_schema)
    claim = await omnigraph.mount_node_target(db, "Claim", node_schema)
    edge_schema = await EdgeSchema.from_class(_EdgeProps)
    edge = await omnigraph.mount_edge_target(db, "Supports", source, claim, edge_schema)
    edge.declare_edge(from_id="s1", to_id="c1", record=_EdgeProps(weight=7))


def test_declare_edge_reaches_the_target_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """`coco_key` for an edge is derived from `(from_id, to_id)`, not from
    any property — verifies `declare_edge`'s own key construction, not just
    `_EdgeHandler.reconcile`'s (already covered elsewhere).

    A first-ever edge declaration through a real `coco.App` run reports
    `prev_may_be_missing=True`, so this lands as a "replace" (delete then
    insert, two commits) rather than a bare insert — see
    `_patch_cli_branch_calls`. Only the insert commit is asserted on."""
    _patch_cli_schema_calls(monkeypatch)
    _patch_cli_branch_calls(monkeypatch)
    captured = _capture_mutations(monkeypatch)
    db = _fresh_db("declare_edge")

    app = coco.App(
        coco.AppConfig(
            name="test_declare_edge_reaches_the_target_state", environment=coco_env
        ),
        _declare_supports_edge,
        db,
    )
    app.update_blocking()

    inserts = [m for m in captured if "insert Supports" in m.expr]
    assert len(inserts) == 1, captured
    m = inserts[0]
    assert m.params["s0_e_from"] == "s1"
    assert m.params["s0_e_to"] == "c1"
    assert m.params["s0_p_weight"] == 7
    assert m.params["s0_p_coco_key"] == derive_coco_key(("s1", "c1"))


@dataclass
class _Reading:
    sensor: str
    at: str
    v: float


async def _declare_reading(db: coco.ContextKey[ConnectionFactory]) -> None:
    schema = await NodeSchema.from_class(_Reading, key="at")
    target = await omnigraph.mount_node_target(db, "Reading", schema)
    target.declare_node(node=_Reading(sensor="s1", at="2026-01-01", v=1.5))


def test_declare_node_keys_on_the_schema_key_not_the_first_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`declare_node`'s `key = tuple(properties[k] for k in self._schema.key)`
    reads the key out of the record by the schema's declared key field. A
    wrong implementation — taking the first field, say — would still produce
    *a* key, just the wrong one, so `_Reading` is deliberately keyed on its
    SECOND field: `derive_coco_key(("2026-01-01",))` and
    `derive_coco_key(("s1",))` are different values."""
    _patch_cli_schema_calls(monkeypatch)
    captured = _capture_mutations(monkeypatch)
    db = _fresh_db("declare_node_key_field")

    app = coco.App(
        coco.AppConfig(name="test_declare_node_key_field", environment=coco_env),
        _declare_reading,
        db,
    )
    app.update_blocking()

    assert len(captured) == 1
    m = captured[0]
    assert "insert Reading" in m.expr
    assert m.params["s0_p_sensor"] == "s1"
    assert m.params["s0_p_at"] == "2026-01-01"
    assert m.params["s0_p_v"] == 1.5
    assert m.params["s0_p_coco_key"] == derive_coco_key(("2026-01-01",))
    assert m.params["s0_p_coco_key"] != derive_coco_key(("s1",))


@dataclass
class _Day:
    on: datetime.date
    note: str | None


async def _declare_a_day(db: coco.ContextKey[ConnectionFactory]) -> None:
    days = await omnigraph.mount_node_target(
        db, "Day", await NodeSchema.from_class(_Day, key="on")
    )
    days.declare_node(node=_Day(on=datetime.date(2026, 1, 5), note="n"))


class TestDateTimeKeyIdentity:
    """Omnigraph identifies a `DateTime`-keyed node by the instant, as epoch
    milliseconds: `12:00+00:00` and `14:00+02:00` are one node, sub-millisecond
    precision is dropped, and a naive value is read as UTC (all verified
    against the engine). Tracking by the ISO spelling gave one graph node two
    tracking keys; removing one declaration then deleted the node the other
    still declared, and its later unchanged updates left it missing."""

    AT = PropertyDef("at", "DateTime", ogt._isoformat)

    def test_two_offsets_of_one_instant_share_a_tracking_key(self) -> None:
        utc = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC)
        plus_two = datetime.datetime(
            2026, 1, 1, 14, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        )
        assert ogt._tracking_key_value(self.AT, utc) == ogt._tracking_key_value(
            self.AT, plus_two
        )
        assert ogt._tracking_key_value(self.AT, utc) == 1767268800000

    def test_a_naive_value_is_read_as_utc(self) -> None:
        naive = datetime.datetime(2026, 1, 1, 12, 0)  # noqa: DTZ001
        assert ogt._tracking_key_value(self.AT, naive) == 1767268800000

    def test_sub_millisecond_precision_is_dropped(self) -> None:
        a = datetime.datetime(2026, 1, 1, 12, 0, 0, 100, tzinfo=datetime.UTC)
        b = datetime.datetime(2026, 1, 1, 12, 0, 0, 900, tzinfo=datetime.UTC)
        c = datetime.datetime(2026, 1, 1, 12, 0, 0, 500000, tzinfo=datetime.UTC)
        assert ogt._tracking_key_value(self.AT, a) == ogt._tracking_key_value(
            self.AT, b
        )
        assert ogt._tracking_key_value(self.AT, a) != ogt._tracking_key_value(
            self.AT, c
        )

    def test_a_string_is_refused_for_a_datetime_key(self) -> None:
        """A hand-built `PropertyDef("at", "DateTime")` lets a dict row carry
        an ISO string, and a string is not normalised: `12:00+00:00` and
        `14:00+02:00` got separate tracking keys for one graph node again.
        The instant can only be derived from a `datetime`, so anything else
        is refused before any state is declared."""
        bare = PropertyDef("at", "DateTime")
        with pytest.raises(TypeError, match=r"key property 'at'.*datetime\.datetime"):
            ogt._tracking_key_value(bare, "2026-01-01T12:00:00+00:00")
        with pytest.raises(TypeError, match=r"key property 'at'.*datetime\.datetime"):
            ogt._tracking_key_value(bare, 1767268800000)

    def test_only_a_date_is_accepted_for_a_date_key(self) -> None:
        on = PropertyDef("on", "Date")
        with pytest.raises(TypeError, match=r"key property 'on'.*datetime\.date"):
            ogt._tracking_key_value(on, "2026-01-05")
        # A datetime is a date subclass, but its ISO form carries a time.
        with pytest.raises(TypeError, match=r"key property 'on'.*datetime\.date"):
            ogt._tracking_key_value(on, datetime.datetime(2026, 1, 5, 12))  # noqa: DTZ001

    def test_declare_node_refuses_a_string_for_a_datetime_key(self) -> None:
        target = _bare_node_target(
            NodeSchema(properties={"at": PropertyDef("at", "DateTime")}, key=("at",)),
            "Event",
        )
        with pytest.raises(TypeError, match=r"key property 'at'.*datetime\.datetime"):
            target.declare_node(node={"at": "2026-01-01T12:00:00+00:00"})

    def test_a_date_key_is_still_tracked_by_its_iso_form(self) -> None:
        on = PropertyDef("on", "Date", ogt._isoformat)
        assert ogt._tracking_key_value(on, datetime.date(2026, 1, 5)) == "2026-01-05"


def test_declare_node_keyed_by_a_date_tracks_the_encoded_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `Date` key is legal in `.pg`, and mounting one was allowed — but
    declaring a node passed the raw `datetime.date` into the target-state
    key, which the engine refused (`Unsupported StableKey Python type`).
    The key is tracked in its encoded (ISO) form, the same value the
    graph is sent, so identity and stored key can never disagree."""
    _patch_cli_schema_calls(monkeypatch)
    captured = _capture_mutations(monkeypatch)
    db = _fresh_db("date_key")

    app = coco.App(
        coco.AppConfig(name="test_declare_node_date_key", environment=coco_env),
        _declare_a_day,
        db,
    )
    app.update_blocking()

    (m,) = captured
    assert m.params["s0_p_on"] == "2026-01-05"
    assert m.params["s0_p_coco_key"] == derive_coco_key(("2026-01-05",))


@dataclass
class _AttendedRel:
    is_organizer: bool


def test_declare_edge_rejects_a_non_scalar_endpoint_id() -> None:
    """An endpoint is addressed by the node's single key value. A tuple (the
    shape a composite key would have had) has no `.pg` type at all, so it
    used to surface as a bare `no Omnigraph type mapping for <class 'tuple'>`
    out of `plan_commits`, with nothing naming the declaration."""
    target: omnigraph.EdgeTarget[Any, Any] = omnigraph.EdgeTarget(
        cast(Any, None),
        None,
        "ATTENDED",
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Person",
        ),
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Meeting",
        ),
    )
    with pytest.raises(TypeError, match=r"to_id=\('m', 1\) is not usable"):
        target.declare_edge(from_id="p", to_id=("m", 1))
    # A date maps to a `.pg` type but not to one the endpoint id rendering
    # can reproduce, so it is refused here for the same reason
    # `mount_edge_target` refuses a Date-keyed endpoint type.
    with pytest.raises(TypeError, match="single string or integer"):
        target.declare_edge(from_id="p", to_id=datetime.date(2026, 1, 5))


def test_declare_edge_rejects_an_endpoint_id_of_the_wrong_key_type() -> None:
    """An endpoint id is rendered with `str()` into the node's `id`. An
    integer given for a String-keyed endpoint would silently address
    whichever node's slug happens to be those digits, and a string given
    for an integer-keyed endpoint can never match a node at all — both are
    a mismatch against the endpoint type's declared key, caught here where
    the `declare_edge` call is on the stack."""
    target: omnigraph.EdgeTarget[Any, Any] = omnigraph.EdgeTarget(
        cast(Any, None),
        None,
        "ATTENDED",
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Person",
        ),
        _bare_node_target(
            NodeSchema(
                properties={"meeting_id": PropertyDef("meeting_id", "I64")},
                key=("meeting_id",),
            ),
            "Meeting",
        ),
    )
    with pytest.raises(TypeError, match=r"from_id=7 .*'Person'.*'slug'.*String"):
        target.declare_edge(from_id=7, to_id=1)
    with pytest.raises(TypeError, match=r"to_id='m1' .*'Meeting'.*'meeting_id'.*I64"):
        target.declare_edge(from_id="p", to_id="m1")


def test_declare_edge_without_a_schema_rejects_a_record() -> None:
    """A schema-less edge type declares no properties in `.pg`, and the
    engine refuses an insert naming one it doesn't have ("type `X` has no
    property `y`") — so a record here can never be written. It used to reach
    `_encode_properties` and die on a bare `KeyError` deep in the sink, long
    after the mistake was made.

    This is the one genuine semantic gap between Omnigraph and the Neo4j
    connector a ported app hits: Neo4j's MERGE creates relationship
    properties on the fly, so the same call works there.
    """
    target: omnigraph.EdgeTarget[_AttendedRel, Any] = omnigraph.EdgeTarget(
        cast(Any, None),
        None,
        "ATTENDED",
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Person",
        ),
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Meeting",
        ),
    )
    with pytest.raises(TypeError, match="mounted without a schema"):
        target.declare_edge(
            from_id="p", to_id="m", record=_AttendedRel(is_organizer=True)
        )


def test_declare_edge_without_a_record_rejects_a_non_nullable_schema() -> None:
    """The mirror of the guard above, and the gap it left open: a schema with
    no record.

    `declare_edge` checked "record but no schema" and not "schema but no
    record", so an edge type declaring non-nullable properties -- exactly the
    example app's `AttendedRel(is_organizer: bool)` -- planned `insert
    ATTENDED { from: ..., to: ..., coco_key: ... }` with every declared
    property missing. Omnigraph rejects that ("must provide non-nullable
    property"), naming neither this call nor the omitted argument.
    """
    schema = EdgeSchema(
        properties={"is_organizer": PropertyDef("is_organizer", "Bool")}
    )
    target: omnigraph.EdgeTarget[_AttendedRel, Any] = omnigraph.EdgeTarget(
        cast(Any, None),
        schema,
        "ATTENDED",
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Person",
        ),
        _bare_node_target(
            NodeSchema(
                properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
            ),
            "Meeting",
        ),
    )
    with pytest.raises(TypeError, match="is_organizer"):
        target.declare_edge(from_id="p", to_id="m")


# ---------------------------------------------------------------------------
# One vocabulary: node/edge. No `row=`, no `declare_record`, and no mount-time
# `key=` — the schema already carries the key.
# ---------------------------------------------------------------------------


def test_declare_node_takes_only_node() -> None:
    schema = NodeSchema(
        properties={"slug": PropertyDef("slug", "String")}, key=("slug",)
    )
    target = _bare_node_target(schema, "Foo")
    with pytest.raises(TypeError):
        target.declare_node()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        target.declare_node(row={"slug": "a"})  # type: ignore[call-arg]


async def _mount_with_redundant_key(db: coco.ContextKey[ConnectionFactory]) -> None:
    schema = await NodeSchema.from_class(_Article, key="slug")
    await omnigraph.mount_node_target(db, "Article", schema, key="slug")  # type: ignore[call-arg]


def test_mount_node_has_no_key_kwarg(monkeypatch: pytest.MonkeyPatch) -> None:
    """The schema already names the key; a second `key=` at the mount call
    site could only agree with it or be an error, so it is gone."""
    _patch_cli_schema_calls(monkeypatch)
    db = _fresh_db("mount_node_no_key_kwarg")

    app = coco.App(
        coco.AppConfig(name="test_mount_node_has_no_key_kwarg", environment=coco_env),
        _mount_with_redundant_key,
        db,
    )
    with pytest.raises(TypeError, match="key"):
        app.update_blocking()


# ---------------------------------------------------------------------------
# Live-engine tests: each "store" is a served graph, booted on first use by
# `omnigraph_server_fixture` (omnigraph-server 0.14.0 from test/bin/v0.14.0,
# git-ignored) with an empty schema, and stopped after the test. Gated on
# OMNIGRAPH_TEST_SERVER=1.
# ---------------------------------------------------------------------------

_live = live_server

#: The server behind each store path a live test touched, by path.
_SERVERS: dict[str, OmnigraphServer] = {}


@pytest.fixture(autouse=True)
def _stop_live_servers() -> Any:
    yield
    while _SERVERS:
        _SERVERS.popitem()[1].stop()


def _store_key(store: str | Path) -> str:
    return str(store).removeprefix("file://")


def _server(store: str | Path) -> OmnigraphServer:
    """The served graph standing in for `store`, booted on first use."""
    key = _store_key(store)
    if key not in _SERVERS:
        _SERVERS[key] = OmnigraphServer.start(Path(key + ".server"), schema_pg="")
    return _SERVERS[key]


def _live_conn(store_dir: str | Path) -> ConnectionFactory:
    return _server(store_dir).connection_factory()


def _deploy_live(store_dir: str | Path, schema_pg: str) -> None:
    r = _init_live(store_dir, schema_pg)
    assert r.returncode == 0, r.stderr


def _live_context(conn: ConnectionFactory) -> tuple[ContextProvider, str]:
    db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
    cp = ContextProvider()
    cp.provide(db, conn)
    return cp, db.key


def _init_live(
    store_dir: str | Path, schema_pg: str
) -> subprocess.CompletedProcess[str]:
    """Deploy `schema_pg` as the graph's schema and hand back the raw CLI
    result, so a test can assert on the engine's own refusal rather than on
    ours. `stderr` carries both streams: the CLI reports a refusal as JSON on
    stdout."""
    r = _server(store_dir).deploy(schema_pg, check=False)
    return subprocess.CompletedProcess(
        r.args, r.returncode, r.stdout, r.stdout + r.stderr
    )


def _mutate_live(
    store: str | Path, mutation: Query, *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """One mutation on `main`, as a CompletedProcess: 0 on success, else the
    status, with the server's error text in `stderr`."""
    r = _server(store).request(
        "POST",
        "/graphs/g/mutate",
        {"query": mutation.expr, "params": mutation.params, "branch": "main"},
    )
    body = json.dumps(r.body) if not isinstance(r.body, str) else r.body
    out = subprocess.CompletedProcess(
        ["mutate"],
        0 if r.status == 200 else r.status,
        body,
        "" if r.status == 200 else body,
    )
    if check:
        assert out.returncode == 0, out.stderr
    return out


@_live
class TestEngineSchemaLimitsLive:
    """The engine-side facts three Python-side guards rest on. Each guard
    exists only because the engine refuses the schema outright, so if a
    future Omnigraph release starts accepting one of these, the guard is the
    thing that's now wrong — and these tests are what say so."""

    def test_two_keys_are_rejected(self, tmp_path: Path) -> None:
        r = _init_live(
            tmp_path / "g.omni",
            (
                "node Reading {\n"
                "  sensor: String @key\n"
                "  at: String @key\n"
                "  coco_key: String\n"
                "}\n"
            ),
        )
        assert r.returncode != 0
        assert "multiple @key constraints" in r.stderr

    def test_an_id_property_is_accepted_and_an_underscore_name_refused(
        self, tmp_path: Path
    ) -> None:
        """Since 0.11 identity is the system field `@id`: `id` is an ordinary
        property, and names starting with `_` are the reserved ones."""
        ok = _init_live(
            tmp_path / "g.omni",
            (
                "node Meeting {\n  slug: String @key\n  id: I64?\n  coco_key: String\n}\n"
            ),
        )
        assert ok.returncode == 0, ok.stderr
        refused = _init_live(
            tmp_path / "g.omni",
            "node Meeting {\n  slug: String @key\n  id: I64?\n  _x: I64?\n"
            "  coco_key: String\n}\n",
        )
        assert refused.returncode != 0
        assert "_x" in refused.stderr

    def test_a_node_and_an_edge_may_share_a_name_but_only_one_is_addressable(
        self, tmp_path: Path
    ) -> None:
        """Both halves of the collision, from the engine itself.

        `schema apply` ACCEPTS `node Link` alongside `edge Link` — which is
        why `_find_type_block` must match on kind, or an edge's fragment
        overwrites the node's block. But a mutation resolves the bare name
        to the NODE type, so the edge is unwritable — which is why
        `_check_no_kind_clash` refuses to create the situation at all. If a
        future release changes either half, this is what says so.
        """
        store_dir = tmp_path / "g.omni"
        assert (
            _init_live(
                store_dir,
                (
                    "node A {\n  slug: String @key\n  coco_key: String\n}\n\n"
                    "node B {\n  slug: String @key\n  coco_key: String\n}\n\n"
                    "node Link {\n  slug: String @key\n  coco_key: String\n}\n\n"
                    "edge Link: A -> B {\n  coco_key: String\n}\n"
                ),
            ).returncode
            == 0
        )

        store = f"file://{store_dir}"
        _mutate_live(
            store,
            render_query(
                [
                    build_node_upsert(
                        "A", (PropertyValue("slug", "String", "a1"),), "ck-a"
                    ),
                    build_node_upsert(
                        "B", (PropertyValue("slug", "String", "b1"),), "ck-b"
                    ),
                ]
            ),
        )
        r = _mutate_live(
            store,
            render_query(
                [
                    build_edge_insert(
                        "Link",
                        PropertyValue("ref", "String", "a1"),
                        PropertyValue("ref", "String", "b1"),
                        (),
                        "ck-e",
                    )
                ]
            ),
            check=False,
        )
        assert r.returncode != 0
        assert "type `Link` has no property `from`" in r.stderr

    def test_key_only_stub_needs_every_non_nullable_property(
        self, tmp_path: Path
    ) -> None:
        """The premise behind `_validate_edge_endpoint`: the key-only stub
        the sink inserts for a missing edge endpoint cannot satisfy a
        non-nullable property outside the key, so referencing such a type as
        an endpoint has to be refused at mount time."""
        store_dir = tmp_path / "g.omni"
        assert (
            _init_live(
                store_dir,
                (
                    "node Meeting {\n"
                    "  slug: String @key\n"
                    "  note_file: String\n"
                    "  coco_key: String\n"
                    "}\n"
                ),
            ).returncode
            == 0
        )
        r = _mutate_live(
            store_dir,
            Query(
                "query m($p_slug: String, $p_coco_key: String) "
                "{ insert Meeting { slug: $p_slug, coco_key: $p_coco_key } }",
                {"p_slug": "m1", "p_coco_key": "ck1"},
            ),
            check=False,
        )
        assert r.returncode != 0
        assert "must provide non-nullable property `note_file`" in r.stderr


@_live
class TestFullCapCommitLive:
    """A commit at the full `_MAX_ENTITIES_PER_TYPE` must actually reach the
    engine.

    Nothing exercised this: every chunking test monkeypatches the cap DOWN
    (to 4), so the suite only ever built commits three orders of magnitude
    smaller than the one a real component produces. At the real cap the
    payload is ~1.3 MB of GQ plus ~1.0 MB of params, and passing those
    inline put both in argv — `OSError: [Errno 7] Argument list too long`
    on darwin, and on Linux the 128 KiB-per-argument limit would have
    failed the expression alone somewhere near 800 entities.

    Deliberately the real cap, not a scaled-down stand-in: the number that
    matters is the one the connector ships with, and the whole defect was
    that nobody multiplied it by the bytes per entity.
    """

    def test_a_sync_at_the_entity_cap_applies(self, tmp_path: Path) -> None:
        """8,192 upserts of one type in one sync reach the graph, each commit
        under the server's 1 MiB request limit (so through a scratch branch,
        as several commits)."""
        store_dir = tmp_path / "cap.omni"
        _deploy_live(
            store_dir,
            "node Source {\n  slug: String @key\n  title: String?\n  coco_key: String\n}\n",
        )
        n = ogt._MAX_ENTITIES_PER_TYPE

        async def sync() -> None:
            conn = _live_conn(store_dir)
            cp, db_key = _live_context(conn)
            key = _TypeKey(db_key, "node", "Source")
            actions: list[ogt._NodeAction | ogt._EdgeAction] = [
                ogt._NodeAction(
                    "upsert",
                    key,
                    "Source",
                    (
                        PropertyValue("slug", "String", f"s{i:06d}"),
                        PropertyValue("title", "String?", f"Title {i}"),
                    ),
                    derive_coco_key((f"s{i:06d}",)),
                )
                for i in range(n)
            ]
            commits = plan_commits(actions)
            assert len(commits) > 1
            for commit in commits:
                body = {"query": commit.expr, "params": commit.params, "branch": "main"}
                assert len(json.dumps(body).encode()) < 1_048_576
            try:
                await ogt._apply_entity_actions(cp, actions)
            finally:
                await conn.close()

        asyncio.run(sync())
        rows = _export_rows(store_dir)
        assert len([r for r in rows if r.get("type") == "Source"]) == n
        assert _branch_names(store_dir) == ["main"]


@_live
class TestMutationAtomicityLive:
    """A failed multi-statement `mutate` must apply NOTHING.

    Everything above rests on this and nothing pinned it. `render_query`
    merges N statements into one query precisely to get one commit, and
    `_mutate_with_endpoint_retry` re-runs the WHOLE commit after stubbing a
    missing endpoint — so if a failed invocation left its earlier statements
    applied, every edge before the failure point would be inserted a second
    time on the retry. Edge insert is strict and never deduplicates, and the
    duplicate would carry the same `coco_key` as the original, making it
    invisible to tracking and unreachable by `delete ... where coco_key = $x`
    (which removes one row). The three endpoint-retry tests each carry
    exactly one edge insert, so none of them can tell the two worlds apart.
    """

    @staticmethod
    def _seeded(tmp_path: Path) -> str:
        store = f"file://{tmp_path / 'atomic.omni'}"
        assert (
            _init_live(
                tmp_path / "atomic.omni",
                (
                    "node Source {\n  slug: String @key\n  title: String?\n"
                    "  coco_key: String\n}\n\n"
                    "node Claim {\n  slug: String @key\n  coco_key: String\n}\n\n"
                    "edge Supports: Source -> Claim {\n  coco_key: String\n}\n"
                ),
            ).returncode
            == 0
        )
        _mutate_live(
            store,
            render_query(
                [
                    build_node_upsert(
                        "Source", (PropertyValue("slug", "String", "s1"),), "ck-s1"
                    ),
                    build_node_upsert(
                        "Claim", (PropertyValue("slug", "String", "c1"),), "ck-c1"
                    ),
                ]
            ),
        )
        return store

    def test_earlier_statements_do_not_land_when_a_later_one_fails(
        self, tmp_path: Path
    ) -> None:
        store = self._seeded(tmp_path)
        before = _commit_count(store)

        good = build_edge_insert(
            "Supports",
            PropertyValue("ref", "String", "s1"),
            PropertyValue("ref", "String", "c1"),
            (),
            "ck-e1",
        )
        # Fails at execution time (not parse time) on a missing endpoint --
        # the same failure mode `_mutate_with_endpoint_retry` recovers from.
        doomed = build_edge_insert(
            "Supports",
            PropertyValue("ref", "String", "s1"),
            PropertyValue("ref", "String", "GHOST"),
            (),
            "ck-e2",
        )
        r = _mutate_live(store, render_query([good, doomed]), check=False)
        assert r.returncode != 0
        assert "not found in Claim" in r.stderr

        rows = _export_rows(store)
        assert [r for r in rows if r.get("type") == "Supports"] == []
        assert _commit_count(store) == before

    def test_a_failed_upsert_batch_leaves_prior_values_untouched(
        self, tmp_path: Path
    ) -> None:
        """The node half of the same property: a node upsert combined ahead
        of a failing statement must not have overwritten anything."""
        store = self._seeded(tmp_path)

        r = _mutate_live(
            store,
            render_query(
                [
                    build_node_upsert(
                        "Source",
                        (
                            PropertyValue("slug", "String", "s1"),
                            PropertyValue("title", "String?", "OVERWRITTEN"),
                        ),
                        "ck-s1",
                    ),
                    build_node_upsert(
                        "Claim", (PropertyValue("slug", "String", "c2"),), "ck-c2"
                    ),
                    build_edge_insert(
                        "Supports",
                        PropertyValue("ref", "String", "GHOST"),
                        PropertyValue("ref", "String", "c1"),
                        (),
                        "ck-e3",
                    ),
                ]
            ),
            check=False,
        )
        assert r.returncode != 0

        rows = _export_rows(store)
        assert [r["data"].get("title") for r in rows if r.get("type") == "Source"] == [
            None
        ]
        assert [r["data"].get("slug") for r in rows if r.get("type") == "Claim"] == [
            "c1"
        ]


@dataclass
class _SimpleNode:
    slug: str
    title: str | None


@_live
class TestApplyTypeActionsLive:
    @pytest.mark.asyncio
    async def test_a_repeated_create_after_an_interrupted_run_converges(
        self, tmp_path: Path
    ) -> None:
        """After an interrupted run the engine re-emits the type's write as
        an "upsert" (`prev_may_be_missing`). Deploying the same schema again
        must succeed and change nothing."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, conn)

        schema = await NodeSchema.from_class(_SimpleNode, key="slug")
        spec = _TypeSpec(
            schema=schema,
            key=schema.key,
            from_type=None,
            to_type=None,
            managed_by=ManagedBy.SYSTEM,
            owner="og",
        )
        key = _TypeKey(db.key, "node", "Simple")
        pg_fragment = schema.render("Simple", owner="og")

        create_action = ogt._TypeAction(
            key, spec, pg_fragment, "insert", {}, owner="og"
        )
        await ogt._apply_type_actions(cp, [create_action])
        assert isinstance(ogt._type_child_handler(cp, create_action), ogt._NodeHandler)
        first = _read_schema_source(store_dir)
        assert "node Simple {" in first

        upsert_action = ogt._TypeAction(
            key, spec, pg_fragment, "upsert", {}, owner="og"
        )
        await ogt._apply_type_actions(cp, [upsert_action])
        assert isinstance(ogt._type_child_handler(cp, upsert_action), ogt._NodeHandler)
        assert _read_schema_source(store_dir) == first

    @pytest.mark.asyncio
    async def test_a_second_type_is_merged_not_wiping_the_first(
        self, tmp_path: Path
    ) -> None:
        """The bug this reopening exists to fix, reproduced and then
        proven closed: Omnigraph's schema is applied whole-graph, not per
        type, so a naive per-type apply of the *second* type's fragment
        alone silently dropped the first. `init` a graph with one type,
        then drive a fresh `create` action for a second type — which Task
        6 always decides as `"create"` regardless of whether it's the
        graph's first type or its Nth — through the real sink, and assert
        `schema show` afterward has BOTH types. Fails hard against the old
        code: `init_graph` refuses an already-initialized store outright,
        and a bare `apply_schema` of just the second type's fragment wipes
        the first (both verified independently against the binary)."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        client = _GraphClient(conn)
        db = ContextKey[ConnectionFactory](f"test_db_{uuid.uuid4().hex}")
        cp = ContextProvider()
        cp.provide(db, conn)

        a_schema = await NodeSchema.from_class(_SimpleNode, key="slug")
        a_spec = _TypeSpec(
            schema=a_schema,
            key=a_schema.key,
            from_type=None,
            to_type=None,
            managed_by=ManagedBy.SYSTEM,
            owner="og",
        )
        await ogt._apply_type_actions(
            cp,
            [
                ogt._TypeAction(
                    _TypeKey(db.key, "node", "A"),
                    a_spec,
                    a_schema.render("A", owner="og"),
                    "insert",
                    {},
                    owner="og",
                ),
            ],
        )

        b_schema = await NodeSchema.from_class(_SimpleNode, key="slug")
        b_spec = _TypeSpec(
            schema=b_schema,
            key=b_schema.key,
            from_type=None,
            to_type=None,
            managed_by=ManagedBy.SYSTEM,
            owner="og",
        )
        b_action = ogt._TypeAction(
            _TypeKey(db.key, "node", "B"),
            b_spec,
            b_schema.render("B", owner="og"),
            "insert",
            {},
            owner="og",
        )
        await ogt._apply_type_actions(cp, [b_action])
        assert isinstance(ogt._type_child_handler(cp, b_action), ogt._NodeHandler)

        schema_source = await client.read_schema()
        assert schema_source is not None
        assert "node A {" in schema_source
        assert "node B {" in schema_source


def _export_rows(store_uri: str | Path) -> list[dict[str, Any]]:
    """Every node and edge on `main`, as the server exports them. Null-valued
    properties are absent from `data`."""
    r = _server(store_uri).request("POST", "/graphs/g/export", {"branch": "main"})
    assert r.status == 200, r
    if r.body is None:  # an empty graph exports an empty body
        return []
    text = r.body if isinstance(r.body, str) else json.dumps(r.body)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _basic_pg(source_extra: str = "") -> str:
    return (
        f"node Source {{\n  slug: String @key\n{source_extra}  coco_key: String\n}}\n\n"
        "node Claim {\n  slug: String @key\n  coco_key: String\n}\n\n"
        "edge Supports: Source -> Claim {\n  weight: I64\n  coco_key: String\n}"
    )


def _upsert(
    db_key: str, type_name: str, slug: str, extra: PropertyValue | None = None
) -> ogt._NodeAction:
    props = (PropertyValue("slug", "String", slug),) + ((extra,) if extra else ())
    return ogt._NodeAction(
        "upsert",
        _TypeKey(db_key, "node", type_name),
        type_name,
        props,
        derive_coco_key((slug,)),
    )


@_live
class TestEdgeIdempotenceLive:
    @pytest.mark.asyncio
    async def test_a_repeated_edge_insert_does_not_duplicate_the_edge(
        self, tmp_path: Path
    ) -> None:
        """A failed merged batch is retried, and a retry after a partial
        application re-sends edge inserts that already landed. System edge
        types are keyed on their endpoints, so the second insert updates."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        cp, db_key = _live_context(conn)
        _deploy_live(
            store_dir,
            "node Source {\n  slug: String @key\n  coco_key: String\n}\n\n"
            "node Claim {\n  slug: String @key\n  coco_key: String\n}\n\n"
            + render_edge_type("Supports", "Source", "Claim", [], owner="og")
            + "\n",
        )
        await ogt._apply_entity_actions(
            cp, [_upsert(db_key, "Source", "s1"), _upsert(db_key, "Claim", "c1")]
        )
        insert = ogt._EdgeAction(
            "insert",
            _TypeKey(db_key, "edge", "Supports"),
            "Supports",
            derive_coco_key(("s1", "c1")),
            "s1",
            "c1",
            (),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        await ogt._apply_entity_actions(cp, [insert])
        await ogt._apply_entity_actions(cp, [insert])
        edges = [r for r in _export_rows(store_dir) if r.get("edge") == "Supports"]
        assert len(edges) == 1
        await conn.close()


@_live
class TestServedMatrixLive:
    """The server-side failures a deployment meets (SPEC F2)."""

    @pytest.mark.asyncio
    async def test_a_graph_the_server_does_not_serve_fails_naming_it(
        self, tmp_path: Path
    ) -> None:
        server = _server(tmp_path / "g.omni")
        client = _GraphClient(server.connection_factory(graph_id="nope"))
        with pytest.raises(ogt.SchemaDeployError, match="'nope' is not served"):
            await ogt._apply_type_schema(
                client,
                [_type_action("insert", "A", "node A {\n  slug: String @key\n}")],
            )

    @pytest.mark.asyncio
    async def test_a_data_only_credential_cannot_deploy_schema(
        self, tmp_path: Path
    ) -> None:
        server = _server(tmp_path / "g.omni")
        client = _GraphClient(server.connection_factory(actor="act-data"))
        with pytest.raises(ogt.SchemaDeployError, match="config_manage"):
            await ogt._apply_type_schema(
                client,
                [_type_action("insert", "A", "node A {\n  slug: String @key\n}")],
            )
        assert server.schema_source() == ""
        assert (server.config_dir / "g.pg").read_text() == ""  # restored

    @pytest.mark.asyncio
    async def test_a_sync_cancelled_on_its_scratch_branch_leaves_no_branch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store_dir = tmp_path / "g.omni"
        _deploy_live(
            store_dir,
            "node Source {\n  slug: String @key\n  coco_key: String\n}\n",
        )
        conn = _live_conn(store_dir)
        cp, db_key = _live_context(conn)
        key = _TypeKey(db_key, "node", "Source")
        await ogt._apply_entity_actions(cp, [_upsert(db_key, "Source", "gone")])

        on_scratch = asyncio.Event()
        real_mutate = _GraphClient.mutate

        async def slow_on_scratch(
            self: _GraphClient, mutation: Query, *, branch: str
        ) -> None:
            await real_mutate(self, mutation, branch=branch)
            if branch != "main":
                on_scratch.set()
                await asyncio.sleep(3600)

        monkeypatch.setattr(_GraphClient, "mutate", slow_on_scratch)
        mixed: list[ogt._NodeAction | ogt._EdgeAction] = [
            _upsert(db_key, "Source", "new"),
            ogt._NodeAction("delete", key, "Source", (), derive_coco_key(("gone",))),
        ]
        task = asyncio.create_task(ogt._apply_entity_actions(cp, mixed))
        await on_scratch.wait()
        assert len(_branch_names(store_dir)) == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert _branch_names(store_dir) == ["main"]
        slugs = {r["data"].get("slug") for r in _export_rows(store_dir)}
        assert slugs == {"gone"}  # nothing of the cancelled sync landed
        await conn.close()

    def test_two_writers_on_separate_loops_both_land(self, tmp_path: Path) -> None:
        """Two writers that share no gate (separate event loops, as two
        processes would) push mixed batches through scratch branches at the
        same time; the server orders the merges and nothing is lost."""
        store_dir = tmp_path / "g.omni"
        _deploy_live(
            store_dir,
            "node Source {\n  slug: String @key\n  coco_key: String\n}\n",
        )

        def writer(prefix: str) -> None:
            async def run() -> None:
                conn = _live_conn(store_dir)
                cp, db_key = _live_context(conn)
                key = _TypeKey(db_key, "node", "Source")
                try:
                    for i in range(4):
                        await ogt._apply_entity_actions(
                            cp,
                            [
                                _upsert(db_key, "Source", f"{prefix}{i}"),
                                ogt._NodeAction(
                                    "delete",
                                    key,
                                    "Source",
                                    (),
                                    derive_coco_key((f"{prefix}gone{i}",)),
                                ),
                            ],
                        )
                finally:
                    await conn.close()

            try:
                asyncio.run(run())
            except BaseException as e:  # surfaced below, not lost in the thread
                errors.append(e)

        errors: list[BaseException] = []
        threads = [threading.Thread(target=writer, args=(p,)) for p in ("a", "b")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        slugs = sorted(r["data"].get("slug") for r in _export_rows(store_dir))
        assert slugs == [f"{p}{i}" for p in ("a", "b") for i in range(4)]
        assert _branch_names(store_dir) == ["main"]

    @pytest.mark.asyncio
    async def test_a_merge_conflict_fails_and_leaves_no_branch(
        self, tmp_path: Path
    ) -> None:
        store_dir = tmp_path / "g.omni"
        _deploy_live(
            store_dir,
            "node Source {\n  slug: String @key\n  title: String?\n  coco_key: String\n}\n",
        )
        client = _GraphClient(_live_conn(store_dir))
        seed = 'query a() { insert Source { slug: "s", title: "base", coco_key: "k" } }'
        await client.mutate(Query(seed, {}), branch="main")
        with pytest.raises(_og.ConflictError) as info:
            async with ogt._scratch_branch(client, frm="main") as scratch:
                await client.mutate(
                    Query(
                        'query u() { update Source set { title: "scratch" } where slug = "s" }',
                        {},
                    ),
                    branch=scratch,
                )
                await client.mutate(
                    Query(
                        'query u() { update Source set { title: "main" } where slug = "s" }',
                        {},
                    ),
                    branch="main",
                )
                await client.branch_merge(scratch, into="main")
        assert info.value.merge_conflicts[0]["kind"] == "divergent_update"
        assert _branch_names(store_dir) == ["main"]
        await client.conn.close()


@_live
class TestServedTopologiesLive:
    """SPEC F2.2, F2.3, F2.7: least privilege, two graphs on one server, and a
    write whose response is lost."""

    def test_least_privilege_credentials_run_a_whole_lifecycle(
        self, store: str
    ) -> None:
        """Data as an actor with only `read`, `change`, `branch_*`; schema as
        one with only `config_manage`, `read`, `schema_apply` (probe P10).
        Create, a mixed sync through a scratch branch, and an app drop."""
        db = ContextKey[ConnectionFactory](f"e2e_lp_{uuid.uuid4().hex}")
        coco_env.context_provider.provide(
            db, _server(store).connection_factory(actor=DATA, cluster_actor=SCHEMA)
        )
        state = {"sources": ["a", "b"]}

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
            )
            claims.declare_node(node=_ScClaim(slug="c1"))
            for slug in state["sources"]:
                sources.declare_node(node=_ScSourceNarrow(slug=slug, title=slug))
                supports.declare_edge(
                    from_id=slug, to_id="c1", record=_ScEdgeProps(weight=1)
                )

        app = coco.App(
            coco.AppConfig(
                name=f"e2e_least_priv_{uuid.uuid4().hex[:8]}", environment=coco_env
            ),
            main,
        )
        app.update_blocking()
        assert len([r for r in _export_rows(store) if r.get("edge") == "Supports"]) == 2

        state["sources"] = ["a", "x"]  # an upsert and a delete: a scratch branch
        app.update_blocking()
        slugs = sorted(
            r["data"].get("slug")
            for r in _export_rows(store)
            if r.get("type") == "Source"
        )
        assert slugs == ["a", "x"]
        assert _branch_names(store) == ["main"]

        app.drop_blocking()
        assert _export_rows(store) == []
        assert "node Source" not in _read_schema_source(store)

    @pytest.mark.asyncio
    async def test_two_graphs_on_one_server_keep_their_schemas_apart(
        self, tmp_path: Path
    ) -> None:
        """Two graphs, one cluster configuration. Concurrent schema writes to
        both are serialized by the configuration lock, each plan touches only
        its own graph, and each graph ends up with only its own types."""
        server = OmnigraphServer.start(
            tmp_path / "two", schema_pg="", graph_ids=("g", "h")
        )
        try:
            g = _GraphClient(server.connection_factory(graph_id="g"))
            h = _GraphClient(server.connection_factory(graph_id="h"))
            await asyncio.gather(
                ogt._apply_type_schema(
                    g,
                    [
                        _type_action(
                            "insert", "OnlyG", "node OnlyG {\n  slug: String @key\n}"
                        )
                    ],
                ),
                ogt._apply_type_schema(
                    h,
                    [
                        _type_action(
                            "insert", "OnlyH", "node OnlyH {\n  slug: String @key\n}"
                        )
                    ],
                ),
            )
            assert "OnlyG" in server.schema_source("g")
            assert "OnlyH" not in server.schema_source("g")
            assert "OnlyH" in server.schema_source("h")
            assert "OnlyG" not in server.schema_source("h")

            await h.mutate(
                Query('query a() { insert OnlyH { slug: "h1" } }', {}), branch="main"
            )
            exported = server.request("POST", "/graphs/g/export", {"branch": "main"})
            assert not exported.body  # g has no rows
            await g.conn.close()
            await h.conn.close()
        finally:
            server.stop()

    def test_a_schema_change_waits_out_another_writers_scratch_branch(
        self, store: str
    ) -> None:
        """Writer A (its own event loop, so no shared gate — as another
        process) holds a scratch branch for two seconds; B's schema change is
        refused by the server meanwhile and retried until A merges."""
        server = _server(store)
        _deploy_live(
            store, "node Source {\n  slug: String @key\n  coco_key: String\n}\n"
        )
        holding = threading.Event()
        errors: list[BaseException] = []

        def writer_a() -> None:
            async def run() -> None:
                client = _GraphClient(server.connection_factory())
                async with ogt._scratch_branch(client, frm="main") as name:
                    holding.set()
                    await asyncio.sleep(2)
                    await client.branch_merge(name, into="main")
                await client.conn.close()

            try:
                asyncio.run(run())
            except BaseException as e:
                errors.append(e)

        thread = threading.Thread(target=writer_a)
        thread.start()
        assert holding.wait(30)
        started = time.monotonic()

        async def deploy_b() -> None:
            client = _GraphClient(server.connection_factory())
            await ogt._apply_type_schema(
                client,
                [
                    _type_action(
                        "insert", "Extra", "node Extra {\n  slug: String @key\n}"
                    )
                ],
            )
            await client.conn.close()

        asyncio.run(deploy_b())
        thread.join()
        assert not errors
        assert time.monotonic() - started > 1  # it had to wait
        assert "node Extra" in _read_schema_source(store)
        assert _branch_names(store) == ["main"]

    def test_a_write_whose_response_is_lost_is_reconciled_by_the_next_run(
        self, store: str
    ) -> None:
        """The proxy forwards one `/mutate` and drops the connection before
        the answer: the write landed, the client only knows the outcome is
        unknown. That sync fails without advancing tracking, and the next run
        converges on exactly one node."""
        server = _server(store)
        proxy = LossyProxy(server.base_url)
        try:
            db = ContextKey[ConnectionFactory](f"e2e_lost_{uuid.uuid4().hex}")
            coco_env.context_provider.provide(
                db, server.connection_factory(base_url=proxy.url)
            )
            title = {"value": "first"}

            async def main() -> None:
                sources = await omnigraph.mount_node_target(
                    db,
                    "Source",
                    await NodeSchema.from_class(_ScSourceNarrow, key="slug"),
                )
                sources.declare_node(
                    node=_ScSourceNarrow(slug="s", title=title["value"])
                )

            app = coco.App(
                coco.AppConfig(
                    name=f"e2e_lost_{uuid.uuid4().hex[:8]}", environment=coco_env
                ),
                main,
            )
            proxy.drop_next("/mutate")
            with pytest.raises(_og.NetworkError) as info:
                app.update_blocking()
            assert proxy.dropped == ["/graphs/g/mutate"]
            assert info.value.request_dispatched and info.value.outcome_unknown
            landed = [r for r in _export_rows(store) if r.get("type") == "Source"]
            assert [r["data"].get("title") for r in landed] == ["first"]

            app.update_blocking()
            title["value"] = "second"
            app.update_blocking()
            rows = [r for r in _export_rows(store) if r.get("type") == "Source"]
            assert [r["data"].get("title") for r in rows] == ["second"]
        finally:
            proxy.close()


@_live
class TestReplaceOrderingLive:
    @pytest.mark.asyncio
    async def test_replace_leaves_exactly_one_edge_with_new_value(
        self, tmp_path: Path
    ) -> None:
        """The check that would have caught the original draft's ordering
        bug: if phase A (the replaced edge's delete) ran after phase B (its
        re-insert) instead of before, this would see zero edges survive —
        insert-then-delete on one coco_key removes both."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        store_uri = f"file://{store_dir}"
        cp, db_key = _live_context(conn)
        _deploy_live(store_dir, _basic_pg())

        # Both endpoints already exist, so the edge insert below needs no
        # retry — that path is exercised separately in TestEndpointRetryLive.
        await ogt._apply_entity_actions(
            cp, [_upsert(db_key, "Source", "s1"), _upsert(db_key, "Claim", "c1")]
        )

        edge_key = _TypeKey(db_key, "edge", "Supports")
        edge_coco_key = derive_coco_key(("s1", "c1"))

        insert_action = ogt._EdgeAction(
            "insert",
            edge_key,
            "Supports",
            edge_coco_key,
            "s1",
            "c1",
            (PropertyValue("weight", "I64", 1),),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        await ogt._apply_entity_actions(cp, [insert_action])

        rows = _export_rows(store_uri)
        edges = [r for r in rows if r.get("edge") == "Supports"]
        assert len(edges) == 1
        assert edges[0]["data"].get("weight") == 1

        replace_action = ogt._EdgeAction(
            "replace",
            edge_key,
            "Supports",
            edge_coco_key,
            "s1",
            "c1",
            (PropertyValue("weight", "I64", 2),),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        commits = plan_commits([replace_action])
        assert len(commits) == 2  # phase A (delete) + phase B (insert)
        await ogt._apply_entity_actions(cp, [replace_action])

        rows = _export_rows(store_uri)
        edges = [r for r in rows if r.get("edge") == "Supports"]
        assert len(edges) == 1
        assert edges[0]["data"].get("weight") == 2


@_live
class TestEndpointRetryLive:
    @pytest.mark.asyncio
    async def test_edge_insert_retries_after_creating_missing_endpoint(
        self, tmp_path: Path
    ) -> None:
        """An edge referencing an endpoint no other component has written
        yet must still succeed: the sink builds a stub for just that
        endpoint after the engine reports it missing, then retries. Only
        one endpoint (Source) is missing here, so this only needs the
        first of the two retries the sink budgets — see
        test_edge_insert_retries_after_creating_both_missing_endpoints for
        the case that needs both."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        store_uri = f"file://{store_dir}"
        cp, db_key = _live_context(conn)
        _deploy_live(store_dir, _basic_pg())

        await ogt._apply_entity_actions(cp, [_upsert(db_key, "Claim", "c1")])

        edge_key = _TypeKey(db_key, "edge", "Supports")
        insert_action = ogt._EdgeAction(
            "insert",
            edge_key,
            "Supports",
            derive_coco_key(("s1", "c1")),
            "s1",
            "c1",
            (PropertyValue("weight", "I64", 1),),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        await ogt._apply_entity_actions(cp, [insert_action])

        rows = _export_rows(store_uri)
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1
        sources = [r for r in rows if r.get("type") == "Source"]
        assert len(sources) == 1
        assert sources[0]["data"].get("slug") == "s1"

    @pytest.mark.asyncio
    async def test_edge_insert_does_not_wipe_existing_nullable_property(
        self, tmp_path: Path
    ) -> None:
        """The regression this redesign exists to fix. Both endpoints
        already exist, so the edge insert must succeed on the FIRST try,
        with no stub ever touching Source — and Source's own nullable
        `title`, already written by its owning component, must survive.
        This fails against the old unconditional-stub design, which nulled
        `title` out on every edge insert (verified against the engine)."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        store_uri = f"file://{store_dir}"
        cp, db_key = _live_context(conn)
        _deploy_live(store_dir, _basic_pg(source_extra="  title: String?\n"))

        await ogt._apply_entity_actions(
            cp,
            [
                _upsert(
                    db_key,
                    "Source",
                    "s1",
                    PropertyValue("title", "String", "Real Title"),
                ),
                _upsert(db_key, "Claim", "c1"),
            ],
        )

        edge_key = _TypeKey(db_key, "edge", "Supports")
        insert_action = ogt._EdgeAction(
            "insert",
            edge_key,
            "Supports",
            derive_coco_key(("s1", "c1")),
            "s1",
            "c1",
            (PropertyValue("weight", "I64", 1),),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        await ogt._apply_entity_actions(cp, [insert_action])

        rows = _export_rows(store_uri)
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1
        sources = [r for r in rows if r.get("type") == "Source"]
        assert len(sources) == 1
        assert sources[0]["data"].get("title") == "Real Title"

    @pytest.mark.asyncio
    async def test_edge_insert_retries_after_creating_both_missing_endpoints(
        self, tmp_path: Path
    ) -> None:
        """Both endpoints absent is ordinary, not exotic: CocoIndex runs up
        to 1024 components concurrently, so an edge component can easily
        precede both of the components that own its endpoints. The engine
        reports only the first missing endpoint per attempt (`src` before
        `dst`), so a single retry isn't enough — this needs both of the
        sink's two retries. Fails against a single-retry implementation:
        the first retry fixes `src`, the second attempt then fails on
        `dst`, and that would propagate instead of getting its own stub."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        store_uri = f"file://{store_dir}"
        cp, db_key = _live_context(conn)
        _deploy_live(store_dir, _basic_pg())

        # Neither Source nor Claim has been written by any other component.
        edge_key = _TypeKey(db_key, "edge", "Supports")
        insert_action = ogt._EdgeAction(
            "insert",
            edge_key,
            "Supports",
            derive_coco_key(("s1", "c1")),
            "s1",
            "c1",
            (PropertyValue("weight", "I64", 1),),
            "Source",
            "Claim",
            PropertyDef("slug", "String"),
            PropertyDef("slug", "String"),
        )
        await ogt._apply_entity_actions(cp, [insert_action])

        rows = _export_rows(store_uri)
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1
        assert len([r for r in rows if r.get("type") == "Source"]) == 1
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1

    @pytest.mark.asyncio
    async def test_two_edges_with_four_missing_endpoints(self, tmp_path: Path) -> None:
        """One commit carrying two edges whose four endpoints are all absent:
        the engine reports one missing endpoint per attempt, so this takes
        four stub rounds. A fixed two-round budget failed on the third."""
        store_dir = tmp_path / "g.omni"
        conn = _live_conn(store_dir)
        store_uri = f"file://{store_dir}"
        cp, db_key = _live_context(conn)
        _deploy_live(store_dir, _basic_pg())

        edge_key = _TypeKey(db_key, "edge", "Supports")
        actions = [
            ogt._EdgeAction(
                "insert",
                edge_key,
                "Supports",
                derive_coco_key((s, c)),
                s,
                c,
                (PropertyValue("weight", "I64", 1),),
                "Source",
                "Claim",
                PropertyDef("slug", "String"),
                PropertyDef("slug", "String"),
            )
            for s, c in (("s1", "c1"), ("s2", "c2"))
        ]
        await ogt._apply_entity_actions(cp, actions)

        rows = _export_rows(store_uri)
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 2
        assert sorted(
            r["data"].get("slug") for r in rows if r.get("type") == "Source"
        ) == [
            "s1",
            "s2",
        ]
        assert sorted(
            r["data"].get("slug") for r in rows if r.get("type") == "Claim"
        ) == [
            "c1",
            "c2",
        ]
        assert _branch_names(store_uri) == ["main"]


@_live
class TestScratchBranchLivenessLive:
    @pytest.mark.asyncio
    async def test_a_schema_deployment_waits_for_a_live_scratch_branch(
        self, tmp_path: Path
    ) -> None:
        """A holds a scratch branch; B, through another factory for the same
        graph, deploys a schema change. The server refuses schema changes
        while a non-main branch lives, so B must wait for A rather than
        reap A's live branch, and A's own merge must still find it."""
        store_dir = tmp_path / "g.omni"
        store_uri = f"file://{store_dir}"
        a = _GraphClient(_live_conn(store_dir))
        b = _GraphClient(_live_conn(store_dir))
        _deploy_live(store_dir, _basic_pg())

        entered = asyncio.Event()
        release = asyncio.Event()

        async def hold_a_scratch_branch() -> None:
            async with ogt._scratch_branch(a, frm="main") as name:
                entered.set()
                await release.wait()
                await a.branch_merge(name, into="main")

        holder = asyncio.create_task(hold_a_scratch_branch())
        await entered.wait()
        assert len(_branch_names(store_uri)) == 2

        applier = asyncio.create_task(
            ogt._apply_type_schema(
                b,
                [
                    _type_action(
                        "insert",
                        "Extra",
                        "node Extra {\n  slug: String @key\n  coco_key: String\n}",
                    )
                ],
            )
        )
        await asyncio.sleep(1.0)
        assert not applier.done(), "B ran ahead instead of waiting for A's branch"
        assert len(_branch_names(store_uri)) == 2  # A's branch was not reaped

        release.set()
        await holder  # A's merge found its branch and deleted it
        await applier
        assert _branch_names(store_uri) == ["main"]
        assert "node Extra" in _read_schema_source(store_uri)


# ---------------------------------------------------------------------------
# End-to-end acceptance tests: drive a real coco.App against a real store,
# the way a user actually would, rather than calling _apply_entity_actions
# or _apply_type_actions directly (as the live tests above do). Every
# scenario declares Source, Claim, and (except where noted) a Supports edge
# TOGETHER in the same run, never a single type in isolation -- the two
# worst defects found while building this connector were both invisible to
# single-type coverage: a keyed insert that turned out to be a full-record
# replace (silently wiping a node's other properties via an endpoint stub),
# and a whole-graph schema apply (a second type's apply alone wiping the
# first). Both only show up once more than one type is in play at once.
# ---------------------------------------------------------------------------


@dataclass
class _ScSourceNarrow:
    slug: str
    # `title` is nullable so Source stays usable as an edge endpoint (see
    # test_mount_edge_rejects_unstubbable_endpoint above): the endpoint
    # stub the sink builds when an edge races ahead of its node's own
    # component can only ever populate the key, so any other non-nullable
    # property makes the type unstubbable.
    title: str | None


@dataclass
class _ScSourceWide:
    slug: str
    title: str | None
    note: str | None


@dataclass
class _ScClaim:
    slug: str


@dataclass
class _ScEdgeProps:
    weight: int


@dataclass
class _ScMeeting:
    """An INTEGER-keyed endpoint, the shape the meeting-notes example uses
    (a generated numeric meeting id). Every other e2e type here is
    string-keyed, and the endpoint reference bug this covers was invisible
    to all of them."""

    meeting_id: int
    note: str | None


@dataclass
class _KcSource:
    """Dedicated to test_key_change_rebuilds: `title` must be non-nullable
    here so it's legal to use as the new `@key` (NodeSchema.from_class
    rejects a nullable key field) -- unlike _ScSourceNarrow, this type is
    never used as an edge endpoint, so it doesn't need the nullability the
    stub-compatibility guard would otherwise require."""

    slug: str
    title: str


def _e2e_db(store: str, label: str) -> ContextKey[ConnectionFactory]:
    db = ContextKey[ConnectionFactory](f"e2e_{label}_{uuid.uuid4().hex}")
    coco_env.context_provider.provide(db, _live_conn(store))
    return db


def _commit_count(store: str | Path, branch: str = "main") -> int:
    r = _server(store).request("GET", f"/graphs/g/commits?branch={branch}&limit=1000")
    assert r.status == 200, r
    return len(r.body["commits"])


def _read_schema_source(store: str | Path) -> str:
    return _server(store).schema_source()


def _branch_names(store: str | Path) -> list[str]:
    r = _server(store).request("GET", "/graphs/g/branches")
    assert r.status == 200, r
    return list(r.body["branches"])


@pytest.fixture
def store(tmp_path: Path) -> str:
    """Names a served graph with an empty schema, booted on first use: the
    connector's own type sink deploys the first type, exactly as on a real
    user's first `app.update()`."""
    return f"file://{tmp_path / 'e2e.omni'}"


@_live
class TestEndToEnd:
    """Seven acceptance cases, one per design decision in the spec. Each
    drives a real `coco.App` -- reused across repeated `update_blocking()`
    calls exactly as a real app would be -- and asserts against the store
    via `_export_rows`/`_commit_count`/`_branch_names`, never against the
    connector's own internal types."""

    def test_preview_plans_without_writing(self, store: str) -> None:
        """`preview=True` lists the type, node, and edge actions and writes
        nothing: before the first update the store does not exist, and after
        it a previewed change leaves the commit count alone."""
        db = _e2e_db(store, "preview")
        title = {"value": "A"}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)
            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title=title["value"]))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(coco.AppConfig(name="e2e_preview", environment=coco_env), main)

        actions = app.update_blocking(preview=True)
        assert actions is not None
        kinds = sorted(type(a).__name__ for a in actions)
        assert kinds == [
            "_EdgeAction",
            "_NodeAction",
            "_NodeAction",
            "_TypeAction",
            "_TypeAction",
            "_TypeAction",
        ]
        assert _read_schema_source(store) == ""

        app.update_blocking()
        commits = _commit_count(store)

        title["value"] = "B"
        actions = app.update_blocking(preview=True)
        assert actions is not None
        (changed,) = [a for a in actions if isinstance(a, ogt._NodeAction)]
        assert changed.op == "upsert"
        assert _commit_count(store) == commits
        (source,) = [r for r in _export_rows(store) if r.get("type") == "Source"]
        assert source["data"].get("title") == "A"

    def test_property_added_is_additive(self, store: str) -> None:
        """Re-mount Source with a wider dataclass; the already-written
        node's existing property must survive the alter, and Claim/Supports
        -- declared in the SAME run but otherwise untouched -- must come
        through unaffected, which is exactly what a whole-graph-schema
        regression would break."""
        db = _e2e_db(store, "prop_added")
        wide = {"on": False}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(
                _ScSourceWide if wide["on"] else _ScSourceNarrow, key="slug"
            )
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            node: Any = (
                _ScSourceWide(slug="a", title="A", note=None)
                if wide["on"]
                else _ScSourceNarrow(slug="a", title="A")
            )
            sources.declare_node(node=node)
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(
            coco.AppConfig(name="e2e_prop_added", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        assert [r["data"].get("title") for r in rows if r.get("type") == "Source"] == [
            "A"
        ]
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1

        wide["on"] = True
        app.update_blocking()

        rows = _export_rows(store)
        (source,) = [r for r in rows if r.get("type") == "Source"]
        assert source["data"].get("title") == "A"  # old value survived the alter
        assert source["data"].get("note") is None  # new column present
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1  # untouched
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1  # untouched

    def test_property_dropped_forces_reupsert(self, store: str) -> None:
        """Dropping a property is lossy: the design says every existing
        node of that type must be re-upserted, not merely the type's own
        schema altered. Two Source rows, so a bug that rewrites zero (or
        only one) of them is distinguishable from rewriting both -- proven
        via the commit count, not just the resulting values, since
        Omnigraph's soft-drop already hides a dropped property from
        `export` on its own, so row contents alone can't tell "dropped and
        re-upserted" apart from "dropped and left alone"."""
        db = _e2e_db(store, "prop_dropped")
        narrow = {"on": False}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(
                _ScSourceNarrow if narrow["on"] else _ScSourceWide, key="slug"
            )
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            for slug, title, note in [("a", "A", "nA"), ("b", "B", "nB")]:
                node: Any = (
                    _ScSourceNarrow(slug=slug, title=title)
                    if narrow["on"]
                    else _ScSourceWide(slug=slug, title=title, note=note)
                )
                sources.declare_node(node=node)
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(
            coco.AppConfig(name="e2e_prop_dropped", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        sources_before = {
            r["data"].get("slug"): r["data"] for r in rows if r.get("type") == "Source"
        }
        assert sources_before["a"]["note"] == "nA"
        assert sources_before["b"]["note"] == "nB"

        before = _commit_count(store)
        narrow["on"] = True
        app.update_blocking()
        after = _commit_count(store)

        # One commit for Source's own schema alter (drops the column), one
        # for the batched re-upsert of both rows -- upserts of the same
        # type always land in a single commit (see
        # test_upsert_only_is_one_commit), so this delta is exact: if
        # either row were skipped, or if only the schema changed without a
        # re-upsert, the delta would be 1, not 2.
        assert after - before == 2

        rows = _export_rows(store)
        sources_after = {
            r["data"].get("slug"): r["data"] for r in rows if r.get("type") == "Source"
        }
        assert set(sources_after) == {"a", "b"}
        assert (
            sources_after["a"]["title"] == "A"
            and sources_after["a"].get("note") is None
        )
        assert (
            sources_after["b"]["title"] == "B"
            and sources_after["b"].get("note") is None
        )
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1  # untouched
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1  # untouched

    def test_key_change_rebuilds(self, store: str) -> None:
        """`@key` changing is destructive: the type action is decided as a
        `replace` specifically because an in-place `alter` can't legally
        change which property is the key, and existing nodes must be
        re-declared under the new key rather than left as orphaned rows
        nothing tracks. Claim and a Claim -> Claim `Cites` edge are
        declared alongside Source and must survive the replace untouched."""
        db = _e2e_db(store, "key_change")
        keyed_by_title = {"on": False}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(
                _KcSource, key=("title" if keyed_by_title["on"] else "slug")
            )
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            cites = await omnigraph.mount_edge_target(
                db, "Cites", claims, claims, edge_schema
            )
            sources.declare_node(node=_KcSource(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            claims.declare_node(node=_ScClaim(slug="c2"))
            cites.declare_edge(from_id="c1", to_id="c2", record=_ScEdgeProps(weight=1))

        app = coco.App(
            coco.AppConfig(name="e2e_key_change", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        (before_source,) = [r for r in rows if r.get("type") == "Source"]
        assert before_source["data"].get("slug") == "a"
        assert before_source["data"].get("title") == "A"

        keyed_by_title["on"] = True
        app.update_blocking()

        rows = _export_rows(store)
        sources_after = [r for r in rows if r.get("type") == "Source"]
        assert len(sources_after) == 1  # rebuilt in place, not duplicated
        (after_source,) = sources_after
        assert after_source["data"].get("slug") == "a"
        assert after_source["data"].get("title") == "A"
        assert after_source["data"].get("coco_key") != before_source["data"].get(
            "coco_key"
        )

        assert len([r for r in rows if r.get("type") == "Claim"]) == 2  # untouched
        assert len([r for r in rows if r.get("edge") == "Cites"]) == 1  # untouched

    def test_edge_repointed(self, store: str) -> None:
        """Edges have no update mutation -- a changed edge is delete-then-
        insert on the same coco_key (plan_commits' phase ordering). Point
        a -> c1, then repoint the same source to c2: exactly one edge must
        survive, pointing at the new target, not two edges and not the
        deleted one."""
        db = _e2e_db(store, "edge_repointed")
        target = {"claim": "c1"}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            claims.declare_node(node=_ScClaim(slug="c2"))
            supports.declare_edge(
                from_id="a", to_id=target["claim"], record=_ScEdgeProps(weight=1)
            )

        app = coco.App(
            coco.AppConfig(name="e2e_edge_repointed", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        (edge,) = [r for r in rows if r.get("edge") == "Supports"]
        assert edge["to"] == "c1"

        target["claim"] = "c2"
        app.update_blocking()

        rows = _export_rows(store)
        edges = [r for r in rows if r.get("edge") == "Supports"]
        assert len(edges) == 1
        assert edges[0]["from"] == "a" and edges[0]["to"] == "c2"
        assert (
            len([r for r in rows if r.get("type") == "Claim"]) == 2
        )  # both endpoints remain

    def test_node_deleted_cascades_to_its_edges(self, store: str) -> None:
        """Undeclaring a node together with the edges that reference it
        removes both (a node undeclared while an edge still references it
        is another matter: test_undeclaring_a_referenced_node_keeps_its_edge).
        This isn't ordinary CocoIndex parent-child cleanup -- Source and
        Supports are independent target-state trees, so nothing
        automatically knows an edge references a node from a SEPARATE
        component going away. It's `plan_commits`' own edge-before-node
        delete ordering (see test_edge_deletes_precede_node_deletes) that
        makes this safe against the real engine, which this proves live:
        two Source nodes, each with an edge to the same Claim, so deleting
        one leaves the other's node and edge provably untouched."""
        db = _e2e_db(store, "node_deleted")
        keep_b = {"on": True}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            claims.declare_node(node=_ScClaim(slug="c1"))
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )
            if keep_b["on"]:
                sources.declare_node(node=_ScSourceNarrow(slug="b", title="B"))
                supports.declare_edge(
                    from_id="b", to_id="c1", record=_ScEdgeProps(weight=2)
                )

        app = coco.App(
            coco.AppConfig(name="e2e_node_deleted", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        assert len([r for r in rows if r.get("type") == "Source"]) == 2
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 2

        keep_b["on"] = False
        app.update_blocking()

        rows = _export_rows(store)
        sources_after = [r for r in rows if r.get("type") == "Source"]
        assert [r["data"].get("slug") for r in sources_after] == ["a"]
        edges_after = [r for r in rows if r.get("edge") == "Supports"]
        assert len(edges_after) == 1
        assert edges_after[0]["from"] == "a" and edges_after[0]["to"] == "c1"

    def test_undeclaring_a_referenced_node_keeps_its_edge(self, store: str) -> None:
        """The edge lives in one component and its source node in another.
        The node's component stops declaring the node, then declares it
        again, while the edge's component declares the edge unchanged
        throughout. Deleting the node cascaded to the edge in the graph
        while the edge's tracking still said it was there, so when the
        node came back the edge's reconcile had nothing to do, and the
        graph ended with both endpoints and no edge. A node an edge still
        references is reduced to a key-only stub instead — the shape an
        edge arriving before its node leaves behind — and the owner's next
        declaration fills it back in."""
        db = _e2e_db(store, "referenced_node")
        declare_a = {"on": True}

        @coco.fn
        async def declare_source(sources: omnigraph.NodeTarget[Any]) -> None:
            if declare_a["on"]:
                sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))

        @coco.fn
        async def declare_support(
            supports: omnigraph.EdgeTarget[Any], claims: omnigraph.NodeTarget[Any]
        ) -> None:
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
            )
            await coco.mount(declare_source, sources)
            await coco.mount(declare_support, supports, claims)

        def graph() -> tuple[list[tuple[str, str | None]], int]:
            rows = _export_rows(store)
            sources = [
                (r["data"].get("slug"), r["data"].get("title"))
                for r in rows
                if r.get("type") == "Source"
            ]
            return sources, len([r for r in rows if r.get("edge") == "Supports"])

        app = coco.App(
            coco.AppConfig(name="e2e_referenced_node", environment=coco_env), main
        )
        app.update_blocking()
        assert graph() == ([("a", "A")], 1)

        declare_a["on"] = False
        app.update_blocking()
        assert graph() == ([("a", None)], 1)

        declare_a["on"] = True
        app.update_blocking()
        assert graph() == ([("a", "A")], 1)

    def test_unchanged_issues_no_writes(self, store: str) -> None:
        """The single most important case: CocoIndex's whole incrementality
        promise rests on an unchanged source producing zero writes. Checked
        via the commit count, not by trusting an absence of exceptions -- a
        silently reissued no-op upsert would still pass a weaker check.
        Three types declared together so a memoization bug in one type
        can't hide behind a passing check on another."""
        db = _e2e_db(store, "unchanged")

        async def main() -> None:
            source_schema = await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")
            edge_schema = await EdgeSchema.from_class(_ScEdgeProps)

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, edge_schema
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(coco.AppConfig(name="e2e_unchanged", environment=coco_env), main)
        app.update_blocking()

        before = _commit_count(store)
        app.update_blocking()
        after = _commit_count(store)

        assert after == before

    def test_oversized_component_uses_branch(
        self, store: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Force the per-type chunking cap down so a batch of new Source
        rows plans into multiple upsert commits (plan_commits' chunking via
        _MAX_ENTITIES_PER_TYPE). More than one commit for a single sync
        must land on `main` as ONE commit, via a scratch branch merged in
        and cleaned up afterward -- not as several commits applied directly
        to `main`, and not with a stray coco_scratch_* branch left behind.
        Warms the store up with 2 Source rows (under the cap) first, so the
        commit-count delta measured across the second run isolates the
        chunked entity phase from the type-creation commits, which are a
        separate, unconditional code path covered elsewhere. A sibling
        Claim row, declared and left unchanged in both runs, proves the
        scratch-branch path doesn't disturb it."""
        monkeypatch.setattr(ogt, "_MAX_ENTITIES_PER_TYPE", 4)
        db = _e2e_db(store, "oversized")
        row_count = {"n": 2}

        async def main() -> None:
            source_schema = await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            claim_schema = await NodeSchema.from_class(_ScClaim, key="slug")

            sources = await omnigraph.mount_node_target(db, "Source", source_schema)
            claims = await omnigraph.mount_node_target(db, "Claim", claim_schema)
            for i in range(row_count["n"]):
                sources.declare_node(node=_ScSourceNarrow(slug=f"s{i}", title=f"S{i}"))
            claims.declare_node(node=_ScClaim(slug="c1"))

        app = coco.App(coco.AppConfig(name="e2e_oversized", environment=coco_env), main)
        app.update_blocking()  # warm-up: creates both types, 2 Source rows, 1 Claim row

        before = _commit_count(store)
        row_count["n"] = 10  # 8 new rows -> 2 upsert chunks of 4 at cap=4
        app.update_blocking()
        after = _commit_count(store)

        assert after - before == 1  # one merge commit, not two direct commits
        assert _branch_names(store) == ["main"]  # no coco_scratch_* branch left behind

        rows = _export_rows(store)
        assert len([r for r in rows if r.get("type") == "Source"]) == 10
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1

    def test_user_managed_first_run_writes_rows_without_touching_the_schema(
        self, store: str
    ) -> None:
        """`managed_by=USER` on a graph this app has never tracked. The type
        already exists (created here the way a user's own `omnigraph init`
        would), and the app's job is only to keep rows in sync.

        On run one there are no tracking records, so the tracked diff is
        empty — which used to raise, making the mode unusable at exactly the
        moment it's meant to be used. The docs advertise it as working.
        """
        store_dir = Path(store[len("file://") :])
        assert (
            _init_live(
                store_dir,
                (
                    "node Source {\n  slug: String @key\n  title: String?\n"
                    "  coco_key: String\n}\n"
                ),
            ).returncode
            == 0
        )
        schema_before = _read_schema_source(store)

        db = _e2e_db(store, "user_managed")
        rows_to_write = {"n": 2}

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db,
                "Source",
                await NodeSchema.from_class(_ScSourceNarrow, key="slug"),
                managed_by=ManagedBy.USER,
            )
            for i in range(rows_to_write["n"]):
                sources.declare_node(node=_ScSourceNarrow(slug=f"s{i}", title=f"S{i}"))

        app = coco.App(
            coco.AppConfig(name="e2e_user_managed", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        assert sorted(
            r["data"].get("slug") for r in rows if r.get("type") == "Source"
        ) == [
            "s0",
            "s1",
        ]
        # Never rewritten: byte-identical to what the user applied.
        assert _read_schema_source(store) == schema_before

        # Run two: rows still reconcile normally, schema still untouched.
        rows_to_write["n"] = 1
        app.update_blocking()
        rows = _export_rows(store)
        assert [r["data"].get("slug") for r in rows if r.get("type") == "Source"] == [
            "s0"
        ]
        assert _read_schema_source(store) == schema_before

    def test_undeclared_type_is_dropped_with_its_rows(self, store: str) -> None:
        """Stop mounting a node type and it must disappear from the graph,
        rows included.

        This is the only thing that removes them: the engine emits no
        per-child deletes when a container target state goes away, so a
        "drop" that wrote nothing left the type and every node in it behind
        forever — still in `schema show`, still in `export`, and no longer
        tracked by anything that could ever clean them up.
        """
        db = _e2e_db(store, "type_dropped")
        keep_extra = {"on": True}

        async def main() -> None:
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            claims.declare_node(node=_ScClaim(slug="c1"))
            if keep_extra["on"]:
                sources = await omnigraph.mount_node_target(
                    db,
                    "Source",
                    await NodeSchema.from_class(_ScSourceNarrow, key="slug"),
                )
                sources.declare_node(node=_ScSourceNarrow(slug="s1", title="S1"))

        app = coco.App(
            coco.AppConfig(name="e2e_type_dropped", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        assert len([r for r in rows if r.get("type") == "Source"]) == 1
        assert "node Source" in _read_schema_source(store)

        keep_extra["on"] = False
        app.update_blocking()

        rows = _export_rows(store)
        assert [r for r in rows if r.get("type") == "Source"] == []
        assert "node Source" not in _read_schema_source(store)
        # The type that stayed declared is untouched by the other's drop.
        assert len([r for r in rows if r.get("type") == "Claim"]) == 1
        assert "node Claim" in _read_schema_source(store)

    def test_undeclared_user_managed_type_survives_with_its_rows(
        self, store: str
    ) -> None:
        """The `managed_by=USER` counterpart of the test above: stop mounting
        the type and BOTH its schema block and its rows must survive.

        Same app-level edit as the drop test, opposite required outcome. The
        connector did not create this type, so it does not get to delete it,
        and deleting it takes the user's own rows with it -- unrecoverable,
        and the exact opposite of what the docs promise.
        """
        store_dir = Path(store[len("file://") :])
        assert (
            _init_live(
                store_dir,
                (
                    "node Source {\n  slug: String @key\n  title: String?\n"
                    "  coco_key: String\n}\n"
                ),
            ).returncode
            == 0
        )

        db = _e2e_db(store, "user_managed_undeclared")
        keep_extra = {"on": True}

        async def main() -> None:
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            claims.declare_node(node=_ScClaim(slug="c1"))
            if keep_extra["on"]:
                sources = await omnigraph.mount_node_target(
                    db,
                    "Source",
                    await NodeSchema.from_class(_ScSourceNarrow, key="slug"),
                    managed_by=ManagedBy.USER,
                )
                sources.declare_node(node=_ScSourceNarrow(slug="s1", title="S1"))

        app = coco.App(
            coco.AppConfig(name="e2e_user_managed_undeclared", environment=coco_env),
            main,
        )
        app.update_blocking()

        rows = _export_rows(store)
        assert [r["data"].get("slug") for r in rows if r.get("type") == "Source"] == [
            "s1"
        ]
        assert "node Source" in _read_schema_source(store)

        keep_extra["on"] = False
        app.update_blocking()

        # The user owns this type: the block stays, and so does its row.
        rows = _export_rows(store)
        assert [r["data"].get("slug") for r in rows if r.get("type") == "Source"] == [
            "s1"
        ]
        assert "node Source" in _read_schema_source(store)

    def test_integer_keyed_endpoint(self, store: str) -> None:
        """An edge whose endpoint node is keyed on an int.

        The endpoint reference is the node's `id`, which is a String
        rendering of the key value — never the key's own type. Inferring
        `I64` from the Python value instead made EVERY edge insert into an
        int-keyed endpoint fail with "cannot assign/compare I64 with String
        for property `to`", which is exactly what the shipped example does.
        """
        db = _e2e_db(store, "int_endpoint")

        async def main() -> None:
            people = await omnigraph.mount_node_target(
                db, "Person", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            meetings = await omnigraph.mount_node_target(
                db, "Meeting", await NodeSchema.from_class(_ScMeeting, key="meeting_id")
            )
            attended = await omnigraph.mount_edge_target(
                db, "Attended", people, meetings
            )
            people.declare_node(node=_ScClaim(slug="Ada"))
            meetings.declare_node(node=_ScMeeting(meeting_id=7, note="Kickoff"))
            attended.declare_edge(from_id="Ada", to_id=7)

        app = coco.App(
            coco.AppConfig(name="e2e_int_endpoint", environment=coco_env), main
        )
        app.update_blocking()

        rows = _export_rows(store)
        (edge,) = [r for r in rows if r.get("edge") == "Attended"]
        assert (edge["from"], edge["to"]) == ("Ada", "7")
        # Re-running must not duplicate it, and must not re-stub the endpoint
        # over the real node (a keyed insert is a full-record replace).
        app.update_blocking()
        rows = _export_rows(store)
        assert len([r for r in rows if r.get("edge") == "Attended"]) == 1
        assert [r["data"].get("note") for r in rows if r.get("type") == "Meeting"] == [
            "Kickoff"
        ]

    def test_list_of_dates_round_trips(self, store: str) -> None:
        """`list[datetime.date]` renders as `[Date]`, which the engine accepts
        at `init` — and then every write of such a node died in `json.dumps`
        because only the bare `Date`/`DateTime` scalars had an encoder."""
        db = _e2e_db(store, "list_of_dates")

        @dataclass
        class _Holiday:
            slug: str
            days: list[datetime.date]

        async def main() -> None:
            holidays = await omnigraph.mount_node_target(
                db, "Holiday", await NodeSchema.from_class(_Holiday, key="slug")
            )
            holidays.declare_node(
                node=_Holiday(
                    slug="xmas",
                    days=[datetime.date(2026, 12, 25), datetime.date(2026, 12, 26)],
                )
            )

        app = coco.App(
            coco.AppConfig(name="e2e_list_of_dates", environment=coco_env), main
        )
        app.update_blocking()

        (row,) = [r for r in _export_rows(store) if r.get("type") == "Holiday"]
        assert len(row["data"].get("days")) == 2

    def test_encoder_change_rewrites_the_stored_value(self, store: str) -> None:
        """Same raw record, different `PropertyDef.encoder`: the value the
        graph holds must follow the encoder, which only happens if change
        detection fingerprints the encoded value rather than the raw one."""
        db = _e2e_db(store, "encoder_change")
        encoder = {"fn": str.lower}

        async def main() -> None:
            schema = NodeSchema(
                properties={
                    "slug": PropertyDef("slug", "String"),
                    "name": PropertyDef("name", "String", encoder["fn"]),
                },
                key=("slug",),
            )
            people = await omnigraph.mount_node_target(db, "Person", schema)
            people.declare_node(node={"slug": "ada", "name": "Ada Lovelace"})

        app = coco.App(
            coco.AppConfig(name="e2e_encoder_change", environment=coco_env), main
        )
        app.update_blocking()
        assert [
            r["data"].get("name") for r in _export_rows(store) if r.get("type")
        ] == ["ada lovelace"]

        encoder["fn"] = str.upper
        app.update_blocking()
        assert [
            r["data"].get("name") for r in _export_rows(store) if r.get("type")
        ] == ["ADA LOVELACE"]

    def test_encoder_change_reaches_a_memoized_component(self, store: str) -> None:
        """The test above declares its node from an unmemoized main. With the
        declaration inside a `@coco.fn(memo=True)` component, the second
        update skipped that component outright: neither its memo key nor
        the type's tracking record knew about the encoder, so no reconcile
        ever saw the new encoding and the lowercase value stayed."""
        db = _e2e_db(store, "encoder_change_memo")
        encoder = {"fn": str.lower}

        @coco.fn(memo=True)
        async def declare_ada(people: omnigraph.NodeTarget[Any]) -> None:
            people.declare_node(node={"slug": "ada", "name": "Ada Lovelace"})

        async def main() -> None:
            schema = NodeSchema(
                properties={
                    "slug": PropertyDef("slug", "String"),
                    "name": PropertyDef("name", "String", encoder["fn"]),
                },
                key=("slug",),
            )
            people = await omnigraph.mount_node_target(db, "Person", schema)
            await coco.mount(declare_ada, people)

        app = coco.App(
            coco.AppConfig(name="e2e_encoder_change_memo", environment=coco_env), main
        )
        app.update_blocking()
        assert [
            r["data"].get("name") for r in _export_rows(store) if r.get("type")
        ] == ["ada lovelace"]

        encoder["fn"] = str.upper
        app.update_blocking()
        assert [
            r["data"].get("name") for r in _export_rows(store) if r.get("type")
        ] == ["ADA LOVELACE"]

    def test_abandoned_scratch_branch_is_reaped_before_a_schema_change(
        self, store: str
    ) -> None:
        """An interrupted update leaves its `coco_scratch_*` branch behind,
        and Omnigraph refuses every later schema change on the store while
        it exists. The next schema change must recover on its own: reap the
        abandoned branch, apply, and leave only `main` behind."""
        db = _e2e_db(store, "abandoned_scratch")
        wide = {"on": False}

        async def main() -> None:
            schema = await NodeSchema.from_class(
                _ScSourceWide if wide["on"] else _ScSourceNarrow, key="slug"
            )
            sources = await omnigraph.mount_node_target(db, "Source", schema)
            node: Any = (
                _ScSourceWide(slug="a", title="A", note=None)
                if wide["on"]
                else _ScSourceNarrow(slug="a", title="A")
            )
            sources.declare_node(node=node)

        app = coco.App(
            coco.AppConfig(name="e2e_abandoned_scratch", environment=coco_env), main
        )
        app.update_blocking()

        # What a process killed mid-sync leaves behind, two hours ago.
        abandoned = ogt._scratch_branch_name(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=2)
        )
        created = _server(store).request(
            "POST", "/graphs/g/branches", {"name": abandoned}
        )
        assert created.status == 200, created
        assert abandoned in _branch_names(store)

        wide["on"] = True
        app.update_blocking()

        assert _branch_names(store) == ["main"]
        assert "note: String?" in _read_schema_source(store)

    def test_user_managed_type_follows_an_external_migration(self, store: str) -> None:
        """`managed_by="user"` means the schema is the user's: they migrate
        it with `omnigraph cluster apply`, then declare the wider dataclass.
        The connector used to compare the new declaration against what it
        had tracked and refuse — advising exactly that migration, which it
        then rejected again on every run because it never looked at the
        live schema. A user-managed type must never be validated against
        tracking history; it must simply write rows."""
        store_dir = Path(store[len("file://") :])
        v1 = "node Doc {\n  slug: String @key\n  coco_key: String\n}\n"
        v2 = (
            "node Doc {\n  slug: String @key\n  title: String?\n  coco_key: String\n}\n"
        )
        assert _init_live(store_dir, v1).returncode == 0

        @dataclass
        class _DocV1:
            slug: str

        @dataclass
        class _DocV2:
            slug: str
            title: str | None

        db = _e2e_db(store, "user_managed_migration")
        wide = {"on": False}

        async def main() -> None:
            docs = await omnigraph.mount_node_target(
                db,
                "Doc",
                await NodeSchema.from_class(
                    _DocV2 if wide["on"] else _DocV1, key="slug"
                ),
                managed_by=ManagedBy.USER,
            )
            node: Any = (
                _DocV2(slug="d1", title="T") if wide["on"] else _DocV1(slug="d1")
            )
            docs.declare_node(node=node)

        app = coco.App(
            coco.AppConfig(name="e2e_user_managed_migration", environment=coco_env),
            main,
        )
        app.update_blocking()

        # The user's own migration, applied outside CocoIndex.
        _deploy_live(store_dir, v2)

        wide["on"] = True
        app.update_blocking()

        (row,) = [r for r in _export_rows(store) if r.get("type") == "Doc"]
        assert row["data"].get("title") == "T"
        # The connector still never wrote the schema: the user's block is verbatim.
        assert _read_schema_source(store) == v2

    def test_hand_formatted_schema_survives_a_merge(self, store: str) -> None:
        """`schema show` hands back the source verbatim, formatting and all,
        so a schema somebody wrote by hand — indented, commented, two
        declarations on one line — is what the merger has to edit. Each of
        those used to produce a schema the engine refused."""
        store_dir = Path(store[len("file://") :])
        hand_written = (
            "  node Source {\n"
            "    slug: String @key // the key } not a block end\n"
            "    title: String?\n"
            "    coco_key: String\n"
            "  }\n"
            "node Other { slug: String @key coco_key: String } "
            "node Extra { slug: String @key coco_key: String }\n"
        )
        assert _init_live(store_dir, hand_written).returncode == 0
        db = _e2e_db(store, "hand_formatted")

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceWide, key="slug")
            )
            sources.declare_node(node=_ScSourceWide(slug="a", title="A", note="n"))

        app = coco.App(
            coco.AppConfig(name="e2e_hand_formatted", environment=coco_env), main
        )
        app.update_blocking()

        source = _read_schema_source(store)
        assert source.count("node Source") == 1
        assert "note: String?" in source
        assert "node Other {" in source and "node Extra {" in source
        (row,) = [r for r in _export_rows(store) if r.get("type") == "Source"]
        assert row["data"].get("note") == "n"

    def test_dropping_a_connected_app_removes_every_type(self, store: str) -> None:
        """Source, Claim and Supports are three processing components, torn
        down concurrently by `drop()`. Whichever node type's removal ran
        ahead of the edge's used to hit the endpoint guard and stay behind,
        so the first drop failed and only a second one finished the job.
        One drop must leave nothing."""
        db = _e2e_db(store, "drop_connected")

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(
            coco.AppConfig(name="e2e_drop_connected", environment=coco_env), main
        )
        app.update_blocking()
        assert "edge Supports" in _read_schema_source(store)

        app.drop_blocking()

        assert _read_schema_source(store).strip() == ""
        assert _export_rows(store) == []

    def test_handing_an_edge_type_to_the_user_protects_it_from_a_drop(
        self, store: str
    ) -> None:
        """Nodes and an edge type created system-managed, then the edge type
        declared `managed_by="user"`, then the app dropped. The drop used to
        succeed and delete the user's edge type with its edges: its block
        still carried the ownership marker, and the node drops read that as
        current ownership. Now the handoff releases the marker, so the drop
        refuses to remove the nodes the user's edge still points at and
        leaves everything in place."""
        db = _e2e_db(store, "handoff_drop")
        edge_owner = {"m": ManagedBy.SYSTEM}

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
                managed_by=edge_owner["m"],
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(
            coco.AppConfig(name="e2e_handoff_drop", environment=coco_env), main
        )
        app.update_blocking()
        assert _read_schema_source(store).count("coco_managed") == 3

        edge_owner["m"] = ManagedBy.USER
        app.update_blocking()
        source = _read_schema_source(store)
        assert source.count("coco_managed") == 2
        assert (
            "edge Supports: Source -> Claim {\n  weight: I64\n  coco_key: String\n"
            "  @key(@src, @dst)\n}" in source
        )

        with pytest.raises(ValueError, match=r"Supports.*not managed by this app"):
            app.drop_blocking()
        assert _read_schema_source(store) == source
        rows = _export_rows(store)
        assert len([r for r in rows if r.get("edge") == "Supports"]) == 1
        assert len([r for r in rows if r.get("type")]) == 2

    def test_a_comment_in_a_user_owned_edge_does_not_authorize_its_deletion(
        self, store: str
    ) -> None:
        """The user's edge type mentions `coco_managed` in a comment only.
        Ownership detection read the raw block text, so an app drop took
        the edge type along with the nodes and deleted the user's edges."""
        store_dir = Path(store[len("file://") :])
        owned = "  coco_key: String\n  coco_managed_by_e2e_comment_ownership: Bool?\n"
        hand_written = (
            f"node Source {{\n  slug: String @key\n  title: String?\n{owned}}}\n\n"
            f"node Claim {{\n  slug: String @key\n{owned}}}\n\n"
            "edge Supports: Source -> Claim {\n"
            "  // coco_managed_by_e2e_comment_ownership: Bool? was deliberately omitted\n"
            "  coco_key: String\n"
            "}\n"
        )
        assert _init_live(store_dir, hand_written).returncode == 0
        db = _e2e_db(store, "comment_ownership")

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db, "Supports", sources, claims, managed_by=ManagedBy.USER
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(from_id="a", to_id="c1")

        app = coco.App(
            coco.AppConfig(name="e2e_comment_ownership", environment=coco_env), main
        )
        app.update_blocking()
        assert len([r for r in _export_rows(store) if r.get("edge") == "Supports"]) == 1

        with pytest.raises(ValueError, match=r"Supports.*not managed by this app"):
            app.drop_blocking()
        assert "was deliberately omitted" in _read_schema_source(store)
        assert len([r for r in _export_rows(store) if r.get("edge") == "Supports"]) == 1

    def test_dropping_an_app_leaves_another_apps_edge_type_alone(
        self, store: str
    ) -> None:
        """App A creates Source. App B adopts Source as user-managed and
        creates Claim and Supports: Source -> Claim. Dropping A removed B's
        Supports with every edge in it: the drop took every referencing
        edge type carrying the ownership property along, and that property
        only said some app of this connector wrote the block. It names the
        owning app now, and a drop refuses to remove a node type another
        app's edge type still points at, naming that app."""
        db_a = _e2e_db(store, "two_apps_a")
        db_b = _e2e_db(store, "two_apps_b")

        async def main_a() -> None:
            sources = await omnigraph.mount_node_target(
                db_a, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))

        async def main_b() -> None:
            sources = await omnigraph.mount_node_target(
                db_b,
                "Source",
                await NodeSchema.from_class(_ScSourceNarrow, key="slug"),
                managed_by=ManagedBy.USER,
            )
            claims = await omnigraph.mount_node_target(
                db_b, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db_b,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
            )
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app_a = coco.App(
            coco.AppConfig(name="e2e_two_apps_a", environment=coco_env), main_a
        )
        app_b = coco.App(
            coco.AppConfig(name="e2e_two_apps_b", environment=coco_env), main_b
        )
        app_a.update_blocking()
        app_b.update_blocking()
        source = _read_schema_source(store)
        assert "coco_managed_by_e2e_two_apps_a" in source
        assert "coco_managed_by_e2e_two_apps_b" in source

        def supports_edges() -> int:
            return len([r for r in _export_rows(store) if r.get("edge") == "Supports"])

        assert supports_edges() == 1
        with pytest.raises(
            ValueError, match=r"Supports.*coco_managed_by_e2e_two_apps_b"
        ):
            app_a.drop_blocking()
        assert _read_schema_source(store) == source
        assert supports_edges() == 1
        app_b.update_blocking()
        assert supports_edges() == 1

    def test_reclaiming_an_edge_type_restores_its_ownership(self, store: str) -> None:
        """system -> user -> system with the schema unchanged throughout.
        The second handoff wrote nothing, so the edge block stayed without
        `coco_managed`, and dropping the connected app then failed because
        its own edge type looked user-owned."""
        db = _e2e_db(store, "reclaim")
        edge_owner = {"m": ManagedBy.SYSTEM}

        async def main() -> None:
            sources = await omnigraph.mount_node_target(
                db, "Source", await NodeSchema.from_class(_ScSourceNarrow, key="slug")
            )
            claims = await omnigraph.mount_node_target(
                db, "Claim", await NodeSchema.from_class(_ScClaim, key="slug")
            )
            supports = await omnigraph.mount_edge_target(
                db,
                "Supports",
                sources,
                claims,
                await EdgeSchema.from_class(_ScEdgeProps),
                managed_by=edge_owner["m"],
            )
            sources.declare_node(node=_ScSourceNarrow(slug="a", title="A"))
            claims.declare_node(node=_ScClaim(slug="c1"))
            supports.declare_edge(
                from_id="a", to_id="c1", record=_ScEdgeProps(weight=1)
            )

        app = coco.App(coco.AppConfig(name="e2e_reclaim", environment=coco_env), main)
        app.update_blocking()
        edge_owner["m"] = ManagedBy.USER
        app.update_blocking()
        assert _read_schema_source(store).count("coco_managed") == 2
        edge_owner["m"] = ManagedBy.SYSTEM
        app.update_blocking()
        assert _read_schema_source(store).count("coco_managed") == 3

        app.drop_blocking()
        assert _read_schema_source(store).strip() == ""
        assert _export_rows(store) == []

    def test_a_date_keyed_node_round_trips(self, store: str) -> None:
        """Declared twice, a `Date`-keyed node must be one node: its identity
        is the ISO form of the key, both in tracking and in the graph."""
        db = _e2e_db(store, "date_key")

        async def main() -> None:
            await _declare_a_day(db)

        app = coco.App(coco.AppConfig(name="e2e_date_key", environment=coco_env), main)
        app.update_blocking()
        app.update_blocking()

        (row,) = [r for r in _export_rows(store) if r.get("type") == "Day"]
        assert row["data"].get("note") == "n"

    def test_two_spellings_of_one_instant_are_one_node(self, store: str) -> None:
        """The regression: `12:00+00:00` and `14:00+02:00` used to get two
        tracking keys for one graph node. Declared together they are now the
        same target state, which the engine refuses as a duplicate; declared
        one after the other they are one node whose removal is one delete."""
        db = _e2e_db(store, "datetime_key")

        @dataclass
        class _Event:
            at: datetime.datetime
            note: str | None

        utc = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC)
        plus_two = datetime.datetime(
            2026, 1, 1, 14, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        )
        declare = {"ats": [utc, plus_two]}

        async def main() -> None:
            events = await omnigraph.mount_node_target(
                db, "Event", await NodeSchema.from_class(_Event, key="at")
            )
            for at in declare["ats"]:
                events.declare_node(node=_Event(at=at, note=at.isoformat()))

        app = coco.App(
            coco.AppConfig(name="e2e_datetime_key", environment=coco_env), main
        )
        with pytest.raises(ValueError, match="already declared"):
            app.update_blocking()

        declare["ats"] = [utc]
        app.update_blocking()
        declare["ats"] = [plus_two]  # same instant, respelled: still that node
        app.update_blocking()
        (row,) = [r for r in _export_rows(store) if r.get("type") == "Event"]
        # 0.14 exports a UTC DateTime as ISO without an offset.
        assert row["data"].get("at") == "2026-01-01T12:00:00"
        assert row["data"].get("note") == plus_two.isoformat()

        declare["ats"] = []
        app.update_blocking()
        assert [r for r in _export_rows(store) if r.get("type") == "Event"] == []

    def test_an_explicit_datetime_keyed_schema_round_trips(self, store: str) -> None:
        """Positive case for a hand-built schema: no dataclass, no encoder
        given, `datetime` values in dict rows. Declared twice, one node."""
        db = _e2e_db(store, "explicit_datetime")
        schema = NodeSchema(
            properties={
                "at": PropertyDef("at", "DateTime"),
                "note": PropertyDef("note", "String?"),
            },
            key=("at",),
        )
        at = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC)

        async def main() -> None:
            events = await omnigraph.mount_node_target(db, "Event", schema)
            events.declare_node(node={"at": at, "note": "n"})

        app = coco.App(
            coco.AppConfig(name="e2e_explicit_datetime", environment=coco_env), main
        )
        app.update_blocking()
        app.update_blocking()
        (row,) = [r for r in _export_rows(store) if r.get("type") == "Event"]
        # 0.14 exports a UTC DateTime as ISO without an offset.
        assert row["data"].get("at") == "2026-01-01T12:00:00"
        assert row["data"].get("note") == "n"

    def test_an_explicit_date_keyed_schema_round_trips(self, store: str) -> None:
        db = _e2e_db(store, "explicit_date")
        schema = NodeSchema(
            properties={
                "on": PropertyDef("on", "Date"),
                "note": PropertyDef("note", "String?"),
            },
            key=("on",),
        )

        async def main() -> None:
            days = await omnigraph.mount_node_target(db, "Day", schema)
            days.declare_node(node={"on": datetime.date(2026, 1, 5), "note": "n"})

        app = coco.App(
            coco.AppConfig(name="e2e_explicit_date", environment=coco_env), main
        )
        app.update_blocking()
        app.update_blocking()
        (row,) = [r for r in _export_rows(store) if r.get("type") == "Day"]
        assert row["data"].get("note") == "n"
