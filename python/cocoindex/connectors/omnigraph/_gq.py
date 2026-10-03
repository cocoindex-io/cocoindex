"""Pure builders for Omnigraph `.pg` schema fragments and `.gq` mutations.

Every function here is a pure function over strings and primitives — no I/O,
no connector types — so the whole module is unit-testable without a running
Omnigraph. Mirrors the role `neo4j/_cypher.py` plays for the Neo4j connector.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from typing import NamedTuple

#: Synthetic property carrying the connector's derived key, declared on every
#: type we manage. Omnigraph gives edges no settable id and its auto-assigned
#: ULID is not filterable, while `where` accepts exactly one equality
#: predicate — so a declared property is the only way to address one specific
#: entity for deletion. Non-nullable, which is legal at init time.
COCO_KEY = "coco_key"

#: Prefix of every synthetic property this connector declares — `coco_key`
#: and the ownership property below. A user's own property may not start
#: with it.
COCO_PREFIX = "coco_"

#: Prefix of the synthetic, nullable, never-written property declared on
#: every block this connector renders, and on nothing else; the rest of the
#: name is the app that owns the block (see `ownership_property`). It is how
#: the schema sink tells this app's types from a user's and from another
#: app's: a `managed_by=user` type must declare `coco_key` as well, so that
#: property alone cannot, and a marker that only said "some app of this
#: connector" let one app's drop take another app's edge types along. It is
#: a property rather than a comment because the engine stores an applied
#: schema's source only when the apply changes something structural — a
#: comment-only change reports `applied: false` and keeps the old source
#: (verified against the binary), so a comment could never be released. And
#: the app is in the property's *name* because the engine refuses to retype
#: a property (`enum(a)` to `enum(b)` fails with "changing property type ...
#: not supported"), while adding one nullable property and dropping another
#: in a single apply is a migration it performs without flags — both
#: verified against the binary.
COCO_MANAGED_PREFIX = "coco_managed_by_"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ownership_property(app_name: str) -> str:
    """The property marking a block as owned by the app named `app_name`.

    An app name that is an identifier is used as is. Any other name is made
    one, with a digest of the original appended so that two names which
    sanitize alike (`my-app`, `my_app`) still get distinct properties.
    """
    if _IDENTIFIER_RE.fullmatch(app_name):
        return f"{COCO_MANAGED_PREFIX}{app_name}"
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", app_name)
    digest = hashlib.sha256(app_name.encode()).hexdigest()[:8]
    return f"{COCO_MANAGED_PREFIX}{sanitized}_{digest}"


#: Scalar type keywords `.pg`/`.gq` accept. `DateTime` must precede `Date` in
#: the alternation below so a caller can't rely on `Date` matching first and
#: leaving `Time...` as unconsumed trailing text (Python's `re` backtracks
#: across alternatives, so match order doesn't actually matter for
#: correctness — but keeping the longer name first avoids relying on that).
_PG_BASE_TYPES = (
    "String",
    "Bool",
    "I32",
    "I64",
    "U32",
    "U64",
    "F32",
    "F64",
    "DateTime",
    "Date",
    "Blob",
)
_PG_BASE_ALT = "|".join(_PG_BASE_TYPES)
_PG_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

#: `Vector(N)`, `[T]` for scalar `T`, and `enum(a, b, ...)`, plus an optional
#: trailing `?` for nullability — the full `.pg`/`.gq` type-expression
#: grammar. Anything else is rejected.
_PG_TYPE_RE = re.compile(
    rf"^(?:{_PG_BASE_ALT}"
    rf"|Vector\([1-9][0-9]*\)"
    rf"|\[(?:{_PG_BASE_ALT})\]"
    rf"|enum\({_PG_IDENT}(?:,\s*{_PG_IDENT})*\)"
    rf")\??$"
)


def validate_identifier(name: str, what: str) -> None:
    """Reject anything that could break out of a `.pg` or `.gq` fragment.

    Omnigraph identifiers have no quoting form we can rely on, so the only
    safe policy is to refuse non-conforming names outright.
    """
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Omnigraph {what}: {name!r}")


def validate_pg_type(pg_type: str) -> None:
    """Reject anything that isn't a legal Omnigraph type expression.

    A mutation's `pg_type` is never bound as a parameter — it is spliced
    directly into the query *signature* (`$p_email: <pg_type>`), so this is
    as much an injection boundary as `validate_identifier` and needs the
    same strictness.
    """
    if not _PG_TYPE_RE.fullmatch(pg_type):
        raise ValueError(f"Invalid Omnigraph property type: {pg_type!r}")


def render_property(name: str, pg_type: str, *, is_key: bool) -> str:
    validate_identifier(name, "property name")
    validate_pg_type(pg_type)
    return f"{name}: {pg_type} @key" if is_key else f"{name}: {pg_type}"


#: Property names Omnigraph itself reserves on a node type. `id` is the
#: engine's own identity column, materialized from the `@key` property:
#: declaring one alongside it fails at `init`/`schema apply` with "physical
#: schema for 'node:X' must contain exactly one top-level `id` field; found
#: 2" (verified against the binary), and using `id` *as* the key fails even
#: more confusingly with "@key must reference declared properties".
_RESERVED_NODE_PROPERTIES = ("id",)

#: The same for an edge type, plus the two endpoint columns. `src`/`dst`
#: collide with the engine's stored endpoint references ("Duplicate field
#: name \"src\" in schema"); `from`/`to` parse fine in `.pg` but are the
#: literal field names an edge `insert` assigns its endpoints to, so a
#: property by either name could never be written (`build_edge_insert`
#: refuses it) — rejecting it here means the failure lands at schema
#: declaration rather than at the first write.
_RESERVED_EDGE_PROPERTIES = ("id", "src", "dst", "from", "to")


def _check_not_reserved(
    properties: Sequence[tuple[str, str]], type_name: str, reserved: Sequence[str]
) -> None:
    synthetic = sorted(name for name, _ in properties if name.startswith(COCO_PREFIX))
    if synthetic:
        raise ValueError(
            f"{synthetic!r} {'is' if len(synthetic) == 1 else 'are'} reserved by "
            f"the CocoIndex connector (every name starting with {COCO_PREFIX!r} "
            f"is) and cannot be declared on {type_name!r}"
        )
    clashes = sorted({name for name, _ in properties} & set(reserved))
    if clashes:
        raise ValueError(
            f"{clashes!r} {'is' if len(clashes) == 1 else 'are'} reserved by "
            f"Omnigraph and cannot be declared on {type_name!r}; rename the "
            f"field (e.g. {clashes[0]!r} -> {type_name.lower()}_{clashes[0]})"
        )


def render_node_type(
    type_name: str,
    properties: Sequence[tuple[str, str]],
    key: tuple[str, ...],
    *,
    owner: str,
) -> str:
    """Render a node type's block, marked as owned by the app `owner`."""
    validate_identifier(type_name, "node type")
    _check_not_reserved(properties, type_name, _RESERVED_NODE_PROPERTIES)
    if len(key) > 1:
        raise ValueError(
            f"Omnigraph node types support exactly one @key property, but "
            f"{type_name} declares {key!r}. The engine rejects the schema "
            f'outright ("node type {type_name} has multiple @key constraints; '
            f'only one is supported"), so a composite key cannot be expressed '
            f"at all — derive a single key field instead."
        )
    by_name = dict(properties)
    for k in key:
        if k not in by_name:
            raise ValueError(f"key property {k!r} is not declared on {type_name}")
        if by_name[k].endswith("?"):
            raise ValueError(f"key property {k!r} must not be nullable on {type_name}")
    lines = [
        f"  {render_property(name, pg_type, is_key=name in key)}"
        for name, pg_type in properties
    ]
    lines.extend(_render_synthetic_properties(owner))
    body = "\n".join(lines)
    return f"node {type_name} {{\n{body}\n}}"


def _render_synthetic_properties(owner: str) -> list[str]:
    return [
        f"  {render_property(COCO_KEY, 'String', is_key=False)}",
        f"  {render_property(ownership_property(owner), 'Bool?', is_key=False)}",
    ]


def render_edge_type(
    type_name: str,
    from_type: str,
    to_type: str,
    properties: Sequence[tuple[str, str]],
    *,
    owner: str,
) -> str:
    """Render an edge type's block, marked as owned by the app `owner`."""
    validate_identifier(type_name, "edge type")
    validate_identifier(from_type, "node type")
    validate_identifier(to_type, "node type")
    _check_not_reserved(properties, type_name, _RESERVED_EDGE_PROPERTIES)
    lines = [
        f"  {render_property(name, pg_type, is_key=False)}"
        for name, pg_type in properties
    ]
    lines.extend(_render_synthetic_properties(owner))
    body = "\n".join(lines)
    return f"edge {type_name}: {from_type} -> {to_type} {{\n{body}\n}}"


#: A `//` line comment. Blanked out — replaced by spaces of the same length,
#: so every offset stays valid — before the schema is scanned: `schema show`
#: returns the source verbatim, comments included, and a `}` or a `node X {`
#: inside one is text, not structure. `//` is the only comment syntax the
#: engine accepts (`#` and `--` are parse errors, verified against the
#: binary).
_COMMENT_RE = re.compile(r"//[^\n]*")

#: The head of a type declaration: `node NAME` or `edge NAME`, anywhere in
#: the source — not only at the start of a line. A hand-written schema may
#: indent its blocks or put two declarations on one line, and `schema show`
#: preserves both, so a merger that only recognised `node X {` at a line
#: start appended a second block and the engine refused the result
#: ("duplicate node name"). The KIND is captured, not just the name: a `.pg`
#: may legally hold `node Link` and `edge Link` side by side (the engine
#: accepts it), and matching on the name alone made an edge's fragment
#: overwrite the node's block.
_TYPE_HEAD_RE = re.compile(r"\b(node|edge)\s+(\w+)\b")

#: The rest of an edge head, right after `edge NAME`: `: FROM -> TO`.
_EDGE_ENDPOINTS_RE = re.compile(r"\s*:\s*(\w+)\s*->\s*(\w+)")

#: Optional whitespace and then the opening brace of a block body.
_BODY_OPEN_RE = re.compile(r"\s*\{")


def _blank_comments(pg: str) -> str:
    """`pg` with every `//` comment replaced by spaces of the same length, so
    offsets into it are offsets into the original."""
    return _COMMENT_RE.sub(lambda m: " " * len(m.group(0)), pg)


class _TypeBlock(NamedTuple):
    kind: str
    name: str
    #: Span into the ORIGINAL source, comments and formatting included.
    start: int
    end: int
    #: Edge endpoints; `None` for a node.
    from_type: str | None
    to_type: str | None


def _scan_type_blocks(existing_pg: str) -> list[_TypeBlock]:
    """Every top-level type block in `existing_pg`, in source order.

    Scans a comment-blanked copy of the same length, so spans line up with
    the original. A block starts at `node NAME` / `edge NAME` and ends at
    the matching `}` of its braced body, or — for the brace-less `edge
    NAME: FROM -> TO` form, a property-less edge the engine both accepts
    and reproduces via `schema show` — right after the `TO` endpoint. This
    connector's own builders always add `coco_key`, so they never emit the
    brace-less form, but the schema being searched can legitimately contain
    it, from another tool or a `managed_by=user` type. Scanning resumes
    after each block's end, so a property named `node` or `edge` inside a
    body is never mistaken for a declaration.

    Raises rather than guessing on a schema it can't edit safely — a node
    head with no body, an edge head with no endpoints, or braces that never
    balance: either way the splice point would land mid-declaration and
    produce corrupt `.pg` that goes straight to `schema apply`.
    """
    text = _blank_comments(existing_pg)
    blocks: list[_TypeBlock] = []
    pos = 0
    while (head := _TYPE_HEAD_RE.search(text, pos)) is not None:
        kind, name = head.group(1), head.group(2)
        body_start = head.end()
        from_type = to_type = None
        if kind == "edge":
            endpoints = _EDGE_ENDPOINTS_RE.match(text, body_start)
            if endpoints is None:
                raise ValueError(
                    f"Omnigraph schema declares edge {name!r} without "
                    f"`: FROM -> TO` endpoints; refusing to edit it"
                )
            from_type, to_type = endpoints.group(1), endpoints.group(2)
            body_start = endpoints.end()
        body = _BODY_OPEN_RE.match(text, body_start)
        if body is None:
            if kind == "node":
                raise ValueError(
                    f"Omnigraph schema declares node {name!r} without a "
                    f"`{{ ... }}` body; refusing to edit it"
                )
            end = body_start
        else:
            depth = 0
            end = -1
            for i in range(body.end() - 1, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if end == -1:
                raise ValueError(
                    f"Omnigraph schema has an unterminated {kind} {name!r} block "
                    f"(unbalanced braces); refusing to edit it"
                )
        blocks.append(_TypeBlock(kind, name, head.start(), end, from_type, to_type))
        pos = end
    return blocks


class IncidentEdge(NamedTuple):
    """An edge type that has a given node type at one of its ends."""

    edge_type: str
    role: str  # "from" | "to"


def incident_edge_patterns(existing_pg: str, node_type_name: str) -> list[IncidentEdge]:
    """Every (edge type, end) in `existing_pg` at which `node_type_name`
    sits, in source order. A self-referencing edge type contributes both
    ends."""
    patterns: list[IncidentEdge] = []
    for block in _scan_type_blocks(existing_pg):
        if block.kind != "edge":
            continue
        if block.from_type == node_type_name:
            patterns.append(IncidentEdge(block.name, "from"))
        if block.to_type == node_type_name:
            patterns.append(IncidentEdge(block.name, "to"))
    return patterns


def edge_types_referencing(existing_pg: str, node_type_name: str) -> list[str]:
    """Names of edge types in `existing_pg` that use `node_type_name` as an
    endpoint. Used to refuse removing a node type out from under one."""
    return list(
        dict.fromkeys(
            p.edge_type for p in incident_edge_patterns(existing_pg, node_type_name)
        )
    )


def _find_type_block(
    existing_pg: str, kind: str, type_name: str
) -> tuple[int, int] | None:
    """Locate the `(start, end)` span of the `kind` type named `type_name`
    in `existing_pg`, or `None` if it isn't present.

    `kind` is `"node"` or `"edge"` and is part of the match, not a hint: a
    node and an edge may share a name in a valid `.pg`, and editing the
    wrong one silently destroys a type. See `_scan_type_blocks` for what a
    block's extent is.

    Raises on two blocks with the same kind and name: only the first would
    ever be rewritten, leaving the second as a stale duplicate.
    """
    if kind not in ("node", "edge"):
        raise ValueError(f"type kind must be 'node' or 'edge', got {kind!r}")
    matching = [
        block
        for block in _scan_type_blocks(existing_pg)
        if (block.kind, block.name) == (kind, type_name)
    ]
    if not matching:
        return None
    if len(matching) > 1:
        raise ValueError(
            f"Omnigraph schema declares {kind} {type_name!r} "
            f"{len(matching)} times; refusing to edit an ambiguous schema"
        )
    return matching[0].start, matching[0].end


#: An ownership declaration together with the whitespace that is its own:
#: the line break and indentation before it, or the run of spaces before it
#: in a one-line block — never more, or a release would eat the (blanked)
#: comment ending the previous line. Group 1 is the property's name.
_COCO_MANAGED_DECL_RE = re.compile(
    rf"(?:\n[ \t]*|[ \t]+)?\b({COCO_MANAGED_PREFIX}\w+)\s*:\s*Bool\??"
)


def ownership_marker(existing_pg: str, kind: str, type_name: str) -> str | None:
    """The ownership property the block for `kind` `type_name` declares
    (`coco_managed_by_<app>`, see `ownership_property`), or `None` for a
    block no app of this connector currently owns — a user's, or another
    tool's.

    Declarations only: the block is searched with its comments blanked, so
    a user-owned block whose comment merely mentions the property is not
    mistaken for the connector's (which once let an app drop delete it).
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is None:
        return None
    start, end = span
    m = _COCO_MANAGED_DECL_RE.search(_blank_comments(existing_pg)[start:end])
    return m.group(1) if m is not None else None


def release_ownership(existing_pg: str, kind: str, type_name: str, marker: str) -> str:
    """Remove the declaration of the ownership property `marker` from the
    block for `kind` `type_name`, leaving everything else in it — comments
    included — and the rest of the schema untouched. A no-op if the block
    is absent or does not declare `marker`; another app's marker on the
    block is that app's to release.

    This is the one schema write a `managed_by=user` declaration causes: a
    type the connector created and the app then handed to the user still
    carried the ownership property, and a later drop of a node type it
    referenced read that as current ownership and removed the user's edge
    type with it. Dropping a nullable, never-written property is a
    migration the engine applies without flags (verified).
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is None:
        return existing_pg
    start, end = span
    block = existing_pg[start:end]
    blanked = _blank_comments(existing_pg)[start:end]
    # Cut the declarations found on the comment-blanked view out of the
    # original text; offsets line up because blanking preserves length.
    for m in reversed(list(_COCO_MANAGED_DECL_RE.finditer(blanked))):
        if m.group(1) == marker:
            block = block[: m.start()] + block[m.end() :]
    return existing_pg[:start] + block + existing_pg[end:]


def merge_type_into_schema(
    existing_pg: str, kind: str, type_name: str, fragment: str
) -> str:
    """Merge one type's rendered `.pg` fragment into the full schema source
    read back from `schema show`. `kind` is `"node"` or `"edge"` — see
    `_find_type_block` for why matching on the name alone is unsafe.

    Omnigraph's schema is applied whole-graph, not per type: `schema apply`
    and `init` both treat their input as the *complete* desired schema and
    silently drop any type omitted from it — verified against the engine:
    applying a single new type's fragment alone dropped every other
    existing type from the graph. Since each type reconciles independently
    and only ever renders its own single-type fragment, applying that
    directly would wipe the rest of the graph's schema on every type-level
    sync. This does the merge textually instead: replace the existing
    block for `type_name` if the current schema already has one, otherwise
    append it.

    Pure and unit-tested directly — deliberately not exercised only via a
    live store, since this is fiddly text handling.

    Read-merge-write is not atomic: two components applying schema
    concurrently can race this, and a lost update would silently drop
    whichever type the loser was adding or changing. Not addressed here.
    Schema actions are rare (only on type creation or change), so the
    window is small; closing it fully would need a much larger change —
    one target owning the whole graph's schema — which is out of scope.
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is not None:
        start, end = span
        return existing_pg[:start] + fragment + existing_pg[end:]
    # Not present in the current schema -- append it.
    sep = "\n\n" if existing_pg.strip() else ""
    return existing_pg.rstrip("\n") + sep + fragment + "\n"


def remove_type_from_schema(existing_pg: str, kind: str, type_name: str) -> str:
    """Remove the `kind` type named `type_name` from the schema source
    entirely, leaving every other type untouched. A no-op if it isn't
    present.

    This is the first half of the two-`schema apply` rebuild a `@key` (or
    edge endpoint) change needs: the engine flatly rejects an in-place key
    change in one step ("removing property constraints ... not supported
    in schema migration v1", verified against the binary, regardless of
    `--allow-data-loss` — this isn't a soft/hard-drop distinction, it's an
    unsupported migration step outright) but accepts dropping the type and
    then re-adding it with its new definition as two separate calls
    (verified live, and a plain soft drop is enough — re-adding a type of
    the same name right after succeeds cleanly, no `--allow-data-loss`
    needed).
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is None:
        return existing_pg
    start, end = span
    before, after = existing_pg[:start], existing_pg[end:]
    # Consume one adjoining blank-line separator so removing a middle block
    # doesn't leave the two neighbors glued together by a doubled one.
    if after.startswith("\n\n"):
        after = after[2:]
    elif before.endswith("\n\n"):
        before = before[:-2]
    return before + after


class PropertyValue(NamedTuple):
    """One bound value: its property name, its GQ type, and the value itself.

    The type travels with the value because a parameterized query must declare
    it in the signature (`query m($p_email: String)`).
    """

    name: str
    pg_type: str
    value: object


class Bind(NamedTuple):
    """One parameter slot of a `Statement`: its label, `.pg` type and value.

    The label (`p_email`, `e_from`, `p_coco_key`, ...) is unique within its
    statement; `render_query` turns it into a query-wide parameter name.
    """

    label: str
    pg_type: str
    value: object


class Statement(NamedTuple):
    """One GQ statement with positional parameter slots.

    `body` holds a `$?` slot wherever a bound value goes, and `binds` lists
    each slot's label, type and value in slot order. Parameter names are
    allocated only when a query is rendered — `s{i}_{label}` for the i-th
    statement — so any number of statements combine into one query with no
    renaming, and a value never enters the statement text at all.
    """

    body: str
    binds: tuple[Bind, ...]


class Query(NamedTuple):
    """A rendered `query m(...) { ... }` source plus its bound params — the
    form the transport sends, whether the block mutates or reads. Values
    are never interpolated into `expr`, so a property value containing
    quotes or braces cannot alter the query.
    """

    expr: str
    params: dict[str, object]


#: Marks a parameter position in a `Statement.body`. `$` never appears in a
#: statement otherwise (identifiers are validated to `[A-Za-z0-9_]`), so the
#: slot can be found by plain splitting.
_SLOT = "$?"


def render_query(statements: Sequence[Statement]) -> Query:
    """Render statements into ONE `query m(...) { ... }` — one CLI
    invocation, one commit. N separate `mutate` calls would be N commits
    and no atomicity.

    Each statement's binds become parameters `s{i}_{label}`, declared in the
    signature in slot order and substituted for the statement's `$?` slots
    in the same order.
    """
    if not statements:
        raise ValueError("render_query requires at least one statement")
    signature: list[str] = []
    params: dict[str, object] = {}
    bodies: list[str] = []
    for i, statement in enumerate(statements):
        parts = statement.body.split(_SLOT)
        if len(parts) != len(statement.binds) + 1:
            raise ValueError(
                f"statement has {len(parts) - 1} slots but "
                f"{len(statement.binds)} bind{'s' if len(statement.binds) != 1 else ''}: "
                f"{statement.body!r}"
            )
        rendered = [parts[0]]
        for bind, rest in zip(statement.binds, parts[1:], strict=True):
            name = f"s{i}_{bind.label}"
            if name in params:
                raise ValueError(
                    f"Duplicate parameter label {bind.label!r} in {statement.body!r}"
                )
            signature.append(f"${name}: {bind.pg_type}")
            params[name] = bind.value
            rendered.append(f"${name}{rest}")
        bodies.append("".join(rendered))
    return Query(f"query m({', '.join(signature)}) {{ {' '.join(bodies)} }}", params)


def _bind(props: Sequence[PropertyValue], prefix: str) -> tuple[list[Bind], list[str]]:
    """Validate `props` and turn them into binds plus `name: $?` assignments."""
    binds: list[Bind] = []
    assigns: list[str] = []
    seen: set[str] = set()
    for prop in props:
        validate_identifier(prop.name, "property name")
        validate_pg_type(prop.pg_type)
        if prop.name.startswith(COCO_PREFIX):
            raise ValueError(
                f"{prop.name!r} is reserved by the CocoIndex connector and cannot be "
                f"supplied as a property value"
            )
        if prop.name in seen:
            raise ValueError(f"Duplicate property name: {prop.name!r}")
        seen.add(prop.name)
        binds.append(Bind(f"{prefix}_{prop.name}", prop.pg_type, prop.value))
        assigns.append(f"{prop.name}: {_SLOT}")
    return binds, assigns


def _coco_key_bind(coco_key: str) -> Bind:
    return Bind(f"p_{COCO_KEY}", "String", coco_key)


def build_node_upsert(
    type_name: str, props: Sequence[PropertyValue], coco_key: str
) -> Statement:
    """Keyed node `insert` is an upsert by the derived key tuple."""
    validate_identifier(type_name, "node type")
    binds, assigns = _bind(props, "p")
    assigns.append(f"{COCO_KEY}: {_SLOT}")
    body = f"insert {type_name} {{ {', '.join(assigns)} }}"
    return Statement(body, (*binds, _coco_key_bind(coco_key)))


def build_endpoint_stub(
    type_name: str, key_props: Sequence[PropertyValue], coco_key: str
) -> Statement:
    """Key-only upsert, so an edge can reference a node its owning component
    has not written yet. The owner's own upsert later fills in the rest."""
    return build_node_upsert(type_name, key_props, coco_key)


def _delete_by_coco_key(type_name: str, coco_key: str) -> Statement:
    body = f"delete {type_name} where {COCO_KEY} = {_SLOT}"
    return Statement(body, (_coco_key_bind(coco_key),))


def build_node_delete(type_name: str, coco_key: str) -> Statement:
    validate_identifier(type_name, "node type")
    return _delete_by_coco_key(type_name, coco_key)


def build_edge_delete(type_name: str, coco_key: str) -> Statement:
    validate_identifier(type_name, "edge type")
    return _delete_by_coco_key(type_name, coco_key)


def build_incident_edges_query(
    node_type: str, coco_key: str, edge: IncidentEdge
) -> Statement:
    """Read the `coco_key` of every `edge.edge_type` edge at the node of
    `node_type` whose `coco_key` is `coco_key`, on the end `edge.role`.

    A read matches an edge type by its traversal spelling, which starts
    with a lowercase letter (`worksAt` for `edge WorksAt`; the engine looks
    it up case-insensitively, verified against the binary), and binds the
    hop as `$e:worksAt` so the edge's own properties can be read back. The
    node is found by `coco_key`, which every type this connector writes to
    declares, rather than by its `@key`, so the query is the same for every
    key type.
    """
    validate_identifier(node_type, "node type")
    validate_identifier(edge.edge_type, "edge type")
    if edge.role not in ("from", "to"):
        raise ValueError(f"edge role must be 'from' or 'to', got {edge.role!r}")
    spelling = edge.edge_type[0].lower() + edge.edge_type[1:]
    hop = f"$n $e:{spelling} $o" if edge.role == "from" else f"$o $e:{spelling} $n"
    body = (
        f"match {{ $n: {node_type} {{ {COCO_KEY}: {_SLOT} }} {hop} }} "
        f"return {{ $e.{COCO_KEY} as edge_key }}"
    )
    return Statement(body, (_coco_key_bind(coco_key),))


def build_edge_insert(
    type_name: str,
    from_ref: PropertyValue,
    to_ref: PropertyValue,
    props: Sequence[PropertyValue],
    coco_key: str,
) -> Statement:
    """Edge `insert` always creates a new edge — Omnigraph never deduplicates
    and provides no settable id. Idempotence comes entirely from the connector
    refusing to re-insert an edge whose `coco_key` it already tracks, and from
    deleting by `coco_key` before re-inserting a changed one.

    `from_ref.name`/`to_ref.name` are ignored — the assignment is always
    literally `from`/`to`; only `.pg_type` and `.value` address the endpoint.
    """
    validate_identifier(type_name, "edge type")
    validate_pg_type(from_ref.pg_type)
    validate_pg_type(to_ref.pg_type)
    for prop in props:
        if prop.name in ("from", "to"):
            raise ValueError(
                f"{prop.name!r} is reserved for the edge endpoint and cannot be "
                f"supplied as a property value"
            )
    binds = [
        Bind("e_from", from_ref.pg_type, from_ref.value),
        Bind("e_to", to_ref.pg_type, to_ref.value),
    ]
    assigns = [f"from: {_SLOT}", f"to: {_SLOT}"]
    prop_binds, prop_assigns = _bind(props, "p")
    binds += prop_binds
    assigns += prop_assigns
    assigns.append(f"{COCO_KEY}: {_SLOT}")
    body = f"insert {type_name} {{ {', '.join(assigns)} }}"
    return Statement(body, (*binds, _coco_key_bind(coco_key)))
