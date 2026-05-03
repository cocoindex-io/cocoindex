# CocoIndex v0 — Neo4j Target Features

Notes on what the Neo4j target offered in CocoIndex v0, before the connector
was retired (page `https://cocoindex.io/docs-v0/targets/neo4j/` no longer
resolves). This is feature-level documentation, not implementation. Sourced
from the Knowledge Graph for Docs blog post + the v0 API surface that still
appears in cached docs and example code.

---

## 1. Where it lived in the API

In v0, the Neo4j target lived under `cocoindex.storages` (not
`cocoindex.connectors`). The whole thing was *spec-driven* — users built
declarative dataclass-style specs and handed them to `collector.export(...)`
or `flow_builder.declare(...)`. There was no `TableTarget`/`RelationTarget`
runtime object, no `mount_*`, no `declare_record` / `declare_relation`
imperative API. The v0 graph connector was therefore much closer in shape to
a SQL-style "export a table" target than to today's imperative graph
connector.

The relevant v0 symbols, all under `cocoindex.storages`:

| Symbol                | Purpose                                                  |
| --------------------- | -------------------------------------------------------- |
| `Neo4jConnection`     | Connection spec — uri, user, password, optional db name  |
| `Neo4j`               | Target spec passed to `.export(...)`                     |
| `Neo4jDeclaration`    | Declaration spec for nodes referenced only by edges      |
| `Nodes`               | Mapping mode: this collector emits node rows             |
| `Relationships`       | Mapping mode: this collector emits relationship rows     |
| `NodeFromFields`      | Reference to a node by mapping fields → node properties  |
| `NodesFromFields`     | Plural alias seen in some examples (same shape)          |
| `ReferencedNode`      | Per-node config (PK fields, vector indexes) for relationship endpoints |
| `TargetFieldMapping`  | Single field rename — `source` (collector) → `target` (graph property) |

---

## 2. Connection setup

Connection went through `cocoindex.add_auth_entry` so credentials weren't
inlined in the flow:

```python
conn_spec = cocoindex.add_auth_entry(
    "Neo4jConnection",
    cocoindex.storages.Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="cocoindex",
    ),
)
```

`Neo4jConnection` carried (at minimum):

- `uri` — Bolt URI
- `user`, `password` — basic auth
- `db` (optional) — database name (Neo4j 4+ multi-database support)

The result was a typed auth reference passed by name into every
`Neo4j(...)` and `Neo4jDeclaration(...)` spec.

---

## 3. Two mapping modes — Nodes vs Relationships

A single `Neo4j(...)` target had a `mapping` field that was either `Nodes` or
`Relationships`. The choice told the engine *what* the rows in the
exporting collector represented.

### 3.1 Nodes — export rows as graph nodes

```python
document_node.export(
    "document_node",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Nodes(label="Document"),
    ),
    primary_key_fields=["filename"],
)
```

Behavior:

- Every collected row becomes one `(:Document)` node.
- All collector fields became node properties.
- `primary_key_fields=["filename"]` was the **upsert key** — re-running the
  flow with a row carrying the same `filename` updated the same node.
- Cypher emitted (conceptually): `MERGE (n:Document {filename: $filename})
  SET n += $other_props`.

`Nodes` itself was minimal — just `label: str`. The PK and other indexing
config lived on the `.export(...)` call, not on `Nodes`.

### 3.2 Relationships — export rows as graph edges

```python
entity_relationship.export(
    "entity_relationship",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="RELATIONSHIP",
            source=cocoindex.storages.NodeFromFields(
                label="Entity",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="subject", target="value"),
                ],
            ),
            target=cocoindex.storages.NodeFromFields(
                label="Entity",
                fields=[
                    cocoindex.storages.TargetFieldMapping(
                        source="object", target="value"),
                ],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

Behavior:

- Every collected row became one `(:Entity)-[:RELATIONSHIP]->(:Entity)` edge.
- The collector fields named in `source.fields` and `target.fields` were
  consumed to identify the endpoint nodes; everything else (`predicate`,
  here) became a relationship property.
- `primary_key_fields=["id"]` identified the **edge** for upserts. The
  `cocoindex.GeneratedField.UUID` pattern in the collector ensured a stable
  edge id across runs.
- Cypher emitted (conceptually): triple-MERGE — `MERGE (s:Entity {value:
  $subject}) MERGE (t:Entity {value: $object}) MERGE (s)-[r:RELATIONSHIP
  {id: $id}]->(t) SET r += $rel_props`.

`Relationships` carried:

- `rel_type: str` — the Cypher relationship type
- `source: NodeFromFields` — how to locate the start node
- `target: NodeFromFields` — how to locate the end node

### 3.3 Document → Entity example (mention edges)

The same `Relationships` mapping handled cross-label edges (one endpoint a
`Document`, the other an `Entity`):

```python
entity_mention.export(
    "entity_mention",
    cocoindex.storages.Neo4j(
        connection=conn_spec,
        mapping=cocoindex.storages.Relationships(
            rel_type="MENTION",
            source=cocoindex.storages.NodesFromFields(
                label="Document",
                fields=[cocoindex.storages.TargetFieldMapping("filename")],
            ),
            target=cocoindex.storages.NodesFromFields(
                label="Entity",
                fields=[cocoindex.storages.TargetFieldMapping(
                    source="entity", target="value")],
            ),
        ),
    ),
    primary_key_fields=["id"],
)
```

The plural `NodesFromFields` appears in this example interchangeably with
`NodeFromFields` — both refer to the same spec.

---

## 4. `NodeFromFields` and `TargetFieldMapping`

`NodeFromFields` was the v0 abstraction for "address a node by content." It
carried:

- `label: str` — the Cypher node label to MERGE against.
- `fields: list[TargetFieldMapping]` — which collector columns identify the
  node (and what they're called *on the node*).

`TargetFieldMapping` was a single field rename:

| Field    | Purpose                                                       |
| -------- | ------------------------------------------------------------- |
| `source` | Name of the field in the collected row.                       |
| `target` | Name of the field on the Neo4j node. Defaults to `source`.    |

So:

- `TargetFieldMapping("filename")` ⇒ collector field `filename` becomes
  `node.filename` (no rename).
- `TargetFieldMapping(source="subject", target="value")` ⇒ collector field
  `subject` becomes `node.value` (rename).

The fields listed in `NodeFromFields.fields` were consumed in the sense that
they did **not** automatically become relationship properties — they had
already been used to identify the endpoint. Other collector columns flowed
to the relationship.

This was the v0 way to express "this collector row is an edge whose
endpoints are derived from these columns of the row" without forcing the
user to materialize endpoint records separately.

---

## 5. `Neo4jDeclaration` — node config without an exporting collector

When a node label was *only* referenced as a relationship endpoint and never
had its own exporting collector, the user still needed somewhere to declare
its primary key (and any vector indexes). That's what `Neo4jDeclaration`
was for, used through `flow_builder.declare(...)`:

```python
flow_builder.declare(
    cocoindex.storages.Neo4jDeclaration(
        connection=conn_spec,
        nodes_label="Entity",
        primary_key_fields=["value"],
    )
)
```

Fields:

| Field                | Required | Purpose                                                                |
| -------------------- | -------- | ---------------------------------------------------------------------- |
| `connection`         | yes      | Auth reference to a `Neo4jConnection` spec.                            |
| `nodes_label`        | yes      | Cypher node label this declaration configures.                         |
| `primary_key_fields` | yes      | Property name(s) that uniquely identify the node — drives the constraint. |
| `vector_indexes`     | no       | Vector index config(s) on this label.                                  |

The combination — exporting collectors emit *content* for nodes they own,
declarations configure *schema* for nodes they don't — is the structural
analogue of today's `mount_table_target` + `declare_table_target` split.

---

## 6. Primary keys and DDL

Every node label and every relationship type had a `primary_key_fields`
parameter on the `.export(...)` call (or on `Neo4jDeclaration` for
referenced-only labels). v0 supported **composite primary keys** —
`primary_key_fields` was always a list, and v0 issued one constraint per
combination. (Today's connectors restrict this to a single PK field.)

DDL emitted by v0 on flow setup, conceptually:

- For each node label: `CREATE CONSTRAINT IF NOT EXISTS FOR (n:Label)
  REQUIRE (n.f1, n.f2, …) IS UNIQUE`
- For each relationship type: a per-PK index (Neo4j edge indexes via
  `CREATE INDEX FOR ()-[r:RT]-() ON (r.id)` once Neo4j 5 supported it).
- For each declared vector index: `CREATE VECTOR INDEX … FOR (n:Label) ON
  n.field OPTIONS { indexConfig: { vector.dimensions: N,
  vector.similarity_function: 'cosine' } }`

DDL was *fully managed* by v0 — there was no `managed_by="user"` escape
hatch. Drop happened on flow teardown.

---

## 7. Vector indexes

v0 surfaced vector indexes via the optional `vector_indexes` field on both
`Nodes` (when the collector owned the label) and `Neo4jDeclaration` (when it
didn't). The index spec carried metric, similarity function, and dimension;
the dimension was either taken from the field's `Vector[float, N]`
annotation or supplied explicitly.

Compared to today's connectors:

- v0 attached vector indexes to label specs declaratively at flow-build
  time.
- Today's `TableTarget.declare_vector_index(...)` is an imperative method
  call, but the underlying handler shape (drop-and-recreate on change) is
  the same.

---

## 8. Auto-generated edge IDs

v0 leaned heavily on `cocoindex.GeneratedField.UUID` to populate the
`primary_key_fields=["id"]` on relationships:

```python
entity_relationship.collect(
    id=cocoindex.GeneratedField.UUID,
    subject=relationship["subject"],
    object=relationship["object"],
    predicate=relationship["predicate"],
)
```

The engine generated a stable UUID per row at collect time so re-runs of the
same logical edge mapped to the same Neo4j relationship. This is the v0
ancestor of the auto-derived `f"{from_table}_{from_id}_{to_table}_{to_id}"`
relation IDs in today's FalkorDB / SurrealDB connectors.

---

## 9. Lifecycle — declarative, not imperative

The v0 flow lifecycle for the Neo4j target:

1. Flow definition collects rows.
2. `.export(...)` and `.declare(...)` calls register *specs* with the
   builder.
3. On flow setup, the engine consumed all specs at once: opened the Neo4j
   connection, ran constraint/index DDL for every spec, recorded a tracking
   record per spec.
4. On every incremental run, collectors produced rows; the engine diffed
   per-row against the prior tracking and issued MERGEs / DELETEs with the
   four-bucket ordering (node-upsert → rel-upsert → rel-delete → node-delete)
   already present in v0.
5. On flow teardown, DDL was reversed.

There was no `mount_*`-style runtime handle. All wiring happened during
`@cocoindex.flow_def`-decorated function evaluation.

---

## 10. Feature comparison — v0 Neo4j vs. today's FalkorDB / SurrealDB

| Feature                         | v0 Neo4j                                   | Today (Falkor / Surreal)                              |
| ------------------------------- | ------------------------------------------ | ----------------------------------------------------- |
| API style                       | Declarative spec → `collector.export(...)` | Imperative `mount_*` + `declare_record/relation`      |
| Module path                     | `cocoindex.storages.*`                     | `cocoindex.connectors.<engine>.*`                     |
| Node spec                       | `Nodes(label=...)`                         | `TableTarget` (with `primary_key=...`)                |
| Relationship spec               | `Relationships(rel_type=, source=, target=)` | `RelationTarget` (`from_table`, `to_table`)         |
| Endpoint addressing             | `NodeFromFields` + `TargetFieldMapping`    | Pass `from_id` / `to_id` literals at write time       |
| Endpoint-only label config      | `Neo4jDeclaration(...)` via `flow_builder.declare(...)` | `declare_table_target(...)`                |
| Composite primary keys          | Yes (`primary_key_fields` list)            | No — single field only                                |
| Vector indexes                  | `vector_indexes=` on Nodes / Declaration   | `table.declare_vector_index(...)` runtime call        |
| Auto edge IDs                   | `GeneratedField.UUID` at collect time      | `f"{from}_{from_id}_{to}_{to_id}"` at write time      |
| DDL lifecycle                   | Always system-managed                      | `managed_by={SYSTEM,USER}` toggle                     |
| Cross-label edges               | `source.label != target.label` in the same `Relationships` spec | Bind one `from_table` and one `to_table` at mount time (Falkor); Surreal allows polymorphic |
| Field-level renaming            | `TargetFieldMapping(source=, target=)`     | None — column name is the field name verbatim        |

---

## 11. Why the model differs today

A few observable shifts from v0 to today's connectors:

- **Imperative beats declarative for graph fan-out.** When a single source
  document yields many nodes and many edges with conditional logic
  (deduping, kind-tagging, stub-then-upsert), the v0 spec-driven model
  required materializing one collector per (label, rel_type) pair. The
  imperative `declare_record` / `declare_relation` API lets a single
  `@coco.fn` write to many tables and many relations from one piece of code.
- **Endpoint addressing moved from row-fields to literal IDs.** v0 inferred
  endpoints by reading collector columns through `NodeFromFields`. Today's
  API takes `from_id` / `to_id` literals — caller decides the value. This
  removes a layer of indirection and removes the renaming step
  (`TargetFieldMapping`) entirely.
- **Connection abstraction unified.** v0's `add_auth_entry` +
  `Neo4jConnection` is replaced by a generic `ContextKey[ConnectionFactory]`
  that works the same way for every connector.
- **DDL became toggleable.** `managed_by="user"` lets users plug into
  pre-existing Neo4j infra without forcing CocoIndex to own the schema.

---

## 12. Sources

Documentation pages I drew from (the v0 page itself is gone, but the API
surface is preserved in the Knowledge Graph for Docs blog post and in
example code):

- [Build Real-Time Knowledge Graph For Documents with LLM — cocoindex.io](https://cocoindex.io/blogs/knowledge-graph-for-docs/)
- [Real-Time Product Recommendation Engine with LLM and Graph Database — cocoindex.io](https://cocoindex.io/blogs/product-recommendation)
- [docs_to_knowledge_graph example — cocoindex GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph)
- v0 docs index page (cached): `cocoindex.io/docs-v0/targets/neo4j/`
