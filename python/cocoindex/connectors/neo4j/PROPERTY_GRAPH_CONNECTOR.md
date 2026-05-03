# Property Graph Connectors in CocoIndex

Design notes for implementing a Neo4j property-graph target, distilled from the
existing `falkordb/` and `surrealdb/` connectors. This is a learning doc, not a
spec — its purpose is to capture the shared skeleton both connectors follow so
a third graph backend can be added by filling in the engine-specific holes.

---

## 1. The property-graph data model

A property graph has two first-class entity kinds:

- **Nodes** — labelled vertices, each with a primary-key property and an open
  set of other properties.
- **Relationships** — directed, typed edges between two nodes, each with their
  own primary key and own properties.

Both connectors map this onto the same user-facing API:

| Concept       | User type                            | Backed by                                      |
| ------------- | ------------------------------------ | ---------------------------------------------- |
| Node table    | `TableTarget[RowT]`                  | Cypher node label / SurrealDB normal table     |
| Relation type | `RelationTarget[RowT]`               | Cypher relationship type / SurrealDB relation  |
| Connection    | `ContextKey[ConnectionFactory]`      | Per-flow runtime injection                     |
| Schema        | `TableSchema[RowT]` + `ColumnDef`    | Optional; metadata for FalkorDB, DDL for Surreal |

The user-facing entry points are always the same triplet per kind:

```python
table_target(...)            # build a TargetState (composable inside handlers)
declare_table_target(...)    # declare for use as a relationship endpoint
mount_table_target(...)      # mount and return a live TableTarget

relation_target(...)
declare_relation_target(...)
mount_relation_target(...)
```

`mount_*` is `async`, returns a target ready for `declare_record` /
`declare_relation`. The other two return descriptors for declarative wiring.

---

## 2. Layered architecture

Both connectors share a six-layer skeleton, with the **top three layers being
identical** (provided by `cocoindex.connectorkits` / `cocoindex._internal`) and
the **bottom three engine-specific**:

```
        ┌──────────────────────────────────────────────────┐
USER    │  TableTarget          RelationTarget             │
        │     declare_record       declare_relation        │
        ├──────────────────────────────────────────────────┤
FRAMEWORK │  coco.TargetHandler                            │
        │   coco.TargetActionSink                          │
        │   coco.TargetReconcileOutput                     │
        │   coco.ChildTargetDef                            │
        │   coco.register_root_target_states_provider      │
        │   connectorkits.statediff (diff_composite, …)    │
        │   connectorkits.fingerprint.fingerprint_object   │
        ├──────────────────────────────────────────────────┤
ENGINE  │  _TableHandler  ──parent of──►  _RecordHandler   │
        │   reconcile()                    reconcile()     │
        │   _apply_actions (DDL)           (per-row diff)  │
        │                                                  │
        │  _SharedRecordApplier                            │
        │   _apply_actions (writes the actual queries)     │
        │                                                  │
        │  _VectorIndexHandler (attachment)                │
        │  ConnectionFactory                               │
        │  Cypher / SurrealQL string builders              │
        │  _LEAF_TYPE_MAPPINGS  (Python type → engine type)│
        └──────────────────────────────────────────────────┘
```

Each connector is **one ~1500 LOC `_target.py`**, optionally with a small
side-module of pure query-string builders (FalkorDB has `_cypher.py`;
SurrealDB inlines its SurrealQL).

---

## 3. The two-level state system

Every graph connector manages **two reconcilers** that share state via
`statediff.MutualTrackingRecord`:

### Table-level (`_TableHandler`)

- Owns DDL — `CREATE INDEX` / `DEFINE TABLE` / vector-index DDL on setup,
  inverse on teardown.
- Subclasses `coco.TargetHandler[_TableSpec, _TableTrackingRecord, _RecordHandler]`.
  The third type parameter makes it a **parent handler**: its `_apply_actions`
  returns `ChildTargetDef[_RecordHandler]`, and the framework wires the child
  in to receive row-level state.
- Registered once per connector at module import:
  ```python
  _table_provider = coco.register_root_target_states_provider(
      "cocoindex/<engine>/table", _TableHandler()
  )
  ```
- `reconcile()` diffs the desired `_TableSpec` against persisted
  `_TableTrackingRecord`s using `statediff.diff_composite`. Output is a
  `main_action ∈ {None, insert, upsert, replace, delete}` plus per-column
  actions. On `replace`, returns `child_invalidation="destructive"` so all
  rows for the table are forced to re-upsert on the next pass.

### Record-level (`_RecordHandler`)

- Owns per-row upsert/delete.
- Subclasses `coco.TargetHandler[_RowValue, _RowFingerprint]`.
- For each declared row, fingerprints the desired state with
  `fingerprint_object()` and compares against `prev_possible_records:
  Collection[_RowFingerprint]`. If unchanged → returns `None` (no-op). If
  changed → emits a `_RecordAction` for the apply step.
- Exposes vector indexes as **attachments**:
  ```python
  def attachments(self) -> dict[str, _VectorIndexHandler]:
      return {"vector_index": _VectorIndexHandler(self._graph, self._table_name)}
  ```

This split is what makes the connector **incremental**: every row carries a
content fingerprint, every reconcile is a fingerprint diff, unchanged rows are
never rewritten.

---

## 4. The four-bucket apply ordering

Inside `_SharedRecordApplier._apply_actions`, batched actions are sorted into
four ordered buckets so an edge is never left referencing a missing endpoint:

```python
upsert_normal:    list[_RecordAction] = []   # node creates/updates
upsert_relation:  list[_RecordAction] = []   # edge creates/updates
delete_relation:  list[_RecordAction] = []   # edge deletes
delete_normal:    list[_RecordAction] = []   # node deletes

for action in upsert_normal:    await self._apply_node_upsert(action)
for action in upsert_relation:  await self._apply_relation_upsert(action)
for action in delete_relation:  await self._apply_relation_delete(action)
for action in delete_normal:    await self._apply_node_delete(action)
```

The same ordering also applies at the **table-DDL level** in
`_TableHandler._apply_actions`:

```python
ordered = create_normal + create_relation + remove_relation + remove_normal
```

so an index/constraint isn't dropped while edges still depend on it.

---

## 5. Engine-specific surface

What changes between `falkordb/` and `surrealdb/` (and what you'd fill in for
Neo4j) is mostly six things:

### 5.1 `ConnectionFactory.acquire`

| | FalkorDB | SurrealDB |
|---|---|---|
| Driver | `falkordb.asyncio.FalkorDB.from_url(uri)` | `surrealdb.AsyncSurreal(url)` |
| Selector | `client.select_graph(graph_name)` | `await conn.signin(creds); await conn.use(ns, db)` |
| Returns | `AsyncGraph` (one method `query(cypher, params)`) | `AsyncSurreal` (one method `query(surql)`) |

For Neo4j: open `neo4j.AsyncGraphDatabase.driver(uri, auth=…)`, return a
`session(database=…)`-like object whose calls translate to `tx.run(cypher,
**params)`.

### 5.2 Type mapping

Every connector defines `_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping]`
shaped:

```python
class _TypeMapping(NamedTuple):
    engine_type: str           # metadata or DDL string
    encoder: Callable | None   # value transform run at write time
```

Common mappings:

| Python | encoder | Notes |
|---|---|---|
| `int`, numpy ints | none | Native integer |
| `float`, numpy floats | none | Native float |
| `bool` | none | Native bool |
| `Decimal` | `str(v)` | Engines without native decimal store as string |
| `bytes` | `base64.b64encode(...).decode()` | When engine has no bytes type |
| `datetime`, `date`, `time` | `v.isoformat()` | When engine has no temporal type |
| `timedelta` | `int(v.total_seconds() * 1000)` | Encode as ms integer |
| `UUID` | `str(v)` | Encode as string |
| `np.ndarray` | `v.tolist()` | Plus a `vector<f32, N>`-style type string |
| `dict` / record / union | none | Map / object |
| `list` / sequence | none | Array |

The mapping is consulted in `TableSchema.from_class` at schema-build time.
Per-field overrides come through `Annotated[T, FalkorType(...)]` /
`Annotated[T, SurrealType(...)]` / a `column_overrides` dict.

For Neo4j (Bolt), no bytes/decimal/uuid encoding is needed if you target the
modern driver (it has native types for all of them); however, you still need a
`vector_size` annotation for `np.ndarray` to emit the vector index DDL.

### 5.3 Query string builders

The query layer is the **only place where the engine's syntax appears**.
FalkorDB factors all of this into [_cypher.py](_cypher.py) — pure functions,
no I/O — so it can be unit-tested without a live database. SurrealDB inlines
its strings inside `_SharedRecordApplier` and `_TableHandler._create_table`.
Either pattern works; the pure-module pattern is cleaner once you have more
than ~5 distinct query shapes.

The query shapes a graph connector needs:

| Operation | Falkor (Cypher)                                                          | Surreal (SurrealQL)                                |
| --------- | ------------------------------------------------------------------------ | -------------------------------------------------- |
| Node upsert     | `MERGE (n:Label {pk: $key_0}) SET n += $props`                        | `UPSERT tbl:id CONTENT {…}`                        |
| Node delete     | `MATCH (n:Label {pk: $key_0}) DETACH DELETE n`                        | `DELETE tbl:id`                                    |
| Rel upsert      | `MERGE (s:From {…}) MERGE (t:To {…}) MERGE (s)-[r:RT {pk: …}]->(t) SET r += $props` | `DELETE rel:id; RELATE in->rel:id->out CONTENT {…}` |
| Rel delete      | `MATCH ()-[r:RT {pk: $key_0}]->() DELETE r`                            | `DELETE rel:id`                                    |
| Node index      | `CREATE INDEX FOR (e:Label) ON (e.pk)` + best-effort GRAPH.CONSTRAINT  | implied by SCHEMAFULL + DEFINE FIELD               |
| Rel index       | `CREATE INDEX FOR ()-[e:RT]-() ON (e.pk)`                              | implied                                            |
| Vector index    | `CREATE VECTOR INDEX FOR (e:Label) ON (e.field) OPTIONS {dimension, similarityFunction}` | `DEFINE INDEX name ON tbl FIELDS field MTREE\|HNSW DIMENSION N DIST <metric> TYPE F32` |
| Table create    | (none — labels are implicit)                                           | `DEFINE TABLE name [TYPE RELATION FROM x|y TO z] [SCHEMAFULL\|SCHEMALESS]` then per-col `DEFINE FIELD` |
| Table drop      | drop index + drop constraint                                           | `REMOVE TABLE IF EXISTS name`                      |

For Neo4j the analogue would be:

```cypher
-- node upsert (Neo4j 5.x)
MERGE (n:Label {pk: $key_0}) SET n += $props

-- relationship upsert (note: same triple-MERGE shape as FalkorDB)
MERGE (s:From {pk: $from_key_0})
MERGE (t:To   {pk: $to_key_0})
MERGE (s)-[r:RT {pk: $rel_key_0}]->(t)
SET r += $props

-- vector index (Neo4j 5.13+)
CREATE VECTOR INDEX `idx_Label_field` IF NOT EXISTS
FOR (e:Label) ON e.field
OPTIONS { indexConfig: {
  `vector.dimensions`: $dim,
  `vector.similarity_function`: $metric
} }
```

Critically, **all identifiers (labels, property names, index names) must be
validated at API entry**, not escaped at query-construction time, because
Cypher labels and property names cannot be parameter-bound. Both existing
connectors share the same regex:

```python
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
```

Values, by contrast, **always** bind via `$param` placeholders.

### 5.4 Transaction shape

| | FalkorDB | SurrealDB | Neo4j (suggested) |
|---|---|---|---|
| Per-batch | N round-trips, one per action | One multi-statement `BEGIN…COMMIT` blob, content inlined as JSON | One `tx` per batch — `async with session.begin_transaction() as tx: for action: await tx.run(...)` |
| Failure mode | partial — connector relies on idempotency of MERGE/DELETE | atomic — whole batch rolls back | atomic |

### 5.5 Endpoint binding semantics

This is **the** subtle difference between the two existing connectors:

- **FalkorDB / Cypher** — relations triple-MERGE the source, target, then the
  edge. Endpoints are auto-created if absent, but their property values are
  owned by their own table's record handler. Endpoint labels are
  monomorphic — `RelationTarget` binds exactly one `from_table` and one
  `to_table` at mount time.
- **SurrealDB** — relations `RELATE` an existing record-id pair. Endpoints
  must already exist (or the COMMIT fails). Endpoint tables can be
  **polymorphic** — `RelationTarget` accepts a `Collection[TableTarget]` for
  `from_table` / `to_table`, and the user disambiguates per
  `declare_relation` call by passing `from_table=person_target`.

For Neo4j, follow the FalkorDB model: monomorphic endpoints, triple-MERGE,
auto-create on absent. Polymorphic endpoints are tempting but they require an
`UNWIND … MATCH` pattern that breaks the simple PK-binding contract.

### 5.6 Auto-derived relation IDs

Both connectors derive a relation primary key when the user doesn't pass one:

```python
record_id = f"{from_table_name}_{from_id}_{to_table_name}_{to_id}"
```

This makes `declare_relation(from_id=A, to_id=B)` idempotent without forcing
the user to invent a key. Keep this contract for any new connector — flows
that don't model edges as records depend on it.

---

## 6. The user-facing facade

`TableTarget` and `RelationTarget` are **thin** (~150 LOC each). Their job is:

1. Hold a `TargetStateProvider` returned by `coco.mount_target(...)` (live)
   or `coco.declare_target_state_with_child(...)` (pending).
2. Hold the table's primary-key field name and (optionally) its `TableSchema`
   for encoder lookup.
3. Convert a row dataclass / NamedTuple / Pydantic model / dict to a
   `dict[str, Any]` via either the schema's `columns` map or `RecordType`
   field introspection — applying any per-column `encoder`.
4. For relations, build a `_RelationRowValue(from_label, from_pk_field,
   from_id, to_label, to_pk_field, to_id, fields)` — endpoints are stored
   **structured**, not pre-formatted, so the actual escape happens at apply
   time.
5. Auto-derive a relation `id` when the user doesn't pass one.
6. End at exactly one line:
   ```python
   coco.declare_target_state(self._provider.target_state(pk_values, row_value))
   ```

Everything else (diffing, batching, ordering, applying) belongs to the
framework + the engine layer.

---

## 7. Recipe: adding a Neo4j connector

A reasonable layout for `cocoindex/connectors/neo4j/`:

```
neo4j/
├── __init__.py        # re-export from _target
├── _cypher.py         # pure query-string builders (mirror falkordb/_cypher.py)
└── _target.py         # everything else
```

Step by step:

1. **`_cypher.py`** — port [falkordb/_cypher.py](../falkordb/_cypher.py) to
   Neo4j. The Cypher dialect is close enough that most builders need only the
   vector-index DDL changing. Keep `validate_identifier` + the `_quote`
   helper unchanged.

2. **`ConnectionFactory`** — wrap `neo4j.AsyncGraphDatabase.driver(uri, auth=…)`,
   plus a `database` parameter (Neo4j supports multi-database). `acquire()`
   returns a thin wrapper exposing `query(cypher, params)` that internally
   does `async with driver.session(database=db) as s: await s.run(cypher,
   **params)`.

3. **`_LEAF_TYPE_MAPPINGS`** — Neo4j's Bolt protocol has native bool, int,
   float, str, bytes, list, map, point, date, time, datetime, duration. So:
   - `Decimal` → `str` (Neo4j has no decimal)
   - `UUID` → `str`
   - `bytes` → native (no encoder)
   - `datetime` / `date` / `time` → native (no encoder)
   - `timedelta` → native `Duration` (no encoder needed if the driver
     accepts `datetime.timedelta`)
   - `np.ndarray` → list (and emit a `LIST<FLOAT>` type string for the
     vector index)

4. **`_TableSpec` / `_TableMainRecord` / `_FieldTrackingRecord`** — copy
   FalkorDB's shape verbatim. Neo4j has *real* per-property indexes
   (`CREATE INDEX FOR (e:L) ON (e.prop)`) that you may want to wire to
   `column_actions`, but for v1 mirror FalkorDB and only emit DDL on the PK.

5. **`_SharedRecordApplier`** — implement the four-bucket sort, but wrap each
   batch in a single transaction:
   ```python
   async with session.begin_transaction() as tx:
       for action in upsert_normal:    await self._apply_node_upsert(tx, action)
       for action in upsert_relation:  await self._apply_relation_upsert(tx, action)
       for action in delete_relation:  await self._apply_relation_delete(tx, action)
       for action in delete_normal:    await self._apply_node_delete(tx, action)
   ```
   This gets you the atomic-batch semantics for free, which matches Neo4j's
   strengths.

6. **`_VectorIndexHandler`** — drop-and-recreate on spec change, mirror
   FalkorDB. Neo4j's DROP is `DROP INDEX <name> IF EXISTS` (named, unlike
   FalkorDB's by-(label,field)), so persist the index name in the tracking
   record.

7. **`_TableHandler`** — copy FalkorDB's `_TableHandler` more or less
   verbatim. The only DDL difference: Neo4j has `CREATE CONSTRAINT … REQUIRE
   n.pk IS UNIQUE` (real, not best-effort), which can be issued as Cypher
   instead of FalkorDB's `GRAPH.CONSTRAINT` redis command.

8. **`TableTarget` + `RelationTarget` + the six entry points** — copy from
   FalkorDB directly. Keep FalkorDB's `primary_key=` configurability (it's
   strictly more general than SurrealDB's hardcoded `id`).

9. **`__all__`** — match the existing connectors' export list:
   ```python
   __all__ = [
       "ColumnDef", "ConnectionFactory", "Neo4jType", "RelationTarget",
       "TableSchema", "TableTarget", "ValueEncoder",
       "declare_relation_target", "declare_table_target",
       "mount_relation_target", "mount_table_target",
       "relation_target", "table_target",
   ]
   ```

The total LOC budget should land near FalkorDB's (~1600 + ~200 for
`_cypher.py`), maybe slightly less because Neo4j gives you native types and
real constraints out of the box.

---

## 8. Reference — what's reusable across all connectors

These primitives come from `cocoindex.connectorkits` and `cocoindex._internal`
and are what you should *never* re-implement:

- `coco.register_root_target_states_provider(name, handler)`
- `coco.TargetHandler[Spec, TrackingRecord, ChildHandler]`
  — base class with `reconcile(key, desired_state, prev_possible_records,
  prev_may_be_missing)`.
- `coco.TargetActionSink.from_async_fn(apply_fn)`
- `coco.TargetReconcileOutput(action, sink, tracking_record, child_invalidation=…)`
- `coco.ChildTargetDef(handler=…)`
- `coco.declare_target_state(...)` / `coco.mount_target(...)` /
  `coco.declare_target_state_with_child(...)`
- `connectorkits.statediff.MutualTrackingRecord` /
  `CompositeTrackingRecord` / `resolve_system_transition` /
  `diff_composite` / `diff` — diffs desired vs. persisted state into
  `DiffAction`s (`insert | upsert | replace | delete`).
- `connectorkits.fingerprint.fingerprint_object` — content-addressing for
  rows, what makes per-row reconciles cheap.
- `connectorkits.target.ManagedBy.{SYSTEM, USER}` — toggles whether DDL is
  emitted at all (`USER` skips DDL entirely; useful for shared infra).
- `cocoindex._internal.datatype.{RecordType, analyze_type_info,
  is_record_type, ...}` — Python-type introspection used by
  `TableSchema.from_class`.
- `cocoindex.resources.schema.VectorSchema` /
  `VectorSchemaProvider` — vector-dimension annotation, consulted in
  `_get_type_mapping`.

---

## 9. References

- [falkordb/_target.py](../falkordb/_target.py) — full FalkorDB connector
- [falkordb/_cypher.py](../falkordb/_cypher.py) — pure Cypher builders
- [surrealdb/_target.py](../surrealdb/_target.py) — full SurrealDB connector
- [docs/connectors/falkordb](https://cocoindex.io/docs/connectors/falkordb/)
- [docs/connectors/surrealdb](https://cocoindex.io/docs/connectors/surrealdb/)
