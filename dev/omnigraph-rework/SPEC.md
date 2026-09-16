# Omnigraph Connector Rework — Technical Spec

**Author:** Roman Pronskiy · **Created:** 2026-09-06

> 📄 **This is a living document.** Status markers, decisions, and guardrail outcomes are meant to be updated as the work happens. See [How to Update This Document](#how-to-update-this-document) before editing.

### Changelog

| Date | Change | Author |
|------|--------|--------|
| 2026-09-06 | Initial spec created from `dev/omnigraph-connector-transport-analysis.md` plus a live probe of `omnigraph-server` 0.10.0 | Roman Pronskiy |
| 2026-09-06 | SDK is ModernRelay's official Python client; renamed the package from `omnigraph-sdk` to `omnigraph` (decisions 17, 18) | Roman Pronskiy |
| 2026-09-06 | SDK repository starts at `pronskiy/omnigraph-python`, moving to the ModernRelay organisation in H1.5 (decisions 19, 20) | Roman Pronskiy |
| 2026-09-06 | Added step C2.7: claim the PyPI name with a functional `0.10.0a1` (decisions 21-23) | Roman Pronskiy |
| 2026-09-07 | **Rework.** The Python SDK is cut and the CLI transport is removed: HTTP is the connector's only transport (decisions 24-30). Old Epics C (SDK package), G (direct S3), and H (SDK polish) are cut; old Epic E (cluster control plane) is replaced by a spike-gated schema epic. Epics renumbered A-E | Roman Pronskiy || 2026-09-07 | Probes P1, P2, P12 answered live: HTTP schema apply is refused on every server configuration, there is no graph-creation endpoint, and no server hot-reloads. `managed_by="system"` is kept — every other connector supports it — through a `cluster apply` control plane behind an `apply_schema()` seam. Epic D rewritten; decisions 31-33 | Roman Pronskiy |

### Status legend

🔲 Not started · 🔄 In progress · ✅ Done · ⏸️ Blocked · ❌ Cut

### Current focus

**Now on:** Epic A → Phase A2 — probes P1, P2, and P12 are answered (see `server-spike.md`); P3 through P11 and the new P13 remain. Phase A1's fixture exists only as a manual setup and still needs to become the reusable pytest fixture described in A1.1-A1.3.

---

## 1. Executive summary

The Omnigraph target connector on branch `omnigraph-connector` drives the `omnigraph` CLI against a direct `file://` store. That works for local and single-host use, but it cannot serve a deployed `omnigraph-server`: every operation is a subprocess, errors are text, locks are host-local, and workers need storage credentials plus a pinned 211 MB binary.

The previous version of this spec answered that by building an official Python SDK and keeping the CLI alongside it. Both are now cut. A published SDK is one more repository, PyPI project, release cadence, and version lock to maintain, and its only consumer would be this connector. Keeping the CLI alongside HTTP means maintaining two transports, two sets of semantics, and two test modes forever.

**The connector talks HTTP to `omnigraph-server`, and that is the only transport.** Roughly nine endpoints, `aiohttp` (already an optional dependency in this repository) and `msgspec` (already a core one), inside `python/cocoindex/connectors/omnigraph/_client.py`. No second repository, no PyPI project, no generated wire models, no drift check, no version-sync automation, and no binary on data-plane workers.

Three costs are accepted and tracked here rather than discovered later:

1. There is no "point it at a directory" mode any more. `omnigraph-server` is cluster-only at boot, so every user needs a running server. cocoindex documents how to start one and ships no process management (decision 27).
2. The existing live suite (~331 tests as of 2026-09-06) runs against a `file://` store through the CLI. All of it migrates to a server + rustfs fixture (Epic E1), and a representative slice migrates early, during Epic C, so server-side semantic differences surface while the client is still soft.
3. `managed_by="system"` — automatic type creation and evolution — cannot be done over HTTP at all. Probed live on 2026-09-07: `POST /schema/apply` returns 409 on **every** server configuration, because `--cluster` is the server's only boot source and the refusal is scoped to cluster-backed serving. Since all fourteen connectors that use `ManagedBy` default to `SYSTEM` and none refuses it, dropping the feature is not acceptable (decision 32). Epic D therefore keeps it through a control plane — edit the cluster config's `.pg`, `omnigraph cluster apply`, restart hook, poll until the served schema matches — behind an `apply_schema()` seam so a future server-side apply replaces it in one place. This is the one place the connector still needs the `omnigraph` binary, and only on the host that runs schema changes.

Everything is open source; nothing here is commercial.

---

## 2. Technical decisions

| Area | Decision | Rationale |
|------|----------|-----------|
| Transport | HTTP to `omnigraph-server` is the connector's only transport. The CLI subprocess path is deleted, not kept alongside. | One code path, one set of semantics, one test mode. The connector is not on `main`, so there is no compatibility obligation. |
| No SDK | The HTTP client is private to the connector (`_client.py`), not a published package. | A public SDK is a repository, a PyPI project, a release cadence, and a version lock whose only consumer is this connector. |
| HTTP stack | `aiohttp>=3.9` behind the optional extra `cocoindex[omnigraph]`; JSON through `msgspec`. | `aiohttp` is already an optional extra here (doris) with a mypy override in place, and `msgspec` is already a core dependency. No new dependency enters the ecosystem. |
| Client lifecycle | `ConnectionFactory` lazily creates and caches one `aiohttp.ClientSession`; the environment lifespan closes it. | Matches the Neo4j convenience API; users provide connection details, not a session. |
| Local development | Documented, not managed. cocoindex ships no server-bootstrap API; the docs give a copy-paste `omnigraph-server` command and the test suite has a private fixture. | Process management and binary discovery are exactly what removing the CLI deleted; re-adding them as public API would undo the change. |
| Object store | rustfs is the S3-compatible backend for the test fixture and CI, in both server boot modes. | Standard for Omnigraph. It also means object-store use is verified by the default suite rather than by a deferred epic. |
| Schema | `managed_by="system"` is supported through a control plane: edit the type's block in the cluster config's `.pg`, run `omnigraph cluster apply`, restart via a user hook, poll `GET /schema` until it matches. It sits behind an `apply_schema()` seam so a future server-side apply replaces it in one place. | Answered live 2026-09-07 (P1, P12): HTTP `schema/apply` returns 409 on **every** server configuration — `--cluster` is the server's only boot source — and no server reloads schema without a restart. Every other cocoindex connector supports `SYSTEM` and defaults to it, so user-managed-only is not an acceptable end state (decision 32). |
| Graph provisioning | An unknown graph fails with a message naming the graph, the base URL, and the `cluster apply` procedure. Where a `ClusterConfig` is present the connector can create the graph itself, since `cluster apply` creates graphs (verified: `graph.probe create applied`). | P2: there is no graph-creation endpoint — `/graphs` is GET-only, unknown graph gives 404 `not_found`. |
| Atomicity | No host-local locks. Scratch branches carry the worker's identity and a UTC timestamp; the reaper deletes only branches past a TTL and never one this process created. `exclusive_store()` is removed once probe P3 confirms the server serializes writes and merges per graph. | `flock` coordinates one host; workers are many hosts. Server-side serialization is the only ordering that generalises. |
| Retries | Never auto-retry writes. Reads may retry with backoff behind an opt-in. Ambiguous write outcomes surface as errors; reconciliation re-reads on the next run. | A lost response after a durable commit must not duplicate work. |
| Errors | Mapped from HTTP status plus the `code` field and structured sub-objects (`merge_conflicts`, `precondition_failure`, `resource_limit`, `key_conflict`). Non-JSON bodies are tolerated verbatim. | The server returns 422 plain text for body deserialization errors (verified live). |
| Version coupling | A `/healthz` check at first use. An unexpected server minor logs one warning and proceeds. | The server routes new capabilities on new paths, so an old server returns 404 rather than a wrong result. Hard-failing on a minor bump would be stricter than the server's own contract. |
| Contract drift | The live suite against a pinned `omnigraph-server` 0.10.0 is the contract test. There are no generated models to drift. | The honest cost of hand-rolling: keep the surface at roughly nine calls so the suite can cover all of it. |
| Baseline | Omnigraph 0.10.0 only; no 0.9 compatibility. | Decided 2026-09-04; the live suite passes on 0.10.0 unchanged. |
| De-risking | The fixture and the probe list come first. No client code is written until every probe has a recorded answer. | Three of the eleven probes change the shape of what gets built. |

---

## 3. Architecture overview

```
      CocoIndex engine: declared target states, change detection
                            │ actions per component
                            ▼
   _target.py   reconciliation · identity (coco_key) · ownership
                scratch-branch atomicity · endpoint stubs
                    │                               │ apply_schema() seam  (D1.1)
                    │ typed calls, typed errors     ▼
                    │                     _cluster.py   ClusterConfig(config_dir, restart)
                    ▼                                   merge the type's block into <graph>.pg
   _client.py   ConnectionFactory(base_url,             `omnigraph cluster apply --config`
                  graph_id, branch, token)              restart hook → poll GET /schema
                _HttpClient — aiohttp,                       │
                  JSON via msgspec                           │ writes cluster state
                    │ POST /query · /mutate                  │ (never contacts the server)
                    │      /branches/*                       │
                    │ GET  /schema · /healthz                │
                    ▼                                        ▼
              omnigraph-server  ────────────────────  rustfs / S3 / cluster dir
                     ▲                                       │
                     └───── restart, then serves the new schema (P12)
```

**The data plane is pure HTTP.** The only exception is schema: the server refuses `POST
/schema/apply` on every configuration (P1), so `managed_by="system"` goes around it through the
cluster config. `cluster apply` writes to the store and never contacts the server, so the control
plane needs the config directory and store credentials but not colocation with the server — only
the restart has to happen where the server runs, which is what the user-supplied hook is for.

`_gq.py` (GQ rendering and `.pg` schema editing) is transport-agnostic today and is untouched by
this rework — the control plane reuses `merge_type_into_schema` and `remove_type_from_schema`
verbatim. A component's actions arrive at `_target.py`, which plans commits and asks the client
resolved from the target's `ContextKey` to execute them.

---

## 4. Epics

| Epic | Name | MVP | Depends on |
|------|------|-----|------------|
| A | Server + rustfs fixture and probe spike | Yes | — |
| B | HTTP client | Yes | A |
| C | Connector adaptation | Yes | B |
| D | Schema management (`managed_by="system"`) | Yes | A, B |
| E | Suite migration, docs, CI | Yes | B, C, D |

B and C do not touch schema, so they proceed in parallel with D.

---

### Epic A — Server + rustfs fixture and probe spike  ·  MVP

**Goal:** A reusable fixture that boots `omnigraph-server` 0.10.0 over rustfs in both boot modes, and a recorded, reproducible answer to every server-behaviour question the connector depends on — before any client code exists. Output is a findings document and decision rows, not shipped code (the fixture itself is shipped and reused by Epics C and E).
**Success metrics:** Every probe in A2 has a command and a response in `dev/omnigraph-rework/server-spike.md`; probes P1, P2, and P3 each have a decision row in §6.

The author has access to the Omnigraph source, so answers may come from reading it, but each one is still confirmed against the running binary and recorded with the command and the response.

#### Phase A1 — Fixture

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| A1.1 | rustfs fixture: start an S3-compatible endpoint, create a bucket, expose credentials and endpoint URL | 🔲 | |
| A1.2 | Server fixture booting `omnigraph-server --unauthenticated` in **both** modes: `--cluster <config dir>` and `--cluster <storage-root URI>` | 🔲 | |
| A1.3 | Probe harness: a small raw-HTTP helper (`aiohttp`, no connector code) plus `restart()` / `stop()` on the fixture | 🔲 | |

**Steps (detail):**

- **A1.1 — rustfs.** Deliverable: `python/tests/connectors/omnigraph_server_fixture.py` gains a fixture that starts rustfs, creates a bucket, and yields the endpoint URL plus credentials. Use a testcontainers-managed container following the postgres pattern in `AGENTS.md` — module-scoped sync fixture for the backend, function-scoped async fixture for per-test resources. If no usable image is available, fall back to the binary with a documented download step, and record which one in a decision row.
- **A1.2 — Server boot.** Deliverable: the same module boots `test/bin/omnigraph-server` on a free `127.0.0.1` port and polls `/healthz` until `{"status":"ok"}`. Two parametrisations, because they are different products for schema purposes: a `cluster.yaml` config directory (`version: 1`, `graphs.<id>.schema: <path>`, applied with `omnigraph cluster apply --config`), and a bare storage-root URI pointing at the rustfs bucket. `omnigraph-server` cannot bind a port inside the Claude Code sandbox, so these tests run with the sandbox disabled.
- **A1.3 — Harness.** Deliverable: an async helper that issues raw requests against the booted server and returns status, headers, and body without interpretation, so probe answers record what the server actually said. `restart()` stops and re-boots with the same storage, for the probes that need it.

**Exit guardrails — Phase A1 → A2**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Both modes boot | A test boots the server over rustfs in cluster-dir mode and in storage-root mode and gets `{"status":"ok"}` from `/healthz` in each | 🔲 | |
| Reproducible | The fixture runs twice in a row from a clean state with no manual step | 🔲 | |

#### Phase A2 — Probes

Answers go to `dev/omnigraph-rework/server-spike.md`, each with the exact request and response.

| Probe | Question | Decides | Status | Answer |
|-------|----------|---------|--------|--------|
| P1 | Does a server booted from a bare storage-root URI accept `POST /graphs/{id}/schema/apply`, or return 409 as the cluster-dir boot does? | **Epic D's path** | ✅ | **No — 409 `conflict`, "server-side schema apply is disabled for cluster-backed serving". `--cluster` is the server's only boot source, so this holds for every server. Epic D keeps `managed_by="system"` through the control plane instead.** |
| P2 | With no CLI, is there any HTTP path that creates a graph that does not yet exist? If not, what does the server answer for an unknown graph id? | **Graph provisioning; the uninitialized-store branch in `_target.py`** | ✅ | **No creation route — `/graphs` is GET-only (405 on POST). Unknown graph → 404 `{"error":"graph 'X' not found","code":"not_found"}`. Provisioning is permanently an operator task.** |
| P12 | Does a running server pick up an applied revision without a restart? | **Whether the control plane needs a restart hook** | ✅ | **No — `cluster apply` succeeds, the served schema never changes; a restart picks it up. `cluster apply` never contacts the server, so the control plane needs the config dir and store credentials, not colocation.** |
| P13 | Does `cluster apply` refuse while non-main branches exist, as direct-store `schema apply` does? | D2.6 — control plane vs. scratch branches | 🔲 | |
| P3 | Does the server serialize concurrent writes and concurrent branch merges per graph? | **Whether `exclusive_store()` can be removed outright** | 🔲 | |
| P4 | Merge conflict over HTTP: status, `code`, and the `merge_conflicts` shape | Error hierarchy, failure matrix | 🔲 | |
| P5 | Endpoint-not-found wording over HTTP — is it the same engine message the CLI printed (`(src\|dst) '…' not found in \w+`)? | Endpoint-stub recovery | 🔲 | |
| P6 | Is the 8,192-entity mutation cap enforced server-side, and as 413 with `resource_limit` or as 400? | Chunking, failure matrix | 🔲 | |
| P7 | Mixed upsert and delete in one mutation — same refusal as the direct store? | Scratch-branch flow | 🔲 | |
| P8 | Does `branch delete` over HTTP need a consent equivalent of the CLI's `--yes` on a non-local store? | Branch teardown | 🔲 | |
| P9 | Is `merge` with `delete_branch: true` reliable enough to drop the explicit delete? | Scratch-branch teardown | 🔲 | |
| P10 | Bearer token setup and the minimal Cedar action set for query, mutate, branch create/merge/delete, schema get | Epic E2 least-privilege test | 🔲 | |
| P11 | `mutate/if-graph-commit`: the 412 body, and whether a read's `graph_commit_id` is usable as the precondition | Optional single-commit fast path | 🔲 | |

**Exit guardrails — Epic A → Epics B, C, D**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Probes answered | All eleven probes have a verified answer with command and response in `server-spike.md` | 🔲 | |
| Path chosen | P1's answer is a decision row in §6 | ✅ | Decisions 31-33: HTTP apply impossible everywhere; SYSTEM supported via the control plane behind an `apply_schema()` seam |
| Provisioning settled | P2's answer is a decision row stating how a missing graph is created or reported | ✅ | Decision 31: no creation endpoint; created via `cluster apply` when a `ClusterConfig` is present, else reported |
| Locking settled | P3's answer is a decision row stating whether `exclusive_store()` is removed or replaced | 🔲 | |

---

### Epic B — HTTP client  ·  MVP

**Goal:** `_client.py` is an `aiohttp`-based client for the roughly nine endpoints the connector needs, with typed results and a typed error hierarchy, and no subprocess, temp file, or file lock anywhere in it.
**Success metrics:** Every method has a mocked-transport unit test for its success path and every error class; a live smoke test passes against the Epic A fixture; `uv run mypy` and `uv run ruff format --check .` clean.

#### Phase B1 — Factory, session, packaging

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| B1.1 | `ConnectionFactory` with `base_url`, `graph_id`, `branch`, `token`, `timeout`; lazy cached `aiohttp.ClientSession`; `aclose()` from the lifespan | 🔲 | |
| B1.2 | Token handling: `OMNIGRAPH_TOKEN` fallback, never in `repr`, logs, or exception text; TLS verification on with no public switch | 🔲 | |
| B1.3 | Optional extra `cocoindex[omnigraph] = ["aiohttp>=3.9.0"]`, `ci-enabled-optional-deps` entry, import guard with a clear message | 🔲 | |
| B1.4 | `/healthz` version check at first use; one warning on an unexpected server minor | 🔲 | |

**Steps (detail):**

- **B1.1 — Factory.** Deliverable in `_client.py`:
  ```python
  @dataclasses.dataclass(frozen=True)
  class ConnectionFactory:
      base_url: str
      graph_id: str
      branch: str = "main"
      token: str | None = None
      timeout: float = 30.0

      def client(self) -> _HttpClient: ...     # creates the session once, caches it
      async def aclose(self) -> None: ...
  ```
  The factory is provided through the same `ContextKey` mechanism as today and resolved at action time, never captured at declare time. The old `store` and `cli` fields are deleted, not aliased.
- **B1.3 — Packaging.** Deliverable: `pyproject.toml` extra plus the mypy override (`aiohttp`, `aiohttp.*` are already listed). The import guard raises a message naming the extra when `aiohttp` is missing.

#### Phase B2 — Calls and typed results

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| B2.1 | `read_schema()`, `query()`, `mutate()` with typed results | 🔲 | |
| B2.2 | `branch_create/list/merge/delete` | 🔲 | |
| B2.3 | ~~`apply_schema()` on the HTTP client~~ | ❌ | Cut: P1 proved the endpoint is refused on every server. Schema writes live in `_cluster.py` (Epic D); revisit only if D3 lands |
| B2.4 | `mutate_if_graph_commit()` — only if P11 says it is usable | 🔲 | Optional |

**Steps (detail):**

- **B2.1 — Reads and writes.** Deliverable:
  ```python
  class MutationResult(NamedTuple):
      affected_nodes: int
      affected_edges: int
      commit_id: str | None

  class MergeOutcome(enum.Enum):
      ALREADY_UP_TO_DATE = "already_up_to_date"
      FAST_FORWARD = "fast_forward"
      MERGED = "merged"

  class MergeResult(NamedTuple):
      outcome: MergeOutcome
      source_deleted: bool
  ```
  `query()` returns rows untouched — no key-case conversion is applied to user data. `Query` (positional `$?` slots, from `_gq.py`) is rendered into the request body with its params; the temp-file plumbing the CLI needed is gone.

#### Phase B3 — Errors and retries

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| B3.1 | `OmnigraphError` hierarchy keyed on status, `code`, and structured fields; 422 plain text tolerated | 🔲 | |
| B3.2 | Classification methods: `uninitialized_graph()`, `missing_endpoint()`, `blocked_by_non_main_branches()` | 🔲 | Wording from P2, P5 |
| B3.3 | Retry policy: none on writes; opt-in backoff on reads for 429, 503, and network errors | 🔲 | |

**Steps (detail):**

- **B3.1 — Errors.** Deliverable: `OmnigraphError` → `HttpError(status, code, message, request_id)` → `BadRequestError` (400, 422), `UnauthorizedError` (401), `ForbiddenError` (403), `NotFoundError` (404), `ConflictError` (409, carrying `merge_conflicts` and `key_conflict`), `PreconditionFailedError` (412), `PayloadTooLargeError` (413, carrying `resource_limit`), `RateLimitedError` (429), `ServerUnavailableError` (503), plus `NetworkError` and `ConfigurationError`. A body that is not the JSON envelope becomes the message verbatim.
- **B3.2 — Classification.** Deliverable: the three predicates `_target.py` needs, as methods on the error rather than regexes in the reconciliation code. `missing_endpoint()` uses the wording confirmed in P5 and lives in exactly one place.

**Exit guardrails — Epic B → Epic C**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Mocked coverage | Every B2 method's success path and every B3.1 error class has a unit test against a mocked transport | 🔲 | |
| Secrets | A test asserts the token is absent from `repr(factory)`, `repr(client)`, and every raised exception's `str()` | 🔲 | |
| Live smoke | `read_schema`, `query`, `mutate`, and the four branch calls pass against the Epic A fixture | 🔲 | |
| No CLI residue | `_client.py` contains no `subprocess`, `tempfile`, `fcntl`, or `msvcrt` import | 🔲 | |

---

### Epic C — Connector adaptation  ·  MVP

**Goal:** `_target.py` reconciles over HTTP with the same data semantics it has today, with no host-local locking and no CLI error text.
**Success metrics:** No occurrence of `_CliClient`, `OmnigraphCliError`, or `_ENDPOINT_NOT_FOUND_RE` anywhere in the connector; the migrated slice of the live suite (C1.4) passes against the Epic A fixture.

#### Phase C1 — Swap the client

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C1.1 | `_apply_entity_actions`, `_apply_type_actions`, `_scratch_branch`, `_reap_abandoned_scratch_branches`, `_keep_referenced_nodes`, `_mutate_with_endpoint_retry` take `_HttpClient` | 🔲 | 26 CLI references today |
| C1.2 | Delete the CLI machinery: `canonical_store`, `_lock_dir`, `_temporary_text_file`, the flock/msvcrt helpers, `OmnigraphCliError`, `_CliClient` | 🔲 | |
| C1.3 | Endpoint-stub recovery and the uninitialized-graph branch use B3.2's classification | 🔲 | |
| C1.4 | Migrate a representative slice of the live suite now, not at the end: entity lifecycle, mixed upsert/delete, endpoint stubs, app drop | 🔲 | Surfaces server-semantic differences while the client is still soft |

#### Phase C2 — Atomicity without host locks

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C2.1 | Scratch branch names `coco_scratch_<app>_<utc ts>_<uuid>` | 🔲 | |
| C2.2 | Reaper deletes only branches past a TTL (default 1 h) and never one this process created | 🔲 | |
| C2.3 | Remove or replace `exclusive_store()` per probe P3 | 🔲 | Blocked on P3 |
| C2.4 | Cancellation before send, during upload, and while awaiting a response leaves no scratch branch | 🔲 | |

#### Phase C3 — Failure matrix

| Condition | Signal | Connector behaviour |
|---|---|---|
| 401 / 403 | `UnauthorizedError` / `ForbiddenError` | Fail the action immediately, naming the operation and graph; no retry |
| 404 graph | `NotFoundError` | Fail with "graph `<id>` is not served by `<base_url>`"; if P2 gave a provisioning path, take it instead |
| Endpoint missing | classified engine message | Existing endpoint-stub recovery on the scratch path |
| 409 merge conflict | `ConflictError.merge_conflicts` | Fail the component sync, delete the scratch branch, list conflicts in the error |
| 409 other | `ConflictError` | Fail; the next run re-reads state |
| 413 | `PayloadTooLargeError.resource_limit` | Fail with the limit and the actual; the connector already chunks at 8,192 |
| 422 | `BadRequestError` with raw text | Fail; indicates a client/server mismatch |
| 429 / 503 on a read | `RateLimitedError` / `ServerUnavailableError` | Retry with backoff up to the policy limit |
| 429 / 503 on a write | same | Fail; no retry |
| Timeout after a write was sent | `NetworkError` | Fail as ambiguous; tracking is not advanced, so the next run reconciles |

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C3.1 | One test per row of the matrix above | 🔲 | |

**Exit guardrails — Epic C → Epic E**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| No transport leak | Grepping the connector for `_CliClient`, `OmnigraphCliError`, `_ENDPOINT_NOT_FOUND_RE`, and `subprocess` finds nothing | 🔲 | |
| Slice passes | The C1.4 tests pass against the Epic A fixture | 🔲 | |
| No leaked branches | After every HTTP-mode test, the graph's branch list contains only `main` | 🔲 | |
| Matrix covered | One passing test per C3 row | 🔲 | |

---

### Epic D — Schema management (`managed_by="system"`)  ·  MVP

**Context — every other connector supports `SYSTEM`, and so must this one.**

`ManagedBy` is a shared enum (`connectorkits/target.py`: `SYSTEM` / `USER`). Fourteen connectors
use it, **every one defaults to `ManagedBy.SYSTEM`, and none refuses it** — including this
connector today, in six public signatures.

| Connector | Reaches store via | How it applies schema under `SYSTEM` |
|---|---|---|
| postgres | asyncpg wire | `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX` |
| neo4j | bolt | `CREATE CONSTRAINT`, index creation |
| bigquery | HTTP API | `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` |
| qdrant | HTTP/gRPC client | `_create_collection` |
| snowflake | driver | `CREATE DATABASE`, `CREATE TABLE`, `ALTER TABLE` |
| doris | aiohttp + MySQL wire | `CREATE TABLE`, `ALTER TABLE` |
| surrealdb | client | `DEFINE TABLE` |
| falkordb | redis protocol | `CREATE INDEX` |
| turbopuffer | HTTP API | namespace implicit on first write; schema rides in the write payload |
| valkey, zvec | protocol / client | schemaless — no DDL needed |
| lancedb, sqlite | embedded / local | `create_table` |
| **omnigraph over HTTP** | **omnigraph-server** | **refused: 409, unconditionally (P1)** |

The structural pattern is that **schema travels the same channel as the data**. DDL is just another
authorized operation on the wire, including for the three that are pure remote HTTP APIs
(bigquery, qdrant, turbopuffer). `omnigraph-server` is the only store here that deliberately closes
that channel: schema is cluster configuration, applied out of band and picked up on restart.

Dropping the SDK did not cause this — dropping **direct store access** did. The published-SDK plan
would have hit the identical 409, which is why the original spec invented a control plane. The
real fork was always *server versus direct store*, and the CLI was silently carrying schema
management. Shipping `managed_by="user"`-only would make omnigraph the single connector whose
default value does not work, so it is rejected as an end state (decision 32).

**Goal:** `managed_by="system"` works against a server-backed graph: the connector owns its type's
block in the graph's `.pg` inside the cluster config, applies it with `omnigraph cluster apply`,
triggers a restart through a user-supplied hook, and proceeds only once the served schema matches.
The operation sits behind one seam so a future server-side apply (D3) is a drop-in replacement.
**Success metrics:** the type lifecycle tests (create, property add, encoder change, release to
user, removal) pass in HTTP mode using the fixture's restart hook; a factory without a restart hook
fails with an actionable message and converges on re-run after a manual restart.

What P1, P2, and P12 established, and what the design must therefore accept: HTTP schema apply is
refused on every server configuration; there is no graph-creation endpoint; and a running server
never reloads schema without a restart. But `cluster apply` needs only the CLI binary, the config
directory, and object-store credentials — **not colocation with the server** — so the control plane
can run from a cocoindex host.

#### Phase D1 — The seam and the config model

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| D1.1 | `apply_schema(schema_pg)` as the one schema-write operation `_target.py` calls, with a pluggable implementation | 🔲 | Makes D3 a drop-in |
| D1.2 | `ClusterConfig` dataclass; validate `cluster.yaml`, including a `storage:` root | 🔲 | |
| D1.3 | Locate or create the graph's `.pg` from `graphs.<graph_id>.schema` | 🔲 | The HTTP-mode equivalent of `init` |
| D1.4 | Reconcile-time refusal of `managed_by="system"` without a `ClusterConfig`, naming the target | 🔲 | Fail early, not mid-sync |

**Steps (detail):**

- **D1.2 — Config.** Deliverable:
  ```python
  @dataclasses.dataclass(frozen=True)
  class ClusterConfig:
      config_dir: pathlib.Path                        # contains cluster.yaml
      cli: str = "omnigraph"                          # runs `cluster apply`
      restart: Callable[[], Awaitable[None]] | None = None
      settle_timeout: float = 120.0                   # wait for GET /schema to match
  ```
  The CLI is required only on the host running the control plane, and object-store credentials only
  when the cluster's `storage:` is an object-store root. Data-plane workers need neither.

#### Phase D2 — Control plane

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| D2.1 | Read-merge-write of the type's block with `_gq.merge_type_into_schema` / `remove_type_from_schema`, under a lock keyed on the config directory | 🔲 | `_gq.py` unchanged |
| D2.2 | Run `omnigraph cluster apply --config <dir> --json`; parse `ok`, `changes`, `errors` | 🔲 | Verified shape: `changes[].{resource, operation, disposition}` |
| D2.3 | Invoke the restart hook; with none, raise `SchemaRestartRequired` naming the graph and config directory | 🔲 | |
| D2.4 | Poll `GET /schema` until `schema_source` matches, else fail after `settle_timeout` | 🔲 | Exponential from 0.5 s |
| D2.5 | Removal semantics: soft drops only; graph deletion never automated | 🔲 | |
| D2.6 | Scratch-branch interaction: confirm whether `cluster apply` refuses while non-main branches exist | 🔲 | New probe P13 |
| D2.7 | Ownership marker handling (`coco_managed_by_<app>`) unchanged across the transport swap | 🔲 | |

**Steps (detail):**

- **D2.3 — Restart.** Deliverable: the hook is awaited once per apply; exceptions propagate and fail
  the sync. `SchemaRestartRequired` is raised after a *successful* apply when no hook is configured,
  so a re-run after the operator's restart finds the served schema already matching and continues.
- **D2.4 — Converge.** Deliverable: on timeout, an error carrying the digest of the expected and the
  served source. P12 confirmed the served schema never changes on its own, so this poll is strictly
  waiting on the restart, not on eventual consistency.

#### Phase D3 — Native server-side apply  ·  Post-MVP  ·  blocked on upstream

The only path that removes both the CLI dependency and the restart, and the only one that puts
omnigraph on equal footing with the other thirteen connectors.

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| D3.1 | Upstream ask to ModernRelay: a per-graph opt-in that permits `POST /schema/apply` and reloads that graph in place | 🔲 | The 409 is a deliberate guard, not an oversight |
| D3.2 | When it lands: a second implementation behind the D1.1 seam; `ClusterConfig` becomes optional | 🔲 | |
| D3.3 | Retire the restart hook wherever the server supports in-place reload | 🔲 | |

**Exit guardrails — Epic D → Epic E**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Lifecycle | Type create, property add, encoder change, release to user, and removal pass in HTTP mode with the fixture's restart hook | 🔲 | |
| No hook | With `restart=None`, the first sync raises `SchemaRestartRequired`; after the fixture restarts the server, the second sync converges | 🔲 | |
| Seam honoured | `_target.py` calls `apply_schema()` only; no `subprocess` or `cluster apply` string appears outside the control-plane module | 🔲 | |
| Data isolation | No data-plane test invokes `cluster apply` | 🔲 | |

---

### Epic E — Suite migration, docs, CI  ·  MVP

**Goal:** The whole live suite runs over HTTP against the Epic A fixture, the docs describe the server-only connection model honestly, and CI runs it.
**Success metrics:** `test_omnigraph_target.py` passes in full against the fixture with a recorded count; `omnigraph.mdx` no longer mentions a `store` URI or a CLI.

#### Phase E1 — Suite migration

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E1.1 | Re-point the remaining tests from the `file://` CLI fixture to the server fixture | 🔲 | ~331 tests as of 2026-09-06 |
| E1.2 | Classify every failure as "server semantics differ" or "fixture gap"; the first kind gets a decision row | 🔲 | |
| E1.3 | Record the final passing count in this spec | 🔲 | |

#### Phase E2 — Live matrix

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E2.1 | Unauthenticated server | 🔲 | |
| E2.2 | Bearer-authenticated server with a least-privilege policy bundle (P10) | 🔲 | |
| E2.3 | Two graph ids on one server; wrong graph id | 🔲 | |
| E2.4 | Missing permission for each of query, mutate, branch create, merge, delete | 🔲 | |
| E2.5 | Writes above one request's entity limit | 🔲 | |
| E2.6 | Merge conflict from a concurrent write on `main` | 🔲 | |
| E2.7 | Cancellation at three points | 🔲 | |
| E2.8 | Connection loss after a successful write (transport fault injection) | 🔲 | |
| E2.9 | Two processes writing to one graph concurrently | 🔲 | |
| E2.10 | Server minor-version mismatch produces one warning and proceeds | 🔲 | |

#### Phase E3 — Documentation

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E3.1 | Rewrite `omnigraph.mdx` around `base_url` / `graph_id`; delete the `store` URI model | 🔲 | |
| E3.2 | Local development section: copy-paste `omnigraph-server --cluster ./og --unauthenticated`, and the rustfs variant the tests use | 🔲 | |
| E3.3 | Schema section per Epic D's path — automatic, or the operator runbook | 🔲 | |
| E3.4 | Failure and retry behaviour table; security notes (token sourcing, least privilege, no `/graphs` enumeration) | 🔲 | |

#### Phase E4 — CI

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E4.1 | Install `omnigraph-server` 0.10.0 and rustfs in `.github/workflows/_test.yml`, cached by version | 🔲 | 211 MB per platform |
| E4.2 | Gate live tests on `OMNIGRAPH_TEST_SERVER=1` | 🔲 | `OMNIGRAPH_TEST_STORE` retires with the CLI |
| E4.3 | `aiohttp` in `ci-enabled-optional-deps` | 🔲 | |

**Exit guardrails — Epic E → MVP done**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Suite green | The full live suite passes in CI on Linux x86_64 and macOS arm64 | 🔲 | |
| Pre-submission | `prek run --all-files` passes | 🔲 | |
| Docs honest | `omnigraph.mdx` states that a running `omnigraph-server` is required and what schema management the connector does | 🔲 | |

---

## 5. Risk register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| ~~Probe P1 says no~~ — **fired 2026-09-07.** HTTP schema apply is refused on every server configuration | — | High | Resolved by Epic D's control plane rather than by dropping the feature: every other connector supports `SYSTEM` and defaults to it (decision 32) |
| The restart hook is operationally awkward — rolling deploys, managed hosting, multi-replica clusters | High | Med | `SchemaRestartRequired` converges on re-run after a manual restart; `managed_by="user"` documented as the zero-restart option; D3 removes it entirely if upstream lands |
| D3 never lands upstream, so the CLI dependency for schema is permanent | Med | Med | The control plane is MVP and ships without D3. The binary is needed only on the host running schema changes, and P12 showed that host need not be the server's |
| No HTTP graph-creation path (P2), so a pure-HTTP deployment cannot bootstrap its own graph | Med | Med | With a `ClusterConfig` the connector creates it via `cluster apply` (verified: `graph.probe create applied`). Without one, fail naming the graph, the base URL, and the procedure; document it in the quickstart ahead of the connector code |
| Suite migration surfaces server semantics that differ from the direct store, forcing reconciliation rework late | Med | High | C1.4 migrates a representative slice during Epic C rather than at the end |
| Host locks are gone and the server does not serialize branch merges as assumed → lost updates between workers | Med | High | Probe P3 before C2.3; the two-process concurrent-writer test in E2.9 |
| The hand-rolled client drifts from the server contract, with no generated models and no drift check | Med | Med | The honest cost of dropping the SDK. Keep the surface at roughly nine calls; the live suite against a pinned server is the contract test |
| Losing the local `file://` mode hurts the five-minute story | High | Med | Accepted (decision 27); docs snippet in E3.2 |
| `omnigraph-server` is a 211 MB asset per platform, plus rustfs, in CI | High | Low | Cache the download by version; run live tests on one Linux runner plus macOS arm64 |
| Server minor release changes the wire contract mid-project | Med | Med | Pin 0.10.0 in fixtures and CI; `/healthz` check at first use |
| Ambiguous write after a timeout leads to duplicate or missing data | Low | High | No write retries; tracking advances only on a confirmed response; E2.8 fault injection |
| `GET /graphs` enumeration is forbidden by default, so a misconfigured graph id fails late | Med | Low | Validate with `GET /graphs/{id}/schema` at first use; the error names the graph and the base URL |

---

## 6. Decision log

Append-only. Rows 1-23 are kept for history; rows 24-30 supersede the ones they name.

| # | Date | Decision | Context | Decided by |
|---|------|----------|---------|------------|
| 1 | 2026-09-04 | Target Omnigraph 0.10.0 only; no 0.9 compatibility | Live suite passes on 0.10.0 unchanged; every 0.9 constraint the connector works around still holds | Roman Pronskiy |
| 2 | 2026-09-06 | Keep the CLI transport as the local/direct-store mode | It needs no server, no extra package, and is fully verified | Roman Pronskiy |
| 3 | 2026-09-06 | Build a public Python SDK (`omnigraph`) rather than a connector-private client | Scope B chosen over Scope A in the interview | Roman Pronskiy |
| 4 | 2026-09-06 | SDK lives in a separate repository, not as a workspace member | Clean ownership and release cadence | Roman Pronskiy |
| 5 | 2026-09-06 | PyPI name `omnigraph-sdk`, import `omnigraph_sdk` | Free on PyPI as of 2026-09-06; mirrors the TypeScript package's positioning without squatting the product name | Roman Pronskiy |
| 6 | 2026-09-06 | The connection factory owns the client; the lifespan closes it | Neo4j parity; users provide connection details, not a client | Roman Pronskiy |
| 7 | 2026-09-06 | `managed_by="system"` in cluster mode via a control plane (`ClusterConfig`, `cluster apply`, restart hook, poll) | Verified live: HTTP schema apply → 409, `cluster apply` refuses `--server`, no hot reload, restart required | Roman Pronskiy |
| 8 | 2026-09-06 | Without a `ClusterConfig`, HTTP targets are `managed_by="user"` only and refuse `system` at reconcile time | Fail early with a clear message rather than mid-sync | Roman Pronskiy |
| 9 | 2026-09-06 | Run a CLI `--server --graph` spike before SDK work; not a supported user mode | Cheapest way to verify server semantics | Roman Pronskiy |
| 10 | 2026-09-06 | Separate factory classes; rename `ConnectionFactory` → `CliConnectionFactory` with no alias | Connector not on `main`; no compatibility obligation | Roman Pronskiy |
| 11 | 2026-09-06 | Generate wire models from the server's served `/openapi.json` with `datamodel-code-generator` → `msgspec.Struct`; handwritten facade | Server publishes its spec; avoids a literal TypeScript port | Roman Pronskiy |
| 12 | 2026-09-06 | No automatic retries on writes; opt-in backoff on reads | Lost responses after durable commits must not be replayed | Roman Pronskiy |
| 13 | 2026-09-06 | SDK minor version tracks server minor version | Same rule as the TypeScript SDK; server fails closed on new paths | Roman Pronskiy |
| 14 | 2026-09-06 | Direct S3 stays unverified until Epic G | Host-local locks and `--yes` consent untested against non-local stores | Roman Pronskiy |
| 15 | 2026-09-06 | The connector never calls `GET /graphs` | Forbidden by default even on an unauthenticated server (verified live) | Roman Pronskiy |
| 16 | 2026-09-06 | Everything is open source; no private spec | No commercial plans in scope | Roman Pronskiy |
| 17 | 2026-09-06 | `omnigraph-sdk` is ModernRelay's official Python SDK, under the ModernRelay organisation | The author is a ModernRelay employee | Roman Pronskiy |
| 18 | 2026-09-06 | PyPI name `omnigraph`, import `omnigraph`, client class `omnigraph.Omnigraph`; supersedes 5 and 17 | Mirrors the npm package `@modernrelay/omnigraph` | Roman Pronskiy |
| 19 | 2026-09-06 | The SDK repository starts at `github.com/pronskiy/omnigraph-python`; supersedes 17 | Development can start without org provisioning | Roman Pronskiy |
| 20 | 2026-09-06 | Step H1.5 is reinstated as the org transfer rather than a handover offer | Decision 19 defers the move | Roman Pronskiy |
| 21 | 2026-09-06 | Claim the PyPI name with a functional `0.10.0a1` at the end of Phase C2 | A pending publisher reserves nothing; an empty package is squatting under PEP 541 | Roman Pronskiy |
| 22 | 2026-09-06 | Trusted publishing from the start rather than a long-lived API token | A long-lived token is a standing secret | Roman Pronskiy |
| 23 | 2026-09-06 | The PyPI project is created personally and moves to a ModernRelay organisation in H1.5 | Follows decision 19 | Roman Pronskiy |
| 24 | 2026-09-07 | **HTTP is the connector's only transport; the CLI transport is removed rather than kept alongside.** Supersedes 2, 9, 10 | Two transports means two sets of semantics, two test modes, and subprocess plumbing maintained forever. The connector is not on `main`, so nothing is owed to existing configurations | Roman Pronskiy |
| 25 | 2026-09-07 | **No separate Python SDK package.** The HTTP client is private to the connector in `_client.py`. Supersedes 3, 4, 5, 11, 17, 18, 19, 20, 21, 22, 23; cuts old Epics C and H | A published SDK is a repository, a PyPI project, a release cadence, and a version lock whose only consumer is this connector | Roman Pronskiy |
| 26 | 2026-09-07 | `aiohttp` behind the optional extra `cocoindex[omnigraph]`; JSON through `msgspec` | Both are already present in this repository — `aiohttp` as the doris extra with a mypy override, `msgspec` as a core dependency. No new dependency enters the ecosystem | Roman Pronskiy |
| 27 | 2026-09-07 | Local development is documented, not managed: cocoindex ships no server-bootstrap API | Process management and binary discovery are what removing the CLI deleted; re-adding them as public API would undo the change | Roman Pronskiy |
| 28 | 2026-09-07 | rustfs is the S3-compatible backend for fixtures and CI, so object-store use is verified by the default suite. **Old Epic G is cut.** Supersedes 14 | rustfs is the standard local backend for Omnigraph, and with the CLI gone there is no direct-store mode left to verify separately | Roman Pronskiy |
| 29 | 2026-09-07 | `managed_by="system"` support is decided by probe P1; the fallback is user-managed-only with an actionable diff. Supersedes 7, 8; replaces the old Epic E with Epic D | The 409 was measured on a `cluster.yaml`-backed server; a config-free storage-root boot is untested and may behave differently | Roman Pronskiy |
| 30 | 2026-09-07 | Version coupling becomes a `/healthz` check at first use; an unexpected minor logs one warning and proceeds. Supersedes 13 | With no published package there is no version to lock. The server routes new capabilities on new paths, so it fails closed on its own | Roman Pronskiy |
| 31 | 2026-09-07 | **P1, P2, and P12 answered live.** HTTP `POST /schema/apply` returns 409 on every server configuration; there is no graph-creation endpoint; a running server never reloads schema without a restart. Supersedes the branch in 29 | `--cluster` is the server's only boot source (RFC-011), and the 409 is scoped to "cluster-backed serving", so it applies to every served graph. Evidence in `server-spike.md` | Roman Pronskiy |
| 32 | 2026-09-07 | **`managed_by="system"` must be supported.** User-managed-only is rejected as an end state | `ManagedBy` is a shared enum used by 14 connectors; every one defaults to `SYSTEM` and none refuses it. Shipping otherwise would make omnigraph the only connector whose default value does not work | Roman Pronskiy |
| 33 | 2026-09-07 | Support it through a `cluster apply` control plane behind an `apply_schema()` seam, and pursue a server-side apply upstream as the eventual replacement (Epic D3) | The control plane works today with no upstream dependency, and P12 showed `cluster apply` needs the config directory and store credentials but not colocation with the server. The seam keeps the eventual native path a one-place change | Roman Pronskiy |

---

## 7. Open questions

The eleven Epic A probes (P1-P11) are the live open questions; they are tracked in the A2 table rather than duplicated here. Beyond them:

- [ ] How should an ambiguous write be surfaced in `App.update()` reporting beyond a failed action?
- [ ] Should `load/ndjson` replace generated GQ upserts for large batches, and can it preserve delete semantics? Deferred past MVP unless E2.5 shows a need.
- [ ] Does the migrated suite need a slower CI lane, given every test now needs a booted server and rustfs?
- [x] ~~SDK repository name, PyPI account ownership, and the org transfer.~~ Moot as of 2026-09-07: no SDK (decision 25).
- [x] ~~Does ModernRelay plan an official Python SDK?~~ Moot; the connector no longer needs one.
- [x] ~~Is the served `/openapi.json` equivalent to the TypeScript repository's copy?~~ Moot; no models are generated. The served document remains useful as reference while writing the nine calls.

---

## How to Update This Document

This spec is the source of truth for the build. Keep it current as work happens:

- **Status markers.** Update a step's status in its tracker table as you go: 🔲 → 🔄 → ✅. Use ⏸️ for blocked (note why in Notes) and ❌ for cut (leave the row; the strikethrough of history is useful).
- **Current focus.** Keep the pointer at the top aimed at the next actionable 🔲 step. Update it the moment you finish a step or cross a phase boundary — a stale pointer is worse than none, since it sends the next reader to the wrong place.
- **Guardrails.** When you hit a phase boundary, fill the **Actual outcome** column with what really happened and set the guardrail status. Don't advance to the next phase until its entry guardrails pass — or log a decision explaining why you're proceeding anyway.
- **Probes.** Epic A's probe answers go in `dev/omnigraph-rework/server-spike.md` with the exact request and response, and the one-line answer goes in the A2 table. Never summarise a probe you have not run.
- **Decisions.** Any non-trivial choice made during the build gets a new row in the Decision Log (§6). It's append-only — reversals are new rows naming what they supersede, not edits. If the choice changes the architecture, also update the Technical Decisions snapshot (§2).
- **Spec changes.** Structural changes (new epic, re-scoped phase) get a Changelog row at the top. Keep the executive summary honest if the project's shape shifts.
- **Open questions.** When one resolves, strike it from §7 and log the decision in §6.
