i think # Omnigraph Connector Rework — Technical Spec

**Author:** Roman Pronskiy · **Created:** 2026-09-06

> 📄 **This is a living document.** Status markers, decisions, and guardrail outcomes are meant to be updated as the work happens. See [How to Update This Document](#how-to-update-this-document) before editing.

### Changelog

| Date | Change | Author |
|------|--------|--------|
| 2026-09-06 | Initial spec created from `dev/omnigraph-connector-transport-analysis.md` plus a live probe of `omnigraph-server` 0.10.0 | Roman Pronskiy |
| 2026-09-06 | SDK is ModernRelay's official Python client; renamed the package from `omnigraph-sdk` to `omnigraph` (decisions 17, 18) | Roman Pronskiy |
| 2026-09-06 | SDK repository starts at `pronskiy/omnigraph-python`, moving to the ModernRelay organisation in H1.5 (decisions 19, 20) | Roman Pronskiy |
| 2026-09-06 | Added step C2.7: claim the PyPI name with a functional `0.10.0a1` (decisions 21-23) | Roman Pronskiy |

### Status legend

🔲 Not started · 🔄 In progress · ✅ Done · ⏸️ Blocked · ❌ Cut

### Current focus

**Now on:** Epic A → Phase A1 → step A1.1 — define the transport protocol and typed result types so `_target.py` stops depending on `_CliClient`.

---

## 1. Executive summary

The Omnigraph target connector on branch `omnigraph-connector` drives the `omnigraph` CLI against a direct `file://` store. That is the right shape for local and single-host use, but it cannot serve a deployed `omnigraph-server` cluster: every operation is a subprocess, errors are text, locks are host-local, and workers need storage credentials plus a pinned binary. This rework keeps the CLI transport for local stores, adds ModernRelay's official asynchronous Python SDK for `omnigraph-server` (`omnigraph`, its own repository), and adds an `HttpConnectionFactory` that uses it. Because a cluster-managed graph refuses schema changes over HTTP and only serves a new schema after a restart, cluster mode also gets a small control plane: the connector edits the graph's `.pg` file in the cluster config directory, runs `omnigraph cluster apply`, triggers a user-supplied restart hook, and waits for the server to serve the new schema. Everything is open source; nothing here is commercial.

---

## 2. Technical decisions

| Area | Decision | Rationale |
|------|----------|-----------|
| Transport split | Two connection factories: `CliConnectionFactory` (direct store, CLI subprocess) and `HttpConnectionFactory` (server, in-process SDK). Both satisfy one private client protocol. | One factory with mutually exclusive fields hides that the two modes have different lifecycle and schema semantics. |
| Naming | Rename today's `ConnectionFactory` to `CliConnectionFactory` with no alias. | The connector is not on `main`; there is no compatibility obligation yet. |
| SDK | Public async package `omnigraph` (import `omnigraph`, class `omnigraph.Omnigraph`, mirroring npm `@modernrelay/omnigraph`) in a separate repository. The connector depends on it through an optional extra. | Chosen in the interview; the author works at ModernRelay, so this is the official Python client. |
| SDK repository | `github.com/pronskiy/omnigraph-python` initially; transferred to the ModernRelay organisation once it has shipped (Epic H). | Lets development start today without waiting on org and PyPI provisioning. A GitHub transfer preserves history, issues, and redirects. |
| SDK stack | Python ≥ 3.11, `httpx` for transport, `msgspec.Struct` wire models generated from the server's served `/openapi.json` with `datamodel-code-generator`, a handwritten facade for errors, streaming, and ergonomics. | The server publishes its own spec (verified live), so generation needs no TypeScript checkout. `msgspec` is fast, typed, and already used in cocoindex. |
| Client lifecycle | `HttpConnectionFactory` lazily creates and caches one SDK client; the environment lifespan closes it. | Matches the Neo4j convenience API; users provide a factory, not a client. |
| Cluster schema | `managed_by="system"` in HTTP mode works only when the factory carries a `ClusterConfig`. Schema changes go: edit `.pg` → `omnigraph cluster apply --config` → restart hook → poll `GET /schema` until it matches. Without a `ClusterConfig`, HTTP targets accept `managed_by="user"` only. | Verified live: HTTP `schema/apply` returns 409 on a cluster-backed server, `cluster apply` refuses `--server`, and the running server does not hot-reload. |
| Version coupling | SDK minor version tracks the server minor version (0.10.x ↔ 0.10.x). cocoindex pins `omnigraph>=0.10,<0.11`. | Same rule the TypeScript SDK uses; the server routes new capabilities on new paths to fail closed. |
| Retries | Never auto-retry writes. Reads may retry with backoff behind an opt-in. Ambiguous write outcomes surface as errors; reconciliation re-reads on the next run. | A lost response after a durable commit must not duplicate work. |
| Errors | `OmnigraphError` hierarchy keyed by HTTP status plus the `code` field and structured sub-objects (`merge_conflicts`, `precondition_failure`, `resource_limit`, `key_conflict`). Non-JSON bodies are tolerated. | The server returns 422 plain text for body deserialization errors (verified live). |
| Baseline | Omnigraph 0.10.0 only; no 0.9 compatibility. | Decided 2026-09-04; the live suite passes on 0.10.0 unchanged. |
| Direct S3 | Accepted as a URI, not advertised as verified, until Epic G ships its own tests. | Host-local locks and the `--yes` consent flag are unverified against a non-local store. |
| De-risking | A CLI `--server --graph` spike runs before SDK work. It is not a supported user mode. | Cheapest way to learn server-side branch, merge, auth, and limit behaviour. |

---

## 3. Architecture overview

```
                     CocoIndex engine: declared target states, change detection
                                              │ actions per component
                                              ▼
              python/cocoindex/connectors/omnigraph/_target.py
              reconciliation · GQ rendering · identity (coco_key) · ownership
              scratch-branch atomicity · endpoint stubs      (transport-agnostic)
                                              │ OmnigraphClient protocol (_transport.py)
                    ┌─────────────────────────┴──────────────────────────┐
                    ▼                                                    ▼
      CliConnectionFactory → _CliClient                 HttpConnectionFactory → _HttpClient
      subprocess `omnigraph …` per call                 omnigraph.Omnigraph (httpx, pooled)
      GQ + params via temp files                        JSON bodies, typed errors, bearer token
      host-local file locks                             │                       │ schema writes
                    │ direct store URI                  │ /graphs/{id}/…        ▼
                    ▼                                   ▼            ClusterConfig control plane
            file:// (verified)                    omnigraph-server       edit graphs/<id>.pg
            s3:// (Epic G)                        cluster mode           `omnigraph cluster apply`
                                                        ▲                restart hook
                                                        │ restart        poll GET /schema
                                                        └────────────────────┘
```

A component's actions arrive at `_target.py`, which plans commits and asks the client resolved from the target's `ContextKey` to execute them. The CLI client runs the binary against the store. The HTTP client calls the server through the SDK. Data writes never touch the control plane; only `managed_by="system"` schema changes in cluster mode do.

---

## 4. Epics

| Epic | Name | MVP | Depends on |
|------|------|-----|------------|
| A | Transport boundary | Yes | — |
| B | Server-mode spike | Yes | A |
| C | `omnigraph` package | Yes | B (findings) |
| D | `HttpConnectionFactory` + connector adaptation | Yes | A, C |
| E | Cluster schema control plane | Yes | D |
| F | Live validation, docs, CI | Yes | D, E |
| G | Direct S3 store verification | No | A |
| H | SDK release polish | No | C |

### Epic A — Transport boundary  ·  MVP

**Goal:** `_target.py` depends on a narrow client protocol with typed results and classified errors, never on `_CliClient` or on CLI error text. The CLI implementation moves behind that protocol unchanged in behaviour.
**Success metrics:** `_target.py` contains no reference to `_CliClient` or `OmnigraphCliError`; the full connector suite (331 live tests as of 2026-09-06) passes unchanged; mypy and ruff clean.

#### Phase A1 — Protocol and typed results

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| A1.1 | Add `_transport.py` with `OmnigraphClient` protocol, `MutationResult`, `MergeResult`, `SchemaWriter` | 🔲 | |
| A1.2 | Add `OmnigraphError` base with classification methods; `OmnigraphCliError` implements them | 🔲 | |
| A1.3 | Rename `ConnectionFactory` → `CliConnectionFactory`; add `client()` returning the protocol | 🔲 | |
| A1.4 | `_CliClient` implements the protocol; `mutate` returns `MutationResult` from `affected_nodes`/`affected_edges` | 🔲 | |
| A1.5 | `_target.py` uses only the protocol and error classification; move regexes into the CLI client | 🔲 | |
| A1.6 | Update tests, docs, and the connector `__all__` for the rename | 🔲 | |

**Steps (detail):**

- **A1.1 — Protocol and results.** Deliverable: `python/cocoindex/connectors/omnigraph/_transport.py`.
  ```python
  class MutationResult(NamedTuple):
      affected_nodes: int
      affected_edges: int
      commit_id: str | None          # None when the transport reports no receipt

  class MergeOutcome(enum.Enum):
      ALREADY_UP_TO_DATE = "already_up_to_date"
      FAST_FORWARD = "fast_forward"
      MERGED = "merged"

  class MergeResult(NamedTuple):
      outcome: MergeOutcome
      source_deleted: bool

  class SchemaWriter(Protocol):
      async def init_graph(self, schema_pg: str) -> None: ...
      async def apply_schema(self, schema_pg: str) -> None: ...

  class OmnigraphClient(Protocol):
      async def read_schema(self) -> str | None: ...
      async def mutate(self, mutation: Query, *, branch: str) -> MutationResult: ...
      async def query(self, query: Query, *, branch: str) -> list[dict[str, object]]: ...
      async def branch_create(self, name: str, *, frm: str) -> None: ...
      async def branch_merge(self, name: str, *, into: str, delete_source: bool = False) -> MergeResult: ...
      async def branch_delete(self, name: str) -> None: ...
      async def branch_list(self) -> list[str]: ...
      def schema_writer(self) -> SchemaWriter | None: ...   # None ⇒ managed_by="user" only
      def exclusive_store(self) -> AbstractAsyncContextManager[None]: ...
      def hold_scratch_branch(self, name: str) -> AbstractAsyncContextManager[None]: ...
      def claim_scratch_branch(self, name: str) -> AbstractAsyncContextManager[bool]: ...
  ```
  `schema_writer()` is what lets Epic D refuse `managed_by="system"` without a `ClusterConfig` at reconcile time instead of failing mid-sync.
- **A1.2 — Error classification.** Deliverable: `OmnigraphError(RuntimeError)` in `_transport.py` with `uninitialized_graph() -> bool`, `missing_endpoint() -> tuple[str, str, str] | None`, and `blocked_by_non_main_branches() -> bool`. `OmnigraphCliError` implements them with the regexes that live in `_target.py` and `_client.py` today (`_ENDPOINT_NOT_FOUND_RE`, the "dataset at path … was not found" check, the "non-main branches" check).
- **A1.3 — Rename the factory.** Deliverable: `CliConnectionFactory` dataclass with `store`, `branch`, `cli` and a `client()` method. The old name is deleted, not aliased.
- **A1.4 — CLI client conforms.** Deliverable: `_CliClient` type-checks against `OmnigraphClient`; `mutate` decodes `affected_nodes` and `affected_edges` from `--json` output; `branch_merge` passes `--delete-branch` when `delete_source` is true and returns the outcome. `schema_writer()` returns `self`.
- **A1.5 — Target uses the protocol.** Deliverable: `_apply_entity_actions`, `_apply_type_actions`, `_scratch_branch`, `_reap_abandoned_scratch_branches`, `_apply_schema_recovering_abandoned_branches`, `_keep_referenced_nodes`, and `_mutate_with_endpoint_retry` take `OmnigraphClient`; `context_provider.get(db_key).client()` is the only construction site. A `managed_by="system"` action against a client whose `schema_writer()` is `None` raises a `ValueError` naming the type and the factory.
- **A1.6 — Rename fallout.** Deliverable: tests, `docs/src/content/docs/connectors/omnigraph.mdx`, and `__all__` use `CliConnectionFactory`.

**Exit guardrails — Phase A1 → Epic B**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Suite unchanged | `OMNIGRAPH_TEST_STORE=1 .venv/bin/python -m pytest python/tests/connectors/test_omnigraph_target.py` passes with the same count as before the refactor | 🔲 | |
| No transport leak | Searching `_target.py` for `_CliClient`, `OmnigraphCliError`, and `_ENDPOINT_NOT_FOUND_RE` finds no occurrence | 🔲 | |
| Static checks | `uv run mypy` and `uv run ruff format --check .` clean | 🔲 | |

---

### Epic B — Server-mode spike  ·  MVP

**Goal:** Learn how `omnigraph-server` 0.10.0 behaves for every operation the connector relies on, using the existing CLI in `--server --graph` mode, before any SDK code exists. Output is a findings document and decision-log rows, not shipped code. The author has access to the Omnigraph source, so answers may come from reading it, but each one is still confirmed against the running binary and recorded with the command and response.
**Success metrics:** Every question in B1.4 has a recorded, reproducible answer; the spike branch is deleted or its fixture reused by Epic F.

#### Phase B1 — Fixture and probes

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| B1.1 | Test fixture that boots `omnigraph-server --cluster <dir> --unauthenticated` on a free port | 🔲 | |
| B1.2 | Test-only `_ServerCliClient` adding `--server URL --graph ID --yes` to every argv | 🔲 | |
| B1.3 | Run the live suite through it with a pre-applied schema and `managed_by="user"` | 🔲 | |
| B1.4 | Probe list answered and written to `dev/omnigraph-rework/server-spike.md` | 🔲 | |

**Steps (detail):**

- **B1.1 — Server fixture.** Deliverable: `python/tests/connectors/omnigraph_server_fixture.py` that writes a `cluster.yaml` (`version: 1`, `graphs.<id>.schema: <path>`), runs `omnigraph cluster apply --config`, starts `test/bin/omnigraph-server` bound to `127.0.0.1:<free port>`, polls `/healthz` until `{"status":"ok"}`, and exposes `restart()` and `stop()`. CI's install step currently extracts only `omnigraph` from the release tarball; extend it to `omnigraph-server`.
- **B1.2 — Server-mode CLI client.** Deliverable: a subclass in the test module only. `--yes` is required because `branch delete` refuses a non-local destructive write under `--json`.
- **B1.3 — Suite pass.** Deliverable: a list of tests that fail in server mode, each classified as "server semantics differ", "schema path (expected, cluster-managed)", or "fixture gap".
- **B1.4 — Probe list.** Deliverable: answers with the exact command and response for each:
  1. Does `cluster apply` refuse while a non-main branch exists, as `schema apply` does on a direct store?
  2. Merge conflict: status, `code`, and `merge_conflicts` shape from `branches/merge`.
  3. Does the server enforce the 8,192-entity mutation cap, and as which status (`413` with `resource_limit`?).
  4. Mixed upsert/delete in one mutation: same refusal as the direct store?
  5. Endpoint-not-found error text over HTTP: identical to the CLI wording matched by `_ENDPOINT_NOT_FOUND_RE`?
  6. `mutate/if-graph-commit` 412 body and whether `ReadOutput.graph_commit_id` is usable as the precondition.
  7. Bearer token setup: `omnigraph login`, policy bundle in `cluster.yaml`, and the minimal Cedar action set for query, mutate, branch create/merge/delete, schema get.
  8. Merge with `delete_branch: true`: is `branch_deleted` reliable enough to drop the explicit delete?
  9. Behaviour of an in-flight mutation when the server restarts (Epic E depends on this).
  10. Whether `GET /schema` after `cluster apply` plus restart returns byte-identical `.pg` source (observed once on 2026-09-06; confirm with a larger schema).

**Exit guardrails — Epic B → Epic C**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Probes answered | All ten B1.4 items have a verified answer in `server-spike.md` | 🔲 | |
| Decisions logged | Each answer that changes Epic C, D, or E is a new row in §6 | 🔲 | |

---

### Epic C — `omnigraph` package  ·  MVP

**Goal:** ModernRelay's official, installable async Python client for `omnigraph-server` 0.10 with generated wire models, typed errors, streaming, and live tests, published to PyPI as `omnigraph`.
**Success metrics:** `pip install omnigraph==0.10.0` works; every endpoint the connector needs plus export and change streams are covered; mocked-transport tests reach ≥ 85 % line coverage of handwritten code; the live suite passes against the pinned server release in CI.

#### Phase C1 — Repository and code generation

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C1.1 | Create the repository skeleton (uv, hatchling, ruff, mypy strict, pytest, respx) | 🔲 | Repo name to confirm; see §7 |
| C1.2 | Capture `spec/openapi-0.10.0.json` from a running server's `/openapi.json` with a fetch script | 🔲 | |
| C1.3 | Generate `omnigraph/_generated/models.py` as `msgspec.Struct` classes; add a drift check | 🔲 | |
| C1.4 | Write `COMPATIBILITY.md`: minor-version lock to the server, supported endpoint list | 🔲 | |

**Steps (detail):**

- **C1.1 — Skeleton.** Deliverable: `pyproject.toml` with `requires-python = ">=3.11"`, runtime deps `httpx>=0.28,<1` and `msgspec>=0.18`, dev group with `pytest`, `pytest-asyncio`, `respx`, `ruff`, `mypy`, `datamodel-code-generator`; `src/omnigraph/__init__.py` exporting `Omnigraph` and the error classes; MIT license to match upstream.
- **C1.2 — Pinned spec.** Deliverable: `scripts/fetch_spec.py <base_url>` that saves `/openapi.json` and `spec/openapi-0.10.0.json` checked in. The served document is the source of truth; the TypeScript repository's copy is reference only.
- **C1.3 — Generated models.** Deliverable: `datamodel-codegen --input spec/openapi-0.10.0.json --output-model-type msgspec.Struct --output src/omnigraph/_generated/models.py` wired into `scripts/generate.py`, and a CI job that regenerates and fails on diff. Generated code is never edited by hand.
- **C1.4 — Compatibility policy.** Deliverable: a document stating that `omnigraph 0.N.x` targets `omnigraph-server 0.N.x`, that new server capabilities appear on new paths (so an older server returns 404, not a wrong result), and which endpoints are supported in this release.

**Exit guardrails — Phase C1 → C2**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Generation reproducible | Running `scripts/generate.py` twice produces no diff | 🔲 | |
| Models import | `python -c "import omnigraph._generated.models"` succeeds under mypy strict | 🔲 | |

#### Phase C2 — Core client

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C2.1 | `Omnigraph` async client: base URL, graph ID, bearer token, timeout, transport injection, `aclose`, `for_graph()` | 🔲 | |
| C2.2 | Error hierarchy mapped from status, `code`, and structured fields; non-JSON bodies handled | 🔲 | |
| C2.3 | `health()`, `schema.get()`, `query()`, `mutate()`, `mutate_if_graph_commit()` | 🔲 | |
| C2.4 | `branches.list/create/merge/delete` | 🔲 | |
| C2.5 | `load_ndjson()` accepting an async byte iterator | 🔲 | |
| C2.6 | Retry policy: none for writes; opt-in backoff for reads on 429/503/network errors | 🔲 | |
| C2.7 | Publish `omnigraph 0.10.0a1` to PyPI to claim the name with a functional package | 🔲 | Earliest honest claim; a placeholder would be squatting under PEP 541 |

**Steps (detail):**

- **C2.1 — Client.** Deliverable:
  ```python
  class Omnigraph:
      def __init__(self, base_url: str, *, graph_id: str | None = None,
                   token: str | None = None, timeout: float = 30.0,
                   transport: httpx.AsyncBaseTransport | None = None) -> None: ...
      def for_graph(self, graph_id: str) -> "Omnigraph": ...   # shares the pool
      async def aclose(self) -> None: ...
      async def __aenter__(self) -> "Omnigraph": ...
  ```
  Graph-scoped calls raise `ConfigurationError` when `graph_id` is unset. The token never appears in `repr`, logs, or exception messages. TLS verification stays on with no public switch.
- **C2.2 — Errors.** Deliverable: `OmnigraphError` → `HttpError(status, code, message, request_id)` → `BadRequestError` (400, 422), `UnauthorizedError` (401), `ForbiddenError` (403), `NotFoundError` (404), `ConflictError` (409, carries `merge_conflicts: list[MergeConflict]` and `key_conflict`), `PreconditionFailedError` (412, carries `expected`/`actual`), `PayloadTooLargeError` (413, carries `resource_limit`), `RateLimitedError` (429), `ServerUnavailableError` (503), `ChangeFeedGapError` (410), plus `NetworkError` and `ConfigurationError`. A body that is not the JSON envelope becomes the message verbatim.
- **C2.3 — Reads and writes.** Deliverable: `query(query, params, *, branch=None, snapshot=None) -> ReadResult(rows, columns, graph_commit_id)`, `mutate(query, params, *, branch=None) -> ChangeResult(affected_nodes, affected_edges, branch, commit)`, and `mutate_if_graph_commit(..., expected_commit)` on the dedicated path. `rows` and `params` pass through untouched; no key-case conversion is applied to user data.
- **C2.4 — Branches.** Deliverable: `create(name, *, from_branch=None)`, `merge(source, *, target=None, delete_branch=False) -> MergeResult(outcome, branch_deleted, branch_delete_error)`, `delete(name)`, `list()`.
- **C2.5 — NDJSON load.** Deliverable: `load_ndjson(lines: AsyncIterable[bytes], *, branch=None, mode=...) -> BatchLoadResult`, streaming the body without buffering.
- **C2.6 — Retries.** Deliverable: `RetryPolicy(max_attempts, backoff)` applied only to methods marked read-only; documented table copied from the analysis doc's retry matrix.
- **C2.7 — Alpha release.** Deliverable: `omnigraph 0.10.0a1` on PyPI, published by a GitHub Actions workflow using trusted publishing from `pronskiy/omnigraph-python`, plus a README stating that the API is unstable until `0.10.0`. This is the step that actually claims the name: a PyPI pending publisher reserves nothing until it publishes, and an empty placeholder is what PEP 541 calls name squatting. The alpha ships the working C2 client, so it is a real package. Add the trusted publisher as a *pending* publisher beforehand, since the project does not exist yet.

**Exit guardrails — Phase C2 → C3**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Mocked tests | respx-based tests cover every C2 method's success path and every error class | 🔲 | |
| Live smoke | `health`, `schema.get`, `query`, `mutate`, and the four branch calls pass against `omnigraph-server` 0.10.0 booted from the Epic B fixture | 🔲 | |
| Secrets | A test asserts the token is absent from `repr(client)` and from every raised exception's `str()` | 🔲 | |
| Name claimed | `pip install omnigraph==0.10.0a1` in a fresh venv imports `omnigraph` and constructs `Omnigraph(...)` | 🔲 | |

#### Phase C3 — Streaming and remaining surface

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C3.1 | `export(type_names, *, branch)` as an async iterator over NDJSON records that closes the response on early exit | 🔲 | |
| C3.2 | `changes.list()` (paginated) and `changes.baseline()` stream with terminal-record validation and `ChangeFeedGapError` | 🔲 | |
| C3.3 | `commits.list/get`, `snapshot()`, stored `queries.list/run` | 🔲 | |
| C3.4 | `schema.apply()` with the 409 cluster behaviour documented on the method | 🔲 | |

**Exit guardrails — Phase C3 → C4**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Stream cleanup | A test breaks out of `export()` after one record and asserts the underlying response is closed | 🔲 | |
| Coverage | ≥ 85 % lines on `src/omnigraph/` excluding `_generated/` | 🔲 | |

#### Phase C4 — Docs and first release

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| C4.1 | README with install, quick start, error handling, and the ambiguous-write guidance | 🔲 | |
| C4.2 | Live test job in CI downloading the pinned server release | 🔲 | |
| C4.3 | Publish the stable `omnigraph 0.10.0` through the same workflow | 🔲 | Name already claimed in C2.7; this is the first release with a stable API |

**Exit guardrails — Phase C4 → Epic D**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Installable | `uv pip install omnigraph==0.10.0` in a fresh venv imports and runs `health()` against a local server | 🔲 | |
| CI green | Lint, type check, unit, live, and drift jobs pass on the tagged commit | 🔲 | |

---

### Epic D — `HttpConnectionFactory` and connector adaptation  ·  MVP

**Goal:** CocoIndex targets can point at an `omnigraph-server` graph through an `HttpConnectionFactory`, with the same reconciliation semantics as the CLI mode for data writes, and with every failure class mapped to a defined behaviour.
**Success metrics:** The live entity lifecycle tests (insert, update, delete, mixed upsert/delete scratch flow, endpoint stubs, app drop) pass in HTTP mode with `managed_by="user"`; no `httpx` symbol appears outside `_http_client.py`.

#### Phase D1 — Factory and client

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| D1.1 | `HttpConnectionFactory` with lazy cached SDK client and lifespan close | 🔲 | |
| D1.2 | `_HttpClient` implementing `OmnigraphClient` on top of `omnigraph` | 🔲 | |
| D1.3 | `OmnigraphHttpError` implements the classification methods from A1.2 | 🔲 | |
| D1.4 | Optional extra `cocoindex[omnigraph]` pinning `omnigraph>=0.10,<0.11`; mypy override if needed | 🔲 | While only the alpha exists, pin `>=0.10.0a1,<0.11` — a plain `>=0.10` excludes prereleases under PEP 440 |

**Steps (detail):**

- **D1.1 — Factory.** Deliverable in `_http_client.py`:
  ```python
  class HttpConnectionFactory:
      def __init__(self, base_url: str, *, graph_id: str, branch: str = "main",
                   token: str | None = None, timeout: float = 30.0,
                   cluster: ClusterConfig | None = None) -> None: ...
      @property
      def branch(self) -> str: ...
      def client(self) -> _HttpClient: ...      # creates the SDK client once
      async def aclose(self) -> None: ...
  ```
  `token=None` falls back to `OMNIGRAPH_TOKEN`. The factory is provided through the same `ContextKey` mechanism as today and resolved at action time, never captured at declare time. The environment lifespan calls `aclose()` on teardown.
- **D1.2 — HTTP client.** Deliverable: `_HttpClient` mapping protocol methods to SDK calls. `exclusive_store()` is a no-op (the server serializes commits); `hold_scratch_branch`/`claim_scratch_branch` follow the policy fixed in D2.2. `schema_writer()` returns the Epic E control plane when `cluster` is set, else `None`.
- **D1.3 — Error classification.** Deliverable: `uninitialized_graph()` is always `False` in HTTP mode (a missing graph is `NotFoundError`, reported as "graph not served"); `missing_endpoint()` parses the engine message carried in `HttpError.message` using the regex confirmed in B1.4 item 5; `blocked_by_non_main_branches()` per B1.4 item 1.
- **D1.4 — Packaging.** Deliverable: `pyproject.toml` extra, `ci-enabled-optional-deps` entry, and an import guard that raises a clear error when `omnigraph` is missing.

**Exit guardrails — Phase D1 → D2**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Protocol conformance | `_HttpClient` and `_CliClient` both type-check against `OmnigraphClient` | 🔲 | |
| Single-commit path | A node insert/update/delete lifecycle passes in HTTP mode with `managed_by="user"` | 🔲 | |

#### Phase D2 — Semantics in server mode

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| D2.1 | Scratch-branch flow over server endpoints; use `delete_branch: true` only if B1.4 item 8 confirms it | 🔲 | |
| D2.2 | Abandoned scratch-branch policy for HTTP mode (no host locks across workers) | 🔲 | |
| D2.3 | Commit receipts in action diagnostics and update reporting | 🔲 | |
| D2.4 | Failure matrix implemented and tested | 🔲 | |
| D2.5 | App drop and disappeared declarations resolve the client from the `ContextKey` alone | 🔲 | |
| D2.6 | Cancellation before send, during upload, and while awaiting a response leaves no scratch branch | 🔲 | |

**Steps (detail):**

- **D2.2 — Abandoned branches.** Deliverable: scratch branch names embed the worker's identity and a UTC timestamp (`coco_scratch_<app>_<ts>_<uuid>`); the reaper deletes only branches older than a TTL (default 1 h) and never a branch created by this process. Record the choice in §6.
- **D2.4 — Failure matrix.** Deliverable: tests for each row.

  | Condition | Signal | Connector behaviour |
  |---|---|---|
  | 401 / 403 | `UnauthorizedError` / `ForbiddenError` | Fail the action immediately, name the operation and graph; no retry |
  | 404 graph | `NotFoundError` | Fail with "graph `<id>` is not served by `<base_url>`" |
  | Endpoint missing | engine message | Existing endpoint-stub recovery on the scratch path |
  | 409 merge conflict | `ConflictError.merge_conflicts` | Fail the component sync, delete the scratch branch, list conflicts in the error |
  | 409 other | `ConflictError` | Fail; next run re-reads state |
  | 413 | `PayloadTooLargeError.resource_limit` | Fail with the limit and actual; the connector already chunks at 8,192 |
  | 422 | `BadRequestError` with raw text | Fail; indicates an SDK/server mismatch |
  | 429 / 503 on a read | `RateLimitedError` / `ServerUnavailableError` | Retry with backoff up to the policy limit |
  | 429 / 503 on a write | same | Fail; no retry |
  | Timeout after a write was sent | `NetworkError` | Fail as ambiguous; tracking is not advanced, so the next run reconciles |

**Exit guardrails — Phase D2 → Epic E**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Lifecycle parity | Entity lifecycle, mixed upsert/delete, endpoint stubs, and app drop tests pass in HTTP mode with `managed_by="user"` | 🔲 | |
| No leaked branches | After every HTTP-mode test, `branches.list()` returns only `main` | 🔲 | |
| Matrix covered | One test per D2.4 row | 🔲 | |

---

### Epic E — Cluster schema control plane  ·  MVP

**Goal:** `managed_by="system"` works against a cluster-managed graph: the connector owns the type's block in the graph's `.pg` file inside the cluster config directory, applies it with `omnigraph cluster apply`, restarts the server through a user hook, and proceeds only once the served schema matches.
**Success metrics:** The type lifecycle tests (create, evolve, release to user, remove) pass in HTTP mode with `managed_by="system"` using the fixture's restart hook; a factory without a restart hook fails with an actionable message and converges on re-run after a manual restart.

#### Phase E1 — Config model and read path

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E1.1 | `ClusterConfig` dataclass and validation of `cluster.yaml` | 🔲 | |
| E1.2 | Locate or create the graph's `.pg` file from `graphs.<graph_id>.schema` | 🔲 | |
| E1.3 | Host-local lock keyed by the config directory around read-merge-write | 🔲 | |
| E1.4 | Reconcile-time refusal of `managed_by="system"` when no `ClusterConfig` is set | 🔲 | |

**Steps (detail):**

- **E1.1 — Config.** Deliverable:
  ```python
  @dataclass(frozen=True)
  class ClusterConfig:
      config_dir: pathlib.Path                        # contains cluster.yaml
      cli: str = "omnigraph"                          # runs `cluster apply`
      restart: Callable[[], Awaitable[None]] | None = None
      settle_timeout: float = 120.0                   # wait for GET /schema to match
  ```
  The CLI is required on the host that runs the control plane, and only there; data-plane workers do not need it.
- **E1.2 — Schema file.** Deliverable: read `cluster.yaml`; if `graphs.<graph_id>` is missing, add it with `schema: graphs/<graph_id>.pg` and create the file. This is the HTTP-mode equivalent of `init`.
- **E1.3 — Locking.** Deliverable: reuse the file-lock helpers from `_client.py` keyed by the resolved config directory. `cluster apply` holds its own `__cluster/lock.json`; this lock covers the read-merge-write of the `.pg` file that precedes it.

**Exit guardrails — Phase E1 → E2**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Refusal path | A `managed_by="system"` target on an `HttpConnectionFactory` without `cluster` fails at reconcile with a message naming the type and the missing `ClusterConfig` | 🔲 | |
| Merge reuse | The `.pg` merge uses `_gq.merge_type_into_schema` and `remove_type_from_schema` unchanged | 🔲 | |

#### Phase E2 — Apply and converge

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| E2.1 | Run `omnigraph cluster apply --config <dir> --json`; parse `ok`, `changes`, `errors` | 🔲 | |
| E2.2 | Invoke the restart hook; without one, raise `SchemaRestartRequired` naming the graph and config directory | 🔲 | |
| E2.3 | Poll `GET /schema` until `schema_source.strip()` equals the applied file, or fail after `settle_timeout` | 🔲 | |
| E2.4 | Removal semantics: soft drops only; graph deletion is never automated | 🔲 | |
| E2.5 | Interaction with scratch branches per B1.4 item 1 | 🔲 | |

**Steps (detail):**

- **E2.1 — Apply.** Deliverable: `_cluster_apply(config) -> ClusterApplyResult(ok, changes, errors, warnings)`; a non-`ok` result raises with the `errors` list. Observed output shape on 2026-09-06: `changes[].{resource, operation, before_digest, after_digest, disposition}`.
- **E2.2 — Restart.** Deliverable: the hook is awaited once per apply; exceptions propagate and fail the sync. `SchemaRestartRequired` is raised after a successful apply when no hook is configured, so a re-run after the operator's restart finds the served schema already matching and continues.
- **E2.3 — Converge.** Deliverable: exponential polling starting at 0.5 s; on timeout, an error that includes the digest of the expected and served sources.

**Exit guardrails — Phase E2 → Epic F**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Lifecycle | Type create, property add, encoder change, release to user, and removal pass in HTTP mode with the fixture's restart hook | 🔲 | |
| No hook | With `restart=None`, the first sync raises `SchemaRestartRequired`; after the fixture restarts the server, the second sync converges | 🔲 | |
| Data isolation | No data-plane test invokes `cluster apply` | 🔲 | |

---

### Epic F — Live validation, docs, CI  ·  MVP

**Goal:** The HTTP mode is verified across authentication, permissions, limits, concurrency, and failure injection; the docs describe both factories and the cluster schema model; CI runs both live suites.
**Success metrics:** Every row in F1 has a passing test; `omnigraph.mdx` no longer says HTTP transport is a future increment.

#### Phase F1 — Live matrix

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| F1.1 | Unauthenticated local server | 🔲 | |
| F1.2 | Bearer-authenticated server with a least-privilege policy bundle | 🔲 | |
| F1.3 | Two graph IDs on one server; wrong graph ID | 🔲 | |
| F1.4 | Missing permission for each of query, mutate, branch create, merge, delete | 🔲 | |
| F1.5 | Writes above one request's entity limit | 🔲 | |
| F1.6 | Merge conflict from a concurrent write on `main` | 🔲 | |
| F1.7 | Cancellation at three points (before send, during upload, awaiting response) | 🔲 | |
| F1.8 | Connection loss after a successful write (proxy or transport fault injection) | 🔲 | |
| F1.9 | Two processes writing to one graph concurrently | 🔲 | |
| F1.10 | Client/server minor-version mismatch produces a clear error | 🔲 | |

#### Phase F2 — Documentation

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| F2.1 | Restructure `omnigraph.mdx`: connection setup for both factories, verified configurations table | 🔲 | |
| F2.2 | "Schema in cluster mode" section: `ClusterConfig`, restart hook, `SchemaRestartRequired`, soft drops | 🔲 | |
| F2.3 | Failure and retry behaviour table for HTTP mode | 🔲 | |
| F2.4 | Security notes: token sourcing, least-privilege actions, no `/graphs` enumeration | 🔲 | |

#### Phase F3 — CI

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| F3.1 | Install both `omnigraph` and `omnigraph-server` 0.10.0 in `.github/workflows/_test.yml` | 🔲 | |
| F3.2 | Gate HTTP-mode live tests on `OMNIGRAPH_TEST_SERVER=1` | 🔲 | |
| F3.3 | Pin `omnigraph` in `ci-enabled-optional-deps` | 🔲 | |

**Exit guardrails — Epic F → MVP done**

| Guardrail | Criteria (pass/fail) | Status | Actual outcome |
|-----------|----------------------|--------|----------------|
| Both suites green | CLI-mode and HTTP-mode live suites pass in CI on Linux x86_64 and macOS arm64 | 🔲 | |
| Pre-submission | `prek run --all-files` passes | 🔲 | |
| Docs honest | `omnigraph.mdx` states what is verified (local `file://`, cluster HTTP) and what is not (direct S3) | 🔲 | |

---

### Epic G — Direct S3 store verification  ·  Post-MVP

**Goal:** Decide whether `CliConnectionFactory(store="s3://…")` is a supported mode, with evidence.
**Success metrics:** A MinIO-backed live suite passes, or the docs state that direct S3 is operator-only.

#### Phase G1 — Evidence

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| G1.1 | MinIO testcontainer fixture and credential plumbing | 🔲 | |
| G1.2 | Pass `--yes` on `branch delete` for non-local stores; verify every destructive path | 🔲 | |
| G1.3 | Two-host locking statement: document that host-local locks do not coordinate, and test the failure | 🔲 | |
| G1.4 | Decision row in §6: supported, operator-only, or cut | 🔲 | |

**Risks:** object-store latency dominates process startup, so the CLI's per-call cost matters less here than lock semantics do.

---

### Epic H — SDK release polish  ·  Post-MVP

**Goal:** Bring `omnigraph` to parity with the TypeScript SDK's edge behaviour and make releases mechanical.
**Success metrics:** Blob and redirect semantics match the TypeScript tests; a server release bump is a scripted change; the Python and TypeScript SDKs ship the same day for a server release; the repository lives under the ModernRelay organisation alongside `omnigraph-ts`.

#### Phase H1 — Parity and automation

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| H1.1 | Blob HEAD/GET with ranges and cache conditions; external redirects returned, never followed | 🔲 | |
| H1.2 | Version-sync script: fetch spec, regenerate, bump, changelog | 🔲 | |
| H1.3 | Coverage threshold and generated-code drift enforced in CI | 🔲 | |
| H1.4 | Security review: token handling, redirect policy, logged request data | 🔲 | |
| H1.5 | Transfer the repository and the PyPI project to ModernRelay; re-point trusted publishing, CI badges, and docs links | 🔲 | Trusted publishing pins the repository owner, so the transfer invalidates it until re-added; see decisions 17, 19, 23 |

---

## 5. Risk register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| `cluster apply` refuses while a scratch branch exists, serializing schema changes against all data writes cluster-wide | Med | High | Answer in B1.4 item 1 before Epic E; if true, the control plane must wait for and reap scratch branches like the CLI path does |
| Restart hook is operationally awkward (rolling deploys, managed hosting) | High | Med | `SchemaRestartRequired` fallback converges on re-run; document `managed_by="user"` as the zero-restart option |
| Engine error text over HTTP differs from CLI text, breaking endpoint-stub recovery | Med | High | B1.4 item 5; keep the regex in one place; add a live test that asserts the wording |
| Server minor release changes the wire contract mid-project | Med | Med | Pin 0.10.0 in fixtures and CI; drift check on the spec; compatibility policy in C1.4 |
| Ambiguous write after timeout leads to duplicate or missing data | Low | High | No write retries; tracking advances only on a confirmed response; F1.8 fault-injection test |
| The control plane reintroduces a CLI dependency on the host running schema changes | High | Low | Documented as a control-plane requirement only; data-plane workers need only `omnigraph` |
| Separate SDK repository slows the connector loop before the first release | High | Med | Develop against a local path dependency (`uv` source override) until 0.10.0 is published |
| The PyPI name `omnigraph` is registered by someone else before C2.7 lands | Low | High | C2.7 claims it at the earliest honest point; the trademark-backed reclaim path in PEP 541 is the backstop |
| `omnigraph-server` release asset is ~200 MB per platform, slowing CI | High | Low | Cache the download by version; run HTTP live tests on one Linux runner plus macOS arm64 |
| `/graphs` enumeration is forbidden by default, so misconfigured graph IDs fail late | Med | Low | Validate with `GET /graphs/{id}/schema` at first use; error names the graph and base URL |

---

## 6. Decision log

| # | Date | Decision | Context | Decided by |
|---|------|----------|---------|------------|
| 1 | 2026-09-04 | Target Omnigraph 0.10.0 only; no 0.9 compatibility | Live suite passes on 0.10.0 unchanged; every 0.9 constraint the connector works around still holds | Roman Pronskiy |
| 2 | 2026-09-06 | Keep the CLI transport as the local/direct-store mode | It needs no server, no extra package, and is fully verified | Roman Pronskiy |
| 3 | 2026-09-06 | Build a public Python SDK (`omnigraph`) rather than a connector-private client | Scope B chosen over Scope A in the interview | Roman Pronskiy |
| 4 | 2026-09-06 | SDK lives in a separate repository, not as a workspace member | Clean ownership and release cadence | Roman Pronskiy |
| 5 | 2026-09-06 | PyPI name `omnigraph-sdk`, import `omnigraph_sdk` | Free on PyPI as of 2026-09-06; mirrors the TypeScript package's positioning without squatting the product name | Roman Pronskiy |
| 6 | 2026-09-06 | `HttpConnectionFactory` owns the SDK client; lifespan closes it | Neo4j parity; users provide connection details, not a client | Roman Pronskiy |
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
| 17 | 2026-09-06 | `omnigraph-sdk` is ModernRelay's official Python SDK; the repository lives under the ModernRelay organisation and ModernRelay owns the PyPI project | The author is a ModernRelay employee; this supersedes the handover concern behind H1.5 and the neutral-name reasoning in decision 5 (the name stays `omnigraph-sdk`) | Roman Pronskiy |
| 18 | 2026-09-06 | PyPI name `omnigraph`, import `omnigraph`, client class `omnigraph.Omnigraph`; supersedes the name in decisions 5 and 17 | Mirrors the npm package `@modernrelay/omnigraph` and its `new Omnigraph(...)` entry point; the author is upstream, so the product name is theirs to take | Roman Pronskiy |
| 19 | 2026-09-06 | The SDK repository starts at `github.com/pronskiy/omnigraph-python` and moves to the ModernRelay organisation later; supersedes the repository location in decision 17 | Development can start immediately without org provisioning; a GitHub transfer preserves history, issues, stars, and redirects, so the early URL costs nothing | Roman Pronskiy |
| 20 | 2026-09-06 | Step H1.5 is reinstated as the org transfer rather than a handover offer | Decision 19 defers the move; the step now tracks it, including re-pointing trusted publishing and docs links | Roman Pronskiy |
| 21 | 2026-09-06 | Claim the PyPI name as early as possible, with a functional `0.10.0a1` at the end of Phase C2 rather than an empty placeholder | Verified: a PyPI pending publisher reserves nothing until it publishes, and PEP 541 lists an empty package as name squatting. `omnigraph`, `omni-graph`, and `omnigraphs` were all unregistered on 2026-09-06 | Roman Pronskiy |
| 22 | 2026-09-06 | Use trusted publishing from the start rather than a long-lived API token, accepting that the org transfer requires re-pointing it | Re-pointing is a settings edit already tracked in H1.5; a long-lived token is a standing secret | Roman Pronskiy |
| 23 | 2026-09-06 | The PyPI project is created under a personal account in C2.7 and moves to a ModernRelay PyPI organisation in H1.5, alongside the repository | Follows decision 19; PyPI organisations support the later transfer | Roman Pronskiy |

---

## 7. Open questions

- [x] ~~SDK repository name under the ModernRelay organisation (proposal: `ModernRelay/omnigraph-python`, mirroring `omnigraph-ts`).~~ Resolved 2026-09-06: `pronskiy/omnigraph-python` now, ModernRelay later (decisions 19, 20).
- [x] ~~Does the PyPI `omnigraph` project get created under a personal account and transferred, or under a ModernRelay account from the start?~~ Resolved 2026-09-06: personal in C2.7, moved in H1.5 (decisions 21, 22, 23).
- [ ] Does `cluster apply` refuse while non-main branches exist? (B1.4 item 1; drives E2.5)
- [ ] Is the engine's endpoint-not-found wording identical over HTTP? (B1.4 item 5)
- [ ] Does the server return 413 with `resource_limit` for the 8,192-entity cap, or a 400? (B1.4 item 3)
- [ ] Exact Cedar action names for a least-privilege connector policy. (B1.4 item 7)
- [ ] Should `mutate/if-graph-commit` guard the single-commit fast path, given `branch merge` has no precondition? (B1.4 item 6)
- [ ] Should `load/ndjson` replace generated GQ upserts for large batches, and can it preserve delete semantics? Deferred past MVP unless F1.5 shows a need.
- [ ] How should an ambiguous write be surfaced in `App.update()` reporting beyond a failed action?
- [x] ~~Does ModernRelay plan an official Python SDK? If so, H1.5 becomes a handover.~~ Resolved 2026-09-06: this is it (decision 17).
- [ ] Is the served `/openapi.json` (94 KB) equivalent to the TypeScript repository's copy (167 KB), or does one carry extra descriptions the generator should use?

---

## How to Update This Document

This spec is the source of truth for the build. Keep it current as work happens:

- **Status markers.** Update a step's status in its tracker table as you go: 🔲 → 🔄 → ✅. Use ⏸️ for blocked (note why in Notes) and ❌ for cut (leave the row; the strikethrough of history is useful).
- **Current focus.** Keep the pointer at the top aimed at the next actionable 🔲 step. Update it the moment you finish a step or cross a phase boundary — a stale pointer is worse than none, since it sends the next reader to the wrong place.
- **Guardrails.** When you hit a phase boundary, fill the **Actual outcome** column with what really happened and set the guardrail status. Don't advance to the next phase until its entry guardrails pass — or log a decision explaining why you're proceeding anyway.
- **Decisions.** Any non-trivial choice made during the build gets a new row in the Decision Log (§6). It's append-only — reversals are new rows, not edits. If the choice changes the architecture, also update the Technical Decisions snapshot (§2).
- **Spec changes.** Structural changes (new epic, re-scoped phase) get a Changelog row at the top. Keep the executive summary honest if the project's shape shifts.
- **Open questions.** When one resolves, strike it from §7 and log the decision in §6.
