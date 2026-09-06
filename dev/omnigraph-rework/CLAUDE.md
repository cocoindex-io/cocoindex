# CLAUDE.md

Project instructions for Claude Code working on the **Omnigraph connector rework** (CocoIndex branch `omnigraph-connector` plus the new `omnigraph` repository).

## Overview

The Omnigraph target connector currently drives the `omnigraph` CLI against a direct `file://` store. The rework keeps that as the local mode, adds a public async Python SDK (`omnigraph`) for `omnigraph-server`, adds an `HttpConnectionFactory` that uses it, and adds a small control plane so `managed_by="system"` schema changes work against a cluster-managed graph. Stage: spec written, no rework code yet; the connector branch is at 37 commits over `main` with a passing live suite on Omnigraph 0.10.0.

**`dev/omnigraph-rework/SPEC.md` is the task list and source of truth.** Start at the **Current focus** pointer near the top of the spec — it names the next actionable step so you don't have to scan the whole file. Work the spec: implement that step's deliverable, update its status (🔲 → 🔄 → ✅) in the phase tracker, and advance the Current focus pointer. Don't skip ahead past a phase's exit guardrails — when you reach a phase boundary, verify the guardrail criteria, fill in the **Actual outcome** column, and only then move on.

The research behind the spec is `dev/omnigraph-connector-transport-analysis.md`. Read it when a step's rationale is unclear; do not re-derive its findings.

## Two repositories

- **cocoindex** (this repo): Epics A, B, D, E, F, G. Repo-wide conventions come from `AGENTS.md` and apply unchanged; this file adds only what is specific to the rework.
- **omnigraph** (separate repo, created in step C1.1): Epics C and H. Lives at `github.com/pronskiy/omnigraph-python` for now and transfers to the ModernRelay organisation in step H1.5, so avoid hardcoding the repository URL in code, package metadata, or docs beyond one place that H1.5 can update. This is ModernRelay's official Python SDK; the author is a ModernRelay employee, so upstream questions (Cedar action names, `cluster apply` behaviour, error wording) can be answered from the Omnigraph source, then confirmed against the binary. Copy this file to that repo's root and change the spec path above to point at a checked-in copy or the cocoindex URL. Develop the connector against a local path dependency (`[tool.uv.sources] omnigraph = { path = "../omnigraph-python", editable = true }`). Step C2.7 publishes `0.10.0a1` to claim the PyPI name; from then on CI can install from PyPI, but local work stays on the path dependency until `0.10.0` is stable. A prerelease needs an explicit pin (`>=0.10.0a1`), since a plain `>=0.10` excludes it.

## Code conventions

- **Directory structure (cocoindex side):**
  ```
  python/cocoindex/connectors/omnigraph/
  ├── __init__.py          # re-exports _target.__all__
  ├── _transport.py        # OmnigraphClient protocol, result types, OmnigraphError   (A1)
  ├── _client.py           # CliConnectionFactory, _CliClient, file locks
  ├── _http_client.py      # HttpConnectionFactory, _HttpClient                       (D1)
  ├── _cluster.py          # ClusterConfig, cluster apply, restart, converge          (E1)
  ├── _gq.py               # .pg schema editing and GQ rendering (transport-agnostic)
  └── _target.py           # reconciliation; must import only _transport, _gq
  python/tests/connectors/
  ├── test_omnigraph_target.py        # CLI-mode suite (existing)
  ├── omnigraph_server_fixture.py     # boots omnigraph-server from a cluster dir     (B1.1)
  └── test_omnigraph_http.py          # HTTP-mode suite                               (D, E, F)
  ```
- **Directory structure (SDK side):**
  ```
  omnigraph-python/
  ├── pyproject.toml                  # name = "omnigraph", requires-python >= 3.11
  ├── spec/openapi-0.10.0.json        # captured from a running server's /openapi.json
  ├── scripts/fetch_spec.py, generate.py
  ├── src/omnigraph/
  │   ├── __init__.py                 # Omnigraph, errors
  │   ├── _client.py                  # httpx facade
  │   ├── _errors.py
  │   ├── _streams.py                 # export / changes baseline iterators
  │   └── _generated/models.py        # msgspec.Struct models — never hand-edit
  └── tests/                          # respx unit tests + live tests
  ```
- **Naming collision:** the SDK's import name `omnigraph` is also the connector package's last path segment (`cocoindex.connectors.omnigraph`). Inside the connector import it as `import omnigraph as omnigraph_sdk` so stack traces and greps stay unambiguous; never add a module named `omnigraph.py` under the connector.
- **Style:** cocoindex side follows `AGENTS.md` (`uv run ruff format .`, `uv run mypy`, external-module underscore rules). SDK side: `ruff format`, `ruff check`, `mypy --strict`; generated code is excluded from lint and never edited by hand.
- **Testing:** cocoindex live tests need `OMNIGRAPH_TEST_STORE=1` (CLI mode) or `OMNIGRAPH_TEST_SERVER=1` (HTTP mode) and the 0.10.0 binaries at `test/bin/omnigraph` and `test/bin/omnigraph-server`. The whole `test_omnigraph_target.py` module must run with the sandbox disabled because `create_test_env` opens LMDB at import; use `.venv/bin/python -m pytest python/tests/connectors/test_omnigraph_target.py`. `omnigraph-server` cannot bind a port inside the sandbox either. Every step with a code deliverable ships a test; live behaviour claims cite the command and output.
- **Commits:** short, human-looking messages, no co-author trailers (user rule). Prefix with the area, e.g. `omnigraph: extract transport protocol (A1.1)`. One step per commit where practical.

## Engine facts to keep in mind

Verified live on Omnigraph 0.10.0; re-verify before relying on them against a newer release.

- A cluster-backed server answers `POST /graphs/{id}/schema/apply` with 409 `conflict`. Schema changes go through `omnigraph cluster apply --config <dir>` and take effect only after the server restarts. `cluster apply` refuses `--server`.
- `GET /graphs` is forbidden unless a policy bundle opts in, even with `--unauthenticated`. Always configure `graph_id`.
- Errors are `{"error": str, "code": ErrorCode}` plus optional structured fields (`merge_conflicts`, `precondition_failure`, `resource_limit`, `key_conflict`). Body deserialization failures return 422 plain text.
- The server serves its own OpenAPI document at `/openapi.json`; `/healthz` returns `{"status","version","internal_schema_version"}`.
- Direct-store rules still hold: no mixing upsert and delete in one mutation, 8,192-entity cap per mutation, `schema apply` refuses while non-main branches exist, `branch merge` has no precondition, `branch delete` on a non-local scope needs `--yes`.

## Workflow

- **Autonomous:** steps whose deliverable is code plus tests inside one repo — all of Epic A, C2–C3, D1–D2, E1–E2 — including running the live suites and recording guardrail outcomes.
- **Needs human input:** creating the SDK repository (C1.1), the PyPI pending publisher and both releases (C2.7, C4.3), and the org transfer (H1.5); anything that adds a row to the Decision Log or resolves an Open Question; restart-hook API shape if E2 finds the spec's signature insufficient; dependency additions beyond `httpx` and `msgspec`; anything touching CI secrets.
- **The loop:** pick the next 🔲 step → implement the deliverable → run tests and linters → update the status table → commit referencing the step. At a phase boundary, stop, verify each guardrail, fill in **Actual outcome**, and only then continue.
- **When blocked or ambiguous:** mark the step ⏸️ with a note, add an Open Question to the spec, and surface it rather than guessing. Spike findings (Epic B) go into `dev/omnigraph-rework/server-spike.md` with the exact command and response.

## Goals

Near term: finish Epic A so the connector's reconciliation code depends only on the `OmnigraphClient` protocol, then run the Epic B spike to answer the ten server-behaviour questions before any SDK code is written. MVP is reached when the HTTP-mode live suite passes for both `managed_by="user"` and `managed_by="system"` targets and `omnigraph 0.10.0` is installable from PyPI.
