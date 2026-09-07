# CLAUDE.md

Project instructions for Claude Code working on the **Omnigraph connector rework** (CocoIndex branch `omnigraph-connector`).

## Overview

The Omnigraph target connector currently drives the `omnigraph` CLI against a direct `file://` store. The rework replaces that with HTTP to `omnigraph-server` as the connector's **only** transport: the CLI subprocess path is deleted, and there is no Python SDK package — the HTTP client is private to the connector. Stage: spec reworked 2026-09-07, no rework code yet; the connector branch is at 37 commits over `main` with a passing live suite on Omnigraph 0.10.0.

**`dev/omnigraph-rework/SPEC.md` is the task list and source of truth.** Start at the **Current focus** pointer near the top — it names the next actionable step so you don't have to scan the whole file. Work the spec: implement that step's deliverable, update its status (🔲 → 🔄 → ✅) in the phase tracker, and advance the Current focus pointer. Don't skip ahead past a phase's exit guardrails — at a phase boundary, verify the criteria, fill in the **Actual outcome** column, and only then move on.

The research behind the spec is `dev/omnigraph-connector-transport-analysis.md`. Read it when a step's rationale is unclear; do not re-derive its findings. Note that it predates the rework: its "Scope A: connector-private minimal HTTP client" is what is being built, and its Scope B/C sections (public SDK, TypeScript parity) are no longer the plan.

## One repository

Everything lives in cocoindex. Repo-wide conventions come from `AGENTS.md` and apply unchanged; this file adds only what is specific to the rework. There is no second repository, no PyPI project, and no published package — if a step seems to call for one, the spec is stale and you should stop and say so.

## Epic A comes first, and it gates the rest

Three probes change the shape of what gets built, so no client code is written until they are answered against the running binary:

- **P1** — does a server booted from a bare storage-root URI accept `POST /graphs/{id}/schema/apply`? Decides whether `managed_by="system"` survives (Epic D path D2a) or the connector becomes user-managed-only (D2b).
- **P2** — with no CLI, is there any HTTP path that creates a graph? Decides how the uninitialized-graph branch in `_target.py` behaves.
- **P3** — does the server serialize concurrent writes and branch merges per graph? Decides whether `exclusive_store()` is removed outright.

Probe answers go in `dev/omnigraph-rework/server-spike.md` with the exact request and response. Never summarise a probe you have not run. The author has access to the Omnigraph source, so an answer may start from reading it — but it is not an answer until the binary confirms it.

## Code conventions

- **Directory structure:**
  ```
  python/cocoindex/connectors/omnigraph/
  ├── __init__.py          # re-exports _target.__all__
  ├── _client.py           # ConnectionFactory, _HttpClient, errors, typed results   (Epic B)
  ├── _gq.py               # .pg schema editing and GQ rendering — transport-agnostic, untouched
  └── _target.py           # reconciliation; imports only _client and _gq
  python/tests/connectors/
  ├── omnigraph_server_fixture.py     # rustfs + omnigraph-server, both boot modes    (Epic A)
  └── test_omnigraph_target.py        # the suite, migrated to the fixture            (Epics C, E)
  ```
  There is no `_transport.py`: with one transport, a Protocol over a single implementation is an abstraction with nothing to abstract. Typed results and a typed error hierarchy live in `_client.py`. There is no `_cluster.py`: the cluster control plane is cut (decision 29) — if P1 fails, the cluster procedure is documentation, not code.
- **Deletions are part of the work.** Epic C1.2 removes `canonical_store`, `_lock_dir`, `_temporary_text_file`, the `fcntl`/`msvcrt` lock helpers, `OmnigraphCliError`, and `_CliClient`. Don't leave them behind "just in case" — the guardrail greps for them.
- **Style:** follow `AGENTS.md` (`uv run ruff format .`, `uv run mypy`, external-module underscore rules). `aiohttp` appears only in `_client.py`; `_target.py` sees typed results and typed errors, never a `ClientResponse`.
- **Dependencies:** `aiohttp>=3.9` via the optional extra `cocoindex[omnigraph]`, and `msgspec` (already core) for JSON. Both are already in this repository, including the `aiohttp` mypy override. Anything beyond these two needs a human decision.
- **Testing:** live tests need `OMNIGRAPH_TEST_SERVER=1` and the 0.10.0 binaries at `test/bin/omnigraph` (fixture setup only — `cluster apply` when booting the cluster-dir mode) and `test/bin/omnigraph-server`. The whole test module must run with the sandbox disabled: `create_test_env` opens LMDB at import and gets EPERM inside it, and `omnigraph-server` cannot bind a port there either. Use `.venv/bin/python -m pytest python/tests/connectors/test_omnigraph_target.py`. With the sandbox off, `$TMPDIR` is the macOS default rather than the scratchpad, so pass absolute paths. Every step with a code deliverable ships a test; live behaviour claims cite the request and the response.
- **Commits:** short, human-looking messages, no co-author trailers (user rule). Prefix with the area, e.g. `omnigraph: rustfs + server fixture (A1.1)`. One step per commit where practical.

## Engine facts to keep in mind

Verified live on Omnigraph 0.10.0; re-verify before relying on them against a newer release.

- `omnigraph-server` is **cluster-only at boot**: `--cluster` takes either a config directory (storage resolved through `cluster.yaml`) or a storage-root URI directly (`s3://`, `az://` — config-free serving). There is no `file://` server mode, which is why the connector can no longer offer a "point it at a directory" option.
- A **cluster-config-backed** server answers `POST /graphs/{id}/schema/apply` with 409 `conflict`; schema changes go through `omnigraph cluster apply --config <dir>` and take effect only after a restart, and `cluster apply` refuses `--server`. Whether a **storage-root-backed** server behaves the same is probe P1 and is currently unknown.
- `GET /graphs` is forbidden unless a policy bundle opts in, even with `--unauthenticated`. Always configure `graph_id`.
- Errors are `{"error": str, "code": ErrorCode}` plus optional structured fields (`merge_conflicts`, `precondition_failure`, `resource_limit`, `key_conflict`). Body deserialization failures return 422 plain text.
- The server serves its own OpenAPI document at `/openapi.json` — useful as reference while writing the calls, though nothing is generated from it. `/healthz` returns `{"status","version","internal_schema_version"}`.
- Engine rules that held on the direct store and are expected to hold over HTTP, each with a probe to confirm it: no mixing upsert and delete in one mutation (P7), an 8,192-entity cap per mutation (P6), `schema apply` refuses while non-main branches exist (P1/D2a.3), `branch merge` has no precondition (P11), destructive branch operations on a non-local scope needed CLI `--yes` consent (P8).

## Workflow

- **Autonomous:** any step whose deliverable is code plus tests — the Epic A fixture, all of Epics B and C, the chosen Epic D path, and Epic E — including running the live suite and recording guardrail outcomes.
- **Needs human input:** the Epic D path choice once P1 is answered (it is a decision row); anything else that adds a row to the Decision Log or resolves an Open Question; dependency additions beyond `aiohttp` and `msgspec`; anything touching CI secrets; and any finding that makes a spec step wrong rather than merely harder.
- **The loop:** pick the next 🔲 step → implement the deliverable → run tests and linters → update the status table → commit referencing the step. At a phase boundary, stop, verify each guardrail, fill in **Actual outcome**, and only then continue.
- **When blocked or ambiguous:** mark the step ⏸️ with a note, add an Open Question to the spec, and surface it rather than guessing.

## Goals

Near term: stand up the rustfs + `omnigraph-server` fixture and answer all eleven Epic A probes, P1 first. MVP is reached when the migrated live suite passes over HTTP against a pinned 0.10.0 server in CI, the connector contains no subprocess or file-lock code, and the docs state plainly what schema management the connector does and what a running server is required for.
