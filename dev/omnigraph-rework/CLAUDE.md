# CLAUDE.md

Project instructions for Claude Code working on the **Omnigraph connector rework** (CocoIndex branch `omnigraph-connector`).

## Overview

The Omnigraph target connector currently drives the `omnigraph` CLI against a direct `file://` store. The rework replaces that with HTTP to `omnigraph-server` as the connector's only **data** transport: the CLI data path is deleted, and there is no Python SDK package — the HTTP client is private to the connector. One CLI use survives by necessity: `omnigraph cluster apply`, because the server refuses schema writes on every configuration (P1) and `managed_by="system"` must keep working (decision 32). Stage: spec reworked 2026-09-07, Epic A probes P1/P2/P12 answered, no rework code yet; the connector branch is at 37 commits over `main` with a passing live suite on Omnigraph 0.10.0.

**`dev/omnigraph-rework/SPEC.md` is the task list and source of truth.** Start at the **Current focus** pointer near the top — it names the next actionable step so you don't have to scan the whole file. Work the spec: implement that step's deliverable, update its status (🔲 → 🔄 → ✅) in the phase tracker, and advance the Current focus pointer. Don't skip ahead past a phase's exit guardrails — at a phase boundary, verify the criteria, fill in the **Actual outcome** column, and only then move on.

The research behind the spec is `dev/omnigraph-connector-transport-analysis.md`. Read it when a step's rationale is unclear; do not re-derive its findings. Note that it predates the rework: its "Scope A: connector-private minimal HTTP client" is what is being built, and its Scope B/C sections (public SDK, TypeScript parity) are no longer the plan.

## One repository

Everything lives in cocoindex. Repo-wide conventions come from `AGENTS.md` and apply unchanged; this file adds only what is specific to the rework. There is no second repository, no PyPI project, and no published package — if a step seems to call for one, the spec is stale and you should stop and say so.

## What the probes already settled

Answered live on 2026-09-07 (`server-spike.md` carries every request and response):

- **P1 — HTTP schema apply is refused on every server configuration.** 409 `conflict`, "server-side schema apply is disabled for cluster-backed serving". `--cluster` is the server's only boot source, and "config-free" storage-root serving still reads a cluster state ledger from the bucket, so there is no server mode that accepts it.
- **P2 — there is no graph-creation endpoint.** `/graphs` is GET-only; an unknown graph gives 404 `not_found`. Graphs are created by `cluster apply`.
- **P12 — no hot reload.** `cluster apply` succeeds and the served schema never changes until the server restarts. Crucially, `cluster apply` never contacts the server: it needs the config directory and store credentials, **not** colocation. A cocoindex host can drive it.

Still open and worth answering before the code they affect: **P3** (does the server serialize concurrent writes and branch merges — decides whether `exclusive_store()` disappears), **P13** (does `cluster apply` refuse while non-main branches exist), and P4-P11.

Probe answers go in `server-spike.md` with the exact request and response. Never summarise a probe you have not run. The author has access to the Omnigraph source, so an answer may start from reading it — but it is not an answer until the binary confirms it.

## Code conventions

- **Directory structure:**
  ```
  python/cocoindex/connectors/omnigraph/
  ├── __init__.py          # re-exports _target.__all__
  ├── _client.py           # ConnectionFactory, _HttpClient, errors, typed results   (Epic B)
  ├── _cluster.py          # ClusterConfig, cluster apply, restart hook, converge     (Epic D)
  ├── _gq.py               # .pg schema editing and GQ rendering — transport-agnostic, untouched
  └── _target.py           # reconciliation; imports only _client, _cluster and _gq
  python/tests/connectors/
  ├── omnigraph_server_fixture.py     # rustfs + omnigraph-server, both boot modes    (Epic A)
  └── test_omnigraph_target.py        # the suite, migrated to the fixture            (Epics C, E)
  ```
  There is no `_transport.py`: with one data transport, a Protocol over a single implementation is an abstraction with nothing to abstract. Typed results and a typed error hierarchy live in `_client.py`. `_cluster.py` is the *only* module allowed to spawn a subprocess, and `_target.py` reaches it through the single `apply_schema()` seam (D1.1) so a future server-side apply (D3) is a one-place change.
- **Deletions are part of the work.** Epic C1.2 removes `canonical_store`, `_lock_dir`, `_temporary_text_file`, the `fcntl`/`msvcrt` lock helpers, `OmnigraphCliError`, and `_CliClient`. Don't leave them behind "just in case" — the guardrail greps for them. The one lock that survives is in `_cluster.py`, guarding the read-merge-write of the `.pg` file, and the one `subprocess` use is `cluster apply`.
- **Style:** follow `AGENTS.md` (`uv run ruff format .`, `uv run mypy`, external-module underscore rules). `aiohttp` appears only in `_client.py`; `_target.py` sees typed results and typed errors, never a `ClientResponse`.
- **Dependencies:** `aiohttp>=3.9` via the optional extra `cocoindex[omnigraph]`, and `msgspec` (already core) for JSON. Both are already in this repository, including the `aiohttp` mypy override. Anything beyond these two needs a human decision.
- **Testing:** live tests need `OMNIGRAPH_TEST_SERVER=1` and the binaries at `test/bin/`: `omnigraph` and `omnigraph-server` 0.10.0, plus `rustfs` 1.0.0-rc.5 for the object store. omnigraph reads standard AWS SDK env vars — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`, and `AWS_ALLOW_HTTP=true` for a plaintext rustfs endpoint. The whole test module must run with the sandbox disabled: `create_test_env` opens LMDB at import and gets EPERM inside it, and `omnigraph-server` cannot bind a port there either. Use `.venv/bin/python -m pytest python/tests/connectors/test_omnigraph_target.py`. With the sandbox off, `$TMPDIR` is the macOS default rather than the scratchpad, so pass absolute paths. Every step with a code deliverable ships a test; live behaviour claims cite the request and the response.
- **Commits:** short, human-looking messages, no co-author trailers (user rule). Prefix with the area, e.g. `omnigraph: rustfs + server fixture (A1.1)`. One step per commit where practical.

## Engine facts to keep in mind

Verified live on Omnigraph 0.10.0; re-verify before relying on them against a newer release.

- `omnigraph-server` is **cluster-only at boot**: `--cluster` takes either a config directory (storage resolved through `cluster.yaml`) or a storage-root URI directly (`s3://`, `az://` — config-free serving). There is no `file://` server mode, which is why the connector can no longer offer a "point it at a directory" option.
- **Every** server answers `POST /graphs/{id}/schema/apply` with 409 `conflict` — storage-root boots included (P1). Schema changes go through `omnigraph cluster apply --config <dir>`, which refuses `--server`, and take effect only after a server restart (P12). `cluster.yaml` takes a `storage:` key naming the root, and `cluster import` seeds the ledger before the first `apply`.
- `GET /graphs` is forbidden unless a policy bundle opts in, even with `--unauthenticated`. Always configure `graph_id`.
- Errors are `{"error": str, "code": ErrorCode}` plus optional structured fields (`merge_conflicts`, `precondition_failure`, `resource_limit`, `key_conflict`). Body deserialization failures return 422 plain text.
- The server serves its own OpenAPI document at `/openapi.json` — useful as reference while writing the calls, though nothing is generated from it. `/healthz` returns `{"status","version","internal_schema_version"}`.
- Engine rules that held on the direct store and are expected to hold over HTTP, each with a probe to confirm it: no mixing upsert and delete in one mutation (P7), an 8,192-entity cap per mutation (P6), `cluster apply` may refuse while non-main branches exist, as direct-store `schema apply` did (P13), `branch merge` has no precondition (P11), destructive branch operations on a non-local scope needed CLI `--yes` consent (P8).

## Workflow

- **Autonomous:** any step whose deliverable is code plus tests — the Epic A fixture, all of Epics B and C, the chosen Epic D path, and Epic E — including running the live suite and recording guardrail outcomes.
- **Needs human input:** the D3 upstream ask (it is a ModernRelay product change, not a cocoindex one); the restart-hook API shape if D2 finds the spec's signature insufficient; anything that adds a row to the Decision Log or resolves an Open Question; dependency additions beyond `aiohttp` and `msgspec`; anything touching CI secrets; and any finding that makes a spec step wrong rather than merely harder.
- **The loop:** pick the next 🔲 step → implement the deliverable → run tests and linters → update the status table → commit referencing the step. At a phase boundary, stop, verify each guardrail, fill in **Actual outcome**, and only then continue.
- **When blocked or ambiguous:** mark the step ⏸️ with a note, add an Open Question to the spec, and surface it rather than guessing.

## Goals

Near term: turn the manual rustfs + `omnigraph-server` setup from the P1 probe into the reusable fixture (A1.1-A1.3), then answer P3 and P13. MVP is reached when the migrated live suite passes over HTTP against a pinned 0.10.0 server in CI for both `managed_by="user"` and `managed_by="system"`, the connector's data path contains no subprocess or file-lock code, and the docs state plainly that schema changes require a server restart.
