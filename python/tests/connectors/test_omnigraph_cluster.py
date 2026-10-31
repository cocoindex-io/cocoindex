"""Schema deployment through the cluster configuration, with the CLI faked."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("omnigraph", reason="the omnigraph SDK is not installed")
from cocoindex.connectors.omnigraph import _cluster
from cocoindex.connectors.omnigraph._cluster import ClusterConfig, SchemaDeployError

SERVED = "node A {\n  k: String @key\n}\n"
DESIRED = SERVED + "\nnode B {\n  k: String @key\n}\n"


def _plan(*changes: dict[str, Any], ok: bool = True) -> dict[str, Any]:
    return {"ok": ok, "changes": list(changes), "diagnostics": []}


def _schema_change(*steps: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "resource": "schema.g",
        "operation": "update",
        "disposition": "applied",
        "migration": {"supported": True, "steps": list(steps)},
        **extra,
    }


ADD_B = {"kind": "add_type", "type_kind": "node", "type_name": "B"}


class FakeCli:
    """Answers `cluster plan` and `cluster apply` from scripted results."""

    def __init__(
        self,
        plans: list[dict[str, Any]],
        applies: list[tuple[int, Any, str]] | None = None,
    ) -> None:
        self.plans = list(plans)
        self.applies = list(applies or [])
        self.calls: list[str] = []

    async def __call__(self, cluster: ClusterConfig, *args: str) -> _cluster._CliResult:
        verb = args[1]
        self.calls.append(verb)
        if verb == "plan":
            body = self.plans.pop(0)
            return _cluster._CliResult(0 if body["ok"] else 1, body, "")
        code, body, stderr = self.applies.pop(0)
        return _cluster._CliResult(code, body, stderr)


@pytest.fixture
def cluster(tmp_path: Path) -> ClusterConfig:
    (tmp_path / "g.pg").write_text(SERVED)
    return ClusterConfig(config_dir=tmp_path, target="srv", schema_file="g.pg")


async def _deploy(
    cluster: ClusterConfig,
    *,
    served: str = SERVED,
    after: str = DESIRED,
    on_blocked: Callable[[], Any] | None = None,
) -> None:
    blocked_calls: list[None] = []
    # Read once under the lock (the schema the change was computed from),
    # then once after the deployment.
    reads = [served, after]

    async def read_served() -> str:
        return reads.pop(0) if len(reads) > 1 else reads[0]

    async def blocked() -> None:
        blocked_calls.append(None)
        if on_blocked is not None:
            on_blocked()

    await _cluster.deploy_schema(
        cluster,
        graph_id="g",
        served=served,
        desired=DESIRED,
        allowed_types={"B"},
        read_served=read_served,
        on_blocked_by_branches=blocked,
    )


def _schema_file(cluster: ClusterConfig) -> str:
    return (Path(cluster.config_dir) / cluster.schema_file).read_text()


@pytest.mark.asyncio
async def test_plans_then_applies_and_keeps_the_file(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli = FakeCli([_plan(_schema_change(ADD_B))], [(0, {"deployment": {}}, "")])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    await _deploy(cluster)
    assert cli.calls == ["plan", "apply"]
    assert _schema_file(cluster) == DESIRED


@pytest.mark.asyncio
async def test_nothing_to_deploy_skips_apply_and_restores_the_file(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A textual-only change (formatting): the plan has nothing to deploy, so
    nothing is applied, the file goes back, and there is no post-check."""
    cli = FakeCli([_plan()])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    await _deploy(cluster, after=SERVED)
    assert cli.calls == ["plan"]
    assert _schema_file(cluster) == SERVED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"resource": "policy.graph-access", "operation": "update"}, "also changes"),
        ({"resource": "graph.g", "operation": "delete"}, "would delete"),
        (
            {"resource": "graph.g", "operation": "update", "disposition": "applied"},
            "settings",
        ),
        (
            _schema_change(
                {"kind": "drop_type", "type_kind": "node", "type_name": "A"}
            ),
            "also change type 'A'",
        ),
    ],
)
async def test_plans_reaching_beyond_this_graphs_own_types_are_refused(
    cluster: ClusterConfig,
    monkeypatch: pytest.MonkeyPatch,
    change: dict[str, Any],
    message: str,
) -> None:
    cli = FakeCli([_plan(change)])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match=message):
        await _deploy(cluster)
    assert cli.calls == ["plan"]
    assert _schema_file(cluster) == SERVED  # restored


@pytest.mark.asyncio
async def test_pending_edits_in_the_schema_file_are_not_deployed(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    edited = SERVED + "\nnode Theirs {\n  k: String @key\n}\n"
    (Path(cluster.config_dir) / "g.pg").write_text(edited)
    cli = FakeCli([])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match="differs from the schema"):
        await _deploy(cluster)
    assert cli.calls == []
    assert _schema_file(cluster) == edited


@pytest.mark.asyncio
async def test_live_branches_are_waited_out(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked = _schema_change(
        ADD_B, disposition="blocked", reason="schema_preflight_failed"
    )
    cli = FakeCli(
        [_plan(blocked, ok=False), _plan(_schema_change(ADD_B))],
        [(0, {"deployment": {}}, "")],
    )
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    monkeypatch.setattr(
        "cocoindex.connectors.omnigraph._cluster.asyncio.sleep", _no_sleep
    )
    reaped: list[None] = []
    await _deploy(cluster, on_blocked=lambda: reaped.append(None))
    assert cli.calls == ["plan", "plan", "apply"]
    assert len(reaped) == 1


@pytest.mark.asyncio
async def test_an_apply_refused_for_live_branches_is_retried(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal = {
        "error": "[schema_preflight_failed] schema apply requires a graph with only "
        "main; found non-main branches: coco_scratch_x",
        "code": "conflict",
        "disposition": {"effects": "unknown", "action": "reconcile"},
    }
    cli = FakeCli(
        [_plan(_schema_change(ADD_B)), _plan(_schema_change(ADD_B))],
        [(1, refusal, ""), (0, {"deployment": {}}, "")],
    )
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    monkeypatch.setattr(
        "cocoindex.connectors.omnigraph._cluster.asyncio.sleep", _no_sleep
    )
    await _deploy(cluster)
    assert cli.calls == ["plan", "apply", "plan", "apply"]


@pytest.mark.asyncio
async def test_branches_that_never_go_away_fail_after_the_bounded_wait(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked = _schema_change(
        ADD_B, disposition="blocked", reason="schema_preflight_failed"
    )
    cli = FakeCli([_plan(blocked, ok=False)] * 3)
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    monkeypatch.setattr(_cluster, "_BLOCKED_WAIT_SECONDS", 0.0)
    with pytest.raises(SchemaDeployError, match="only `main`"):
        await _deploy(cluster)
    assert _schema_file(cluster) == SERVED


@pytest.mark.asyncio
async def test_an_unrecognised_apply_failure_keeps_the_file(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After `apply` was launched, a failure the CLI does not answer as a
    refusal without effect is an unknown outcome: the file keeps `desired`."""
    cli = FakeCli(
        [_plan(_schema_change(ADD_B))],
        [(1, {"error": "connection reset"}, "Deployment-ID: dep-9\n")],
    )
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match="dep-9"):
        await _deploy(cluster)
    assert _schema_file(cluster) == DESIRED


@pytest.mark.asyncio
async def test_a_refusal_without_effect_restores_the_file(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal = {
        "error": "policy denied action 'config_manage'",
        "code": "forbidden",
        "disposition": {"effects": "none", "action": "refresh"},
    }
    cli = FakeCli([_plan(_schema_change(ADD_B))], [(1, refusal, "")])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match="config_manage"):
        await _deploy(cluster)
    assert _schema_file(cluster) == SERVED


@pytest.mark.asyncio
async def test_a_cancelled_apply_keeps_the_file(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def cancelled_apply(c: ClusterConfig, *args: str) -> _cluster._CliResult:
        if args[1] == "plan":
            return _cluster._CliResult(0, _plan(_schema_change(ADD_B)), "")
        raise asyncio.CancelledError

    monkeypatch.setattr(_cluster, "_run_cli", cancelled_apply)
    with pytest.raises(asyncio.CancelledError):
        await _deploy(cluster)
    assert _schema_file(cluster) == DESIRED


@pytest.mark.asyncio
async def test_a_busy_operator_configuration_is_retried(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    busy = _cluster._CliResult(
        2, None, "Error: could not lock operator configuration: would block\n"
    )
    calls: list[str] = []

    async def cli(c: ClusterConfig, *args: str) -> _cluster._CliResult:
        calls.append(args[1])
        if len(calls) == 1:
            return busy
        if args[1] == "plan":
            return _cluster._CliResult(0, _plan(_schema_change(ADD_B)), "")
        return _cluster._CliResult(0, {"deployment": {}}, "")

    monkeypatch.setattr(_cluster, "_run_cli", cli)
    monkeypatch.setattr(
        "cocoindex.connectors.omnigraph._cluster.asyncio.sleep", _no_sleep
    )
    await _deploy(cluster)
    assert calls == ["plan", "plan", "apply"]


@pytest.mark.asyncio
async def test_a_schema_that_moved_meanwhile_is_not_overwritten(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another process deployed something else between this sync's read and
    its lock: refuse (the next run re-reads) rather than deploy a stale merge."""
    cli = FakeCli([])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    moved = SERVED + "\nnode C {\n  k: String @key\n}\n"

    async def read_served() -> str:
        return moved

    async def blocked() -> None:
        return None

    with pytest.raises(SchemaDeployError, match="changed while this sync"):
        await _cluster.deploy_schema(
            cluster,
            graph_id="g",
            served=SERVED,
            desired=DESIRED,
            allowed_types={"B"},
            read_served=read_served,
            on_blocked_by_branches=blocked,
        )
    assert cli.calls == []


@pytest.mark.asyncio
async def test_an_expired_wait_keeps_the_file_and_names_the_deployment(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli = FakeCli(
        [_plan(_schema_change(ADD_B))],
        [(5, {"error": "timed out"}, "Deployment-ID: dep-7\n")],
    )
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match="dep-7"):
        await _deploy(cluster)
    # The next run finds `desired` there and completes or confirms it.
    assert _schema_file(cluster) == DESIRED


@pytest.mark.asyncio
async def test_a_previous_unknown_outcome_is_completed(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    (Path(cluster.config_dir) / "g.pg").write_text(DESIRED)
    cli = FakeCli([_plan(_schema_change(ADD_B))], [(0, {"deployment": {}}, "")])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    await _deploy(cluster)
    assert cli.calls == ["plan", "apply"]


@pytest.mark.asyncio
async def test_a_deployment_that_serves_something_else_is_an_error(
    cluster: ClusterConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli = FakeCli([_plan(_schema_change(ADD_B))], [(0, {"deployment": {}}, "")])
    monkeypatch.setattr(_cluster, "_run_cli", cli)
    with pytest.raises(SchemaDeployError, match="serves a different schema"):
        await _deploy(cluster, after=SERVED)


def test_cli_output_is_parsed_and_the_deployment_id_found() -> None:
    result = _cluster._CliResult(
        1, json.loads('{"error": "x"}'), "target srv\nDeployment-ID: a:1:b\n"
    )
    assert result.deployment_id == "a:1:b"
    colored = _cluster._CliResult(1, None, "\x1b[2mDeployment-ID: c:2:d\x1b[0m\n")
    assert colored.deployment_id == "c:2:d"
    assert _cluster._error_text(result) == "x"


async def _no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_the_cli_runs_with_the_callers_omnigraph_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No private OMNIGRAPH_HOME: with `operator_config=None` the CLI must
    find the caller's default operator configuration and stored credentials."""
    seen: dict[str, Any] = {}

    class _Proc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"{}", b""

    async def fake_exec(*argv: str, **kwargs: Any) -> _Proc:
        seen["argv"] = argv
        seen["env"] = kwargs.get("env")
        return _Proc()

    monkeypatch.setattr(
        "cocoindex.connectors.omnigraph._cluster.asyncio.create_subprocess_exec",
        fake_exec,
    )
    config = ClusterConfig(config_dir=tmp_path, target="srv", schema_file="g.pg")
    await _cluster._run_cli(config, "cluster", "plan")
    assert seen["env"] is None  # inherits the caller's environment as is
    assert "--operator-config" not in seen["argv"]
