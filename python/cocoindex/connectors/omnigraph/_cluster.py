"""Schema writes through a live cluster deployment (`managed_by="system"`).

`omnigraph-server` 0.14 takes schema changes only as a cluster deployment,
and only the `omnigraph` CLI captures one from a configuration directory. So
a schema write here is: put the graph's desired `.pg` into the cluster
configuration, `omnigraph cluster plan` it, refuse anything beyond this
graph's own types, then `omnigraph cluster apply` it to the running server,
which activates it without a restart.

This is the only module that spawns a subprocess.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import pathlib
import re
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from typing import Any, BinaryIO

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

#: How long a deployment blocked by live branches keeps retrying.
_BLOCKED_WAIT_SECONDS = 60.0
#: Bound on the CLI's own wait for activation (its `--timeout`).
_APPLY_TIMEOUT_SECONDS = 300
#: The CLI's exit code when its wait expired: the outcome is unknown.
_EXIT_WAIT_EXPIRED = 5
_LOCK_NAME = ".cocoindex-omnigraph.lock"
_DEPLOYMENT_ID_RE = re.compile(r"Deployment-ID: (\S+)")
#: The CLI colors its stderr even when it is not a terminal.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
#: A CLI refused before sending anything because another CLI process holds
#: the operator configuration of the same `OMNIGRAPH_HOME`.
_OPERATOR_LOCK_BUSY = "could not lock operator configuration"


@dataclasses.dataclass(frozen=True)
class ClusterConfig:
    """The cluster configuration the connector deploys schema changes through.

    ``config_dir`` holds ``cluster.yaml``; ``schema_file`` is this graph's
    ``.pg`` as ``cluster.yaml`` declares it (``graphs.<id>.schema``), relative
    to ``config_dir``. ``target`` names a ``kind: server`` target in the
    ``omnigraph`` operator configuration: ``operator_config``, or else the
    CLI's own default (``$OMNIGRAPH_HOME/config.yaml``, normally
    ``~/.omnigraph/config.yaml``). Its credential needs ``config_manage``
    and ``schema_apply``.

    Every schema change of every app must go through this one directory: a
    deployment applies the whole bundle, so a stale copy elsewhere would undo
    changes made here. The connector never adds or removes ``graphs:``
    entries.
    """

    config_dir: str | os.PathLike[str]
    target: str
    schema_file: str
    cli: str = "omnigraph"
    operator_config: str | os.PathLike[str] | None = None


class SchemaDeployError(RuntimeError):
    """A schema deployment was refused, blocked, or ended with an unknown outcome."""


class _OutcomeUnknown(SchemaDeployError):
    """The deployment was submitted and its outcome is not known."""


@dataclasses.dataclass
class _Submission:
    """Whether a `cluster apply` may have reached the server and not been
    answered as refused, in which case the schema file must keep `desired`."""

    pending: bool = False


@dataclasses.dataclass(frozen=True)
class _CliResult:
    returncode: int
    body: Any
    stderr: str

    @property
    def deployment_id(self) -> str | None:
        m = _DEPLOYMENT_ID_RE.search(_ANSI_RE.sub("", self.stderr))
        return m.group(1) if m else None

    @property
    def operator_lock_busy(self) -> bool:
        return self.returncode != 0 and _OPERATOR_LOCK_BUSY in self.stderr

    @property
    def refused_without_effect(self) -> bool:
        """The server answered that the request ran nothing."""
        if not isinstance(self.body, dict):
            return False
        disposition = self.body.get("disposition")
        return isinstance(disposition, dict) and disposition.get("effects") == "none"


if sys.platform == "win32":

    def _lock_file(f: BinaryIO) -> None:
        f.seek(0, 2)
        if f.tell() == 0:
            f.write(b"\0")
            f.flush()
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f: BinaryIO) -> None:
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

else:

    def _lock_file(f: BinaryIO) -> None:
        fcntl.flock(f, fcntl.LOCK_EX)

    def _unlock_file(f: BinaryIO) -> None:
        fcntl.flock(f, fcntl.LOCK_UN)


@contextlib.asynccontextmanager
async def _config_lock(config_dir: pathlib.Path) -> AsyncIterator[None]:
    """Serialize read-merge-deploy of one configuration directory across
    processes on hosts that share it."""
    f = await asyncio.to_thread((config_dir / _LOCK_NAME).open, "a+b")
    try:
        await asyncio.to_thread(_lock_file, f)
        yield
    finally:
        await asyncio.to_thread(_unlock_file, f)
        await asyncio.to_thread(f.close)


async def _run_cli(cluster: ClusterConfig, *args: str) -> _CliResult:
    argv = [cluster.cli]
    if cluster.operator_config is not None:
        argv += ["--operator-config", os.fspath(cluster.operator_config)]
    argv += list(args)
    # The caller's OMNIGRAPH_HOME, so its default operator configuration and
    # stored credentials apply. Two CLIs sharing one home cannot run at once;
    # the second refuses before sending anything (`operator_lock_busy`), and
    # the deploy loop retries it.
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await proc.communicate()
    except BaseException:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
        raise
    text = out.decode(errors="replace").strip()
    try:
        body: Any = json.loads(text) if text else None
    except ValueError:
        body = text
    assert proc.returncode is not None
    stderr = _ANSI_RE.sub("", err.decode(errors="replace"))
    return _CliResult(proc.returncode, body, stderr)


def _check_plan(
    plan: dict[str, Any], *, graph_id: str, allowed_types: Collection[str]
) -> None:
    """Refuse a plan that reaches beyond this graph's schema or its own types.

    The bundle is the whole cluster, so edits someone left in it would ride
    along with this deployment; deleting a graph deletes its storage.
    """
    own = {f"graph.{graph_id}", f"schema.{graph_id}"}
    for change in plan.get("changes") or []:
        resource = change.get("resource")
        if resource not in own:
            raise SchemaDeployError(
                f"the cluster configuration also changes {resource!r}; CocoIndex "
                f"deploys only graph {graph_id!r}'s schema. Apply or revert the "
                f"other pending edits first."
            )
        if change.get("operation") == "delete":
            raise SchemaDeployError(
                f"the cluster configuration would delete {resource!r}, which "
                f"deletes the graph's storage; CocoIndex never does that."
            )
        if resource == f"graph.{graph_id}" and change.get("disposition") not in (
            "derived",
            "blocked",
        ):
            # A schema change only ever derives a graph update; anything else
            # is an edit to the graph's own `cluster.yaml` entry.
            raise SchemaDeployError(
                f"the cluster configuration also changes graph {graph_id!r}'s "
                f"settings; CocoIndex deploys only its schema. Apply or revert "
                f"that edit first."
            )
        for step in (change.get("migration") or {}).get("steps") or []:
            type_name = step.get("type_name")
            if type_name is not None and type_name not in allowed_types:
                raise SchemaDeployError(
                    f"the deployment would also change type {type_name!r} "
                    f"({step.get('kind')}), which this sync does not manage; apply "
                    f"or revert that edit in {graph_id!r}'s schema file first."
                )


def _blocked_reasons(plan: dict[str, Any]) -> set[str]:
    return {
        str(c.get("reason"))
        for c in plan.get("changes") or []
        if c.get("disposition") == "blocked"
    }


def _error_text(result: _CliResult) -> str:
    if isinstance(result.body, dict):
        if "error" in result.body:
            return str(result.body["error"])
        diags = result.body.get("diagnostics")
        if diags:
            return "; ".join(f"{d.get('code')}: {d.get('message')}" for d in diags)
    return result.stderr.strip() or str(result.body)


async def deploy_schema(
    cluster: ClusterConfig,
    *,
    graph_id: str,
    served: str,
    desired: str,
    allowed_types: Collection[str],
    read_served: Callable[[], Awaitable[str]],
    on_blocked_by_branches: Callable[[], Awaitable[None]],
) -> None:
    """Deploy `desired` as graph `graph_id`'s complete schema.

    `served` is the schema `desired` was computed from. The schema file must
    hold either that or `desired` (an earlier run whose outcome was unknown);
    anything else is someone's pending edit, and this refuses rather than
    deploying it. A refusal that recorded nothing restores the file; once an
    apply may have reached the server unanswered (it timed out, failed in an
    unrecognised way, or was cancelled), the file keeps `desired` so the next
    run completes or confirms it.

    The server refuses a schema change while any non-main branch lives.
    `on_blocked_by_branches` is called each time (to reap abandoned scratch
    branches) and the plan is retried with backoff for a bounded time.
    """
    config_dir = pathlib.Path(cluster.config_dir)
    path = config_dir / cluster.schema_file
    async with _config_lock(config_dir):
        current = await read_served()
        if current == desired:
            return  # another process deployed the same change meanwhile
        if current != served:
            raise SchemaDeployError(
                f"graph {graph_id!r}'s schema changed while this sync prepared its "
                f"change; the next run re-reads it and retries"
            )
        before = path.read_text(encoding="utf-8") if path.exists() else ""
        if before not in (served, desired):
            raise SchemaDeployError(
                f"{path} differs from the schema graph {graph_id!r} serves. Apply "
                f"or revert those edits before CocoIndex changes the schema."
            )
        path.write_text(desired, encoding="utf-8")
        submission = _Submission()
        try:
            applied = await _plan_and_apply(
                cluster,
                graph_id=graph_id,
                allowed_types=allowed_types,
                on_blocked_by_branches=on_blocked_by_branches,
                submission=submission,
            )
        except BaseException:
            if not submission.pending:
                path.write_text(before, encoding="utf-8")
            raise
        if not applied:
            # Nothing to deploy: the change is textual only (formatting).
            path.write_text(before, encoding="utf-8")
            return
    now = await read_served()
    if now != desired:
        raise SchemaDeployError(
            f"the deployment completed but graph {graph_id!r} serves a different "
            f"schema than the one deployed"
        )


async def _plan_and_apply(
    cluster: ClusterConfig,
    *,
    graph_id: str,
    allowed_types: Collection[str],
    on_blocked_by_branches: Callable[[], Awaitable[None]],
    submission: _Submission,
) -> bool:
    """Returns True once the deployment is active, False when the plan has
    nothing to deploy. Raises `_OutcomeUnknown` with the deployment's id when
    its outcome is unknown; `submission.pending` says so for cancellation."""
    config = os.fspath(cluster.config_dir)
    common = ["--target", cluster.target, "--config", config, "--json"]
    deadline = time.monotonic() + _BLOCKED_WAIT_SECONDS
    delay = 0.25
    last = ""
    while True:
        plan = await _run_cli(cluster, "cluster", "plan", *common)
        retry = plan.operator_lock_busy
        last = _error_text(plan)
        if not retry:
            if not isinstance(plan.body, dict):
                raise SchemaDeployError(f"cluster plan failed: {_error_text(plan)}")
            _check_plan(plan.body, graph_id=graph_id, allowed_types=allowed_types)
            reasons = _blocked_reasons(plan.body)
            retry = reasons == {"schema_preflight_failed"}
        if not retry:
            if reasons or not plan.body.get("ok", False):
                raise SchemaDeployError(f"cluster plan refused: {_error_text(plan)}")
            if not plan.body.get("changes"):
                return False
            submission.pending = True
            applied = await _run_cli(
                cluster,
                "cluster",
                "apply",
                *common,
                "--timeout",
                str(_APPLY_TIMEOUT_SECONDS),
            )
            if applied.returncode == 0:
                return True
            text = last = _error_text(applied)
            # Refusals that record nothing and clear on their own: another CLI
            # holding the operator configuration, live branches, or another
            # deployment in progress.
            retry = (
                applied.operator_lock_busy
                or "schema_preflight_failed" in text
                or "already running" in text
            )
            if retry or applied.refused_without_effect:
                submission.pending = False
            if not retry:
                if applied.refused_without_effect:
                    raise SchemaDeployError(f"cluster apply refused: {text}")
                raise _OutcomeUnknown(
                    f"schema deployment {applied.deployment_id} did not finish "
                    f"cleanly ({text}); its outcome is unknown. Check `omnigraph "
                    f"cluster status --deployment-id {applied.deployment_id}` before "
                    f"changing the schema again."
                )
        await on_blocked_by_branches()
        if time.monotonic() + delay > deadline:
            raise SchemaDeployError(
                f"graph {graph_id!r} kept refusing the schema deployment for "
                f"{_BLOCKED_WAIT_SECONDS:.0f}s: schema changes need a graph with only "
                f"`main`, or no other deployment in progress. "
                f"Last answer: {last}"
            )
        await asyncio.sleep(delay)
        delay = min(delay * 2, 5.0)
