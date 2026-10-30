"""Transport for the Omnigraph connector: the official `omnigraph` SDK over HTTP.

`_target.py` reaches the server only through `_GraphClient`, which turns SDK
receipts into the few values reconciliation needs and lets SDK errors through
unchanged. Schema writes go through `_cluster.py` instead: the server accepts
them only as a cluster deployment.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Collection
from typing import Any, TypeVar

from cocoindex._internal.extras import install_hint

try:
    import omnigraph
except ImportError as e:  # pragma: no cover - depends on the environment
    raise ImportError(
        "The Omnigraph connector needs the `omnigraph` Python SDK. "
        + install_hint("omnigraph")
    ) from e

from cocoindex.connectorkits._per_loop_connections import PerLoopConnections
from cocoindex.connectors.omnigraph._cluster import (
    ClusterConfig,
    SchemaDeployError,
    deploy_schema,
)
from cocoindex.connectors.omnigraph._gq import Query

_T = TypeVar("_T")

#: Attempts for a read the server refused without effect because it was
#: busy or unavailable. Writes are never retried.
_READ_ATTEMPTS = 3
_READ_BACKOFF_SECONDS = 0.5


def _transient(error: omnigraph.OmnigraphError) -> bool:
    """A refusal that ran nothing and may clear on its own: 429/503 with
    `effects: none`, or a connection that was never established."""
    if isinstance(error, omnigraph.NetworkError):
        return not error.request_dispatched
    disposition = error.disposition
    effects = disposition["effects"] if disposition else None
    return error.status in (429, 503) and effects == "none"


async def _read(call: Callable[[], Awaitable[_T]]) -> _T:
    for attempt in range(_READ_ATTEMPTS):
        try:
            return await call()
        except omnigraph.OmnigraphError as e:
            if attempt + 1 == _READ_ATTEMPTS or not _transient(e):
                raise
            await asyncio.sleep(_READ_BACKOFF_SECONDS * 2**attempt)
    raise AssertionError("unreachable")


class ConnectionFactory:
    """Identifies an `omnigraph-server` graph and the branch to write to.

    Provided once in the app lifespan and resolved at action time through the
    ContextProvider. Keeps one SDK client per event loop, shared by every
    sink on that loop; call :meth:`close` at shutdown.

    ``base_url`` is the server root (including any proxy prefix). ``token``
    falls back to ``OMNIGRAPH_TOKEN``. ``cluster`` is required for
    ``managed_by="system"`` types: the server takes schema changes only as a
    cluster deployment, which the connector submits through the ``omnigraph``
    CLI (see :class:`ClusterConfig`).

    Example::

        factory = omnigraph.ConnectionFactory(
            "https://graph.example.com",
            graph_id="knowledge",
            cluster=omnigraph.ClusterConfig(config_dir="./cluster", target="prod"),
        )
        builder.provide(OMNIGRAPH_DB, factory)
    """

    def __init__(
        self,
        base_url: str,
        *,
        graph_id: str,
        branch: str = "main",
        token: str | None = None,
        cluster: ClusterConfig | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._graph_id = graph_id
        self._branch = branch
        self._token = token if token is not None else os.environ.get("OMNIGRAPH_TOKEN")
        self._cluster = cluster
        self._connections: PerLoopConnections[omnigraph.Omnigraph] = PerLoopConnections(
            self._open, _close, owner="Omnigraph ConnectionFactory"
        )

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def graph_id(self) -> str:
        return self._graph_id

    @property
    def branch(self) -> str:
        return self._branch

    @property
    def cluster(self) -> ClusterConfig | None:
        return self._cluster

    def __coco_memo_key__(self) -> tuple[str, str, str]:
        # Where the data lives, not how to sign in.
        return (self._base_url, self._graph_id, self._branch)

    def __repr__(self) -> str:
        return (
            f"ConnectionFactory(base_url={self._base_url!r}, "
            f"graph_id={self._graph_id!r}, branch={self._branch!r})"
        )

    async def _open(self) -> omnigraph.Omnigraph:
        return omnigraph.Omnigraph(
            self._base_url, graph_id=self._graph_id, token=self._token
        )

    async def acquire(self) -> omnigraph.Omnigraph:
        """The running event loop's SDK client, created on its first call."""
        return await self._connections.acquire()

    async def close(self) -> None:
        """Close the running event loop's client."""
        await self._connections.close()


async def _close(client: omnigraph.Omnigraph) -> None:
    await client.aclose()


class _GraphClient:
    """The operations reconciliation performs on one graph."""

    def __init__(self, conn: ConnectionFactory) -> None:
        self._conn = conn

    @property
    def conn(self) -> ConnectionFactory:
        return self._conn

    async def read_schema(self) -> str:
        """The graph's current, complete `.pg` source as served."""
        og = await self._conn.acquire()
        return (await _read(og.schema.get))["schema_source"]

    async def mutate(self, mutation: Query, *, branch: str) -> None:
        og = await self._conn.acquire()
        await og.mutate(mutation.expr, params=mutation.params, branch=branch)

    async def query(self, query: Query, *, branch: str) -> list[dict[str, Any]]:
        """Run a read and return its rows, keyed by the `return` aliases.
        Null-valued fields are absent from a row."""
        og = await self._conn.acquire()
        result = await _read(
            lambda: og.query(query.expr, params=query.params, branch=branch)
        )
        rows: list[dict[str, Any]] = result["rows"]
        return rows

    async def branch_create(self, name: str, *, frm: str) -> None:
        og = await self._conn.acquire()
        await og.branches.create(name, from_=frm)

    async def branch_merge(self, name: str, *, into: str) -> None:
        """Merge `name` into `into` and delete it in the same call.

        A merge that landed but whose source deletion failed raises: the
        branch is still there, and a leftover branch blocks schema
        deployments on the graph.
        """
        og = await self._conn.acquire()
        result = await og.branches.merge(name, target=into, delete_branch=True)
        if not result.get("branch_deleted"):
            details = result.get("branch_delete_error_details")
            reason = details["error"] if details else "no reason given"
            raise RuntimeError(
                f"merged scratch branch {name!r} into {into!r} but could not delete "
                f"it: {reason}"
            )

    async def branch_delete(self, name: str) -> None:
        og = await self._conn.acquire()
        await og.branches.delete(name)

    async def branch_list(self) -> list[str]:
        og = await self._conn.acquire()
        return await _read(og.branches.list)

    async def apply_schema(
        self,
        schema_pg: str,
        *,
        served: str,
        types: Collection[str],
        on_blocked_by_branches: Callable[[], Awaitable[None]],
    ) -> None:
        """Deploy `schema_pg` as the graph's complete schema (see
        `_cluster.deploy_schema`). `types` are the type names this write may
        change; `served` is the schema it was computed from."""
        conn = self._conn
        if conn.cluster is None:
            raise SchemaDeployError(
                f"graph {conn.graph_id!r} at {conn.base_url}: a managed_by='system' "
                f"type needs a schema deployment, which needs "
                f"ConnectionFactory(cluster=ClusterConfig(...)); declare the type "
                f"with managed_by='user' to manage its schema yourself"
            )
        await deploy_schema(
            conn.cluster,
            graph_id=conn.graph_id,
            served=served,
            desired=schema_pg,
            allowed_types=types,
            read_served=self.read_schema,
            on_blocked_by_branches=on_blocked_by_branches,
        )
