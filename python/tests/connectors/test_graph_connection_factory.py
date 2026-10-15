"""Per-event-loop connection caching of the graph connectors' ``ConnectionFactory``
(neo4j, falkordb, surrealdb), with fake drivers and no server."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any

import pytest


class _FakeConnection:
    """Stands in for a Neo4j driver, a FalkorDB client and a SurrealDB
    connection alike."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.closed = False

    # FalkorDB client
    @classmethod
    def from_url(cls, uri: str) -> _FakeConnection:
        return cls()

    def select_graph(self, name: str) -> _FakeConnection:
        return self

    async def aclose(self) -> None:
        self.closed = True

    # SurrealDB connection
    async def connect(self) -> None:
        pass

    async def signin(self, credentials: dict[str, str]) -> None:
        pass

    async def use(self, namespace: str, database: str) -> None:
        pass

    # Neo4j driver, SurrealDB connection
    async def close(self) -> None:
        self.closed = True


def _neo4j(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[Any], Any]]:
    neo4j_driver = pytest.importorskip("neo4j")
    from cocoindex.connectors import neo4j

    monkeypatch.setattr(
        neo4j_driver.AsyncGraphDatabase, "driver", staticmethod(_FakeConnection)
    )
    return neo4j.ConnectionFactory("bolt://fake"), lambda handle: handle._driver


def _falkordb(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[Any], Any]]:
    pytest.importorskip("falkordb")
    from cocoindex.connectors import falkordb
    from cocoindex.connectors.falkordb import _target

    monkeypatch.setattr(_target, "AsyncFalkorDB", _FakeConnection)
    return falkordb.ConnectionFactory("falkor://fake"), lambda graph: graph


def _surrealdb(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[Any], Any]]:
    pytest.importorskip("surrealdb")
    from cocoindex.connectors import surrealdb
    from cocoindex.connectors.surrealdb import _target

    monkeypatch.setattr(_target, "AsyncSurreal", _FakeConnection)
    factory = surrealdb.ConnectionFactory("ws://fake", namespace="n", database="d")
    return factory, lambda conn: conn


_FACTORIES = pytest.mark.parametrize(
    "make_factory", [_neo4j, _falkordb, _surrealdb], ids=lambda f: f.__name__[1:]
)


@_FACTORIES
def test_each_event_loop_gets_its_own_connection(
    make_factory: Callable[..., tuple[Any, Callable[[Any], Any]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One factory used from two event loops on two threads: each loop gets
    its own connection, reused across its acquires; neither is dropped by the
    other; ``close()`` closes only the running loop's, warning about the
    other."""
    factory, connection_of = make_factory(monkeypatch)
    both_acquired = threading.Barrier(2)
    may_close = [threading.Event(), threading.Event()]
    acquired: list[list[_FakeConnection]] = [[], []]

    async def use_factory(i: int) -> None:
        acquired[i].append(connection_of(await factory.acquire()))
        both_acquired.wait()
        acquired[i].append(connection_of(await factory.acquire()))
        may_close[i].wait()
        await factory.close()

    threads = [
        threading.Thread(target=asyncio.run, args=(use_factory(i),)) for i in (0, 1)
    ]
    for t in threads:
        t.start()
    with caplog.at_level(logging.WARNING):
        may_close[0].set()
        threads[0].join()
        may_close[1].set()
        threads[1].join()

    (first_0, again_0), (first_1, again_1) = acquired
    assert first_0 is again_0
    assert first_1 is again_1
    assert first_0 is not first_1
    assert first_0.closed and first_1.closed
    # The first close() left the other loop's connection open and said so;
    # the second found nothing else to warn about.
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name.startswith("cocoindex.connectors.")
    ]
    assert len(warnings) == 1
    assert "1 " in warnings[0] and "other event loops" in warnings[0]
