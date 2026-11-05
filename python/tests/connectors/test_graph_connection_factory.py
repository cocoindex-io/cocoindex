"""Per-event-loop connection caching of the graph connectors' ``ConnectionFactory``
(neo4j, falkordb, surrealdb, omnigraph), with fake drivers and no server."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any

import pytest

from cocoindex._internal.memo_fingerprint import memo_fingerprint


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


def _omnigraph(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[Any], Any]]:
    sdk = pytest.importorskip("omnigraph")
    from cocoindex.connectors import omnigraph

    monkeypatch.setattr(sdk, "Omnigraph", _FakeConnection)
    factory = omnigraph.ConnectionFactory("http://fake", graph_id="g")
    return factory, lambda client: client


_FACTORIES = pytest.mark.parametrize(
    "make_factory",
    [_neo4j, _falkordb, _surrealdb, _omnigraph],
    ids=lambda f: f.__name__[1:],
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
        r.getMessage() for r in caplog.records if r.name.startswith("cocoindex.")
    ]
    assert len(warnings) == 1
    assert "1 " in warnings[0] and "other event loops" in warnings[0]


@_FACTORIES
def test_closed_loops_are_dropped(
    make_factory: Callable[..., tuple[Any, Callable[[Any], Any]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A factory outliving its event loops (one per ``asyncio.run``, per test)
    drops the entries of closed loops, so they neither accumulate nor count
    as connections left open."""
    factory, _ = make_factory(monkeypatch)
    # Kept referenced, as a real driver would keep its loop referenced.
    loops = [asyncio.new_event_loop() for _ in range(3)]
    entries = factory._connections._entries
    for loop in loops:
        loop.run_until_complete(factory.acquire())
        loop.close()
        # Each acquire() dropped the previous loop's entry, closed by then.
        assert list(entries) == [loop]

    with caplog.at_level(logging.WARNING):
        asyncio.run(factory.close())
    assert not entries
    assert not [r for r in caplog.records if r.name.startswith("cocoindex.")]


def _neo4j_factories() -> tuple[Any, Any, Any]:
    pytest.importorskip("neo4j")
    from cocoindex.connectors import neo4j

    return (
        neo4j.ConnectionFactory("bolt://h:7687", auth=("u", "p1")),
        neo4j.ConnectionFactory("bolt://h:7687", auth=("u", "p2")),
        neo4j.ConnectionFactory("bolt://h:7687", database="other"),
    )


def _falkordb_factories() -> tuple[Any, Any, Any]:
    pytest.importorskip("falkordb")
    from cocoindex.connectors import falkordb

    return (
        falkordb.ConnectionFactory("falkor://u:p1@h:6379", graph="g"),
        falkordb.ConnectionFactory("falkor://u:p2@h:6379?password=x", graph="g"),
        falkordb.ConnectionFactory("falkor://u:p1@h:6379", graph="g2"),
    )


def _surrealdb_factories() -> tuple[Any, Any, Any]:
    pytest.importorskip("surrealdb")
    from cocoindex.connectors import surrealdb

    def factory(password: str, namespace: str = "n", database: str = "d") -> Any:
        return surrealdb.ConnectionFactory(
            "ws://h:8000/rpc",
            namespace=namespace,
            database=database,
            credentials={"username": "u", "password": password},
        )

    return (factory("p1"), factory("p2"), factory("p1", "n/d", "x"))


def _omnigraph_factories() -> tuple[Any, Any, Any]:
    pytest.importorskip("omnigraph")
    from cocoindex.connectors import omnigraph

    return (
        omnigraph.ConnectionFactory("https://h", graph_id="g", token="t1"),
        omnigraph.ConnectionFactory("https://h", graph_id="g", token="t2"),
        omnigraph.ConnectionFactory("https://h", graph_id="g2", token="t1"),
    )


@pytest.mark.parametrize(
    "factories",
    [_neo4j_factories, _falkordb_factories, _surrealdb_factories, _omnigraph_factories],
    ids=lambda f: f.__name__[1:].removesuffix("_factories"),
)
def test_factory_is_memo_keyable_without_its_credentials(
    factories: Callable[[], tuple[Any, Any, Any]],
) -> None:
    """A factory can be a memo key (e.g. a `detect_change=True` context value):
    its fingerprint follows where the data lives, not the credentials (also
    when the URI carries them) or the open connections."""
    same, other_credentials, other_place = factories()
    fp = memo_fingerprint(same)
    assert memo_fingerprint(other_credentials) == fp
    assert memo_fingerprint(other_place) != fp
