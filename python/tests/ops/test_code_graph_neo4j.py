"""Real-server validation of the example; optional dependencies are required."""

import importlib.util
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

pytest.importorskip("neo4j")
pytest.importorskip("testcontainers.neo4j")

import cocoindex as coco
from cocoindex.connectors import neo4j
from tests import common

_IMAGE = "neo4j:5-community"
# This exercises a real JVM startup plus index creation, updates and full drop.
# A healthy local run takes ~35 seconds, exceeding the 30-second unit default.
pytestmark = [pytest.mark.requires_docker(_IMAGE), pytest.mark.timeout(120)]


@pytest.fixture(scope="module")
def neo4j_url() -> Any:
    from testcontainers.neo4j import Neo4jContainer

    with Neo4jContainer(_IMAGE, username="neo4j", password="password") as container:
        yield container.get_connection_url()


@pytest_asyncio.fixture
async def graph_env(neo4j_url: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    example_dir = Path(__file__).parents[3] / "examples" / "code_graph_neo4j"
    monkeypatch.syspath_prepend(str(example_dir))
    spec = importlib.util.spec_from_file_location(
        "code_graph_example_test", example_dir / "main.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    factory = neo4j.ConnectionFactory(uri=neo4j_url, auth=("neo4j", "password"))
    env = common.create_test_env(__file__)
    env.context_provider.provide(module.DB, factory)
    try:
        yield module, factory, env
    finally:
        await factory.close()


@pytest.mark.asyncio
async def test_neo4j_update_delete_and_drop(tmp_path: Path, graph_env: Any) -> None:
    module, factory, env = graph_env
    file = tmp_path / "sample.cpp"
    file.write_text(
        "struct Base {}; struct C : Base { int f() { return missing(); } };"
    )
    app = coco.App(
        coco.AppConfig(name="code_graph_neo4j", environment=env),
        module.app_main,
        sourcedir=tmp_path,
    )
    await app.update()
    graph = await factory.acquire()
    rows = await graph.query("MATCH (e:CppEntity) RETURN e.name AS name, e.id AS id")
    before = {r["name"]: r["id"] for r in rows}
    assert set(before) == {"Base", "C", "C::f"}
    refs = await graph.query(
        "MATCH (e)-[r:CPP_CALLS_SPELLING|CPP_INHERITS_SPELLING]->(s:CppReference) RETURN s.spelling AS spelling, s.resolution AS resolution"
    )
    assert {r["spelling"] for r in refs} == {"Base", "missing"}
    assert all(r["resolution"] == "unresolved" for r in refs)
    assert await graph.query("MATCH ()-[r:CPP_CONTAINS]->() RETURN count(r) AS n") == [
        {"n": 1}
    ]
    await app.update()
    file.write_text(
        "struct Base {}; struct C : Base { int f() { return changed(12); } };"
    )
    await app.update()
    rows = await graph.query("MATCH (e:CppEntity) RETURN e.name AS name, e.id AS id")
    assert {r["name"]: r["id"] for r in rows} == before
    assert await graph.query(
        "MATCH (r:CppReference) WHERE r.spelling = 'missing' RETURN count(r) AS n"
    ) == [{"n": 0}]
    file.unlink()
    await app.update()
    assert await graph.query("MATCH (e:CppEntity) RETURN count(e) AS n") == [{"n": 0}]
    assert await graph.query("MATCH (r:CppReference) RETURN count(r) AS n") == [
        {"n": 0}
    ]
    await app.drop()
