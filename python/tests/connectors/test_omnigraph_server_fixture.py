"""Exit guardrails for the served-Omnigraph fixture (SPEC Phase B1)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

pytest.importorskip("omnigraph", reason="the omnigraph SDK is not installed")

from .omnigraph_server_fixture import (
    DATA,
    DEFAULT_SCHEMA,
    HTTP_API_CONTRACT,
    HTTP_API_HEADER,
    OmnigraphServer,
    live_server,
)

pytestmark = [live_server]


@pytest.fixture(autouse=True)
def _omnigraph_home(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """The connector runs the omnigraph CLI with the caller's environment,
    and the CLI keeps authentication state in `OMNIGRAPH_HOME`; point it at
    a fresh directory so tests never write into the developer's own."""
    home = tmp_path_factory.mktemp("omnigraph-home")
    monkeypatch.setenv("OMNIGRAPH_HOME", str(home))
    return home


@pytest.fixture(scope="module")
def server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[OmnigraphServer]:
    s = OmnigraphServer.start(tmp_path_factory.mktemp("omnigraph-server"))
    yield s
    s.stop()


def test_boots_ready_and_serves_the_bundle_schema(server: OmnigraphServer) -> None:
    ready = server.request("GET", "/readyz", actor=None, contract=False)
    assert ready.status == 200
    assert ready.body["ready"] is True
    assert ready.body["ready_graph_count"] == 1
    assert server.schema_source() == DEFAULT_SCHEMA


def test_every_response_carries_the_contract_header(server: OmnigraphServer) -> None:
    ok = server.request("GET", f"/graphs/{server.graph_id}/schema")
    missing = server.request("GET", f"/graphs/{server.graph_id}/schema", contract=False)
    assert ok.headers.get(HTTP_API_HEADER) == HTTP_API_CONTRACT
    assert missing.status == 400
    assert missing.body["code"] == "api_contract_mismatch"
    assert missing.headers.get(HTTP_API_HEADER) == HTTP_API_CONTRACT


def test_schema_deploys_live_without_a_restart(server: OmnigraphServer) -> None:
    pid = server.pid
    schema = DEFAULT_SCHEMA.replace("}\n", "  note: String?\n}\n")
    server.deploy(schema)
    assert server.schema_source() == schema
    assert server.pid == pid


def test_the_data_actor_writes_but_cannot_deploy(server: OmnigraphServer) -> None:
    w = server.request(
        "POST",
        f"/graphs/{server.graph_id}/mutate",
        {"query": 'query a() { insert FixtureMarker { name: "from-data" } }'},
        actor=DATA,
    )
    assert w.status == 200, w
    denied = server.deploy(server.schema_source(), actor=DATA, check=False)
    assert denied.returncode != 0
    assert "config_manage" in denied.stdout + denied.stderr


def test_data_survives_a_restart(server: OmnigraphServer) -> None:
    server.request(
        "POST",
        f"/graphs/{server.graph_id}/mutate",
        {"query": 'query a() { insert FixtureMarker { name: "durable" } }'},
    )
    server.restart()
    r = server.request(
        "POST",
        f"/graphs/{server.graph_id}/query",
        {
            "query": 'query q() { match { $m: FixtureMarker { name: "durable" } } return { $m.name as n } }'
        },
    )
    assert r.status == 200, r
    assert r.body["rows"] == [{"n": "durable"}]


@pytest.mark.parametrize("attempt", [1, 2])
def test_boots_from_a_clean_state_repeatedly(tmp_path: Path, attempt: int) -> None:
    s = OmnigraphServer.start(tmp_path / f"run{attempt}")
    try:
        assert s.schema_source() == DEFAULT_SCHEMA
        assert s.request("GET", f"/graphs/{s.graph_id}/branches").body["branches"] == [
            "main"
        ]
    finally:
        s.stop()
