"""A served Omnigraph 0.14 cluster for live tests (SPEC Epic B, Phase B1).

`OmnigraphServer.start()` bootstraps a one-graph cluster on a local `file://`
root, hands its admission lock over, and boots `omnigraph-server` with bearer
tokens and a policy bundle for two actors:

- `OPERATOR` may read, write, branch, publish, apply schema, and manage cluster
  configuration (deployments);
- `DATA` holds only the data-plane minimum from probe P10;
- `SCHEMA` holds only what a schema deployment needs (`config_manage` on the
  cluster, `read` and `schema_apply` on the graphs).

The 0.14 CLI refuses plain HTTP for an authenticated server target, and the
server has no TLS option, so the fixture also runs a TLS-terminating proxy with
a throwaway CA. The data plane is plain HTTP (`base_url`); the control plane
CLI goes through the proxy (`tls_url`) via a generated operator config.

The root is `file://` rather than rustfs: the 0.14 CLI applies directly to
local storage only ("inline access supports local storage only"), and a server
booted on an empty `s3://` root refuses with `cluster_state_missing`.

The binaries are expected at `test/bin/v0.14.0/` (git-ignored). Tests using
this module are gated on `OMNIGRAPH_TEST_SERVER=1` and must run with the
Claude Code sandbox disabled (the server cannot bind a port inside it).
"""

from __future__ import annotations

import dataclasses
import http.client
import http.server
import json
import os
import re
import selectors
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any

import pytest
from cocoindex.connectors.omnigraph import ClusterConfig, ConnectionFactory

BIN_DIR = Path(__file__).resolve().parents[3] / "test" / "bin" / "v0.14.0"
CLI = BIN_DIR / "omnigraph"
SERVER = BIN_DIR / "omnigraph-server"

HTTP_API_HEADER = "Omnigraph-Http-Api"
HTTP_API_CONTRACT = "0.14"

GRAPH_ID = "g"
OPERATOR = "act-op"
DATA = "act-data"
SCHEMA = "act-schema"
DEFAULT_SCHEMA = "node FixtureMarker {\n  name: String @key\n}\n"


live_server = pytest.mark.skipif(
    os.environ.get("OMNIGRAPH_TEST_SERVER") != "1",
    reason="requires omnigraph-server 0.14.0 at test/bin/v0.14.0; set OMNIGRAPH_TEST_SERVER=1",
)

_GRAPH_POLICY = f"""\
version: 1
groups:
  ops: [{OPERATOR}]
  data: [{DATA}]
  schema: [{SCHEMA}]
rules:
  - id: ops-data
    allow:
      actors: {{ group: ops }}
      actions: [read, change, export]
      branch_scope: any
  - id: ops-branches
    allow:
      actors: {{ group: ops }}
      actions: [branch_create, branch_delete, branch_merge]
      target_branch_scope: any
  - id: ops-commits
    allow:
      actors: {{ group: ops }}
      actions: [commit_publish, commit_drop]
      target_branch_scope: any
  - id: ops-schema
    allow:
      actors: {{ group: ops }}
      actions: [schema_apply]
  - id: data-rw
    allow:
      actors: {{ group: data }}
      actions: [read, change]
      branch_scope: any
  - id: data-branches
    allow:
      actors: {{ group: data }}
      actions: [branch_create, branch_delete, branch_merge]
      target_branch_scope: any
  - id: schema-read
    allow:
      actors: {{ group: schema }}
      actions: [read]
      branch_scope: any
  - id: schema-apply
    allow:
      actors: {{ group: schema }}
      actions: [schema_apply]
"""

_CLUSTER_POLICY = f"""\
version: 1
groups:
  ops: [{OPERATOR}, {SCHEMA}]
rules:
  - id: ops-config
    allow:
      actors: {{ group: ops }}
      actions: [config_manage]
"""


def _cluster_yaml(graph_ids: tuple[str, ...]) -> str:
    graphs = "".join(f"  {gid}:\n    schema: {gid}.pg\n" for gid in graph_ids)
    return (
        "version: 1\nmetadata:\n  name: coco-test\ngraphs:\n" + graphs + "policies:\n"
        "  graph-access:\n    file: graph.policy.yaml\n"
        f"    applies_to: [{', '.join(graph_ids)}]\n"
        "  cluster-access:\n    file: cluster.policy.yaml\n    applies_to: [cluster]\n"
    )


_LOCK_ID_RE = re.compile(r"Admission lock: (\S+);")
#: The operator-config target each actor's CLI calls go through.
_TARGETS = {OPERATOR: "srv", DATA: "srv-data", SCHEMA: "srv-schema"}


@dataclasses.dataclass(frozen=True)
class Response:
    status: int
    headers: Message
    body: Any


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _make_tls(directory: Path) -> tuple[Path, Path, Path]:
    """A throwaway CA and a 127.0.0.1 leaf it signs, via the `openssl` CLI."""

    def openssl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    ca_key, ca_pem = directory / "ca.key", directory / "ca.pem"
    key, csr, pem = (
        directory / "leaf.key",
        directory / "leaf.csr",
        directory / "leaf.pem",
    )
    ext = directory / "leaf.ext"
    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-keyout", str(ca_key), "-out", str(ca_pem), "-subj", "/CN=coco-test-ca",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    )  # fmt: skip
    openssl(
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(csr), "-subj", "/CN=127.0.0.1",
    )  # fmt: skip
    ext.write_text(
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=IP:127.0.0.1\n"
    )
    openssl(
        "x509", "-req", "-in", str(csr), "-CA", str(ca_pem), "-CAkey", str(ca_key),
        "-CAcreateserial", "-out", str(pem), "-days", "2", "-extfile", str(ext),
    )  # fmt: skip
    return ca_pem, pem, key


class _TlsProxy:
    """Terminates TLS on `port` and forwards bytes to `upstream_port`."""

    def __init__(self, cert: Path, key: Path, upstream_port: int) -> None:
        self._ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._ctx.load_cert_chain(str(cert), str(key))
        self._upstream = upstream_port
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = int(self._listener.getsockname()[1])
        self._closed = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while not self._closed.is_set():
            try:
                raw, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(raw,), daemon=True).start()

    def _serve(self, raw: socket.socket) -> None:
        try:
            client = self._ctx.wrap_socket(raw, server_side=True)
            upstream = socket.create_connection(("127.0.0.1", self._upstream))
        except (OSError, ssl.SSLError):
            raw.close()
            return
        sel = selectors.DefaultSelector()
        sel.register(client, selectors.EVENT_READ, upstream)
        sel.register(upstream, selectors.EVENT_READ, client)
        try:
            while not self._closed.is_set():
                for key, _ in sel.select(timeout=0.5):
                    src: socket.socket = key.fileobj  # type: ignore[assignment]
                    data = src.recv(65536)
                    if not data:
                        return
                    key.data.sendall(data)
        except (OSError, ssl.SSLError):
            return
        finally:
            sel.close()
            client.close()
            upstream.close()

    def close(self) -> None:
        self._closed.set()
        self._listener.close()


class LossyProxy:
    """A plain-HTTP proxy to the server that, once armed, forwards the next
    request whose path ends with the armed suffix and then closes the
    connection without answering: the write lands, its response is lost."""

    def __init__(self, upstream: str) -> None:
        target = urllib.parse.urlsplit(upstream)
        self._host, self._port = target.hostname or "127.0.0.1", target.port or 80
        self._armed: str | None = None
        self._lock = threading.Lock()
        self.dropped: list[str] = []
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                pass

            def _forward(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                headers = {k: v for k, v in self.headers.items() if k.lower() != "host"}
                conn = http.client.HTTPConnection(proxy._host, proxy._port, timeout=60)
                conn.request(self.command, self.path, body=body, headers=headers)
                upstream = conn.getresponse()
                data = upstream.read()
                with proxy._lock:
                    suffix = proxy._armed
                    drop = suffix is not None and self.path.split("?")[0].endswith(
                        suffix
                    )
                    if drop:
                        proxy._armed = None
                        proxy.dropped.append(self.path)
                conn.close()
                if drop:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.send_response(upstream.status)
                for k, v in upstream.getheaders():
                    if k.lower() not in (
                        "transfer-encoding",
                        "connection",
                        "content-length",
                    ):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _forward

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def drop_next(self, path_suffix: str) -> None:
        with self._lock:
            self._armed = path_suffix

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@dataclasses.dataclass
class OmnigraphServer:
    """A booted cluster. Use `start()`; call `stop()` when done."""

    workdir: Path
    tokens: dict[str, str]
    port: int = 0
    tls_port: int = 0
    _process: subprocess.Popen[bytes] | None = None
    _proxy: _TlsProxy | None = None

    graph_id: str = GRAPH_ID
    graph_ids: tuple[str, ...] = (GRAPH_ID,)

    @property
    def config_dir(self) -> Path:
        return self.workdir / "bundle"

    @property
    def root_uri(self) -> str:
        return f"file://{self.workdir / 'root'}"

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def tls_url(self) -> str:
        return f"https://127.0.0.1:{self.tls_port}"

    @property
    def operator_config(self) -> Path:
        return self.workdir / "operator.yaml"

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def start(
        cls,
        workdir: Path,
        schema_pg: str = DEFAULT_SCHEMA,
        *,
        graph_ids: tuple[str, ...] = (GRAPH_ID,),
    ) -> OmnigraphServer:
        """Every graph in `graph_ids` starts with `schema_pg`; the first is
        the default `graph_id`."""
        workdir.mkdir(parents=True, exist_ok=True)
        server = cls(
            workdir=workdir,
            tokens={actor: f"{actor}-{os.urandom(8).hex()}" for actor in _TARGETS},
            graph_id=graph_ids[0],
            graph_ids=graph_ids,
        )
        server._write_bundle(schema_pg)
        server._bootstrap()
        server.port = _free_port()
        server._boot()
        ca, cert, key = _make_tls(workdir)
        server._proxy = _TlsProxy(cert, key, server.port)
        server.tls_port = server._proxy.port
        server._write_operator_config(ca)
        return server

    def restart(self) -> None:
        """Stop and re-boot the server on the same root and port."""
        self._terminate()
        self._boot()

    def stop(self) -> None:
        self._terminate()
        if self._proxy is not None:
            self._proxy.close()
            self._proxy = None

    def _write_bundle(self, schema_pg: str) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        (self.config_dir / "cluster.yaml").write_text(_cluster_yaml(self.graph_ids))
        (self.config_dir / "graph.policy.yaml").write_text(_GRAPH_POLICY)
        (self.config_dir / "cluster.policy.yaml").write_text(_CLUSTER_POLICY)
        for gid in self.graph_ids:
            (self.config_dir / f"{gid}.pg").write_text(schema_pg)

    def _bootstrap(self) -> None:
        (self.workdir / "root").mkdir(exist_ok=True)
        r = self._run_cli(
            "cluster", "apply", "--target", self.root_uri,
            "--config", str(self.config_dir), "--as", OPERATOR, "--json",
        )  # fmt: skip
        match = _LOCK_ID_RE.search(r.stderr)
        if match is None:
            raise RuntimeError(f"bootstrap printed no admission lock:\n{r.stderr}")
        self._run_cli(
            "--target", self.root_uri, "cluster", "force-unlock", match.group(1)
        )

    def _boot(self) -> None:
        env = {
            **os.environ,
            "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON": json.dumps(self.tokens),
        }
        log = (self.workdir / "server.log").open("ab")
        self._process = subprocess.Popen(
            [
                str(SERVER),
                "--cluster",
                self.root_uri,
                "--bind",
                f"127.0.0.1:{self.port}",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"omnigraph-server exited; see {self.workdir / 'server.log'}"
                )
            try:
                with urllib.request.urlopen(
                    f"{self.base_url}/readyz", timeout=1
                ) as resp:
                    if json.loads(resp.read()).get("ready"):
                        return
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        raise TimeoutError(
            f"omnigraph-server not ready; see {self.workdir / 'server.log'}"
        )

    def _terminate(self) -> None:
        if self._process is not None:
            self._process.terminate()
            try:
                self._process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            self._process = None

    def _write_operator_config(self, ca: Path) -> None:
        targets = []
        for actor, target in _TARGETS.items():
            token_file = self.workdir / f"{target}.token"
            token_file.write_text(self.tokens[actor])
            targets.append(
                f"  {target}:\n"
                "    kind: server\n"
                f"    endpoint: {self.tls_url}\n"
                f"    default_graph: {GRAPH_ID}\n"
                f"    auth: {{provider: token, source: {{file: '{token_file}'}}}}\n"
                "    transport:\n"
                "      tls:\n"
                f"        ca: {{mode: replace, file: '{ca}'}}\n"
            )
        self.operator_config.write_text("version: 1\ntargets:\n" + "".join(targets))

    # -- control plane -----------------------------------------------------

    def _run_cli(
        self, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        # A private OMNIGRAPH_HOME per call: concurrent CLIs sharing one home
        # fail on the operator-config lock (probe P20).
        home = tempfile.mkdtemp(prefix="oghome-", dir=self.workdir)
        env = {**os.environ, "OMNIGRAPH_HOME": home}
        try:
            r = subprocess.run(
                [str(CLI), *args], capture_output=True, text=True, env=env, check=False
            )
        finally:
            shutil.rmtree(home, ignore_errors=True)
        if check and r.returncode != 0:
            raise RuntimeError(
                f"omnigraph {' '.join(args)} failed ({r.returncode}):\n{r.stdout}\n{r.stderr}"
            )
        return r

    def cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Run the CLI with this cluster's operator configuration; pick the
        actor with `--target srv` (operator) or `--target srv-data`."""
        return self._run_cli(
            "--operator-config", str(self.operator_config), *args, check=check
        )

    def deploy(
        self,
        schema_pg: str,
        *,
        actor: str = OPERATOR,
        graph_id: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        """Write a graph's schema into the bundle and apply it live."""
        (self.config_dir / f"{graph_id or self.graph_id}.pg").write_text(schema_pg)
        return self.cli(
            "cluster", "apply", "--target", _TARGETS[actor],
            "--config", str(self.config_dir), "--json",
            check=check,
        )  # fmt: skip

    def cluster_config(
        self, *, actor: str = OPERATOR, graph_id: str | None = None
    ) -> ClusterConfig:
        """What the connector deploys a graph's schema through, as `actor`."""
        return ClusterConfig(
            config_dir=self.config_dir,
            target=_TARGETS[actor],
            schema_file=f"{graph_id or self.graph_id}.pg",
            cli=str(CLI),
            operator_config=self.operator_config,
        )

    def connection_factory(
        self,
        *,
        branch: str = "main",
        cluster: bool = True,
        actor: str = OPERATOR,
        cluster_actor: str | None = None,
        graph_id: str | None = None,
        base_url: str | None = None,
    ) -> ConnectionFactory:
        """A connector factory for a graph: data as `actor`, schema deployments
        as `cluster_actor` (default: the same actor)."""
        gid = graph_id or self.graph_id
        return ConnectionFactory(
            base_url or self.base_url,
            graph_id=gid,
            branch=branch,
            token=self.tokens[actor],
            cluster=(
                self.cluster_config(actor=cluster_actor or actor, graph_id=gid)
                if cluster
                else None
            ),
        )

    # -- raw HTTP harness (B1.5) --------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        actor: str | None = OPERATOR,
        headers: dict[str, str] | None = None,
        contract: bool = True,
    ) -> Response:
        """One uninterpreted request: status, headers, and the body as JSON
        when it parses, else as text."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        if contract:
            req.add_header(HTTP_API_HEADER, HTTP_API_CONTRACT)
        if actor is not None:
            req.add_header("Authorization", f"Bearer {self.tokens[actor]}")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                status, hdrs, raw = resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as e:
            status, hdrs, raw = e.code, e.headers, e.read()
        try:
            parsed: Any = json.loads(raw) if raw else None
        except ValueError:
            parsed = raw.decode(errors="replace")
        return Response(status, hdrs, parsed)

    def schema_source(self, graph_id: str | None = None) -> str:
        r = self.request("GET", f"/graphs/{graph_id or self.graph_id}/schema")
        assert r.status == 200, r
        return str(r.body["schema_source"])
