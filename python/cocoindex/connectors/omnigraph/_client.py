"""Transport for the Omnigraph connector — the only module that does I/O."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import json
import pathlib
import posixpath
import sys
import tempfile
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator, Iterator
from typing import BinaryIO

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

from cocoindex.connectors.omnigraph._gq import Query

if sys.platform == "win32":

    def _lock_file(lock_file: BinaryIO) -> None:
        # ``msvcrt.locking`` locks a byte range, so make sure byte zero exists
        # before asking for an exclusive lock on it.
        lock_file.seek(0, 2)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)

    def _try_lock_file(lock_file: BinaryIO) -> bool:
        lock_file.seek(0, 2)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        try:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock_file(lock_file: BinaryIO) -> None:
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)

else:

    def _lock_file(lock_file: BinaryIO) -> None:
        fcntl.flock(lock_file, fcntl.LOCK_EX)

    def _try_lock_file(lock_file: BinaryIO) -> bool:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        return True

    def _unlock_file(lock_file: BinaryIO) -> None:
        fcntl.flock(lock_file, fcntl.LOCK_UN)


def canonical_store(uri: str) -> str:
    """One identity for every spelling of the same store.

    The store lock is keyed by this. Keyed by the raw URI text, two
    processes addressing one store as `file:///a/store` and
    `file:///a/./store` took different locks — and one of them reaped the
    scratch branch the other was still using (verified live). A `file://`
    URI resolves to its real path; any other scheme gets a lower-cased
    scheme and host and a normalised path.
    """
    parts = urllib.parse.urlsplit(uri)
    scheme = parts.scheme.lower()
    if scheme == "file":
        return pathlib.Path(urllib.request.url2pathname(parts.path)).resolve().as_uri()
    path = posixpath.normpath(parts.path) if parts.path else ""
    return urllib.parse.urlunsplit((scheme, parts.netloc.lower(), path, "", ""))


def _lock_dir() -> pathlib.Path:
    return pathlib.Path(tempfile.gettempdir())


@contextlib.contextmanager
def _temporary_text_file(content: str, *, suffix: str) -> Iterator[str]:
    """Write a closed, short-lived file that another process can reopen.

    Windows denies reopening a ``NamedTemporaryFile`` while its original
    handle is still open. Creating it with ``delete=False`` lets us close the
    handle before invoking the CLI and still remove the file deterministically
    afterward on every platform.
    """
    path: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", suffix=suffix, encoding="utf-8", delete=False
        ) as f:
            f.write(content)
            path = pathlib.Path(f.name)
        yield str(path)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)


@dataclasses.dataclass(frozen=True)
class ConnectionFactory:
    """Identifies an Omnigraph store and the branch to write to.

    Provided once in the app lifespan and resolved at action time via the
    ContextProvider — never captured at declare time, since delete actions run
    in a process where the declaring code never executes.

    ``store`` is a URI (``file:///abs/path/g.omni``), not a filesystem path.
    """

    store: str
    branch: str = "main"
    cli: str = "omnigraph"


class OmnigraphCliError(RuntimeError):
    """Non-zero exit from the omnigraph CLI, carrying its stderr."""


class _CliClient:
    def __init__(self, conn: ConnectionFactory) -> None:
        self._conn = conn

    @property
    def store_lock_path(self) -> pathlib.Path:
        digest = hashlib.sha256(canonical_store(self._conn.store).encode()).hexdigest()
        return _lock_dir() / f"cocoindex-omnigraph-{digest}.lock"

    @staticmethod
    def _scratch_branch_lock_path(name: str) -> pathlib.Path:
        return _lock_dir() / f"cocoindex-omnigraph-scratch-{name}.lock"

    @contextlib.asynccontextmanager
    async def store_lock(self) -> AsyncIterator[None]:
        """Serialize operations that require exclusive access to this store,
        across components and processes on this host."""
        lock_file = await asyncio.to_thread(self.store_lock_path.open, "a+b")
        try:
            await asyncio.to_thread(_lock_file, lock_file)
            yield
        finally:
            await asyncio.to_thread(_unlock_file, lock_file)
            await asyncio.to_thread(lock_file.close)

    @contextlib.asynccontextmanager
    async def hold_scratch_branch(self, name: str) -> AsyncIterator[None]:
        """Hold the liveness lock of scratch branch `name` for the block —
        from before the branch is created until after it is deleted — so a
        reaper anywhere on this host can tell the branch is in use."""
        path = self._scratch_branch_lock_path(name)
        lock_file = await asyncio.to_thread(path.open, "a+b")
        try:
            await asyncio.to_thread(_lock_file, lock_file)
            yield
        finally:
            await asyncio.to_thread(_unlock_file, lock_file)
            await asyncio.to_thread(lock_file.close)
            with contextlib.suppress(OSError):
                await asyncio.to_thread(path.unlink)

    @contextlib.asynccontextmanager
    async def claim_scratch_branch(self, name: str) -> AsyncIterator[bool]:
        """Try to take the liveness lock of scratch branch `name` without
        waiting. Yields True if it was free — its owner is gone, so the
        branch is abandoned and the block may delete it — and False while
        another process still holds it."""
        path = self._scratch_branch_lock_path(name)
        lock_file = await asyncio.to_thread(path.open, "a+b")
        claimed = False
        try:
            claimed = await asyncio.to_thread(_try_lock_file, lock_file)
            yield claimed
        finally:
            if claimed:
                await asyncio.to_thread(_unlock_file, lock_file)
            await asyncio.to_thread(lock_file.close)
            if claimed:
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(path.unlink)

    # --- argv builders (pure, unit-tested) ---

    def _mutate_argv(
        self, query_path: str, params_path: str, *, branch: str
    ) -> list[str]:
        """Both the GQ source and the bound params go by FILE, never inline.

        The inline forms (`-e <gq>`, `--params <json>`) put the whole commit
        into argv, and a commit is a whole component's writes: at the 8,192
        entity cap that is ~2.3 MB across the two arguments, well past
        darwin's 1 MB `ARG_MAX` — `create_subprocess_exec` raises `OSError:
        [Errno 7] Argument list too long`, which isn't an `OmnigraphCliError`
        and so isn't caught anywhere. Linux binds tighter still: its
        128 KiB-per-argument `MAX_ARG_STRLEN` caps the expression alone at
        roughly 800 entities whatever `ARG_MAX` allows.

        `--query <path>` and `--params-file <path>` take exactly the same
        input with no size limit at all (verified against the binary, both
        with and without the positional query name, and at the full 8,192
        cap). So the transport simply doesn't put payloads in argv.
        """
        return [
            self._conn.cli,
            "mutate",
            "--store",
            self._conn.store,
            "--branch",
            branch,
            "--query",
            query_path,
            "--params-file",
            params_path,
            "--json",
            "--quiet",
        ]

    def _query_argv(
        self, query_path: str, params_path: str, *, branch: str
    ) -> list[str]:
        """The read side of `_mutate_argv`: same file-borne payloads."""
        return [
            self._conn.cli,
            "query",
            "--store",
            self._conn.store,
            "--branch",
            branch,
            "--query",
            query_path,
            "--params-file",
            params_path,
            "--json",
            "--quiet",
        ]

    def _merge_argv(self, name: str, *, into: str) -> list[str]:
        # `branch merge` has no compare-and-swap precondition (as of 0.10.0
        # only `mutate` takes `--if-commit`). A conflict is a non-zero exit.
        return [
            self._conn.cli,
            "branch",
            "merge",
            name,
            "--into",
            into,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _init_argv(self, schema_path: str) -> list[str]:
        # `init` takes the graph URI POSITIONALLY, not via --store, and has
        # no --json flag at all — it prints a plain "initialized <uri>" line
        # to stdout instead of JSON.
        return [
            self._conn.cli,
            "init",
            "--schema",
            schema_path,
            "--quiet",
            self._conn.store,
        ]

    def _apply_schema_argv(self, schema_path: str) -> list[str]:
        return [
            self._conn.cli,
            "schema",
            "apply",
            "--schema",
            schema_path,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _schema_show_argv(self) -> list[str]:
        return [
            self._conn.cli,
            "schema",
            "show",
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _branch_create_argv(self, name: str, *, frm: str) -> list[str]:
        return [
            self._conn.cli,
            "branch",
            "create",
            name,
            "--from",
            frm,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _branch_delete_argv(self, name: str) -> list[str]:
        return [
            self._conn.cli,
            "branch",
            "delete",
            name,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _branch_list_argv(self) -> list[str]:
        return [
            self._conn.cli,
            "branch",
            "list",
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    # --- execution ---

    async def _run(self, argv: list[str]) -> dict[str, object]:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await proc.communicate()
        except BaseException:
            # Cancellation must not leave the CLI running: `communicate()`
            # does nothing to the child when the awaiting task is cancelled,
            # so a cancelled `mutate` kept writing to the store after the
            # connector had given up on it. Kill it and reap it, then let
            # the cancellation propagate.
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()
            raise
        if proc.returncode != 0:
            raise OmnigraphCliError(
                f"{' '.join(argv[:3])} exited {proc.returncode}: "
                f"{err.decode(errors='replace').strip()}"
            )
        if "--json" not in argv:
            # `init` has no --json flag; its stdout is a plain diagnostic
            # line, not JSON, so there is nothing to parse.
            return {}
        text = out.decode(errors="replace").strip()
        return json.loads(text) if text else {}

    async def init_graph(self, schema_pg: str) -> None:
        with _temporary_text_file(schema_pg, suffix=".pg") as path:
            await self._run(self._init_argv(path))

    async def apply_schema(self, schema_pg: str) -> None:
        with _temporary_text_file(schema_pg, suffix=".pg") as path:
            await self._run(self._apply_schema_argv(path))

    async def read_schema(self) -> str | None:
        """Return the graph's current, complete `.pg` schema source, or
        `None` if the graph hasn't been `init`'d yet.

        Omnigraph's schema is applied whole-graph, not per type: `schema
        apply`/`init` treat their input as the complete desired schema, so
        callers that reconcile one type at a time must read this back and
        merge before writing, rather than applying a single type's
        fragment directly (see `_gq.merge_type_into_schema`).

        Not-yet-initialized is detected on the CLI's stderr text — the raw
        Lance "Dataset ... not found" (verified against the binary) — since
        the exit code alone doesn't distinguish it from any other failure.
        """
        try:
            result = await self._run(self._schema_show_argv())
        except OmnigraphCliError as e:
            msg = str(e).lower()
            # Match the engine's actual phrasing, not the two words
            # separately: the store URI is echoed into every error, so a store
            # under `~/datasets/` would otherwise turn any unrelated
            # "not found" failure into a bogus "graph not initialized".
            if "dataset at path" in msg and "was not found" in msg:
                return None
            raise
        source = result["schema_source"]
        assert isinstance(source, str)
        return source

    async def mutate(self, mutation: Query, *, branch: str) -> None:
        with (
            _temporary_text_file(mutation.expr, suffix=".gq") as query_path,
            _temporary_text_file(
                json.dumps(mutation.params), suffix=".json"
            ) as params_path,
        ):
            await self._run(self._mutate_argv(query_path, params_path, branch=branch))

    async def query(self, query: Query, *, branch: str) -> list[dict[str, object]]:
        """Run a read query and return its rows, one dict per row keyed by
        the `return` clause's column aliases."""
        with (
            _temporary_text_file(query.expr, suffix=".gq") as query_path,
            _temporary_text_file(
                json.dumps(query.params), suffix=".json"
            ) as params_path,
        ):
            result = await self._run(
                self._query_argv(query_path, params_path, branch=branch)
            )
        rows = result["rows"]
        assert isinstance(rows, list)
        return rows

    async def branch_create(self, name: str, *, frm: str) -> None:
        await self._run(self._branch_create_argv(name, frm=frm))

    async def branch_merge(self, name: str, *, into: str) -> None:
        await self._run(self._merge_argv(name, into=into))

    async def branch_delete(self, name: str) -> None:
        await self._run(self._branch_delete_argv(name))

    async def branch_list(self) -> list[str]:
        result = await self._run(self._branch_list_argv())
        branches = result["branches"]
        assert isinstance(branches, list)
        return [str(name) for name in branches]
