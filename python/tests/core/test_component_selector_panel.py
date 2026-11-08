"""CocoInsight panel flow, end to end, with random source edits.

Each iteration: 1-3 random edits; list the app's paths with ``list_stable_paths``;
pick a page of 10 or 50 paths; preview the page; update the page with the same
selector. Checked every iteration:

- Preview and update agree: the same error, or the same actions (the update's
  actions are recorded at the sinks). Extra update actions that are deletes of
  target states owned by a page component no longer mounted are counted apart
  (the docs say a preview does not plan those).
- Unselected components are untouched: every LMDB row whose subject is outside
  the page (not covered, not an ancestor of a page path, not the root's
  use_mount child) stays byte for byte, and so does every external target
  state such a component owns. A takeover by a page component is the one
  allowed edit (old owner's tracking record and the memos up its chain).
- Every 4th iteration, a plain update; afterwards external target states and
  owners equal those of a replay app that only ever ran plain updates.

Longer runs: set ``COCO_PANEL_SEEDS`` (for example ``0-99``) and
``COCO_PANEL_ITERS``, and ``COCO_FUZZ_LMDB_SERVER`` as for
``test_component_selector_fuzz.py`` to compare LMDB rows byte for byte.
"""

from __future__ import annotations

import asyncio
import collections
import os
import random
import threading
from dataclasses import dataclass, field
from typing import Any

import pytest

import cocoindex as coco
import cocoindex.inspect as coco_inspect
from cocoindex._internal.inspect_api import list_stable_paths

from tests import common
from tests.common.environment import get_env_db_path
from tests.common.target_states import DictsTargetStateStore, DictTargetStateStore
from tests.core.test_component_selector_fuzz import RawReader, classify_row

SIDES = ("sut", "replay")
_lock = threading.Lock()
# Actions the sinks applied, per side, while recording.
APPLIED: dict[str, list[tuple[Any, ...]] | None] = {s: None for s in SIDES}


def _norm_value(v: Any) -> Any:
    if coco.is_non_existence(v):
        return None
    return (v.data, tuple(sorted(map(repr, v.prev))), v.prev_may_be_missing)


class RecFlat(DictTargetStateStore):
    def __init__(self, side: str, tag: str) -> None:
        self.side, self.tag = side, tag
        super().__init__()

    def _sink(self, context_provider: Any, actions: Any, /) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with _lock:
            rec = APPLIED[self.side]
            for key, value in actions:
                if rec is not None:
                    rec.append((self.tag, key, _norm_value(value)))
                if coco.is_non_existence(value):
                    self.data.pop(key, None)
                else:
                    self.data[key] = value


class RecCont(DictsTargetStateStore):
    def __init__(self, side: str) -> None:
        self.side = side
        super().__init__()

    def _child_handler(self, context_provider: Any, action: Any, /) -> Any:
        with self._lock:
            existing = self._stores.get(action.name)
            if existing is not None and not action.destructive:
                return existing
            staged = self._staged.get(action.name)
            if staged is None:
                staged = RecFlat(self.side, f"row:{action.name}")
                self._staged[action.name] = staged
            return staged

    def _new_store(self, name: str) -> DictTargetStateStore:
        staged = self._staged.pop(name, None)
        return staged if staged is not None else RecFlat(self.side, f"row:{name}")

    def _sink(self, context_provider: Any, actions: Any, /) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            rec = APPLIED[self.side]
            for name, exists, action, destructive in actions:
                if rec is not None:
                    rec.append(("cont", name, (exists, action, destructive)))
                if action == "insert" or (
                    action == "upsert" and (destructive or name not in self._stores)
                ):
                    self._stores[name] = self._new_store(name)
                elif action == "delete":
                    self._stores.pop(name, None)


FLAT = {s: RecFlat(s, "flat") for s in SIDES}
FLAT_P = {
    s: coco.register_root_target_states_provider(f"panel_flat/{s}", FLAT[s])
    for s in SIDES
}
CONT = {s: RecCont(s) for s in SIDES}
CONT_P = {
    s: coco.register_root_target_states_provider(f"panel_cont/{s}", CONT[s])
    for s in SIDES
}


def _pv(action: Any) -> tuple[Any, ...]:
    """A preview action, normalized as the sinks record theirs."""
    if isinstance(action, tuple) and len(action) == 4 and isinstance(action[1], bool):
        name, exists, act, destructive = action
        return ("cont", name, (exists, act, destructive))
    key, value = action
    tag = "flat" if not str(key).startswith("row|") else f"row:{CONT_NAME}"
    return (tag, key, _norm_value(value))


CONT_NAME = "C"

# ---------------------------------------------------------------------------
# The app
# ---------------------------------------------------------------------------


@dataclass
class Source:
    files: dict[Any, tuple[int, int]] = field(default_factory=dict)  # key->(content, nchunks)
    archive: dict[Any, int] = field(default_factory=dict)
    grp: dict[str, int] = field(default_factory=dict)
    um: dict[str, int] = field(default_factory=dict)
    version: int = 0


SRC = Source()
EXT: dict[str, int] = {}


def _kr(k: Any) -> str:
    return f"{type(k).__name__}:{k}"


def _flat(side: str, key: str, value: Any) -> None:
    coco.declare_target_state(FLAT_P[side].target_state(key, value))


def _validate_ext(key: str, prev: int) -> coco.MemoStateOutcome:
    cur = EXT.get(key, 0)
    return coco.MemoStateOutcome(state=cur, memo_valid=cur == prev)


EXT_DEP = coco.DataDep("panel-ext", validate=_validate_ext)


@coco.fn(memo=True)
async def chunk_fn(side: str, k: Any, i: int, content: int, provider: Any) -> None:
    coco.declare_target_state(provider.target_state(f"row|{_kr(k)}#{i}", content * 10 + i))
    _flat(side, f"c|{_kr(k)}#{i}", content + i)


async def _file_body(entry: tuple[Any, tuple[int, int]], side: str, provider: Any) -> None:
    k, (content, n) = entry
    e = EXT.get(_kr(k), 0)
    coco.use_data_dep(EXT_DEP, key=_kr(k), state=e)
    _flat(side, f"f|{_kr(k)}", (content, e))
    for i in range(n):
        await coco.mount(coco.component_subpath(i), chunk_fn, side, k, i, content, provider)


_FILE_FNS: dict[int, Any] = {}


def file_fn(version: int) -> Any:
    with _lock:
        if version not in _FILE_FNS:
            _FILE_FNS[version] = coco.fn(memo=True, version=version)(_file_body)
        return _FILE_FNS[version]


@coco.fn(memo=True)
async def archive_fn(entry: tuple[Any, int], side: str) -> None:
    k, v = entry
    # The same key as a file: moving a file to the archive is a takeover.
    _flat(side, f"f|{_kr(k)}", ("archived", v))


@coco.fn
async def grp_leaf(side: str, k: str, v: int) -> None:
    _flat(side, f"g|{k}", v)


@coco.fn
async def um_child(side: str, k: str, v: int) -> None:
    _flat(side, f"u|{k}", v)


@coco.fn
async def um_fn(side: str, items: dict[str, int]) -> int:
    _flat(side, "um|n", len(items))
    for k, v in sorted(items.items()):
        await coco.mount(coco.component_subpath(k), um_child, side, k, v)
    return len(items)


@coco.fn
def declare_cont(side: str) -> coco.PendingTargetStateProvider[str, None]:
    return coco.declare_target_state_with_child(CONT_P[side].target_state(CONT_NAME, None))


@coco.fn
async def app_main(side: str) -> None:
    provider = await coco.use_mount(coco.component_subpath("setup"), declare_cont, side)
    n = await coco.use_mount(coco.component_subpath("um"), um_fn, side, dict(SRC.um))
    _flat(side, "root|n", (len(SRC.files), n))
    await coco.mount_each(
        coco.component_subpath("files"),
        file_fn(SRC.version),
        sorted(((k, (k, v)) for k, v in SRC.files.items()), key=repr),
        side,
        provider,
    )
    await coco.mount_each(
        coco.component_subpath("archive"),
        archive_fn,
        sorted(((k, (k, v)) for k, v in SRC.archive.items()), key=repr),
        side,
    )
    for k, v in sorted(SRC.grp.items()):
        await coco.mount(coco.component_subpath("grp", "sub", k), grp_leaf, side, k, v)


# ---------------------------------------------------------------------------
# Observing
# ---------------------------------------------------------------------------

Path = tuple[Any, ...]


def _tuple(p: coco.StablePath) -> Path:
    return tuple(p.parts())


def _sp(p: Path) -> coco.StablePath:
    out = coco.ROOT_PATH
    for k in p:
        out = out / k
    return out


def external(side: str) -> dict[tuple[str, ...], Any]:
    out: dict[tuple[str, ...], Any] = {}
    for k, v in FLAT[side].data.items():
        out[("flat", k)] = v.data
    for name, store in CONT[side].data.items():
        out[("cont", name)] = None
        for k, v in store.items():
            out[("row", k)] = v.data
    return out


def owners(app: coco.App[Any, Any]) -> dict[tuple[str, ...], Path]:
    async def collect() -> dict[tuple[str, ...], Path]:
        out: dict[tuple[str, ...], Path] = {}
        async for entry in coco_inspect.iter_target_states(app):
            assert not entry.dangling, entry.readable_path
            segs = [s[1:-1] if s.startswith('"') else s for s in entry.readable_segments]
            kind = "flat" if "panel_flat" in segs[0] else "cont"
            if kind == "cont" and len(segs) > 2:
                key: tuple[str, ...] = ("row", segs[-1])
            else:
                key = (kind, segs[-1])
            out[key] = _tuple(coco.StablePath(entry.owner_component_path))
        return out

    return asyncio.run(collect())


def _is_prefix(p: Path, q: Path) -> bool:
    return len(p) <= len(q) and all(
        type(a) is type(b) and a == b for a, b in zip(p, q)
    )


class Failure(AssertionError):
    pass


# Component failures the engine logs and swallows (background mounts). Any
# not injected is a failure of the seed.
ERRORS: list[str] = []


def _record_error(exc: BaseException, ctx: Any) -> None:
    ERRORS.append(f"{ctx.stable_path}: {exc!r}")


STATS: collections.Counter[str] = collections.Counter()


@dataclass
class Snap:
    raw: dict[bytes, bytes] | None
    ext: dict[tuple[str, ...], Any]
    own: dict[tuple[str, ...], Path]


class Panel:
    def __init__(self, seed: int, raw: RawReader | None) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        suffix = f"p{seed}_{os.getpid()}"
        self.env = common.create_test_env(
            __file__, suffix=suffix, exception_handler=_record_error
        )
        self.env_dir = str(get_env_db_path(f"test_panel_flow__{suffix}") / "mdb")
        self.raw = raw
        self.apps = {
            s: coco.App(coco.AppConfig(name=f"panel_{s}", environment=self.env), app_main, s)
            for s in SIDES
        }

    def snap(self) -> Snap:
        raw = self.raw.rows(self.env_dir, "panel_sut") if self.raw else None
        return Snap(raw, external("sut"), owners(self.apps["sut"]))

    # -- edits --------------------------------------------------------------
    def edit(self) -> str:
        r = self.rng.random()
        keys = list(SRC.files)
        pool: list[Any] = ["a", "b", "c", "d", "e", 1, 2, 3, "1", "x y"] + [
            f"k{i}" for i in range(20)
        ]
        if r < 0.25 and keys:
            k = self.rng.choice(keys)
            c, n = SRC.files[k]
            SRC.files[k] = (self.rng.randint(0, 9), n)
            return f"edit {k!r}"
        if r < 0.38:
            k = self.rng.choice(pool)
            if k not in SRC.archive:
                SRC.files[k] = (self.rng.randint(0, 9), self.rng.randint(0, 3))
                return f"add {k!r}"
        if r < 0.50 and keys:
            k = self.rng.choice(keys)
            del SRC.files[k]
            return f"delete {k!r}"
        if r < 0.58 and keys:
            k = self.rng.choice(keys)
            c, n = SRC.files[k]
            SRC.files[k] = (c, self.rng.randint(0, 3))
            return f"rechunk {k!r}"
        if r < 0.66:
            if keys and self.rng.random() < 0.6:
                k = self.rng.choice(keys)
                c, _ = SRC.files.pop(k)
                SRC.archive[k] = c
                return f"archive {k!r}"
            if SRC.archive:
                k = self.rng.choice(list(SRC.archive))
                SRC.files[k] = (SRC.archive.pop(k), 1)
                return f"unarchive {k!r}"
        if r < 0.72 and keys:
            k = self.rng.choice(keys)
            EXT[_kr(k)] = self.rng.randint(0, 3)
            return f"ext {k!r}"
        if r < 0.75:
            SRC.version += 1
            return "bump file_fn version"
        if r < 0.87:
            k = self.rng.choice(["p", "q", "r", "s"])
            if k in SRC.grp and self.rng.random() < 0.5:
                del SRC.grp[k]
                return f"grp del {k}"
            SRC.grp[k] = self.rng.randint(0, 9)
            return f"grp set {k}"
        k = self.rng.choice(["m", "n", "o"])
        if k in SRC.um and self.rng.random() < 0.5:
            del SRC.um[k]
            return f"um del {k}"
        SRC.um[k] = self.rng.randint(0, 9)
        return f"um set {k}"

    # -- one update ---------------------------------------------------------
    def run(self, side: str, **kw: Any) -> tuple[Any, Exception | None]:
        try:
            return self.apps[side].update_blocking(**kw), None
        except Exception as e:  # noqa: BLE001
            return None, e

    def recorded(self, side: str, **kw: Any) -> tuple[Any, Exception | None, list[Any]]:
        APPLIED[side] = []
        try:
            out, err = self.run(side, **kw)
        finally:
            rec, APPLIED[side] = APPLIED[side], None
        return out, err, rec or []


def _uncovered(page: list[Path], path: Path) -> bool:
    """True for a path the page update must leave alone: not covered, not an
    ancestor of a page path (the root included), not under the root's
    use_mount children."""
    if any(_is_prefix(p, path) for p in page):
        return False
    if any(_is_prefix(path, p) for p in page):
        return False
    if path in (("setup",), ("um",)):
        return False
    return True


def check_untouched(page: list[Path], before: Snap, after: Snap) -> None:
    taken_from = {
        before.own[k]
        for k in before.own
        if k in after.own
        and after.own[k] != before.own[k]
        and _uncovered(page, before.own[k])
        and not _uncovered(page, after.own[k])
    }
    if taken_from:
        STATS["takeovers from an unselected owner"] += 1
    if before.raw is not None and after.raw is not None:
        for key in sorted(set(before.raw) | set(after.raw)):
            b, a = before.raw.get(key), after.raw.get(key)
            if a == b:
                if b is not None:
                    row = classify_row(key, b)
                    if row.subject is not None and _uncovered(page, row.subject):
                        STATS["unselected rows byte-compared"] += 1
                continue
            row = classify_row(key, b if b is not None else a)  # type: ignore[arg-type]
            if row.kind == "global" or row.subject is None:
                continue
            if not _uncovered(page, row.subject):
                continue
            if row.entry == "0x40" and row.subject in taken_from:
                continue
            if row.entry == "0x20" and any(_is_prefix(row.subject, t) for t in taken_from):
                continue
            if row.kind == "target_state" and a is not None:
                new_owner = classify_row(key, a).subject
                if row.subject in taken_from and new_owner is not None and not _uncovered(
                    page, new_owner
                ):
                    continue
            change = "added" if b is None else "removed" if a is None else "changed"
            raise Failure(f"LMDB row {row.entry} of unselected {row.subject} {change}")
    for k in sorted(set(before.ext) | set(after.ext), key=repr):
        if before.ext.get(k) == after.ext.get(k) and before.own.get(k) == after.own.get(k):
            continue
        ob, oa = before.own.get(k), after.own.get(k)
        if ob is not None and ob in taken_from and oa is not None and not _uncovered(page, oa):
            continue
        if any(o is not None and _uncovered(page, o) for o in (ob, oa)):
            raise Failure(
                f"external {k} of unselected owner {ob}->{oa}: "
                f"{before.ext.get(k)} -> {after.ext.get(k)}"
            )


def compare_actions(
    panel: Panel,
    page: list[Path],
    before: Snap,
    preview: Any,
    perr: Exception | None,
    uerr: Exception | None,
    applied: list[Any],
) -> None:
    if (perr is None) != (uerr is None):
        raise Failure(f"preview error {perr!r} vs update error {uerr!r}")
    if perr is not None:
        if type(perr) is not type(uerr) or str(perr) != str(uerr):
            raise Failure(f"different errors: {perr!r} vs {uerr!r}")
        STATS["page refused alike in preview and update"] += 1
        return
    p = collections.Counter(_pv(a) for a in preview)
    u = collections.Counter(applied)
    extra_u = u - p
    extra_p = p - u
    # Extra update actions allowed: deletes of target states whose owner (before)
    # is a page component that the update deleted (no longer mounted).
    gone_deletes = 0
    for action, n in list(extra_u.items()):
        tag, key, value = action
        ext_key = ("flat", key) if tag == "flat" else ("row", key) if tag.startswith("row") else ("cont", key)
        owner = before.own.get(ext_key)
        if value is None and owner is not None and not _uncovered(page, owner):
            gone_deletes += n
            del extra_u[action]
    if gone_deletes:
        STATS["update deletes a preview did not plan (gone components)"] += gone_deletes
    if extra_u or extra_p:
        raise Failure(
            f"preview/update disagree: only in update {dict(extra_u)}, "
            f"only in preview {dict(extra_p)}"
        )
    STATS["pages where preview == update"] += 1


def compare_with_replay(panel: Panel, where: str) -> None:
    s_ext, r_ext = external("sut"), external("replay")
    s_own, r_own = owners(panel.apps["sut"]), owners(panel.apps["replay"])
    if s_ext != r_ext:
        diff = {k: (s_ext.get(k), r_ext.get(k)) for k in set(s_ext) | set(r_ext) if s_ext.get(k) != r_ext.get(k)}
        raise Failure(f"{where}: external sut != replay: {diff}")
    if s_own != r_own:
        diff = {k: (s_own.get(k), r_own.get(k)) for k in set(s_own) | set(r_own) if s_own.get(k) != r_own.get(k)}
        raise Failure(f"{where}: owners sut != replay: {diff}")
    STATS["plain updates equal to replay"] += 1


def run_panel(seed: int, iters: int, raw: RawReader | None) -> list[str]:
    global SRC
    SRC = Source()
    EXT.clear()
    ERRORS.clear()
    for s in SIDES:
        FLAT[s].data.clear()
        CONT[s].clear()
    panel = Panel(seed, raw)
    log: list[str] = []
    rng = panel.rng
    try:
        pool: list[Any] = ["a", "b", 1, 2, "1", "c"] + [f"k{i}" for i in range(20)]
        for k in rng.sample(pool, rng.randint(4, 22)):
            SRC.files[k] = (rng.randint(0, 9), rng.randint(0, 3))
        SRC.grp.update(p=1)
        SRC.um.update(m=1)
        for s in SIDES:
            _, err = panel.run(s)
            if err:
                raise Failure(f"initial update failed: {err!r}")
        for it in range(iters):
            for _ in range(rng.randint(1, 3)):
                log.append(panel.edit())
            destructive = rng.random() < 0.06
            if it % 4 == 3:
                log.append(f"[{it}] plain{' destructive' if destructive else ''}")
                for s in SIDES:
                    CONT[s].child_invalidation = "destructive" if destructive else None
                    _, err = panel.run(s)
                    CONT[s].child_invalidation = None
                    if err:
                        raise Failure(f"[{it}] plain update of {s} failed: {err!r}")
                if ERRORS:
                    raise Failure(f"[{it}] component errors: {ERRORS[:3]}")
                compare_with_replay(panel, f"[{it}]")
                continue
            paths = sorted(
                (_tuple(p) for p in asyncio.run(list_stable_paths(panel.apps["sut"]))),
                key=repr,
            )
            paths = [p for p in paths if p]
            size = rng.choice([10, 50])
            pages = max(1, (len(paths) + size - 1) // size)
            pi = rng.randrange(pages)
            page = paths[pi * size : (pi + 1) * size]
            STATS[f"page size {size}"] += 1
            log.append(f"[{it}] page {pi}/{pages} size {size}: {page}{' destructive' if destructive else ''}")
            sel = [_sp(p) for p in page]
            CONT["sut"].child_invalidation = "destructive" if destructive else None
            try:
                before = panel.snap()
                preview, perr = panel.run("sut", component_selector=sel, preview=True)
                mid = panel.snap()
                if mid.raw != before.raw or mid.ext != before.ext:
                    raise Failure(f"[{it}] preview changed state")
                _, uerr, applied = panel.recorded("sut", component_selector=sel)
            finally:
                CONT["sut"].child_invalidation = None
            after = panel.snap()
            if uerr is not None and "A full update is needed" not in str(uerr):
                raise Failure(f"[{it}] page update failed: {uerr!r}")
            if ERRORS:
                raise Failure(f"[{it}] component errors: {ERRORS[:3]}")
            compare_actions(panel, page, before, preview, perr, uerr, applied)
            check_untouched(page, before, after)
        # The end: one plain update converges.
        for s in SIDES:
            _, err = panel.run(s)
            if err:
                raise Failure(f"final plain update of {s} failed: {err!r}")
        compare_with_replay(panel, "final")
        STATS["seeds passed"] += 1
        return log
    except Failure as e:
        raise Failure(f"seed {seed}: {e}\nlog:\n" + "\n".join(log[-25:])) from None
    finally:
        panel.env.close()


_RAW: RawReader | None = None


def _raw() -> RawReader | None:
    global _RAW
    cmd = os.environ.get("COCO_FUZZ_LMDB_SERVER")
    if cmd and _RAW is None:
        _RAW = RawReader(cmd)
    return _RAW


def _seeds() -> list[int]:
    spec = os.environ.get("COCO_PANEL_SEEDS", "0-3")
    lo, _, hi = spec.partition("-")
    return list(range(int(lo), int(hi or lo) + 1))


@pytest.mark.parametrize("seed", _seeds())
def test_panel_flow(seed: int) -> None:
    run_panel(seed, int(os.environ.get("COCO_PANEL_ITERS", "20")), _raw())


if __name__ == "__main__":
    import sys
    import time

    os.environ["COCO_PANEL_SEEDS"] = sys.argv[1]
    iters = int(sys.argv[2])
    t = time.time()
    fails: list[str] = []
    for seed in _seeds():
        try:
            run_panel(seed, iters, _raw())
        except Failure as e:
            fails.append(str(e))
            print(f"FAIL {e}", flush=True)
    print(f"{time.time() - t:.0f}s", dict(sorted(STATS.items())), flush=True)
    print(f"{len(fails)} failing seeds", flush=True)
