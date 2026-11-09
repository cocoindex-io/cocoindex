"""Model-based fuzzing of sliced execution: ``App.update(component_selector=...)``.

Each seed generates a random app shape (mount_each, directory subpaths, nested
components, two-level nesting, use_mount, container targets with child rows;
memoized or not) and a random sequence of steps: source edits, adds, deletes and
key-type changes, memo version and logic changes, shape changes (a component
turning into a directory and back, a group moving under a directory prefix),
selective updates with random selectors (exact, prefix, ancestor, missing,
empty, root, ancestor-and-descendant), previews, plain updates, full reprocess,
container schema changes, and injected sink and body failures.

Apps run the same program side by side:

- ``sut`` runs the steps as generated.
- ``replay`` runs every update as a plain one (the selector dropped), and skips
  previews and the updates a failure was injected into.
- ``fresh``, at the end only, is a new app that runs one plain update.

Checked after each step of ``sut``:

- A selective update, failing or not, changes nothing the store holds for an
  unselected path: every LMDB row whose subject (the component a path entry is
  for, the child an existence row or a tombstone names) is unselected stays byte
  for byte, and so do the external target states such a component owns.
- A preview changes no LMDB row and no external target state.
- After a plain update that succeeded, the external target states, their owners,
  the stable paths and the shape of the LMDB rows equal ``replay``'s.

After the last step, one plain update must bring ``sut`` to ``replay``'s and
``fresh``'s target states and owners.

The byte comparison reads LMDB from a helper process, since it needs the
``lmdb`` package, which the project does not depend on. Set
``COCO_FUZZ_LMDB_SERVER`` to a command that runs ``lmdb_dump_server.py`` to
enable it; without it, the check uses the inspect API (node type, memo flag,
version and target-state items per path) instead.

More seeds: ``COCO_FUZZ_SEEDS=0-500 COCO_FUZZ_STEPS=40``. A failing seed is
shrunk to a minimal step list, printed with the failure.
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import random
import shlex
import subprocess
import threading
from dataclasses import dataclass, field, replace
from typing import Any

import msgspec
import pytest

import cocoindex as coco
import cocoindex.inspect as coco_inspect

from tests import common
from tests.common.environment import get_env_db_path
from tests.common.target_states import DictsTargetStateStore, DictTargetStateStore

Key = str | int
Path = tuple[Key, ...]

KINDS = ["each", "dir", "nested", "deep", "usemount", "container"]
SIDES = ["sut", "replay", "fresh"]

# ---------------------------------------------------------------------------
# Targets: one flat store and one container store per side
# ---------------------------------------------------------------------------


class _TolerantDictStore(DictTargetStateStore):
    """Deletes a missing key as a no-op, as a real target does: after a failed
    first insert, the engine deletes a row that may never have landed."""

    def _sink(self, context_provider: Any, actions: Any, /) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            for key, value in actions:
                if coco.is_non_existence(value):
                    self.data.pop(key, None)
                else:
                    self.data[key] = value


class _TolerantDictsStore(DictsTargetStateStore):
    """A container store whose deletes, its rows' included, are idempotent."""

    def _child_handler(self, context_provider: Any, action: Any, /) -> Any:
        with self._lock:
            existing = self._stores.get(action.name)
            if existing is not None and not action.destructive:
                return existing
            staged = self._staged.get(action.name)
            if staged is None:
                staged = _TolerantDictStore()
                self._staged[action.name] = staged
            return staged

    def _new_store(self, name: str) -> DictTargetStateStore:
        staged = self._staged.pop(name, None)
        return staged if staged is not None else _TolerantDictStore()

    def _sink(self, context_provider: Any, actions: Any, /) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            for name, _exists, action, destructive in actions:
                if action == "insert" or (
                    action == "upsert" and (destructive or name not in self._stores)
                ):
                    self._stores[name] = self._new_store(name)
                elif action == "delete":
                    self._stores.pop(name, None)


FLAT: dict[str, DictTargetStateStore] = {}
FLAT_PROVIDER: dict[str, Any] = {}
CONT: dict[str, _TolerantDictsStore] = {}
CONT_PROVIDER: dict[str, Any] = {}
for _side in SIDES:
    FLAT[_side] = _TolerantDictStore()
    FLAT_PROVIDER[_side] = coco.register_root_target_states_provider(
        f"fuzz_flat/{_side}", FLAT[_side]
    )
    CONT[_side] = _TolerantDictsStore()
    CONT_PROVIDER[_side] = coco.register_root_target_states_provider(
        f"fuzz_cont/{_side}", CONT[_side]
    )

# ---------------------------------------------------------------------------
# The program. The root reads these globals (it always runs); every other
# component gets its inputs as arguments, so that memos stay sound.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Group:
    name: str
    kind: str
    memo: bool
    prefix: tuple[str, ...] = ()
    version: int = 0
    variant: int = 0

    @property
    def base(self) -> Path:
        return self.prefix + (self.name,)


@dataclass
class Program:
    groups: dict[str, Group] = field(default_factory=dict)
    source: dict[str, dict[Key, int]] = field(default_factory=dict)


PROGRAM = Program()
# (side, path) of a component whose body raises when it runs.
FAIL: set[tuple[str, Path]] = set()
_lock = threading.Lock()
# Counters for a run's report: steps by kind, rows and target states judged.
STATS: collections.Counter[str] = collections.Counter()


def _kr(k: Key) -> str:
    """A key in a target-state key, typed, with no quotes to escape."""
    return f"{type(k).__name__}:{k}"


def _flat(side: str, key: str, value: Any) -> None:
    coco.declare_target_state(FLAT_PROVIDER[side].target_state(key, value))


def _check_fail(side: str, path: Path) -> None:
    if (side, path) in FAIL:
        raise ValueError(f"injected body failure at {path}")


# Values the leaves of groups g1 and g3 read besides their arguments, recorded
# as data dependencies: a change reaches a memoized leaf only through the
# DataDep's validation. Shared by every side, like the source.
EXT: dict[str, int] = {}
DATADEP_GROUPS = ("g1", "g3")


def _validate_ext(key: str, prev: int) -> coco.MemoStateOutcome:
    current = EXT.get(key, 0)
    return coco.MemoStateOutcome(state=current, memo_valid=current == prev)


EXT_DEP = coco.DataDep("fuzz-ext", validate=_validate_ext)


def _read_ext(g: str, k: Key) -> int:
    key = f"{g.split('/')[0]}|{_kr(k)}"
    value = EXT.get(key, 0)
    coco.use_data_dep(EXT_DEP, key=key, state=value)
    return value


def _leaf_body(side: str, path: Path, g: str, k: Key, v: int, version: int) -> None:
    _check_fail(side, path)
    state = coco.use_state("last", None)
    state.value = v
    if g.split("/")[0] in DATADEP_GROUPS:
        v = v * 100 + _read_ext(g, k)
    _flat(side, f"{g}|{_kr(k)}", (v, version))
    if v % 2 == 0:
        _flat(side, f"{g}|{_kr(k)}|x", version)


async def _leaf0(
    entry: tuple[Key, int], side: str, base: Path, g: str, version: int
) -> None:
    k, v = entry
    _leaf_body(side, base + (k,), g, k, v, version)


async def _leaf1(
    entry: tuple[Key, int], side: str, base: Path, g: str, version: int
) -> None:
    # The other logic: also declares a third target state.
    k, v = entry
    _leaf_body(side, base + (k,), g, k, v, version)
    _flat(side, f"{g}|{_kr(k)}|v1", -v)


async def _cleaf(
    entry: tuple[Key, int], side: str, base: Path, provider: Any, version: int
) -> None:
    k, v = entry
    _check_fail(side, base + (k,))
    coco.declare_target_state(provider.target_state(_kr(k), (v, version)))


_FNS: dict[tuple[str, bool, int], Any] = {}


def _wrapped(name: str, body: Any, memo: bool, version: int) -> Any:
    """One `coco.fn` per (body, memo, version), so that a memo stays valid across
    steps and a version or body change is a logic change."""
    key = (name, memo, version)
    with _lock:
        if key not in _FNS:
            _FNS[key] = (
                coco.fn(memo=True, version=version)(body) if memo else coco.fn(body)
            )
        return _FNS[key]


def leaf_fn(g: Group) -> Any:
    return _wrapped(
        f"leaf{g.variant}", _leaf0 if g.variant == 0 else _leaf1, g.memo, g.version
    )


@coco.fn(memo=True)
async def _sum_helper(values: tuple[int, ...]) -> int:
    return sum(values)


async def _mount_leaves(
    side: str, g: Group, base: Path, items: dict[Key, int], relative: bool
) -> None:
    fn = leaf_fn(g)
    for k, v in items.items():
        sub = (
            coco.component_subpath(k) if relative else coco.component_subpath(*base, k)
        )
        await coco.mount(sub, fn, (k, v), side, base, g.name, g.version)


async def nested_mid(side: str, g: Group, items: dict[Key, int]) -> None:
    await _mount_leaves(side, g, g.base, items, relative=True)
    total = await _sum_helper(tuple(sorted(items.values())))
    _flat(side, f"{g.name}|mid", total)
    _check_fail(side, g.base)


async def deep_mid2(side: str, g: Group, k: Key, v: int) -> None:
    path = g.base + (k,)
    fn = leaf_fn(g)
    for j in range(v % 3 + 1):
        await coco.mount(
            coco.component_subpath(j),
            fn,
            (j, v + j),
            side,
            path,
            f"{g.name}/{_kr(k)}",
            g.version,
        )
    _check_fail(side, path)


async def deep_mid(side: str, g: Group, items: dict[Key, int]) -> None:
    for k, v in items.items():
        await coco.mount(
            coco.component_subpath(k),
            _wrapped("deep_mid2", deep_mid2, g.memo, g.version),
            side,
            g,
            k,
            v,
        )
    _flat(side, f"{g.name}|deep", len(items))


@coco.fn
async def use_mount_mid(side: str, g: Group, items: dict[Key, int]) -> int:
    await _mount_leaves(side, g, g.base, items, relative=True)
    _check_fail(side, g.base)
    return len(items)


@coco.fn
def declare_container(
    side: str, name: str
) -> coco.PendingTargetStateProvider[str, None]:
    return coco.declare_target_state_with_child(
        CONT_PROVIDER[side].target_state(name, None)
    )


@coco.fn
async def app_main(side: str) -> None:
    for g in list(PROGRAM.groups.values()):
        items = dict(PROGRAM.source.get(g.name, {}))
        base = g.base
        if g.kind == "each":
            await coco.mount_each(
                coco.component_subpath(*base),
                leaf_fn(g),
                [(k, (k, v)) for k, v in items.items()],
                side,
                base,
                g.name,
                g.version,
            )
        elif g.kind == "dir":
            await _mount_leaves(side, g, base, items, relative=False)
        elif g.kind == "nested":
            await coco.mount(
                coco.component_subpath(*base),
                _wrapped("nested_mid", nested_mid, g.memo, g.version),
                side,
                g,
                items,
            )
        elif g.kind == "deep":
            await coco.mount(
                coco.component_subpath(*base),
                _wrapped("deep_mid", deep_mid, g.memo, g.version),
                side,
                g,
                items,
            )
        elif g.kind == "usemount":
            n = await coco.use_mount(
                coco.component_subpath(*base), use_mount_mid, side, g, items
            )
            _flat(side, f"{g.name}|um", n)
        elif g.kind == "container":
            provider = await coco.use_mount(
                coco.component_subpath(*base, "setup"), declare_container, side, g.name
            )
            await coco.mount_each(
                coco.component_subpath(*base, "rows"),
                _wrapped("cleaf", _cleaf, g.memo, g.version),
                [(k, (k, v)) for k, v in items.items()],
                side,
                base + ("rows",),
                provider,
                g.version,
            )


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    op: str
    group: str = ""
    key: Key = ""
    value: int = 0
    kind: str = ""
    # For "update": "plain", "selective" or "preview".
    mode: str = ""
    selector: tuple[Path, ...] = ()
    full_reprocess: bool = False
    # For "update": None, "sink", "body:<path repr>", "schema:destructive|lossy".
    inject: str = ""
    fail_path: Path = ()

    def __str__(self) -> str:
        if self.op == "update":
            parts = [f"update {self.mode}"]
            if self.mode != "plain":
                parts.append(f"selector={list(self.selector)}")
            if self.full_reprocess:
                parts.append("full_reprocess")
            if self.inject:
                parts.append(f"inject={self.inject}")
            if self.fail_path:
                parts.append(f"fail_path={self.fail_path}")
            return " ".join(parts)
        fields = [f"{self.op}", self.group]
        if self.op in ("edit", "add", "delete", "retype", "ext_edit"):
            fields.append(repr(self.key))
        if self.op in ("edit", "add", "ext_edit"):
            fields.append(str(self.value))
        if self.kind:
            fields.append(self.kind)
        if self.op == "add_group":
            fields.append("memo" if self.value else "no-memo")
        return " ".join(fields)


KEY_POOL: list[Key] = ["a", "b", "c", "d", 1, 2, 3, "1"]


def apply_program_step(program: Program, step: Step) -> None:
    """Apply a non-update step to the program; tolerant of steps a shrink made
    meaningless (editing a group that is gone, deleting a key that is not there)."""
    g = program.groups.get(step.group)
    src = program.source.setdefault(step.group, {})
    if step.op == "ext_edit":
        EXT[f"{step.group}|{_kr(step.key)}"] = step.value
    elif step.op in ("edit", "add"):
        if g is not None:
            src[step.key] = step.value
    elif step.op == "delete":
        src.pop(step.key, None)
    elif step.op == "retype":
        if step.key in src:
            v = src.pop(step.key)
            other: Key = (
                str(step.key)
                if isinstance(step.key, int)
                else (int(step.key) if str(step.key).isdigit() else step.key + "_")
            )
            src[other] = v
    elif g is None:
        if step.op == "add_group":
            program.groups[step.group] = Group(
                step.group, step.kind, memo=bool(step.value)
            )
    elif step.op == "bump_version":
        program.groups[step.group] = replace(g, version=g.version + 1)
    elif step.op == "flip_variant":
        program.groups[step.group] = replace(g, variant=1 - g.variant)
    elif step.op == "toggle_memo":
        program.groups[step.group] = replace(g, memo=not g.memo)
    elif step.op == "reshape":
        program.groups[step.group] = replace(g, kind=step.kind)
    elif step.op == "move":
        program.groups[step.group] = replace(g, prefix=() if g.prefix else ("d",))
    elif step.op == "drop_group":
        del program.groups[step.group]


def model_paths(program: Program) -> list[Path]:
    """Every component and directory path the program mounts."""
    out: list[Path] = [()]
    for g in program.groups.values():
        base = g.base
        out.extend(base[:i] for i in range(1, len(base)))
        items = program.source.get(g.name, {})
        out.append(base)
        if g.kind == "container":
            out += [base + ("setup",), base + ("rows",)]
            out += [base + ("rows", k) for k in items]
        elif g.kind == "deep":
            for k, v in items.items():
                out.append(base + (k,))
                out += [base + (k, j) for j in range(v % 3 + 1)]
        else:
            out += [base + (k,) for k in items]
    return out


def model_components(program: Program) -> set[Path]:
    """The component paths the program mounts (the root included)."""
    out: set[Path] = {()}
    for g in program.groups.values():
        base = g.base
        items = program.source.get(g.name, {})
        if g.kind in ("nested", "deep", "usemount"):
            out.add(base)
        if g.kind == "container":
            out.add(base + ("setup",))
            out.update(base + ("rows", k) for k in items)
        elif g.kind == "deep":
            for k, v in items.items():
                out.add(base + (k,))
                out.update(base + (k, j) for j in range(v % 3 + 1))
        else:
            out.update(base + (k,) for k in items)
    return out


def model_dirs(program: Program) -> set[Path]:
    """The paths the program mounts as directories: the strict prefixes of
    its components that are not components. A directory nothing is mounted
    under is not declared at all."""
    components = model_components(program)
    return {
        c[:i] for c in components for i in range(1, len(c)) if c[:i] not in components
    }


def generate(seed: int, n_steps: int) -> list[Step]:
    rng = random.Random(seed)
    program = Program()
    steps: list[Step] = []
    seen_paths: set[Path] = set()
    n_groups = rng.randint(1, 4)
    for i in range(n_groups):
        name = f"g{i}"
        kind = rng.choice(KINDS)
        s = Step("add_group", name, kind=kind, value=rng.randint(0, 1))
        steps.append(s)
        apply_program_step(program, s)
        if rng.random() < 0.3:
            s = Step("move", name)
            steps.append(s)
            apply_program_step(program, s)
        for k in rng.sample(KEY_POOL, rng.randint(0, 4)):
            s = Step("add", name, key=k, value=rng.randint(0, 9))
            steps.append(s)
            apply_program_step(program, s)
    # The first update is selective now and then: a first-ever run with a selector.
    while len(steps) < n_steps + n_groups:
        seen_paths.update(model_paths(program))
        r = rng.random()
        names = list(program.groups) or ["g0"]
        g = rng.choice(names)
        src = program.source.get(g, {})
        if r < 0.45:
            s = _gen_update(rng, program, seen_paths)
        elif r < 0.60:
            s = Step(
                "edit",
                g,
                key=rng.choice(list(src) or KEY_POOL),
                value=rng.randint(0, 9),
            )
        elif r < 0.68:
            s = Step("add", g, key=rng.choice(KEY_POOL), value=rng.randint(0, 9))
        elif r < 0.76:
            s = Step("delete", g, key=rng.choice(list(src) or KEY_POOL))
        elif r < 0.79:
            s = Step("retype", g, key=rng.choice(list(src) or KEY_POOL))
        elif r < 0.81 and g in DATADEP_GROUPS:
            s = Step(
                "ext_edit",
                g,
                key=rng.choice(list(src) or KEY_POOL),
                value=rng.randint(0, 9),
            )
        elif r < 0.83:
            s = Step(rng.choice(["bump_version", "flip_variant", "toggle_memo"]), g)
        elif r < 0.90:
            s = Step("reshape", g, kind=rng.choice(KINDS))
        elif r < 0.93:
            s = Step("move", g)
        elif r < 0.96:
            s = Step("drop_group", g)
        else:
            missing = [f"g{i}" for i in range(5) if f"g{i}" not in program.groups]
            if not missing:
                continue
            s = Step(
                "add_group",
                rng.choice(missing),
                kind=rng.choice(KINDS),
                value=rng.randint(0, 1),
            )
        steps.append(s)
        apply_program_step(program, s)
    return steps


def _gen_update(rng: random.Random, program: Program, seen: set[Path]) -> Step:
    mode = rng.choices(["plain", "selective", "preview"], [0.25, 0.6, 0.15])[0]
    current = model_paths(program)
    pool = sorted(seen | set(current), key=repr)
    selector: list[Path] = []
    if mode != "plain":
        shape = rng.random()
        if shape < 0.08:
            selector = []
        elif shape < 0.12:
            selector = [()]
        elif shape < 0.17:
            selector = [("zz", "missing")]
        elif shape < 0.27 and pool:
            # An ancestor and a descendant of it, at once.
            p = rng.choice([p for p in pool if len(p) >= 2] or pool)
            selector = [p[:1], p] if rng.random() < 0.5 else [p, p[:1]]
        else:
            selector = rng.sample(pool, min(len(pool), rng.randint(1, 3)))
    inject = ""
    fail_path: Path = ()
    r = rng.random()
    if mode != "preview" and r < 0.08:
        inject = "sink"
    elif mode != "preview" and r < 0.18:
        candidates = [p for p in current if p]
        if candidates:
            inject = "body"
            fail_path = rng.choice(candidates)
    elif r < 0.24 and any(g.kind == "container" for g in program.groups.values()):
        inject = "schema:" + rng.choice(["destructive", "lossy"])
    return Step(
        "update",
        mode=mode,
        selector=tuple(selector),
        full_reprocess=rng.random() < 0.15,
        inject=inject,
        fail_path=fail_path,
    )


# ---------------------------------------------------------------------------
# Observing an app: LMDB rows, external target states, owners
# ---------------------------------------------------------------------------


class RawReader:
    """The helper process that dumps an app's LMDB database."""

    def __init__(self, command: str) -> None:
        self._proc = subprocess.Popen(
            shlex.split(command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )

    def rows(self, env_dir: str, app_name: str) -> dict[bytes, bytes]:
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.write(json.dumps({"path": env_dir, "db": app_name}) + "\n")
        self._proc.stdin.flush()
        out = json.loads(self._proc.stdout.readline())
        if "error" in out:
            raise RuntimeError(out["error"])
        return {bytes.fromhex(k): bytes.fromhex(v) for k, v in out["rows"]}

    def close(self) -> None:
        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.wait(timeout=10)


class _Reader:
    def __init__(self, data: bytes, pos: int) -> None:
        self.data = data
        self.pos = pos

    def byte(self) -> int:
        b = self.data[self.pos]
        self.pos += 1
        return b

    def slice(self) -> bytes:
        out = bytearray()
        while True:
            b = self.byte()
            if b == 0:
                return bytes(out)
            if b == 1:
                b = self.byte()
            out.append(b)

    def key(self) -> Key | None:
        """One storekey-encoded StableKey; None at the end of a path."""
        tag = self.byte()
        if tag == 0:
            return None
        if tag == 5:
            raw = self.data[self.pos : self.pos + 8]
            self.pos += 8
            return int.from_bytes(raw, "big") - (1 << 63)
        if tag == 6:
            return self.slice().decode()
        if tag == 3:
            return "<sym>" + self.slice().decode()
        raise ValueError(f"unsupported key tag {tag}")

    def path(self) -> Path:
        parts: list[Key] = []
        while (k := self.key()) is not None:
            parts.append(k)
        return tuple(parts)


@dataclass(frozen=True)
class RowId:
    """What one LMDB row is about: `subject` is the path the selector judges it
    by."""

    kind: str  # "path" or "target_state" or "global"
    subject: Path | None
    entry: str
    raw_key: bytes


def classify_row(key: bytes, value: bytes) -> RowId:
    if key[0] == 0x10:
        r = _Reader(key, 1)
        path = r.path()
        tag = r.byte()
        if tag in (0x20, 0x30, 0x34, 0x40):
            return RowId("path", path, f"{tag:#x}", key)
        if tag == 0xA0:
            k = r.key()
            assert k is not None
            return RowId("path", path + (k,), "child_existence", key)
        if tag == 0xB0:
            return RowId("path", path + r.path(), "tombstone", key)
        raise ValueError(f"unknown path entry {tag:#x}")
    if key[0] == 0x20:
        owner = msgspec.msgpack.decode(value)
        parts = owner.get("C") if isinstance(owner, dict) else None
        return RowId(
            "target_state", tuple(parts) if parts is not None else None, "ts", key
        )
    return RowId("global", None, f"{key[0]:#x}", key)


def use_mount_paths(program: Program) -> list[Path]:
    """The components the root reaches through `use_mount`: they run whatever
    the selector says, as the root waits for their value."""
    out: list[Path] = []
    for g in program.groups.values():
        if g.kind == "usemount":
            out.append(g.base)
        elif g.kind == "container":
            out.append(g.base + ("setup",))
    return out


def selection(
    selector: tuple[Path, ...], path: Path, always_run: list[Path] = []
) -> str:
    """Python model of `ComponentSelector::classify`, plus the components that
    run whatever it says (the root, a use_mount child of the root)."""

    def same(a: Key, b: Key) -> bool:
        return type(a) is type(b) and a == b

    def is_prefix(p: Path, q: Path) -> bool:
        return len(p) <= len(q) and all(same(x, y) for x, y in zip(p, q))

    if any(is_prefix(s, path) for s in selector):
        return "selected"
    # The root always runs, so it counts as an ancestor even of nothing.
    if not path or any(is_prefix(path, s) for s in selector):
        return "ancestor"
    if any(is_prefix(path, u) for u in always_run):
        return "use_mount"
    return "unselected"


def external(side: str) -> dict[tuple[str, ...], Any]:
    out: dict[tuple[str, ...], Any] = {}
    for k, v in FLAT[side].data.items():
        out[("flat", k)] = v.data
    for name, store in CONT[side].data.items():
        out[("cont", name)] = None
        for k, v in store.items():
            out[("cont", name, k)] = v.data
    return out


def owners(app: coco.App[Any, Any], side: str) -> dict[tuple[str, ...], Path]:
    """External target-state key -> owning component path."""

    async def collect() -> dict[tuple[str, ...], Path]:
        out: dict[tuple[str, ...], Path] = {}
        async for entry in coco_inspect.iter_target_states(app):
            assert not entry.dangling, f"dangling owner row {entry.readable_path}"
            segs = list(entry.readable_segments)
            out[_ext_key(segs, side)] = _path_of(
                coco.StablePath(entry.owner_component_path)
            )
        return out

    return asyncio.run(collect())


def _ext_key(segs: list[str], side: str) -> tuple[str, ...]:
    def unq(s: str) -> str:
        return s[1:-1] if len(s) >= 2 and s[0] == s[-1] == '"' else s

    provider = segs[0]
    rest = [unq(s) for s in segs[1:]]
    if "fuzz_flat" in provider:
        return ("flat", *rest)
    if "fuzz_cont" in provider:
        return ("cont", *rest)
    return ("?", provider, *rest)


def _path_of(path: coco.StablePath) -> Path:
    """A stable path as the model's tuple (the fuzzer uses str and int keys)."""
    return tuple(k for k in path.parts() if isinstance(k, (str, int)))


def _plain(obj: Any, depth: int = 0) -> Any:
    """An inspect object as nested tuples of its public fields, to compare."""
    if depth > 6 or obj is None or isinstance(obj, (str, int, float, bytes, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return tuple(_plain(x, depth + 1) for x in obj)
    if isinstance(obj, dict):
        return tuple(sorted((repr(k), _plain(v, depth + 1)) for k, v in obj.items()))
    fields = [f for f in sorted(dir(obj)) if not f.startswith("_")]
    out = []
    for f in fields:
        value = getattr(obj, f)
        if not callable(value):
            out.append((f, _plain(value, depth + 1)))
    return (type(obj).__name__, tuple(out)) if out else repr(obj)


def path_details(app: coco.App[Any, Any]) -> dict[Path, Any]:
    async def collect() -> dict[Path, Any]:
        out: dict[Path, Any] = {}
        async for d in coco_inspect.iter_stable_path_details(app):
            out[_path_of(coco.StablePath(d.path))] = (
                str(d.node_type),
                d.version,
                d.processor_name,
                d.has_memoization,
                sorted(repr(_plain(i)) for i in d.target_state_items),
            )
        return out

    return asyncio.run(collect())


# ---------------------------------------------------------------------------
# Running a seed
# ---------------------------------------------------------------------------


class Failure(AssertionError):
    pass


@dataclass
class Snapshot:
    raw: dict[bytes, bytes] | None
    details: dict[Path, Any]
    external: dict[tuple[str, ...], Any]
    owners: dict[tuple[str, ...], Path]


class Runner:
    def __init__(self, tag: str, raw_reader: RawReader | None) -> None:
        self.env = common.create_test_env(__file__, suffix=tag)
        self.env_dir = str(
            get_env_db_path(f"core__test_component_selector_fuzz__{tag}") / "mdb"
        )
        self.raw_reader = raw_reader
        self.tag = tag
        self.apps: dict[str, coco.App[Any, Any]] = {}
        self.log: list[str] = []

    def app(self, side: str) -> coco.App[Any, Any]:
        if side not in self.apps:
            self.apps[side] = coco.App(
                coco.AppConfig(name=f"fz_{side}", environment=self.env), app_main, side
            )
        return self.apps[side]

    def snapshot(self, side: str) -> Snapshot:
        app = self.app(side)
        raw = (
            self.raw_reader.rows(self.env_dir, f"fz_{side}")
            if self.raw_reader is not None
            else None
        )
        return Snapshot(raw, path_details(app), external(side), owners(app, side))

    def update(self, side: str, step: Step) -> Exception | None:
        app = self.app(side)
        kwargs: dict[str, Any] = {"full_reprocess": step.full_reprocess}
        # COCO_FUZZ_MUTANT=ignore_selector checks the checker: it must fail.
        mutant = os.environ.get("COCO_FUZZ_MUTANT") == "ignore_selector"
        if side == "sut" and step.mode != "plain" and not mutant:
            kwargs["component_selector"] = [_to_stable_path(p) for p in step.selector]
        if side == "sut" and step.mode == "preview":
            kwargs["preview"] = True
        try:
            app.update_blocking(**kwargs)
        except Exception as e:  # noqa: BLE001
            return e
        return None


def _to_stable_path(p: Path) -> coco.StablePath:
    out = coco.ROOT_PATH
    for k in p:
        out = out / k
    return out


def _reset_targets() -> None:
    for side in SIDES:
        FLAT[side].clear()
        FLAT[side].sink_exception = False
        CONT[side].clear()
        CONT[side].sink_exception = False
    FAIL.clear()


def check_unselected_untouched(
    step: Step, before: Snapshot, after: Snapshot, preview: bool
) -> None:
    sel = step.selector
    always = use_mount_paths(PROGRAM)

    def unselected(subject: Path) -> bool:
        return selection(sel, subject, always) == "unselected"

    # A selected component that declares a target state an unselected one owns
    # takes it over, as in a plain update; the takeover removes the item from
    # the old owner's tracking record. That edit, and only it, is allowed.
    taken_from = {
        before.owners[k]
        for k in before.owners
        if k in after.owners
        and after.owners[k] != before.owners[k]
        and unselected(before.owners[k])
        and not unselected(after.owners[k])
    }

    def judged(subject: Path | None, entry: str = "") -> bool:
        if preview or subject is None:
            return True
        if entry == "0x40" and subject in taken_from:
            STATS["takeover edits of an unselected tracking record"] += 1
            return False
        if entry == "0x20" and any(t[: len(subject)] == subject for t in taken_from):
            # A takeover drops the component memo of the old owner and of each
            # ancestor: it no longer matches the tracking record.
            STATS["takeover drops of an unselected component memo"] += 1
            return False
        return unselected(subject)

    if before.raw is not None and after.raw is not None:
        for key in before.raw:
            row = classify_row(key, before.raw[key])
            if row.kind != "global" and judged(row.subject):
                STATS["unselected rows byte-compared"] += 1
        for key in sorted(set(before.raw) | set(after.raw)):
            b, a = before.raw.get(key), after.raw.get(key)
            if a == b:
                continue
            row = classify_row(key, b if b is not None else a)  # type: ignore[arg-type]
            if row.kind == "global" and not preview:
                continue
            if row.kind == "global" and row.entry == "0x28":
                continue  # segment names: write-once, harmless
            if row.kind == "target_state" and b is not None and a is not None:
                after_owner = classify_row(key, a).subject
                if (
                    not preview
                    and row.subject in taken_from
                    and after_owner is not None
                    and not unselected(after_owner)
                ):
                    continue
            if judged(row.subject, row.entry):
                change = "added" if b is None else "removed" if a is None else "changed"
                raise Failure(
                    f"LMDB row {row.entry} for unselected {row.subject} {change}"
                    f" (key {key.hex()})"
                )
    else:
        for path in sorted(set(before.details) | set(after.details), key=repr):
            if any(t[: len(path)] == path for t in taken_from) and not preview:
                continue  # A takeover edits the old owner and drops memos up the chain.
            if before.details.get(path) != after.details.get(path) and judged(path):
                raise Failure(
                    f"stable path detail of unselected {path} changed:"
                    f" {before.details.get(path)} -> {after.details.get(path)}"
                )

    for ext_key in sorted(set(before.external) | set(after.external), key=repr):
        b, a = before.external.get(ext_key), after.external.get(ext_key)
        if a == b and before.owners.get(ext_key) == after.owners.get(ext_key):
            continue
        owner_paths = {
            p for p in (before.owners.get(ext_key), after.owners.get(ext_key)) if p
        }
        if before.owners.get(ext_key) in taken_from and not unselected(
            after.owners.get(ext_key, ())
        ):
            continue
        if (
            not preview
            and ext_key[0] == "cont"
            and len(ext_key) == 3
            and ("cont", ext_key[1]) not in after.external
        ):
            # A selected container owner that is gone takes the rows with it.
            # Nothing can declare under it any more; the next plain update
            # cleans up their tracking (checked against replay).
            STATS["container rows gone with their selected, deleted owner"] += 1
            continue
        if preview or any(unselected(p) for p in owner_paths):
            raise Failure(
                f"external target state {ext_key} of unselected owner {owner_paths}:"
                f" {b} -> {a}; owner {before.owners.get(ext_key)} ->"
                f" {after.owners.get(ext_key)}"
            )


def check_reached_like_replay(
    step: Step, before: Snapshot, sut: Snapshot, replay: Snapshot
) -> None:
    """What a selective update runs ends as a plain update leaves it: every
    target state owned, on either side, by a component the update reached
    (selected, an ancestor of a selected path, a use_mount child of the root)
    is the same on both sides, with the same owner."""
    sel = step.selector
    always = use_mount_paths(PROGRAM)

    dirs = model_dirs(PROGRAM)
    mounted = model_components(PROGRAM) | dirs

    def reached(path: Path | None) -> bool:
        """The update runs the component at `path`, or deletes it. A path the
        program no longer mounts is deleted when the walk from the root gets
        to it: each ancestor still mounted, covered by the selector, or a
        directory on disk. An ancestor that is a component on disk and no
        longer mounted stays whole, and so does what lies below it."""
        if path is None or selection(sel, path, always) == "unselected":
            return False
        if path in mounted and path not in dirs:
            return True
        if selection(sel, path, always) != "selected":
            # Gone, and only an ancestor of a selected path: kept whole.
            return False
        for i in range(1, len(path)):
            a = path[:i]
            if a in mounted:
                continue
            if selection(sel, a, always) == "selected":
                return True
            detail = before.details.get(a)
            # A component whose delete failed has lost its entry in its
            # parent, so inspect lists it as a directory; its processor
            # name still marks it a component.
            if detail is not None and ("Component" in detail[0] or detail[2]):
                return False
        return True

    keys = (
        set(sut.external) | set(replay.external) | set(sut.owners) | set(replay.owners)
    )
    for k in sorted(keys, key=repr):
        if k[0] == "cont" and len(k) == 2:
            continue
        so, ro = sut.owners.get(k), replay.owners.get(k)
        if not (reached(so) or reached(ro)):
            continue
        # An unselected component that still declares the key wins it in the
        # replay only because the replay ran it.
        if (so is not None and not reached(so)) or (ro is not None and not reached(ro)):
            continue
        STATS["reached target states compared with replay"] += 1
        if sut.external.get(k) != replay.external.get(k) or so != ro:
            raise Failure(
                f"reached target state {k}: sut {sut.external.get(k)} owned by {so},"
                f" replay {replay.external.get(k)} owned by {ro}"
            )


def row_shape(raw: dict[bytes, bytes] | None) -> set[tuple[Any, ...]]:
    if raw is None:
        return set()
    out: set[tuple[Any, ...]] = set()
    for k, v in raw.items():
        row = classify_row(k, v)
        if row.kind == "path":
            out.add((row.entry, row.subject))
        elif row.kind == "target_state":
            out.add(("ts_owner", row.subject))
    return out


def compare_sides(r: Runner, a: str, b: str, where: str) -> None:
    sa, sb = r.snapshot(a), r.snapshot(b)
    if sa.external != sb.external:
        diff = {
            k: (sa.external.get(k), sb.external.get(k))
            for k in set(sa.external) | set(sb.external)
            if sa.external.get(k) != sb.external.get(k)
        }
        raise Failure(f"{where}: external target states of {a} != {b}: {diff}")
    if sa.owners != sb.owners:
        diff = {
            k: (sa.owners.get(k), sb.owners.get(k))
            for k in set(sa.owners) | set(sb.owners)
            if sa.owners.get(k) != sb.owners.get(k)
        }
        raise Failure(f"{where}: target-state owners of {a} != {b}: {diff}")
    pa = set(sa.details)
    pb = set(sb.details)
    if pa != pb:
        raise Failure(
            f"{where}: stable paths of {a} - {b}: {pa - pb}; {b} - {a}: {pb - pa}"
        )
    ra, rb = row_shape(sa.raw), row_shape(sb.raw)
    # Function memos depend on which runs happened; component memos on whether
    # an earlier run left one. Compare the rest.
    keep = {"0x40", "child_existence", "tombstone", "0x34", "ts_owner"}
    ra = {x for x in ra if x[0] in keep}
    rb = {x for x in rb if x[0] in keep}
    if ra != rb:
        raise Failure(
            f"{where}: LMDB rows of {a} - {b}: {sorted(ra - rb, key=repr)}; {b} - {a}: {sorted(rb - ra, key=repr)}"
        )


def run_steps(steps: list[Step], tag: str, raw_reader: RawReader | None) -> list[str]:
    """Run `steps`; raise Failure on a contract violation. Returns the log."""
    global PROGRAM
    PROGRAM = Program()
    EXT.clear()
    _reset_targets()
    r = Runner(tag, raw_reader)
    try:
        for i, step in enumerate(steps):
            if step.op != "update":
                apply_program_step(PROGRAM, step)
                continue
            where = f"step {i} ({step})"
            injected = bool(step.inject) and not step.inject.startswith("schema")
            # replay: plain, without failures; previews are skipped.
            if step.mode != "preview" and not injected:
                if step.inject.startswith("schema"):
                    CONT["replay"].child_invalidation = step.inject.split(":")[1]  # type: ignore[assignment]
                err = r.update("replay", step)
                CONT["replay"].child_invalidation = None
                if err is not None:
                    raise Failure(f"{where}: replay failed: {err!r}")
            before = r.snapshot("sut")
            if step.inject == "sink":
                FLAT["sut"].sink_exception = True
                CONT["sut"].sink_exception = True
            elif step.inject == "body":
                FAIL.add(("sut", step.fail_path))
            elif step.inject.startswith("schema"):
                CONT["sut"].child_invalidation = step.inject.split(":")[1]  # type: ignore[assignment]
            err = r.update("sut", step)
            STATS[
                f"sut {step.mode}{' full_reprocess' if step.full_reprocess else ''}{' inject=' + step.inject.split(':')[0] if step.inject else ''}{' (failed)' if err else ''}"
            ] += 1
            FLAT["sut"].sink_exception = False
            CONT["sut"].sink_exception = False
            CONT["sut"].child_invalidation = None
            FAIL.clear()
            r.log.append(f"{where}: {'error ' + repr(err)[:120] if err else 'ok'}")
            # A selective update refuses a container change, as it would leave
            # the unselected rows behind. Nothing of the change may apply.
            refused = (
                err is not None
                and step.mode in ("selective", "preview")
                and step.inject.startswith("schema")
                and "without component_selector" in str(err)
            )
            if refused:
                STATS["container changes refused under a selector"] += 1
            if err is not None and not injected and not refused:
                raise Failure(f"{where}: sut failed without injection: {err!r}")
            after = r.snapshot("sut")
            if step.mode in ("selective", "preview"):
                check_unselected_untouched(step, before, after, step.mode == "preview")
            if step.mode == "selective" and not injected and not refused:
                check_reached_like_replay(step, before, after, r.snapshot("replay"))
            if step.mode == "plain" and not injected:
                compare_sides(r, "sut", "replay", where)
                STATS["plain steps compared with replay"] += 1
        # The end: one plain update on each side, and a fresh app.
        final = Step("update", mode="plain")
        for side in ("sut", "replay", "fresh"):
            err = r.update(side, final)
            if err is not None:
                raise Failure(f"final plain update of {side} failed: {err!r}")
        compare_sides(r, "sut", "replay", "final")
        sut, fresh = r.snapshot("sut"), r.snapshot("fresh")
        if sut.external != fresh.external:
            raise Failure(f"final: sut != fresh: {sut.external} vs {fresh.external}")
        if sut.owners != fresh.owners:
            raise Failure(f"final: owners sut != fresh: {sut.owners} vs {fresh.owners}")
        STATS["seeds passed"] += 1
        return r.log
    finally:
        r.env.close()


_attempt = 0


def run_seed(
    seed: int, n_steps: int, raw_reader: RawReader | None, shrink: bool = True
) -> None:
    global _attempt
    steps = generate(seed, n_steps)
    try:
        _attempt += 1
        run_steps(steps, f"s{seed}_{_attempt}", raw_reader)
        return
    except Failure as first:
        if not shrink:
            raise
        minimal, message = shrink_steps(steps, raw_reader, str(first))
        lines = "\n".join(f"  {s}" for s in minimal)
        raise Failure(
            f"seed {seed}: {first}\nminimal steps ({len(minimal)}):\n{lines}\n"
            f"minimal failure: {message}"
        ) from None


def _fails(steps: list[Step], raw_reader: RawReader | None) -> str | None:
    global _attempt
    _attempt += 1
    try:
        run_steps(steps, f"shrink_{_attempt}", raw_reader)
    except Failure as e:
        return str(e)
    except Exception as e:  # noqa: BLE001
        return f"crash: {e!r}"
    return None


def _signature(message: str) -> str:
    """The kind of failure, so a shrink does not wander to a different bug."""
    for marker in (
        "LMDB row",
        "external target state",
        "external target states",
        "target-state owners",
        "stable paths",
        "LMDB rows",
        "failed",
        "crash",
    ):
        if marker in message:
            return marker
    return message[:40]


def shrink_steps(
    steps: list[Step], raw_reader: RawReader | None, message: str
) -> tuple[list[Step], str]:
    sig = _signature(message)
    chunk = max(1, len(steps) // 2)
    while chunk >= 1:
        i = 0
        changed = False
        while i < len(steps):
            candidate = steps[:i] + steps[i + chunk :]
            got = _fails(candidate, raw_reader)
            if got is not None and _signature(got) == sig:
                steps, message, changed = candidate, got, True
            else:
                i += chunk
        if not changed:
            chunk //= 2
    return steps, message


def _seeds() -> list[int]:
    spec = os.environ.get("COCO_FUZZ_SEEDS", "0-11")
    if "-" in spec:
        lo, hi = spec.split("-")
        return list(range(int(lo), int(hi) + 1))
    return [int(s) for s in spec.split(",")]


_RAW_READER: RawReader | None = None


def _raw_reader() -> RawReader | None:
    global _RAW_READER
    command = os.environ.get("COCO_FUZZ_LMDB_SERVER")
    if command and _RAW_READER is None:
        _RAW_READER = RawReader(command)
    return _RAW_READER


@pytest.mark.parametrize("seed", _seeds())
def test_selective_updates_against_a_plain_replay(seed: int) -> None:
    run_seed(seed, int(os.environ.get("COCO_FUZZ_STEPS", "25")), _raw_reader())


if __name__ == "__main__":
    # python -m tests.core.test_component_selector_fuzz SEEDS STEPS
    import sys
    import time

    seeds_spec, n = sys.argv[1], int(sys.argv[2])
    os.environ["COCO_FUZZ_SEEDS"] = seeds_spec
    start = time.time()
    failures: list[str] = []
    for seed in _seeds():
        try:
            run_seed(seed, n, _raw_reader())
        except Failure as e:
            failures.append(str(e))
            print(f"FAIL {e}", flush=True)
    print(f"{time.time() - start:.0f}s", dict(sorted(STATS.items())), flush=True)
    print(f"{len(failures)} failing seeds", flush=True)
