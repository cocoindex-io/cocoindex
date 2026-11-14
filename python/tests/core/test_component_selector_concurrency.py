"""Sliced execution: parallel selective, plain and preview updates."""

from __future__ import annotations

import asyncio
import os
import random
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget

ITERS = int(os.environ.get("SELECTOR_STRESS_ITERS", "40"))


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


class _Model:
    """Source: app -> group -> key -> value. Target key "<app>|<group>|<key>"."""

    def __init__(self, app_name: str, rng: random.Random) -> None:
        self.app_name = app_name
        self.rng = rng
        self.src: dict[str, dict[str, int]] = {
            f"g{g}": {f"k{k}": 0 for k in range(6)} for g in range(4)
        }
        self.flat: dict[str, int] = {f"k{k}": 0 for k in range(10)}

    def expected(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for g, items in self.src.items():
            for k, v in items.items():
                out[f"{self.app_name}|{g}|{k}"] = v
        for k, v in self.flat.items():
            out[f"{self.app_name}|flat|{k}"] = v
        return out

    def mutate(self) -> None:
        rng = self.rng
        for _ in range(rng.randint(1, 6)):
            r = rng.random()
            if r < 0.15 and self.src:
                del self.src[rng.choice(sorted(self.src))]
            elif r < 0.25:
                self.src[f"g{rng.randint(0, 6)}"] = {
                    f"k{rng.randint(0, 3)}": rng.randint(0, 99)
                }
            elif r < 0.6 and self.src:
                g = rng.choice(sorted(self.src))
                k = f"k{rng.randint(0, 8)}"
                if k in self.src[g] and rng.random() < 0.3:
                    del self.src[g][k]
                else:
                    self.src[g][k] = rng.randint(0, 99)
            else:
                k = f"k{rng.randint(0, 14)}"
                if k in self.flat and rng.random() < 0.3:
                    del self.flat[k]
                else:
                    self.flat[k] = rng.randint(0, 99)

    def random_selector(self) -> tuple[list[coco.StablePath], list[tuple[str, ...]]]:
        rng = self.rng
        paths: list[coco.StablePath] = []
        raw: list[tuple[str, ...]] = []
        for _ in range(rng.randint(0, 4)):
            r = rng.random()
            if r < 0.3:
                g = f"g{rng.randint(0, 6)}"
                raw.append(("groups", g))
            elif r < 0.7:
                raw.append(("groups", f"g{rng.randint(0, 6)}", f"k{rng.randint(0, 8)}"))
            elif r < 0.95:
                raw.append(("flat", f"k{rng.randint(0, 14)}"))
            else:
                raw.append(("groups",))
        for parts in raw:
            p = coco.ROOT_PATH
            for part in parts:
                p = p / part
            paths.append(p)
        return paths, raw


def _covered(raw: list[tuple[str, ...]], key: str) -> bool:
    _, group, item = key.split("|")
    parts = ("flat", item) if group == "flat" else ("groups", group, item)
    return any(parts[: len(sel)] == sel for sel in raw)


def _make_app(
    env: coco.Environment, model: _Model, max_inflight: int | None
) -> coco.App[Any, Any]:
    name = model.app_name

    @coco.fn
    async def item(group: str, key: str, value: int) -> None:
        if value % 7 == 3:
            await asyncio.sleep(0.001 * (value % 5))
        coco.declare_target_state(
            GlobalDictTarget.target_state(f"{name}|{group}|{key}", value)
        )

    @coco.fn(memo=True)
    async def memo_item(group: str, key: str, value: int) -> None:
        coco.declare_target_state(
            GlobalDictTarget.target_state(f"{name}|{group}|{key}", value)
        )

    @coco.fn
    async def group_fn(group: str, items: dict[str, int]) -> None:
        for key, value in sorted(items.items()):
            await coco.mount(coco.component_subpath(key), item, group, key, value)

    @coco.fn
    async def main() -> None:
        snapshot = {g: dict(items) for g, items in model.src.items()}
        flat = dict(model.flat)
        with coco.component_subpath("groups"):
            for g, items in sorted(snapshot.items()):
                await coco.mount(coco.component_subpath(g), group_fn, g, items)
        with coco.component_subpath("flat"):
            for k, v in sorted(flat.items()):
                await coco.mount(coco.component_subpath(k), memo_item, "flat", k, v)

    return coco.App(
        coco.AppConfig(
            name=f"selector_conc_{name}",
            environment=env,
            max_inflight_components=max_inflight,
        ),
        main,
    )


async def _batch(
    app: coco.App[Any, Any], model: _Model, allow_plain: bool
) -> list[list[tuple[str, ...]]]:
    """Run a random parallel batch; returns raw selectors of non-preview selective runs
    (or [[()]] meaning a plain run happened)."""
    rng = model.rng
    coros = []
    applied: list[list[tuple[str, ...]]] = []
    for _ in range(rng.randint(2, 5)):
        r = rng.random()
        if r < 0.55:
            paths, raw = model.random_selector()
            coros.append(app.update(component_selector=paths).result())
            applied.append(raw)
        elif r < 0.7 and allow_plain:
            coros.append(app.update().result())
            applied.append([()])
        elif r < 0.85:
            paths, raw = model.random_selector()
            coros.append(app.update(preview=True, component_selector=paths).result())
        else:
            coros.append(app.update(preview=True).result())
        if rng.random() < 0.2:
            fpaths, fraw = model.random_selector()
            coros.append(
                app.update(full_reprocess=True, component_selector=fpaths).result()
            )
            applied.append(fraw)
    results = await asyncio.wait_for(asyncio.gather(*coros, return_exceptions=True), 60)
    errors = [r for r in results if isinstance(r, BaseException)]
    assert not errors, errors
    return applied


def _check_batch(
    before: dict[str, Any],
    model: _Model,
    applied: list[list[tuple[str, ...]]],
    app_name: str,
) -> None:
    after = {k: v for k, v in _stored().items() if k.startswith(app_name + "|")}
    exp = model.expected()
    if any(raw == [()] for raw in applied):
        assert after == exp, "plain run in batch must converge"
        return
    sels = [s for raw in applied for s in raw]
    for key in set(before) | set(exp) | set(after):
        _, group, _item = key.split("|")
        group_gone_uncovered = (
            group != "flat"
            and group not in model.src
            and not any(("groups", group)[: len(sel)] == sel for sel in sels)
        )
        if _covered(sels, key) and not group_gone_uncovered:
            assert after.get(key) == exp.get(key), (
                key,
                before.get(key),
                exp.get(key),
                after.get(key),
                sels,
            )
        else:
            assert after.get(key) == before.get(key), (
                "unselected changed",
                key,
                before.get(key),
                after.get(key),
                sels,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("max_inflight", [None, 1, 2])
async def test_stress_parallel_selective_plain_preview_one_app(
    request: pytest.FixtureRequest, max_inflight: int | None
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    rng = random.Random(
        int(os.environ.get("SELECTOR_STRESS_SEED", "0")) * 1000
        + 1234
        + (max_inflight or 0)
    )
    model = _Model("a1", rng)
    app = _make_app(env, model, max_inflight)
    await app.update()
    assert _stored() == model.expected()
    for it in range(ITERS):
        model.mutate()
        before = dict(_stored())
        allow_plain = it % 3 == 0
        applied = await _batch(app, model, allow_plain)
        _check_batch(before, model, applied, "a1")
        if it % 5 == 4:
            await app.update()
            assert _stored() == model.expected(), (
                f"iter {it}: plain run did not converge"
            )
    await app.update()
    assert _stored() == model.expected()


@pytest.mark.asyncio
async def test_stress_two_apps_one_env(request: pytest.FixtureRequest) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    seed = int(os.environ.get("SELECTOR_STRESS_SEED", "0")) * 1000
    m1, m2 = (
        _Model("x1", random.Random(seed + 7)),
        _Model("x2", random.Random(seed + 8)),
    )
    a1, a2 = _make_app(env, m1, None), _make_app(env, m2, 1)
    await asyncio.gather(a1.update().result(), a2.update().result())
    for it in range(ITERS):
        m1.mutate()
        m2.mutate()
        b1 = {k: v for k, v in _stored().items() if k.startswith("x1|")}
        b2 = {k: v for k, v in _stored().items() if k.startswith("x2|")}
        r1, r2 = await asyncio.gather(
            _batch(a1, m1, it % 4 == 0), _batch(a2, m2, it % 4 == 1)
        )
        _check_batch(b1, m1, r1, "x1")
        _check_batch(b2, m2, r2, "x2")
    await asyncio.gather(a1.update().result(), a2.update().result())
    exp = {**m1.expected(), **m2.expected()}
    assert _stored() == exp


@pytest.mark.asyncio
async def test_drop_after_selective_runs_removes_everything(
    request: pytest.FixtureRequest,
) -> None:
    env = common.create_test_env(__file__, suffix=request.node.name)
    model = _Model("d2", random.Random(5))
    app = _make_app(env, model, None)
    await app.update()
    for _ in range(10):
        model.mutate()
        await app.update(component_selector=model.random_selector()[0])
    await app.drop()
    assert _stored() == {}
