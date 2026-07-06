from __future__ import annotations

import importlib.util
import asyncio
import pathlib
import sys
import types
from datetime import timedelta
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal import core


class _FakeClock:
    def __init__(self, real_sleep: Any) -> None:
        self._now = 0.0
        self.sleeps: list[float] = []
        self._real_sleep = real_sleep
        core.testing_reset_deadline_clock()

    @property
    def now(self) -> float:
        return self._now

    @now.setter
    def now(self, value: float) -> None:
        if value < self._now:
            core.testing_reset_deadline_clock()
            self._now = 0.0
        delta = value - self._now
        if delta:
            core.testing_advance_deadline_clock(round(delta * 1000))
        self._now = value

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay
        await self._real_sleep(0)


class _FakeCompletions:
    def __init__(self, module: Any, responses: list[str | None]) -> None:
        self._module = module
        self._responses = responses
        self.calls = 0

    async def create(self, **kwargs: object) -> Any:
        self.calls += 1
        idx = min(self.calls - 1, len(self._responses) - 1)
        return self._module._LlmResponse(matched=self._responses[idx])


class _FakeClient:
    def __init__(self, completions: _FakeCompletions) -> None:
        self.chat = types.SimpleNamespace(completions=completions)


def _load_resolver_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    fake_instructor: Any = types.ModuleType("instructor")
    fake_instructor.Mode = types.SimpleNamespace(JSON=object())
    fake_instructor.from_litellm = lambda completion, mode: None
    fake_litellm: Any = types.ModuleType("litellm")
    fake_litellm.acompletion = object()
    fake_faiss: Any = types.ModuleType("faiss")
    fake_faiss.IndexFlatIP = object
    fake_faiss.normalize_L2 = lambda vec: None
    monkeypatch.setitem(sys.modules, "instructor", fake_instructor)
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)
    monkeypatch.setitem(sys.modules, "faiss", fake_faiss)

    module_path = (
        pathlib.Path(__file__).parents[2]
        / "cocoindex"
        / "ops"
        / "entity_resolution"
        / "llm_resolver.py"
    )
    module_name = f"_test_llm_resolver_deadline_{id(monkeypatch)}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_llm_resolver_without_deadline_keeps_numeric_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No CocoIndex deadline:
    #
    # initial attempt + configured retries = 1 + 2 calls
    # all attempts return invalid "ghost" -> resolver gives up normally
    module = _load_resolver_module(monkeypatch)
    completions = _FakeCompletions(module, ["ghost"])
    resolver = module.LlmPairResolver(model="fake", retries=2)
    resolver._client = _FakeClient(completions)

    result = await resolver("foo", ["bar", "baz"])

    assert result == module._PairDecision()
    assert completions.calls == 3


@pytest.mark.asyncio
async def test_llm_resolver_deadline_never_extends_the_retry_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Active CocoIndex deadline, monotone semantics:
    #
    # the attempt cap ALWAYS applies; a generous deadline never extends it.
    # retries=0 -> exactly one call, then the empty-decision fallback, even
    # though 10s of deadline budget remains.
    real_sleep = asyncio.sleep
    monkeypatch.setenv("COCOINDEX_TESTING", "1")
    clock = _FakeClock(real_sleep)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)
    module = _load_resolver_module(monkeypatch)
    completions = _FakeCompletions(module, ["ghost"])
    resolver = module.LlmPairResolver(model="fake", retries=0)
    resolver._client = _FakeClient(completions)

    with coco.timeout(timedelta(seconds=10)):
        result = await resolver("foo", ["bar", "baz"])

    assert result == module._PairDecision()
    assert completions.calls == 1
    core.testing_disable_deadline_clock()


@pytest.mark.asyncio
async def test_llm_resolver_retry_effort_is_monotone_in_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Cross-mode property (the fix for "removing the deadline gives less
    # retry duration"): attempts with a deadline are never more than
    # attempts without one, for the same failing input.
    real_sleep = asyncio.sleep
    monkeypatch.setenv("COCOINDEX_TESTING", "1")
    clock = _FakeClock(real_sleep)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)
    module = _load_resolver_module(monkeypatch)

    async def run(with_deadline: bool) -> int:
        completions = _FakeCompletions(module, ["ghost"])
        resolver = module.LlmPairResolver(model="fake", retries=2)
        resolver._client = _FakeClient(completions)
        if with_deadline:
            with coco.timeout(timedelta(seconds=10)):
                result = await resolver("foo", ["bar", "baz"])
        else:
            result = await resolver("foo", ["bar", "baz"])
        assert result == module._PairDecision()
        return completions.calls

    without = await run(with_deadline=False)
    with_deadline = await run(with_deadline=True)
    assert with_deadline <= without == 3
    core.testing_disable_deadline_clock()


@pytest.mark.asyncio
async def test_llm_resolver_expired_deadline_stops_retries_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A deadline that expires mid-retry surfaces DeadlineExceededError from
    # the retry checkpoint before the next attempt starts.
    real_sleep = asyncio.sleep
    monkeypatch.setenv("COCOINDEX_TESTING", "1")
    clock = _FakeClock(real_sleep)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)
    module = _load_resolver_module(monkeypatch)

    class _ExpiringCompletions(_FakeCompletions):
        async def create(self, **kwargs: object) -> Any:
            result = await super().create(**kwargs)
            clock.now += 6.0  # each attempt burns 6s of virtual time
            return result

    completions = _ExpiringCompletions(module, ["ghost"])
    resolver = module.LlmPairResolver(model="fake", retries=5)
    resolver._client = _FakeClient(completions)

    with coco.timeout(timedelta(seconds=10)):
        with pytest.raises(coco.DeadlineExceededError):
            await resolver("foo", ["bar", "baz"])

    assert completions.calls == 2  # 6s, 12s > 10s: third attempt never starts
    core.testing_disable_deadline_clock()
