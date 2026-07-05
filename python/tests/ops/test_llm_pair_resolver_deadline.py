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
async def test_llm_resolver_with_deadline_retries_until_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Active CocoIndex deadline:
    #
    # retries=0 is ignored as a retry budget
    # invalid outputs sleep/back off while time remains
    # valid output before deadline returns successfully
    real_sleep = asyncio.sleep
    monkeypatch.setenv("COCOINDEX_TESTING", "1")
    clock = _FakeClock(real_sleep)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)
    module = _load_resolver_module(monkeypatch)
    completions = _FakeCompletions(module, ["ghost", "ghost", "ghost", "bar"])
    resolver = module.LlmPairResolver(model="fake", retries=0)
    resolver._client = _FakeClient(completions)

    with coco.timeout(timedelta(seconds=10)):
        result = await resolver("foo", ["bar", "baz"])

    assert result == module._PairDecision(
        matched="bar", canonical=module._CanonicalSide.MATCHED
    )
    assert completions.calls == 4
    assert clock.sleeps == [1.0, 1.0, 1.0]
    core.testing_disable_deadline_clock()
