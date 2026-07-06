"""Prove _retry_litellm_call delegates to the shared retry helper with the
policy that preserves its historical behavior. Loads litellm.py with a fake
`litellm` module so the test runs without the optional dependency."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("numpy")


def _load_litellm_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    fake_litellm: Any = types.ModuleType("litellm")
    fake_litellm.aembedding = object()
    fake_litellm.atranscription = object()
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)

    module_path = pathlib.Path(__file__).parents[2] / "cocoindex" / "ops" / "litellm.py"
    module_name = f"_test_litellm_delegation_{id(monkeypatch)}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_retry_litellm_call_delegates_with_historical_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_litellm_module(monkeypatch)
    captured: dict[str, Any] = {}

    async def fake_retry_transient(fn: Any, **kwargs: Any) -> str:
        captured.update(kwargs)
        return "result"

    monkeypatch.setattr(module._deadline, "retry_transient", fake_retry_transient)

    async def op() -> str:
        return "unused"

    result = await module._retry_litellm_call(op, "embedding call")
    assert result == "result"

    # Time is the brake (no attempt cap), 10-minute budget, bounded attempts,
    # the transient-classification predicate, and the historical backoff
    # schedule (1s doubling, capped at 30s).
    assert captured["max_attempts"] is None
    assert captured["budget"] == timedelta(seconds=600)
    assert captured["bound_attempt"] is True
    assert captured["retry_on"] is module._is_retryable_litellm_error
    assert captured["operation_name"] == "embedding call"
    backoff = captured["backoff"]
    assert [backoff(n) for n in (0, 1, 2, 5, 10)] == [1.0, 2.0, 4.0, 30.0, 30.0]
