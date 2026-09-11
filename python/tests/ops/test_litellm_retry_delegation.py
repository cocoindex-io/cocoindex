"""Prove _retry_litellm_call delegates to the shared retry helper with the
policy that preserves its historical behavior, and that both classes' per-
instance ``timeout`` reaches it. Runs against the stub
``litellm`` module from the ``litellm_module`` fixture, so it needs neither
the optional dependency nor a live backend."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("numpy")


@pytest.mark.asyncio
async def test_retry_litellm_call_delegates_with_historical_policy(
    litellm_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = litellm_module
    captured: dict[str, Any] = {}

    async def fake_retry_transient(fn: Any, **kwargs: Any) -> str:
        captured.update(kwargs)
        return "result"

    monkeypatch.setattr(module._deadline, "retry_transient", fake_retry_transient)

    async def op() -> str:
        return "unused"

    result = await module._retry_litellm_call(
        op, "embedding call", timedelta(seconds=600)
    )
    assert result == "result"

    # Time is the brake (no attempt cap), the caller's deadline scope,
    # bounded attempts, the transient-classification predicate, and the
    # historical backoff schedule (1s doubling, capped at 30s).
    assert "max_attempts" not in captured  # default None: no attempt cap
    assert captured["timeout"] == timedelta(seconds=600)
    assert captured["bound_attempt"] is True
    assert captured["retry_on"] is module._is_retryable_litellm_error
    assert captured["operation_name"] == "embedding call"
    backoff = captured["backoff"]  # stateful: successive calls advance it
    assert [backoff(n) for n in range(7)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_embedder_timeout_reaches_retry_transient(
    litellm_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The per-instance timeout is what bounds the embedder's retry loop;
    omitting it falls back to the 10-minute default."""
    module = litellm_module
    captured: list[timedelta | None] = []

    async def fake_retry_transient(fn: Any, **kwargs: Any) -> str:
        captured.append(kwargs["timeout"])
        return "result"

    monkeypatch.setattr(module._deadline, "retry_transient", fake_retry_transient)

    await module.LiteLLMEmbedder(
        "fake-model", timeout=timedelta(seconds=42)
    )._aembedding_with_retry(["hello"])
    await module.LiteLLMEmbedder("fake-model", timeout=42)._aembedding_with_retry(
        ["hello"]
    )
    await module.LiteLLMEmbedder("fake-model")._aembedding_with_retry(["hello"])

    assert captured == [
        timedelta(seconds=42),
        timedelta(seconds=42),  # seconds as a number, converted
        timedelta(minutes=10),
    ]


@pytest.mark.asyncio
async def test_transcriber_timeout_reaches_retry_transient(
    litellm_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same wiring on the transcription side, under its own operation name."""
    module = litellm_module
    captured: list[dict[str, Any]] = []

    async def fake_retry_transient(fn: Any, **kwargs: Any) -> Any:
        captured.append(kwargs)
        return SimpleNamespace(text="hello world")

    monkeypatch.setattr(module._deadline, "retry_transient", fake_retry_transient)

    def audio_file() -> Any:
        return SimpleNamespace(
            file_path=SimpleNamespace(name="segment.mp3"),
            read=AsyncMock(return_value=b"fake-audio"),
        )

    await module.LiteLLMTranscriber(
        "fake-model", timeout=timedelta(seconds=42)
    ).transcribe(audio_file())
    await module.LiteLLMTranscriber("fake-model", timeout=42).transcribe(audio_file())
    await module.LiteLLMTranscriber("fake-model").transcribe(audio_file())

    assert [c["timeout"] for c in captured] == [
        timedelta(seconds=42),
        timedelta(seconds=42),  # seconds as a number, converted
        timedelta(minutes=10),
    ]
    assert {c["operation_name"] for c in captured} == {"litellm.atranscription"}
