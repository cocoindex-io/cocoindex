"""Lightweight tests for ``LiteLLMTranscriber`` (mocked LiteLLM)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("litellm", reason="litellm not installed")

from cocoindex.ops.litellm import LiteLLMTranscriber  # noqa: E402
from cocoindex.resources.file import FileLike  # noqa: E402


@pytest.mark.asyncio
async def test_litellm_transcriber_reads_file_and_merges_kwargs() -> None:
    audio_bytes = b"fake-audio"
    file_like = cast(
        FileLike[Any],
        SimpleNamespace(
            file_path=SimpleNamespace(name="segment.mp3"),
            read=AsyncMock(return_value=audio_bytes),
        ),
    )
    transcriber = LiteLLMTranscriber("fake-model", api_key="k-default", language="en")

    fake_response = type("R", (), {"text": "hello world"})()

    with patch(
        "cocoindex.ops.litellm.litellm.transcription",
        new=MagicMock(return_value=fake_response),
    ) as mocked:
        text = await transcriber.transcribe(file_like, response_format="verbose_json")

    assert text == "hello world"
    mocked.assert_called_once()
    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["model"] == "fake-model"
    assert call_kwargs["api_key"] == "k-default"
    assert call_kwargs["language"] == "en"
    assert call_kwargs["response_format"] == "verbose_json"

    sent_file = call_kwargs["file"]
    assert sent_file.name == "segment.mp3"
    assert sent_file.read() == audio_bytes
