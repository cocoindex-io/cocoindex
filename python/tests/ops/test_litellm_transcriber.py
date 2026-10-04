"""``LiteLLMTranscriber``: request plumbing, and the one clock on a request.

``timeout`` bounds a transcription end to end — fast failures (429, 5xx, a
dropped connection) are retried with backoff inside it, a timeout is terminal,
and litellm's own per-request ``timeout`` is no longer forwarded. Same contract
as ``LiteLLMEmbedder``; the difference is that one request carries one whole
audio file, so there is no batch to split and the upload has to survive being
replayed.

Runs against the stub ``litellm`` module from the ``litellm_module`` fixture,
so CI (which does not install the optional dependency) actually executes them.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("numpy")

import cocoindex as coco  # noqa: E402


class _FakeHTTPError(Exception):
    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


def _audio_file(content: bytes = b"fake-audio", name: str = "segment.mp3") -> Any:
    """A minimal ``FileLike``: async ``read()`` plus a ``file_path.name``."""
    return SimpleNamespace(
        file_path=SimpleNamespace(name=name),
        read=AsyncMock(return_value=content),
    )


@pytest.mark.asyncio
async def test_reads_file_and_merges_kwargs(litellm_module: Any) -> None:
    transcriber = litellm_module.LiteLLMTranscriber(
        "fake-model", api_key="k-default", language="en"
    )
    fake_response = SimpleNamespace(text="hello world")

    with patch.object(
        litellm_module.litellm,
        "atranscription",
        new=AsyncMock(return_value=fake_response),
    ) as mocked:
        text = await transcriber.transcribe(
            _audio_file(), response_format="verbose_json"
        )

    assert text == "hello world"
    mocked.assert_called_once()
    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["model"] == "fake-model"
    assert call_kwargs["api_key"] == "k-default"
    assert call_kwargs["language"] == "en"
    assert call_kwargs["response_format"] == "verbose_json"

    sent_file = call_kwargs["file"]
    assert sent_file.name == "segment.mp3"
    assert sent_file.read() == b"fake-audio"


@pytest.mark.asyncio
async def test_retry_resends_the_whole_audio(litellm_module: Any) -> None:
    """Each attempt uploads from a fresh buffer.

    litellm reads the audio to EOF, so replaying one exhausted ``BytesIO``
    would upload an empty body — the retry would "succeed" at transcribing
    silence, which no assertion on the returned text would catch.
    """
    uploads: list[bytes] = []

    async def flaky(*, model: str, file: Any, **kwargs: Any) -> Any:
        uploads.append(file.read())
        if len(uploads) == 1:
            raise _FakeHTTPError(429, "slow down")
        return SimpleNamespace(text="hello world")

    transcriber = litellm_module.LiteLLMTranscriber("fake-model")

    with (
        patch.object(litellm_module.litellm, "atranscription", new=flaky),
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        transcript = await transcriber.transcribe(_audio_file())

    assert transcript == "hello world"
    assert uploads == [b"fake-audio", b"fake-audio"]


@pytest.mark.asyncio
async def test_timeout_is_terminal(
    litellm_module: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """A timeout has already spent its duration; retrying it inside the
    request's own bound only re-spends what is left."""
    transcriber = litellm_module.LiteLLMTranscriber("fake-model")
    error = coco.DeadlineExceededError("CocoIndex timeout deadline exceeded")
    mocked = AsyncMock(side_effect=error)

    with (
        caplog.at_level(logging.WARNING),
        patch.object(litellm_module.litellm, "atranscription", new=mocked),
        patch("asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        with pytest.raises(coco.DeadlineExceededError):
            await transcriber.transcribe(_audio_file())

    mocked.assert_awaited_once()
    sleep.assert_not_called()
    assert "failed with transient error" not in caplog.text


@pytest.mark.asyncio
async def test_timeout_is_not_forwarded_to_litellm(litellm_module: Any) -> None:
    """One clock per request: `timeout` is CocoIndex's bound on the whole
    request, not a provider argument."""
    seen: list[dict[str, Any]] = []

    async def record(*, model: str, file: Any, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return SimpleNamespace(text="hello world")

    transcriber = litellm_module.LiteLLMTranscriber(
        "fake-model", timeout=timedelta(seconds=30), language="en"
    )
    with patch.object(litellm_module.litellm, "atranscription", new=record):
        await transcriber.transcribe(_audio_file())

    assert len(seen) == 1
    assert "timeout" not in seen[0]
    assert seen[0]["language"] == "en"  # other kwargs still pass through


@pytest.mark.asyncio
async def test_rejects_a_per_call_timeout(litellm_module: Any) -> None:
    """Per-call kwargs are provider arguments, so a `timeout` among them would
    reach litellm and put a second clock on the request."""
    transcriber = litellm_module.LiteLLMTranscriber("fake-model")

    with pytest.raises(TypeError, match="timeout is not a per-call argument"):
        await transcriber.transcribe(_audio_file(), timeout=30)


def test_timeout_is_not_part_of_the_memo_key(litellm_module: Any) -> None:
    """Retuning the bound changes how long we wait, not which transcript we
    get, so it must not invalidate memoized transcriptions."""
    default = litellm_module.LiteLLMTranscriber("fake-model", api_key="k")
    patient = litellm_module.LiteLLMTranscriber(
        "fake-model", api_key="k", timeout=timedelta(hours=1)
    )

    assert default.__coco_memo_key__() == patient.__coco_memo_key__()


@pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1), 0, -1.5])
def test_rejects_non_positive_timeout(
    litellm_module: Any, value: timedelta | float
) -> None:
    with pytest.raises(ValueError, match="timeout must be positive"):
        litellm_module.LiteLLMTranscriber("fake-model", timeout=value)
