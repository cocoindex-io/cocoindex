"""YouTube audio download + AssemblyAI diarized transcription."""

from __future__ import annotations

import os
import tempfile
from typing import Any

import assemblyai as aai
import cocoindex as coco

from .models import SessionTranscript, Utterance


@coco.fn(memo=True)
async def fetch_transcript(youtube_id: str) -> SessionTranscript:
    """Download audio via yt-dlp, transcribe with speaker diarization via AssemblyAI."""
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={youtube_id}"

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Download audio + metadata via yt-dlp
        outtmpl = os.path.join(tmpdir, "audio.%(ext)s")
        ydl_opts: dict[str, Any] = {
            "format": "bestaudio/best",
            "outtmpl": outtmpl,
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "64",
                }
            ],
            "quiet": True,
            "no_warnings": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        yt_channel = info.get("channel", info.get("uploader", "")) if info else ""
        yt_title = info.get("title", youtube_id) if info else youtube_id
        yt_description = info.get("description") if info else None
        yt_upload_date = info.get("upload_date") if info else None
        # yt-dlp upload_date is YYYYMMDD; convert to ISO
        if yt_upload_date and len(yt_upload_date) == 8:
            yt_upload_date = (
                f"{yt_upload_date[:4]}-{yt_upload_date[4:6]}-{yt_upload_date[6:]}"
            )

        # Find the .mp3 file produced by FFmpegExtractAudio
        actual_path = os.path.join(tmpdir, "audio.mp3")
        if not os.path.exists(actual_path):
            mp3_files = [f for f in os.listdir(tmpdir) if f.endswith(".mp3")]
            if mp3_files:
                actual_path = os.path.join(tmpdir, mp3_files[0])
            else:
                raise FileNotFoundError(
                    f"No .mp3 file found in {tmpdir} after yt-dlp download. "
                    f"Files present: {os.listdir(tmpdir)}"
                )

        # 2. Transcribe with AssemblyAI (speaker diarization)
        aai.settings.api_key = os.environ["ASSEMBLYAI_API_KEY"]
        config = aai.TranscriptionConfig(
            speaker_labels=True,
        )
        # SDK 0.58.0 enum doesn't include newer models; pass raw string.
        config.speech_models = ["universal-3-pro"]  # type: ignore[assignment]
        transcript = aai.Transcriber().transcribe(actual_path, config)

        if transcript.status == aai.TranscriptStatus.error:
            raise RuntimeError(f"AssemblyAI transcription failed: {transcript.error}")

    # 3. Build structured utterances
    utterances = _extract_utterances(transcript)

    return SessionTranscript(
        utterances=utterances,
        yt_channel=yt_channel,
        yt_title=yt_title,
        yt_description=yt_description,
        yt_upload_date=yt_upload_date,
    )


def _extract_utterances(transcript: aai.Transcript) -> list[Utterance]:
    """Extract structured utterances from AssemblyAI transcript."""
    if transcript.utterances:
        return [
            Utterance(speaker=u.speaker, text=u.text) for u in transcript.utterances
        ]
    # Fallback: no diarization info — single utterance with unknown speaker
    return [Utterance(speaker="A", text=transcript.text or "")]
