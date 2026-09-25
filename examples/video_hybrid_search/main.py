"""
Video Hybrid Search (v1) - CocoIndex pipeline definition.

Walk local videos -> segment into fixed-interval scenes -> per scene, embed one
keyframe with CLIP and transcribe the audio slice with faster-whisper -> write one
document per scene into a zvec collection (dense keyframe vector + transcript FTS
field + scalar fields).

CocoIndex owns the freshness: add a clip and only its scenes process, delete a clip
and its scenes drop, swap the CLIP model and the embedding step reruns while
transcripts stay cached. Querying runs straight against zvec, see `query.py`.

This module is imported by `query.py` for the CLIP helpers. To index, run:

    cocoindex update main.py
"""

from __future__ import annotations

import asyncio
import functools
import io
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Iterator

import numpy as np
from dotenv import load_dotenv
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import localfs, zvec
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import VectorSchema

# torch, transformers, and PIL are heavy and only needed for CLIP embedding. Import
# them inside the embedding functions so an FTS-only query (which imports this module
# for its config) does not pay for them.
if TYPE_CHECKING:
    import torch
    from transformers import CLIPModel, CLIPProcessor

# Anchor every path to this file's directory, so the state db and the zvec collection
# live next to the example and stay put no matter where you run from.
BASE_DIR = pathlib.Path(__file__).resolve().parent
# Load HF_TOKEN and any model overrides from a local .env if present.
load_dotenv(BASE_DIR / ".env")

# --- Configuration ---------------------------------------------------------

ZVEC_DB = coco.ContextKey[zvec.ManagedConnection]("video_hybrid_search_db")
DB_PATH = BASE_DIR / "cocoindex.db"  # CocoIndex internal state store
ZVEC_BASE_PATH = BASE_DIR / "zvec_data"
VIDEOS_DIR = BASE_DIR / "videos"
COLLECTION_NAME = "scenes"

# CLIP gives a shared text/image space. base-patch32 (512-dim) is the light default;
# set CLIP_MODEL=openai/clip-vit-large-patch14 (768-dim) for higher quality.
CLIP_MODEL_NAME = os.getenv("CLIP_MODEL", "openai/clip-vit-base-patch32")
# faster-whisper size: tiny / base / small / medium / large-v3.
WHISPER_MODEL_NAME = os.getenv("WHISPER_MODEL", "base")

SCENE_SECONDS = 5.0  # fixed-interval segmentation (v1)
KEYFRAME_WIDTH = 384  # downscale keyframes before CLIP


@dataclass
class Scene:
    id: str  # deterministic per (video_path, start) -> zvec document id
    video_path: str  # scalar filter
    start: float  # scalar seconds
    end: float  # scalar seconds
    transcript: Annotated[str, zvec.ZvecFtsType()]  # FTS over spoken words
    # Dense keyframe vector. The VectorSchema (size from the CLIP model) is supplied
    # at mount time via column_overrides; metric defaults to cosine.
    embedding: NDArray[np.float32]


# --- CLIP helpers (shared with query.py) -----------------------------------


@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    from transformers import CLIPModel, CLIPProcessor

    model = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    return model, processor


def _projected_features(out: Any) -> "torch.Tensor":
    # transformers >=5 returns BaseModelOutputWithPooling with the projected features
    # in pooler_output; transformers <5 returns the projected features tensor directly.
    return out.pooler_output if hasattr(out, "pooler_output") else out


def embed_query(text: str) -> list[float]:
    import torch

    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        out = model.get_text_features(**inputs)
    return _projected_features(out)[0].tolist()


def embed_image_bytes(img_bytes: bytes) -> list[float]:
    import torch
    from PIL import Image

    model, processor = get_clip_model()
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor(images=image, return_tensors="pt")
    with torch.no_grad():
        out = model.get_image_features(**inputs)
    return _projected_features(out)[0].tolist()


# --- faster-whisper helper -------------------------------------------------


@functools.cache
def get_whisper_model() -> Any:
    from faster_whisper import WhisperModel

    return WhisperModel(WHISPER_MODEL_NAME, device="cpu", compute_type="int8")


def _transcribe_wav_bytes(wav_bytes: bytes) -> str:
    model = get_whisper_model()
    segments, _info = model.transcribe(io.BytesIO(wav_bytes), vad_filter=True)
    return " ".join(seg.text.strip() for seg in segments).strip()


# --- ffmpeg helpers --------------------------------------------------------


def _require_ffmpeg() -> None:
    missing = [tool for tool in ("ffmpeg", "ffprobe") if shutil.which(tool) is None]
    if missing:
        raise RuntimeError(
            f"{' and '.join(missing)} not found on PATH. Install ffmpeg first "
            "(brew install ffmpeg, or apt install ffmpeg)."
        )


def _ffprobe_duration(video_path: str) -> float:
    out = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
            video_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return float(out.stdout.strip())


def _extract_keyframe(video_path: str, ts: float) -> bytes:
    out = subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-ss",
            f"{ts:.3f}",
            "-i",
            video_path,
            "-frames:v",
            "1",
            "-vf",
            f"scale={KEYFRAME_WIDTH}:-1",
            "-f",
            "image2",
            "-c:v",
            "mjpeg",
            "pipe:1",
        ],
        capture_output=True,
        check=True,
    )
    return out.stdout


def _has_audio_stream(video_path: str) -> bool:
    out = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a",
            "-show_entries",
            "stream=index",
            "-of",
            "csv=p=0",
            video_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return bool(out.stdout.strip())


def _extract_audio(video_path: str, start: float, dur: float) -> bytes:
    out = subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-ss",
            f"{start:.3f}",
            "-t",
            f"{dur:.3f}",
            "-i",
            video_path,
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "wav",
            "pipe:1",
        ],
        capture_output=True,
        check=True,
    )
    return out.stdout


def _scene_id(video_path: str, start: float) -> str:
    # Deterministic and stable across runs so re-runs match previous rows. zvec
    # doc ids allow [A-Za-z0-9._-] only, so sanitize path separators and colons.
    return re.sub(r"[^A-Za-z0-9._-]", "_", f"{video_path}_{start:.2f}")


def _scene_bounds(duration: float) -> list[tuple[float, float]]:
    bounds: list[tuple[float, float]] = []
    start = 0.0
    while start < duration:
        end = min(start + SCENE_SECONDS, duration)
        bounds.append((start, end))
        start = end
    # Merge a sub-second trailing sliver into the previous scene. Otherwise a clip
    # whose length is just over a multiple of SCENE_SECONDS makes a near-zero-length
    # final scene, and seeking a keyframe at its midpoint runs off the end of the file.
    if len(bounds) >= 2 and bounds[-1][1] - bounds[-1][0] < 1.0:
        prev_start, _ = bounds[-2]
        _, last_end = bounds.pop()
        bounds[-1] = (prev_start, last_end)
    return bounds


# --- Memoized per-scene steps ----------------------------------------------
# Each expensive stage is its own memoized function, composed via use_mount, so a
# change to one reuses the others. deps= ties the memo to the model name, so
# swapping a model invalidates only that step.


@coco.fn(memo=True, deps=CLIP_MODEL_NAME)
async def embed_keyframe(frame_bytes: bytes) -> list[float]:
    return await asyncio.to_thread(embed_image_bytes, frame_bytes)


@coco.fn(memo=True, deps=WHISPER_MODEL_NAME)
async def transcribe_audio(audio_bytes: bytes) -> str:
    return await asyncio.to_thread(_transcribe_wav_bytes, audio_bytes)


# --- Per-video processor ---------------------------------------------------


@coco.fn(memo=True)
async def process_video(
    file: FileLike,
    target: zvec.CollectionTarget[Scene],
) -> None:
    # Key on the path relative to the videos dir so ids stay short, stable, and
    # portable across machines (sourcedir is absolute for cwd-independence).
    raw_path = pathlib.Path(str(file.file_path.path))
    try:
        video_path = raw_path.relative_to(VIDEOS_DIR).as_posix()
    except ValueError:
        video_path = raw_path.name
    content = await file.read()

    with tempfile.NamedTemporaryFile(suffix=pathlib.Path(video_path).suffix) as tmp:
        tmp.write(content)
        tmp.flush()
        duration = await asyncio.to_thread(_ffprobe_duration, tmp.name)
        # Some videos have no audio track. Skip transcription for those so the
        # visual side still indexes, instead of failing on the audio extraction.
        has_audio = await asyncio.to_thread(_has_audio_stream, tmp.name)

        for start, end in _scene_bounds(duration):
            scene_id = _scene_id(video_path, start)
            mid = (start + end) / 2.0
            frame_bytes = await asyncio.to_thread(_extract_keyframe, tmp.name, mid)

            embedding = await coco.use_mount(
                coco.component_subpath("embed", scene_id),
                embed_keyframe,
                frame_bytes,
            )
            transcript = ""
            if has_audio:
                audio_bytes = await asyncio.to_thread(
                    _extract_audio, tmp.name, start, end - start
                )
                transcript = await coco.use_mount(
                    coco.component_subpath("transcribe", scene_id),
                    transcribe_audio,
                    audio_bytes,
                )

            target.declare_row(
                row=Scene(
                    id=scene_id,
                    video_path=video_path,
                    start=start,
                    end=end,
                    transcript=transcript,
                    embedding=np.asarray(embedding, dtype=np.float32),
                )
            )


# --- App wiring ------------------------------------------------------------


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    # Pin the state db here so it does not depend on COCOINDEX_DB or a stray .env.
    builder.settings.db_path = DB_PATH
    with zvec.managed_connection(ZVEC_BASE_PATH) as conn:
        builder.provide(ZVEC_DB, conn)
        yield


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    _require_ffmpeg()

    model, _ = get_clip_model()
    dim: int = model.config.projection_dim  # type: ignore[assignment]

    target = await zvec.mount_collection_target(
        ZVEC_DB,
        COLLECTION_NAME,
        await zvec.CollectionSchema.from_class(
            Scene,
            primary_key=["id"],
            column_overrides={
                "embedding": VectorSchema(dtype=np.dtype(np.float32), size=dim)
            },
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["**/*.mp4", "**/*.mov", "**/*.mkv", "**/*.webm"]
        ),
        live=True,
    )
    await coco.mount_each(process_video, files.items(), target)


app = coco.App(
    coco.AppConfig(name="VideoHybridSearchV1"),
    app_main,
    sourcedir=VIDEOS_DIR,
)


if __name__ == "__main__":
    app.update_blocking(report_to_stdout=True)
