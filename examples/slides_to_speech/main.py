"""
Slides to Speech (v1) — CocoIndex pipeline example.

Turn a slide deck (PDF) into a narrated, searchable index. For each slide:
render it to an image, use a vision LLM to write speaker notes, synthesize
those notes to audio with Pocket TTS (Kyutai's local, CPU-only TTS), embed the
notes for semantic search, and store everything in LanceDB.

Index (use `-L` for live mode, omit for one-shot catch-up):
    cocoindex update main

Query the index (semantic search over the speaker notes):
    python main.py "the quarterly roadmap"
"""

from __future__ import annotations

import asyncio
import base64
import functools
import io
import os
import pathlib
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

import dspy
import numpy as np
from numpy.typing import NDArray
from pocket_tts import TTSModel
from pydub import AudioSegment

import cocoindex as coco
from cocoindex.connectors import localfs, lancedb
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

LANCEDB_TABLE = "slides_to_speech"
LANCEDB_URI = os.environ.get("LANCEDB_URI", "./lancedb_data")
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("slides_lancedb")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)
TTS_VOICE = coco.ContextKey[str]("tts_voice", detect_change=True)


# ---------------------------------------------------------------------------
# Per-slide image rendering (pymupdf)
# ---------------------------------------------------------------------------


@dataclass
class SlidePage:
    page_number: int
    image: bytes


@coco.fn.as_async(runner=coco.GPU, memo=True)
def pdf_to_slides(content: bytes) -> list[SlidePage]:
    import pymupdf

    slides: list[SlidePage] = []
    doc = pymupdf.open(stream=content, filetype="pdf")
    for i, page in enumerate(doc):
        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))
        slides.append(SlidePage(page_number=i + 1, image=pix.tobytes("png")))
    doc.close()
    return slides


# ---------------------------------------------------------------------------
# Vision LLM: slide image -> speaker notes (DSPy)
# ---------------------------------------------------------------------------


class SlideNotes(dspy.Signature):
    """Write natural spoken narration for a slide, as a presenter would say it aloud."""

    slide: dspy.Image = dspy.InputField(desc="the rendered slide image")
    speaker_notes: str = dspy.OutputField(
        desc="a few sentences of spoken narration — no markdown or bullet symbols"
    )


_speaker_notes = dspy.Predict(SlideNotes)


@functools.cache
def _get_lm(model: str) -> dspy.LM:
    # max_tokens leaves headroom for the model's hidden reasoning plus the answer.
    return dspy.LM(model, max_tokens=8192)


@coco.fn(memo=True)
async def extract_speaker_notes(image: bytes) -> str:
    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
    with dspy.context(lm=_get_lm(coco.use_context(LLM_MODEL))):
        result = await _speaker_notes.acall(slide=dspy.Image(url=data_url))
    return result.speaker_notes


# ---------------------------------------------------------------------------
# Pocket TTS: text -> mp3 bytes (local, CPU-only)
# ---------------------------------------------------------------------------


@functools.cache
def get_tts_model() -> TTSModel:
    # ~100M-param model; weights download from Hugging Face on first use, then cache.
    return TTSModel.load_model()


@functools.cache
def get_voice_state(voice: str) -> dict:
    # A voice state is a reusable conditioning template. Loading it is slow, so we
    # cache one per voice; generate_audio(copy_state=True) leaves it intact to reuse.
    return get_tts_model().get_state_for_audio_prompt(voice)


@coco.fn.as_async(runner=coco.GPU, memo=True)
def text_to_speech(text: str, voice: str) -> bytes:
    model = get_tts_model()
    # Pocket TTS is not thread-safe; the coco.GPU runner serializes calls so the one
    # cached model is only ever driving a single synthesis at a time.
    audio = model.generate_audio(get_voice_state(voice), text)
    # audio is a 1D float tensor in [-1, 1] at model.sample_rate; pack it as int16 PCM.
    samples = np.clip(audio.to("cpu").numpy().reshape(-1), -1.0, 1.0)
    pcm16 = (samples * 32767.0).astype("<i2").tobytes()
    segment = AudioSegment(
        data=pcm16, sample_width=2, frame_rate=model.sample_rate, channels=1
    )
    out = io.BytesIO()
    segment.export(out, format="mp3", bitrate="64k")
    return out.getvalue()


# ---------------------------------------------------------------------------
# LanceDB row
# ---------------------------------------------------------------------------


@dataclass
class SlideRecord:
    id: str  # primary key — "{filename}#{page}"
    filename: str
    page: int
    speaker_notes: str
    voice: bytes
    embedding: Annotated[NDArray, EMBEDDER]


@coco.fn(memo=True)
async def process_slide(
    slide: SlidePage, filename: str, table: lancedb.TableTarget[SlideRecord]
) -> None:
    notes = await extract_speaker_notes(slide.image)
    voice, embedding = await asyncio.gather(
        text_to_speech(notes, coco.use_context(TTS_VOICE)),
        coco.use_context(EMBEDDER).embed(notes),
    )
    table.declare_row(
        row=SlideRecord(
            id=f"{filename}#{slide.page_number}",
            filename=filename,
            page=slide.page_number,
            speaker_notes=notes,
            voice=voice,
            embedding=embedding,
        )
    )


@coco.fn(memo=True)
async def process_file(file: FileLike, table: lancedb.TableTarget[SlideRecord]) -> None:
    slides = await pdf_to_slides(await file.read())
    await coco.mount_each(
        process_slide,
        ((slide.page_number, slide) for slide in slides),
        str(file.file_path.path),
        table,
    )


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "gemini/gemini-2.5-flash"))
    builder.provide(TTS_VOICE, os.environ.get("POCKET_TTS_VOICE", "alba"))
    yield


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    table = await lancedb.mount_table_target(
        LANCE_DB,
        table_name=LANCEDB_TABLE,
        table_schema=await lancedb.TableSchema.from_class(
            SlideRecord, primary_key=["id"]
        ),
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), table)


app = coco.App(
    coco.AppConfig(name="SlidesToSpeech"),
    app_main,
    sourcedir=pathlib.Path("./slides"),
)


# ---------------------------------------------------------------------------
# Query demo
# ---------------------------------------------------------------------------


def query(text: str, *, top_k: int = 5) -> None:
    import lancedb as lancedb_client

    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    vec = asyncio.run(embedder.embed(text))
    db = lancedb_client.connect(os.environ.get("LANCEDB_URI", "./lancedb_data"))
    table = db.open_table(LANCEDB_TABLE)
    for row in table.search(vec).limit(top_k).to_list():
        print(f"{row['filename']} slide {row['page']}")
        print(f"    {row['speaker_notes'][:120]}")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        query(" ".join(sys.argv[1:]))
    else:
        print(__doc__)
