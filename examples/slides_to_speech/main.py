"""
Slides to Speech (v1) — CocoIndex pipeline example.

Turn a slide deck (PDF) into a narrated, searchable index. For each slide:
render it to an image, use a vision LLM to write speaker notes, synthesize
those notes to audio with Piper (local TTS), embed the notes for semantic
search, and store everything in LanceDB.

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

import instructor
import litellm
import pydantic
from numpy.typing import NDArray
from piper import PiperVoice
from pydub import AudioSegment

import cocoindex as coco
from cocoindex.connectors import localfs, lancedb
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

litellm.drop_params = True

LANCEDB_TABLE = "slides_to_speech"
LANCEDB_URI = os.environ.get("LANCEDB_URI", "./lancedb_data")
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PIPER_MODEL = os.environ.get("PIPER_MODEL_NAME", "en_US-lessac-medium")

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("slides_lancedb")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)


# ---------------------------------------------------------------------------
# Per-slide image rendering (pymupdf)
# ---------------------------------------------------------------------------


@dataclass
class SlidePage:
    page_number: int
    image: bytes


@coco.fn.as_async(runner=coco.GPU)
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
# Vision LLM: slide image -> speaker notes
# ---------------------------------------------------------------------------


class SlideTranscript(pydantic.BaseModel):
    speaker_notes: str = pydantic.Field(
        description="Natural spoken narration for this slide, as a presenter would "
        "say it aloud — a few sentences, no markdown or bullet symbols."
    )


@coco.fn(memo=True)
async def extract_speaker_notes(image: bytes) -> SlideTranscript:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=SlideTranscript,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Write speaker notes for this slide."},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
    )
    return SlideTranscript.model_validate(result.model_dump())


# ---------------------------------------------------------------------------
# Piper TTS: text -> mp3 bytes
# ---------------------------------------------------------------------------


@functools.cache
def get_piper_voice() -> PiperVoice:
    return PiperVoice.load(f"{PIPER_MODEL}.onnx")


@coco.fn.as_async(runner=coco.GPU)
def text_to_speech(text: str) -> bytes:
    voice = get_piper_voice()
    chunks = list(voice.synthesize(text))
    pcm = b"".join(c.audio_int16_bytes for c in chunks)
    audio = AudioSegment(
        data=pcm,
        sample_width=chunks[0].sample_width,
        frame_rate=chunks[0].sample_rate,
        channels=chunks[0].sample_channels,
    )
    out = io.BytesIO()
    audio.export(out, format="mp3", bitrate="64k")
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


@coco.fn
async def process_slide(
    slide: SlidePage, filename: str, table: lancedb.TableTarget[SlideRecord]
) -> None:
    transcript = await extract_speaker_notes(slide.image)
    notes = transcript.speaker_notes
    voice, embedding = await asyncio.gather(
        text_to_speech(notes),
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
    await coco.map(process_slide, slides, str(file.file_path.path), table)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "gemini/gemini-2.5-flash"))
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
        print(f"[{1.0 - row['_distance']:.3f}] {row['filename']} slide {row['page']}")
        print(f"    {row['speaker_notes'][:120]}")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        query(" ".join(sys.argv[1:]))
    else:
        print(__doc__)
