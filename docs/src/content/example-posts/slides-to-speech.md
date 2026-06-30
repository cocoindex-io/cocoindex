---
title: Slides to Narrated Search
description: 'Turn slide decks into a narrated, searchable index with CocoIndex V1 — a vision LLM writes speaker notes for each slide, Piper synthesizes them to audio locally, and the notes are embedded into LanceDB for semantic search.'
slug: slides-to-speech
image: https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/cover.png
tags: [multimodal, text-to-speech]
---

![Turn slide decks into narrated, searchable audio with a vision LLM and Piper TTS](https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/cover.png)

A slide deck is a great outline and a terrible thing to *listen to* or *search*. In this tutorial we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that fixes both: for each slide, a vision LLM writes natural speaker notes, [Piper](https://github.com/OHF-Voice/piper1-gpl) synthesizes them to audio locally, and the notes are embedded into [LanceDB](https://lancedb.com/) so you can search the deck by meaning and play back the narration for any hit.

The whole pipeline is ordinary `async` Python. The vision and TTS steps run on a [GPU runner](https://cocoindex.io/docs/programming_guide/function/), and the Rust engine handles [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) — add a deck and only its slides get processed.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/slides_to_speech)

## Flow overview

![CocoIndex flow: render each slide to an image, a vision LLM writes speaker notes, Piper TTS narrates them, the notes are embedded, and everything is stored per-slide in LanceDB](https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/flow-v1.png)

A deck fans out to **slides**, and each slide produces text, audio, and a vector:

1. Render each slide of the PDF to an image (pymupdf).
2. A vision LLM writes speaker notes for the slide.
3. Piper synthesizes the notes to MP3 audio; a sentence-transformer embeds the notes.
4. Store one LanceDB row per slide — page, notes, audio, and embedding.

## Speaker notes from a slide image

The vision LLM reads the rendered slide and writes presenter narration. Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/), so the image goes in as a data URL and a typed `SlideTranscript` comes back:

```python title="main.py"
class SlideTranscript(pydantic.BaseModel):
    speaker_notes: str = pydantic.Field(
        description="Natural spoken narration for this slide, as a presenter would say it."
    )


@coco.fn(memo=True)
async def extract_speaker_notes(image: bytes) -> SlideTranscript:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),          # e.g. gemini/gemini-2.5-flash
        response_model=SlideTranscript,
        messages=[{"role": "user", "content": [
            {"type": "text", "text": "Write speaker notes for this slide."},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]}],
    )
    return SlideTranscript.model_validate(result.model_dump())
```

> **A note on the port.** The v0 example pulled slides from Google Drive and used BAML for the vision call; this v1 port reads slides from a local folder and uses instructor + LiteLLM (any vision model — Gemini, GPT-4o, …). Point the source at a [Google Drive folder](https://cocoindex.io/docs/connectors/google_drive/) to reproduce the original.

## Narrate locally with Piper

Piper is a fast, fully local neural TTS — no API, no per-character billing. The voice model loads once and synthesizes the notes to MP3:

```python title="main.py"
@coco.fn.as_async(runner=coco.GPU)
def text_to_speech(text: str) -> bytes:
    voice = get_piper_voice()                       # cached PiperVoice
    chunks = list(voice.synthesize(text))
    pcm = b"".join(c.audio_int16_bytes for c in chunks)
    audio = AudioSegment(data=pcm, sample_width=chunks[0].sample_width,
                         frame_rate=chunks[0].sample_rate, channels=chunks[0].sample_channels)
    out = io.BytesIO(); audio.export(out, format="mp3", bitrate="64k")
    return out.getvalue()
```

## Fan out per slide and store

`process_file` renders the deck to slides, then maps each through `process_slide`, which runs the vision LLM, then synthesizes audio *and* embeds the notes concurrently before declaring the row:

```python title="main.py"
@coco.fn
async def process_slide(slide, filename, table) -> None:
    notes = (await extract_speaker_notes(slide.image)).speaker_notes
    voice, embedding = await asyncio.gather(
        text_to_speech(notes),
        coco.use_context(EMBEDDER).embed(notes),
    )
    table.declare_row(row=SlideRecord(
        id=f"{filename}#{slide.page_number}", filename=filename, page=slide.page_number,
        speaker_notes=notes, voice=voice, embedding=embedding,
    ))
```

The MP3 audio is stored right in the LanceDB row (a binary column), so a search hit comes with playable narration attached.

## Run the pipeline

```sh
python3 -m piper.download_voices en_US-lessac-medium   # ~60 MB local voice
cp .env.example .env                                    # set GEMINI_API_KEY (or OPENAI_API_KEY)
pip install -e .                                        # needs ffmpeg for MP3 export
cocoindex update main
```

Drop a slide-deck PDF into `slides/`. On a 3-slide sample deck, this produces three LanceDB rows, each with Gemini-written speaker notes and ~170–280 KB of Piper MP3 audio.

## Search the deck

Embed a query the same way and search LanceDB:

```sh
python main.py "reducing latency and reliability"
```

On the sample deck, that query ranks the **Engineering Priorities** slide first — above the roadmap and go-to-market slides — matching the spoken notes by meaning, not keywords. Each hit carries the slide's MP3 narration, ready to play.

## Incremental updates

- **Add a deck** — only its slides are rendered, narrated, and embedded.
- **Edit a deck** — slides reconcile against LanceDB; unchanged slides keep their notes and audio.
- **Swap the voice or LLM** — change `PIPER_MODEL_NAME` or `LLM_MODEL`; the affected steps re-run, the rest is served from cache.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/slides_to_speech](https://github.com/cocoindex-io/cocoindex/tree/main/examples/slides_to_speech). For transcribing existing audio instead of generating it, see [Audio → Text](https://cocoindex.io/docs/examples/audio-to-text/).

Got a deck library you want to narrate and search? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
