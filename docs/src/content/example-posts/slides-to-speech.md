---
title: Slides to Narrated Search
description: 'Turn slide decks into a narrated, searchable index with CocoIndex V1 — a vision LLM writes speaker notes for each slide, Pocket TTS synthesizes them to audio locally on the CPU, and the notes are embedded into LanceDB for semantic search.'
slug: slides-to-speech
image: https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/cover.png
tags: [multimodal, text-to-speech]
---

![Turn slide decks into narrated, searchable audio with a vision LLM and Pocket TTS](https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/cover.png)

A slide deck is a great outline and a terrible thing to *listen to* or *search*. In this tutorial we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that fixes both: for each slide, a vision LLM writes natural speaker notes, [Pocket TTS](https://github.com/kyutai-labs/pocket-tts) synthesizes them to audio locally on the CPU, and the notes are embedded into [LanceDB](https://lancedb.com/) so you can search the deck by meaning and play back the narration for any hit.

The whole pipeline is ordinary `async` Python. The vision and TTS steps run on a [`coco.GPU` runner](https://cocoindex.io/docs/programming_guide/function/) that offloads each blocking call off the event loop and serializes it — Pocket TTS is CPU-only, so no GPU is required — and the Rust engine handles [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) — add a deck and only its slides get processed.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/slides_to_speech)

## Flow overview

![CocoIndex flow: render each slide to an image, a vision LLM writes speaker notes, Pocket TTS narrates them, the notes are embedded, and everything is stored per-slide in LanceDB](https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/flow-v1.png)

A deck fans out to **slides**, and each slide produces text, audio, and a vector:

1. Render each slide of the PDF to an image (pymupdf).
2. A vision LLM writes speaker notes for the slide.
3. Pocket TTS synthesizes the notes to MP3 audio; a sentence-transformer embeds the notes.
4. Store one LanceDB row per slide — page, notes, audio, and embedding.

## Speaker notes from a slide image

The vision LLM reads the rendered slide and writes presenter narration. Extraction uses [DSPy](https://dspy.ai/): a typed signature declares the slide image going in and the narration coming out, and `dspy.Predict` handles the call — no hand-written prompt or JSON parsing:

```python title="main.py"
class SlideNotes(dspy.Signature):
    """Write natural spoken narration for a slide, as a presenter would say it aloud."""

    slide: dspy.Image = dspy.InputField(desc="the rendered slide image")
    speaker_notes: str = dspy.OutputField(desc="a few sentences — no markdown or bullet symbols")


_speaker_notes = dspy.Predict(SlideNotes)


@coco.fn(memo=True)
async def extract_speaker_notes(image: bytes) -> str:
    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
    with dspy.context(lm=_get_lm(coco.use_context(LLM_MODEL))):   # e.g. gemini/gemini-3.5-flash
        result = await _speaker_notes.acall(slide=dspy.Image(url=data_url))
    return result.speaker_notes
```

> **A note on the port.** The v0 example pulled slides from Google Drive and used BAML for the vision call; this v1 port reads slides from a local folder and uses DSPy (any vision model — Gemini, GPT-4o, …). Point the source at a [Google Drive folder](https://cocoindex.io/docs/connectors/google_drive/) to reproduce the original.

## Narrate locally with Pocket TTS

[Pocket TTS](https://github.com/kyutai-labs/pocket-tts) is a fast, ~100M-parameter neural TTS that runs entirely on the CPU — no API, no GPU, no per-character billing. The model and voice state load once (via `@functools.cache`) and synthesize the notes to MP3. The model isn't thread-safe, so the `coco.GPU` runner serializes each call on a worker thread — off the event loop, one at a time:

```python title="main.py"
@coco.fn.as_async(runner=coco.GPU)                  # offloaded + serialized off the event loop
def text_to_speech(text: str) -> bytes:
    model = get_tts_model()                          # cached TTSModel — loads once
    audio = model.generate_audio(get_voice_state(POCKET_TTS_VOICE), text)  # 1D float PCM
    samples = np.clip(audio.to("cpu").numpy().reshape(-1), -1.0, 1.0)
    pcm16 = (samples * 32767.0).astype("<i2").tobytes()   # float -> int16 PCM
    seg = AudioSegment(data=pcm16, sample_width=2, frame_rate=model.sample_rate, channels=1)
    out = io.BytesIO(); seg.export(out, format="mp3", bitrate="64k")
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
cp .env.example .env      # set GEMINI_API_KEY (or OPENAI_API_KEY)
pip install -e .          # needs ffmpeg for MP3 export
cocoindex update main     # first run downloads the Pocket TTS + embedder weights (~100M params)
```

Drop a slide-deck PDF into `slides/`. On a 3-slide sample deck, this produces three LanceDB rows, each with Gemini-written speaker notes and ~170–280 KB of Pocket TTS MP3 audio.

## Search the deck

Embed a query the same way and search LanceDB:

```sh
python main.py "reducing latency and reliability"
```

On the sample deck, that query ranks the **Engineering Priorities** slide first — above the roadmap and go-to-market slides — matching the spoken notes by meaning, not keywords. Each hit carries the slide's MP3 narration, ready to play.

## Incremental updates

- **Add a deck** — only its slides are rendered, narrated, and embedded.
- **Edit a deck** — slides reconcile against LanceDB; unchanged slides keep their notes and audio.
- **Swap the LLM** — change `LLM_MODEL` and only the narration step re-runs; the rest is served from cache. (A new `POCKET_TTS_VOICE` takes effect on a fresh build.)

## Run it

The full, runnable example is in the CocoIndex repo: [examples/slides_to_speech](https://github.com/cocoindex-io/cocoindex/tree/main/examples/slides_to_speech). For transcribing existing audio instead of generating it, see [Audio → Text](https://cocoindex.io/docs/examples/audio-to-text/).

Got a deck library you want to narrate and search? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
