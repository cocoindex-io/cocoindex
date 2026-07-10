<p align="center">
  <a href="https://cocoindex.io/docs/examples/slides-to-speech/" title="Turn slide decks into a narrated, searchable index with CocoIndex — vision LLM speaker notes, local Pocket TTS, and LanceDB, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/slides-to-speech/cover.svg" alt="Turn slide decks into narrated, searchable audio with CocoIndex — a vision LLM writes speaker notes for each slide, Pocket TTS synthesizes them to MP3 locally on the CPU, and the notes are embedded into LanceDB for semantic search with playable narration attached" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn a slide deck into <em>narrated</em>, searchable audio.</h1>

<p align="center">
  <b>A vision LLM writes speaker notes for each slide, Pocket TTS synthesizes them to audio <em>locally on the CPU</em>, and the notes are embedded into LanceDB — so you search the deck by meaning and play back the narration for any hit.</b><br/>
  A deck is a great outline and a terrible thing to listen to or search; this fixes both — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/slides-to-speech/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A slide deck is a great outline and a terrible thing to *listen to* or *search*. This pipeline fixes both: for each slide, a vision LLM writes natural speaker notes, [Pocket TTS](https://github.com/kyutai-labs/pocket-tts) synthesizes them to audio locally on the CPU, and the notes are embedded into [LanceDB](https://lancedb.com/) so you can search the deck by meaning and play back the narration for any hit. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — the vision and TTS steps run on a [`coco.GPU` runner](https://cocoindex.io/docs/programming_guide/function/) that serializes them off the event loop, and the Rust engine handles incremental processing, so adding a deck processes only its slides.

## How it works

A deck fans out to **slides**, and each slide produces text, audio, and a vector:

- **Render** each slide of the PDF to an image (pymupdf).
- **Narrate** — a vision LLM (via DSPy) writes natural speaker notes for the slide image.
- **Voice + embed** — Pocket TTS synthesizes the notes to MP3 while a sentence-transformer embeds them, concurrently.
- **Store** one LanceDB row per slide — page, notes, audio (a binary column), and the embedding.

`process_file` renders the deck, then mounts one `process_slide` component per page. Each slide component runs the vision LLM, synthesizes audio *and* embeds the notes with `asyncio.gather`, and declares its own row. Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)  # unchanged slide replays its previously declared row
async def process_slide(slide: SlidePage, filename: str, table: lancedb.TableTarget[SlideRecord]) -> None:
    notes = await extract_speaker_notes(slide.image)                  # vision LLM
    voice, embedding = await asyncio.gather(
        text_to_speech(notes, coco.use_context(TTS_VOICE)),  # Pocket TTS — local CPU
        coco.use_context(EMBEDDER).embed(notes),     # sentence-transformer
    )
    table.declare_row(row=SlideRecord(
        id=f"{filename}#{slide.page_number}", filename=filename, page=slide.page_number,
        speaker_notes=notes, voice=voice, embedding=embedding,
    ))

@coco.fn
async def process_file(file: FileLike, table: lancedb.TableTarget[SlideRecord]) -> None:
    slides = await pdf_to_slides(await file.read())
    await coco.mount_each(
        process_slide,
        ((slide.page_number, slide) for slide in slides),
        str(file.file_path.path),
        table,
    )
```

The MP3 audio is stored right in the LanceDB row, so a semantic-search hit comes with playable narration attached.

When a PDF changes, its pages are rendered again to discover the new slide inputs. Each page is then matched to its own memoized component, so unchanged slides carry their existing rows forward while changed slides recompute and synchronize independently.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/slides-to-speech/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the vision-LLM speaker notes, local Pocket TTS, the per-slide LanceDB row, and searching the deck by meaning.
</p>

## Why it's worth a star ⭐

- **Three modalities, one row.** Each slide becomes text (LLM notes), audio (Pocket TTS MP3), and a vector (sentence-transformer) — declared as a single LanceDB `SlideRecord`.
- **Local TTS, no per-character billing.** Pocket TTS is a fast, ~100M-param neural voice that runs entirely on the CPU — no API, no GPU, no streaming costs; the model and voice state load once via `@functools.cache`.
- **Audio travels with the hit.** The MP3 lives in a binary LanceDB column, so a search result carries its own playable narration.
- **Concurrent per slide.** `asyncio.gather` runs TTS and embedding side by side; the heavy vision and TTS steps run on a `coco.GPU` runner.
- **Incremental & swappable.** Each slide is its own memoized processing component, so an unchanged slide replays its prior LanceDB row without rerunning vision, TTS, or embedding. `LLM_MODEL`, `EMBEDDER`, and `TTS_VOICE` use `detect_change=True`, so configuration changes invalidate the affected work.

## Run it

> Needs **LLM credentials** for the vision model (default `gemini/gemini-2.5-flash` → `GEMINI_API_KEY`) and **ffmpeg** for MP3 export. **Pocket TTS** runs locally on the CPU — its weights download automatically on first run, no GPU or API key required.

**1. Configure & install:**

```sh
cp .env.example .env     # set GEMINI_API_KEY (or swap LLM_MODEL, e.g. OpenAI)
pip install -e .
```

**2. Build the index** — drop a slide-deck PDF into `slides/`, then:

```sh
cocoindex update main        # or: cocoindex update -L main   (keep watching the folder)
```

The first run downloads the Pocket TTS weights (~100M params) from Hugging Face and caches them. On a 3-slide sample deck this produces three LanceDB rows, each with vision-LLM speaker notes and ~170–280 KB of MP3 narration. Pick a different voice with `POCKET_TTS_VOICE` (e.g. `alba`, `charles`, `vera`).

**3. Search the deck** — embed a query the same way and search LanceDB:

```sh
python main.py "reducing latency and reliability"
```

On the sample deck, that query ranks the **Engineering Priorities** slide first — above the roadmap and go-to-market slides — matching the spoken notes by meaning, not keywords. Each hit carries the slide's MP3 narration, ready to play.

---

<p align="center">
  If this gave your decks a voice and a search box, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/slides-to-speech/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/slides_to_speech" alt="" width="1" height="1" />
