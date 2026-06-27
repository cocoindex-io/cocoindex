<p align="center">
  <a href="https://cocoindex.io/docs/examples/audio-to-text/" title="Transcribe a folder of audio files with CocoIndex — a LiteLLM speech-to-text model into Postgres, one row per file, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/audio-to-text/cover.svg" alt="Transcribe audio to text with CocoIndex and LiteLLM — walk a folder of voice memos and recordings, send each to a speech-to-text model, and write one transcript row per file into Postgres keyed by filename" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn a folder of audio into a <em>transcript</em> table.</h1>

<p align="center">
  <b>Walk a directory of recordings, send each file to a LiteLLM speech-to-text model, and write one transcript row per file into Postgres — keyed by filename.</b><br/>
  A plain table you can query, join, or feed into an embedding pipeline — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/audio-to-text/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A folder of voice memos, meeting recordings, and podcast clips is dead weight until it's text. CocoIndex walks the directory, sends every file to a [LiteLLM](https://docs.litellm.ai/) transcription model, and writes the result to Postgres as one row per file, keyed by filename. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only new or changed files get re-transcribed and removed files have their rows cleaned up automatically.

## How it works

The indexing path is the shortest there is — no chunking, one row per file:

- **Walk** a local directory (recursive), matching common audio extensions (`.mp3`, `.wav`, `.m4a`, `.flac`, `.ogg`, `.webm`, `.aac`, `.aiff`).
- **Transcribe** each file with a LiteLLM speech-to-text model (`whisper-1` by default).
- **Store** one `AudioTranscription` row per file in Postgres, keyed by filename.

`process_file` runs once per file: read the audio, transcribe it, declare a single target row. Read it in [`main.py`](main.py):

```python
_transcriber = LiteLLMTranscriber("whisper-1")

@dataclass
class AudioTranscription:
    filename: str
    text: str

@coco.fn(memo=True)   # unchanged file is never re-transcribed
async def process_file(
    file: localfs.File,
    table: postgres.TableTarget[AudioTranscription],
) -> None:
    transcript = await _transcriber.transcribe(file)
    table.declare_row(
        row=AudioTranscription(filename=str(file.file_path.path), text=transcript),
    )
```

`mount_table_target` creates and manages the Postgres table for you with `primary_key=["filename"]` — so each file maps to exactly one row, the table doubles as an index of what's been transcribed, and re-runs upsert only what changed.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/audio-to-text/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the row schema, the LiteLLM transcriber, the managed Postgres target, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Swap the model with one string.** `LiteLLMTranscriber("whisper-1")` wraps LiteLLM's transcription API — change that string (and the matching credential) for `elevenlabs/scribe_v1`, a self-hosted endpoint, whatever.
- **The table is the index.** `filename` is the primary key, so the output table doubles as a record of which files have been transcribed — no separate bookkeeping.
- **Incremental by default.** `@coco.fn(memo=True)` skips a file when its content and the function's code are both unchanged, so you never pay for the same transcription twice.
- **Managed Postgres target.** `mount_table_target` handles schema, idempotent upserts, and orphan cleanup — delete a file and its row is removed automatically.
- **Logic changes reconcile too.** Swap the transcription model and CocoIndex re-transcribes against it, compares with what's in Postgres, and applies only the difference.

## Run it

> Needs a running **Postgres** and **LiteLLM credentials** for the transcription model (the default `whisper-1` uses `OPENAI_API_KEY`).

**1. Start Postgres** — a ready compose file ships in the repo:

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set OPENAI_API_KEY; POSTGRES_URL defaults to the local container
pip install -e .
```

**3. Build the table** — drop a few audio files into `audio_files/`, then:

```sh
cocoindex update main.py
```

This writes to `coco_examples.audio_transcriptions`, with `filename` as the primary key and `text` as the transcript.

**4. Check the results** with plain SQL:

```sh
psql "$POSTGRES_URL" -c \
  'SELECT filename, left(text, 200) AS preview FROM coco_examples.audio_transcriptions ORDER BY filename;'
```

Re-running `cocoindex update main.py` incrementally processes only added, changed, and removed files.

---

<p align="center">
  If this turned your recordings into a searchable table, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/audio-to-text/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/audio_to_text" alt="" width="1" height="1" />
