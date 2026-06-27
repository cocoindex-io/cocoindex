---
title: Audio to Text with *LiteLLM*
description: 'Transcribe a folder of local audio files with CocoIndex V1 — call a LiteLLM speech-to-text model on each file and store one transcript row per file in Postgres, keyed by filename.'
slug: audio-to-text
image: https://cocoindex.io/blobs/docs-v1/img/examples/audio-to-text/cover.png
tags: [transcription, litellm]
---

![Audio to Text with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/audio-to-text/cover.png)

We'll take a folder of audio files — voice memos, meeting recordings, podcast clips — and turn each one into a searchable transcript. CocoIndex walks the directory, sends every file to a [LiteLLM](https://docs.litellm.ai/) transcription model, and writes the text to Postgres as one row per file, keyed by filename. The result is a plain table you can query, join, or feed into a downstream embedding pipeline.

The whole thing is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed targets — runs in a Rust engine underneath, so only files that are new or changed get re-transcribed, and removed files have their rows cleaned up automatically.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/audio_to_text)

## Flow overview

![CocoIndex audio-to-text flow: read audio files from a local directory, transcribe each with LiteLLM, and store one transcript row per file in Postgres](https://cocoindex.io/blobs/docs-v1/img/examples/audio-to-text/flow-v1.png)

From a high level, these are the steps:

1. Read audio files from a local directory (recursively, matching common audio extensions).
2. [Transcribe each file](https://cocoindex.io/docs/ops/litellm/) with a LiteLLM speech-to-text model (`whisper-1` by default).
3. Store one transcript per file in Postgres (as a [target state](https://cocoindex.io/docs/programming_guide/target_state/)), keyed by filename — no chunking.

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Setup

- A running Postgres. CocoIndex supports [many targets](https://cocoindex.io/docs/connectors/postgres/), so you can pick another store. If you don't have one, start a local instance:

  ```sh
  docker compose -f dev/postgres.yaml up -d
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- LiteLLM credentials for the transcription model. For the default `whisper-1`, set your OpenAI key:

  ```sh
  export OPENAI_API_KEY="..."
  ```

- Install CocoIndex with the extras this example uses:

  ```sh
  pip install -U "cocoindex[litellm,postgres]" asyncpg
  ```

- A few audio files to transcribe. Drop them into an `audio_files/` directory — the example recursively picks up `.aac`, `.aiff`, `.flac`, `.m4a`, `.mp3`, `.ogg`, `.wav`, and `.webm`.

## Define the data and shared resources

[Apps](https://cocoindex.io/docs/programming_guide/app/) are the top-level runnable unit in CocoIndex. Before the App, we set up the pieces the rest of the code builds on. `AudioTranscription` defines one row of the output table — each audio file becomes one row, with its filename and transcript text. `_transcriber` is the LiteLLM model, created once and reused. `coco_lifespan` provides the [shared resource](https://cocoindex.io/docs/programming_guide/context/) every step needs — the Postgres connection pool — once at startup.

```python title="main.py"
import os
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.litellm import LiteLLMTranscriber
from cocoindex.resources.file import PatternFilePathMatcher

DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex")
TABLE_NAME = "audio_transcriptions"
PG_SCHEMA_NAME = "coco_examples"
PG_DB = coco.ContextKey[asyncpg.Pool]("audio_to_text_db")

_transcriber = LiteLLMTranscriber("whisper-1")


@dataclass
class AudioTranscription:
    filename: str
    text: str


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

[`LiteLLMTranscriber("whisper-1")`](https://cocoindex.io/docs/ops/litellm/) wraps LiteLLM's transcription API, so you can swap in any model LiteLLM supports — `elevenlabs/scribe_v1`, a self-hosted endpoint, whatever — by changing that one string (and the matching credential).

## Process a file

![One processing component per file: each audio file is transcribed with LiteLLM, producing one AudioTranscription row written to Postgres](https://cocoindex.io/blobs/docs-v1/img/examples/audio-to-text/stage-file-process.png)

`process_file` runs once per file. It reads the audio, [transcribes it](https://cocoindex.io/docs/ops/litellm/), and declares a single target row — no chunking, one row per file.

```python title="main.py"
@coco.fn(memo=True)
async def process_file(
    file: localfs.File,
    table: postgres.TableTarget[AudioTranscription],
) -> None:
    transcript = await _transcriber.transcribe(file)
    table.declare_row(
        row=AudioTranscription(
            filename=str(file.file_path.path),
            text=transcript,
        ),
    )
```

`_transcriber.transcribe(file)` reads the file's bytes and calls the LiteLLM model, returning plain text. `table.declare_row` declares that row as a target state; CocoIndex handles inserting, updating, or deleting it to match. Because the filename is the primary key, the table doubles as an index of which files have been transcribed.

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a file's content and this function's code are both unchanged, the whole file is skipped on the next run — so you don't pay for the same transcription twice.

## Define the main function

`app_main` wires the source to the target. It mounts the Postgres table, walks the source directory for audio files, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            AudioTranscription,
            primary_key=["filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/*.aac", "**/*.aiff", "**/*.flac", "**/*.m4a",
                "**/*.mp3", "**/*.ogg", "**/*.wav", "**/*.webm",
            ],
        ),
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

`mount_table_target` creates and manages the Postgres table for you: schema, idempotent upserts, and orphan cleanup when a file disappears. `primary_key=["filename"]` is what makes each file map to exactly one row. `mount_each` runs one component per file so the engine can track and update them independently.

## Create the App

Bind `app_main` into a `coco.App` and point it at the folder of audio files.

```python title="main.py"
app = coco.App(
    "AudioToText",
    app_main,
    sourcedir=pathlib.Path("./audio_files"),
)
```

That is the entire indexing path.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build and update the table:

```sh
cocoindex update main.py
```

The target table is `coco_examples.audio_transcriptions`, with `filename` as the primary key and `text` as the transcript. Check the results with plain SQL:

```sh
psql "$POSTGRES_URL" -c \
  'SELECT filename, left(text, 200) AS preview FROM coco_examples.audio_transcriptions ORDER BY filename;'
```

## Incremental updates

CocoIndex keeps the table in sync with your files and does the **minimum work** to get there. You never compute a diff or write update logic: you change something, and CocoIndex works out exactly what to transcribe, upsert, and delete. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged, so an unchanged file never hits the transcription API again. `mount_table_target` decides what to *write* — each row is keyed by `filename`, so it upserts only the rows that actually changed and deletes rows whose source file is gone.

- **A file is added** — only that file is transcribed, and its one row is inserted. The rest is untouched.
- **A file is changed** — it is re-transcribed and its row is updated in place. Files with identical content keep their cached transcript and are left as-is.
- **A file is deleted** — its row is removed from the target automatically.

The same machinery covers **logic** changes too: swap the transcription model and CocoIndex re-transcribes against the new model, comparing the result with what is already in Postgres and applying only the difference. Re-running `cocoindex update main.py` does this once and exits.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/audio_to_text](https://github.com/cocoindex-io/cocoindex/tree/main/examples/audio_to_text). Once this clicks, [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) is the natural next step — embed those transcripts and search them by meaning.

If CocoIndex helps you, star us on [GitHub](https://github.com/cocoindex-io/cocoindex) and come say hi in our [Discord](https://discord.com/invite/zpA9S2DR7s).
