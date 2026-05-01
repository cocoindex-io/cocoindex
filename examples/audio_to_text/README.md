# Audio to Text (v1)

This example transcribes local audio files with LiteLLM and stores one row per file in Postgres. The filename is the primary key, so the table can be used as an index of available transcriptions.

## Prerequisites

- A running Postgres database.
- LiteLLM credentials for the transcription model. For the default `whisper-1` model, set `OPENAI_API_KEY`.
- `POSTGRES_URL` set, e.g.

```sh
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
export OPENAI_API_KEY="..."
```

## Input Files

Put audio files under `./audio_files`. The example recursively picks up common audio extensions such as `.mp3`, `.wav`, `.m4a`, `.flac`, `.ogg`, and `.webm`.

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index:

```sh
cocoindex update main.py
```

The target table is `coco_examples.audio_transcriptions` with:

- `filename` as the primary key
- `text` as the transcript

Check the results:

```sh
psql "$POSTGRES_URL" -c 'SELECT filename, left(text, 200) AS preview FROM coco_examples.audio_transcriptions ORDER BY filename;'
```

Re-running `cocoindex update main.py` incrementally processes added, changed, and removed audio files.
