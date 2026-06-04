# HackerNews Trending Topics (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example scrapes recent HackerNews threads (and their comments) via the [Algolia HN API](https://hn.algolia.com/api), uses an LLM to extract topics from each message, and stores everything in Postgres. A small CLI demo prints trending topics ranked by mention score and lets you search messages by topic.

## Prerequisites

- A running Postgres. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- `POSTGRES_URL` set, e.g.

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- An API key for the LLM. The default model is `gemini/gemini-2.5-flash` (set `GEMINI_API_KEY`). Any provider supported by [litellm](https://docs.litellm.ai/docs/providers) works — change `LLM_MODEL` in `main.py` and set the matching credential.

You can put these in a `.env` file in this directory; `python main.py` loads it automatically.

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (one-shot catch-up; this example doesn't use a live source):

```sh
cocoindex update main
```

Each run fetches the latest `MAX_THREADS` (default 10) threads, runs LLM topic extraction on the thread + each comment, and writes rows into `coco_examples.hn_messages` and `coco_examples.hn_topics`. CocoIndex memoizes per-message extraction, so re-running is incremental.

Query — show top trending topics, then enter a search loop:

```sh
python main.py
```

Or jump straight to a topic search:

```sh
python main.py "rust"
```
