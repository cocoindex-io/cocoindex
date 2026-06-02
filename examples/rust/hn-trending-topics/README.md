# HackerNews Trending Topics (Rust)

Rust port of the Python [`hn_trending_topics`](../../hn_trending_topics) example.

Scrapes recent HackerNews threads + comments via the public [Algolia HN API](https://hn.algolia.com/api), extracts topics from each message with an LLM, stores them in Postgres, and offers a small CLI for trending topics and topic search.

## How it maps to the Python example

| Step | Python | Rust |
|------|--------|------|
| Fetch threads/comments | `aiohttp` + Algolia HN API | `reqwest` + Algolia HN API (no key) |
| Per-thread incremental skip | component memo | `#[cocoindex::function(memo)]` on `process_thread` |
| Topic extraction | `litellm` (`gemini-2.5-flash`) | `reqwest` → OpenAI JSON (`gpt-4o-mini`) |
| Store | `postgres.TableTarget` (`hn_messages`, `hn_topics`) | hand-rolled `sqlx` (same two tables) |
| Trending / search | SQL | SQL (same scoring: thread mention = 5, comment = 1) |

**Differences:** Python defaults to Gemini; this uses OpenAI (`OPENAI_API_KEY`). The Rust SDK has no declarative Postgres target, so rows are upserted and a reconcile pass drops threads that fall out of the latest list (the incremental win is the per-thread memo).

## Prerequisites

- **Postgres** (no extensions needed):
  ```sh
  docker run -d --name hn-pg -p 5432:5432 \
    -e POSTGRES_USER=cocoindex -e POSTGRES_PASSWORD=cocoindex -e POSTGRES_DB=cocoindex \
    postgres:16-alpine
  ```
  Override with `POSTGRES_URL` (default `postgres://cocoindex:cocoindex@localhost/cocoindex`).
- `export OPENAI_API_KEY=sk-...` (override model with `LLM_MODEL`, default `gpt-4o-mini`).

Optional caps (handy to limit LLM calls): `HN_MAX_THREADS` (default 10), `HN_MAX_COMMENTS` (default unlimited).

## Usage

```sh
cargo run -- index                 # fetch + extract + store (incremental; re-runs skip done threads)
cargo run -- trending              # top trending topics by score
cargo run -- search "rust"         # messages mentioning a topic
```
