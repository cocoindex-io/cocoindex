<p align="center">
  <a href="https://cocoindex.io/docs/examples/hackernews-trending-topics/" title="Scrape HackerNews, LLM-extract topics, and rank what's trending with CocoIndex — Postgres, incremental, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/hackernews-trending-topics/cover.svg" alt="Rank what HackerNews is talking about with CocoIndex — fetch recent threads and comments from the Algolia HN API, an LLM pulls the topics out of every message, and a SQL score surfaces what's trending in Postgres" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">What is HackerNews <em>talking about</em> right now?</h1>

<p align="center">
  <b>Scrape recent HN threads and comments from a live HTTP API, let an LLM pull the <em>topics</em> out of every message, and rank what's trending in Postgres — in plain async Python.</b><br/>
  The source isn't a folder of files; it's a public API — so this doubles as a recipe for plugging any custom source into a pipeline.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/hackernews-trending-topics/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

The tech community's attention is hiding in plain sight — scattered across story titles and thousands of comments. This pipeline fetches recent HackerNews threads on the fly with the [Algolia HN API](https://hn.algolia.com/api), asks an LLM to normalize each message into a list of topics, and writes messages and topics into two Postgres tables you can rank and search. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so re-running only does the work for content it hasn't seen.

## How it works

The data source is a custom one: two small `async` functions over the Algolia HN API fetch the recent story IDs and flatten each thread's comment tree — no special decorator, just plain HTTP. Then each thread becomes its own processing component:

- **`extract_topics`** — text in, a list of topics out, via [litellm](https://docs.litellm.ai/) with a strict Pydantic schema. The prompt in `TopicsResponse` does the heavy lifting: it normalizes phrases into separate topics, keeps proper nouns canonical (`PostgreSQL`, `Claude`), and emits aliases for the same thing (`JFK`, `John Kennedy`).
- **`process_thread`** — fetches one story and its comments, extracts topics for each, and declares an `HnMessage` row plus one `HnTopic` row per topic.

The same topic mentioned in many messages becomes many `HnTopic` rows keyed on `(topic, message_id)` — exactly what makes the trending ranking meaningful. Read it in [`main.py`](main.py):

```python
@coco.fn
async def process_thread(thread_id: str, targets: TableTargets) -> None:
    async with aiohttp.ClientSession() as session:
        thread = await fetch_thread(session, thread_id)
    thread_topics = await extract_topics(thread.text)

    targets.messages.declare_row(row=HnMessage(
        id=thread.id, thread_id=thread.id, content_type="thread",
        author=thread.author, text=thread.text, url=thread.url, created_at=thread.created_at,
    ))
    for topic in thread_topics:
        targets.topics.declare_row(row=HnTopic(
            topic=topic, message_id=thread.id, thread_id=thread.id,
            content_type="thread", created_at=thread.created_at,
        ))
    for comment in thread.comments:
        comment_topics = await extract_topics(comment.text)
        targets.messages.declare_row(row=HnMessage(..., content_type="comment", ...))
        for topic in comment_topics:
            targets.topics.declare_row(row=HnTopic(..., content_type="comment", ...))
```

You *declare* what rows should exist — no inserts or deletes. `app_main` mounts the two Postgres tables, fetches the recent thread IDs, and fans out one `process_thread` component per thread with `mount_each`. If a thread drops out of the feed, the rows it owned are cleaned up automatically.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/hackernews-trending-topics/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the data models, the custom HTTP source, the per-thread component, and the trending SQL.
</p>

## Why it's worth a star ⭐

- **Any source, not just files.** The source here is a live HTTP API — `fetch_thread_list` and `fetch_thread` are ordinary `async def` functions. Any API, queue, or SDK plugs in the same way: fetch in plain Python, hand it to the pipeline.
- **A component per thread.** Each thread's work and its rows are grouped together, run in parallel, and committed to Postgres as soon as that thread is done — no waiting for the batch.
- **Incremental by default.** Per-message extraction is tracked, so the expensive LLM calls only run for content CocoIndex hasn't seen; re-running catches up on new stories and reuses committed rows.
- **The prompt is the product.** Topic normalization (splitting phrases, canonical proper nouns, aliases) is what makes the trending ranking meaningful — and it lives in one Pydantic `Field` description.
- **Plain Python, your stack.** Extraction is litellm, so swapping `LLM_MODEL` switches providers (Gemini, OpenAI, Ollama, …). No DSL.

## Run it

**1. Start Postgres:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — the default model is `gemini/gemini-2.5-flash`:

```sh
cp .env.example .env     # set POSTGRES_URL and GEMINI_API_KEY (or LLM_MODEL + matching key)
pip install -e .
```

**3. Build the index** — a one-shot catch-up over the latest threads (this source isn't live):

```sh
cocoindex update main
```

This fetches the most recent `MAX_THREADS` (default 10) stories and their comments, runs LLM topic extraction on every message, and writes `coco_examples.hn_messages` and `coco_examples.hn_topics`.

**4. Explore the results** — show the top trending topics and drop into a search loop, or jump straight to a topic:

```sh
python main.py            # top 20 trending topics + interactive search
python main.py "rust"     # search content for one topic
```

The trending score is computed in SQL: a thread-level mention counts for more than a comment-level one, grouped by topic and ordered by score. Re-run `cocoindex update main` anytime to catch up on new stories — only new content hits the LLM.

---

<p align="center">
  If this surfaced what HN is buzzing about, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/hackernews-trending-topics/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/hn_trending_topics" alt="" width="1" height="1" />
