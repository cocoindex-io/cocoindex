---
title: Discord Support Threads to an *FAQ*
description: 'Turn the support threads on a Discord server into FAQ.md, a knowledge base an agent can use: an LLM extracts each thread''s question and answer, entity resolution merges questions asked in other words, and only the threads and entries a change touches go back to the LLM.'
slug: discord-support-faq
tags: [llm-extraction, entity-resolution, discord]
---

A support forum holds the answers, but not in a form that's easy to use. The same question comes up in many posts, worded differently, each buried among thanks and dead ends; a correction posted later sits next to the answer it corrects; and nothing says which questions are still open. An agent reading the raw forum has to read every post and guess which answer is current.

This example does that work once. It reads a Discord server's support threads with the [Discord connector](/docs/connectors/discord/) and writes `FAQ.md`: one entry per question, with how often it was asked and links back to the threads, kept up to date as the threads change.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/discord_support_faq)

## What we're building

`FAQ.md` is a knowledge base for an agent, or a person:

- **One answer per question.** Posts asking the same thing in other words are merged into one entry.
- **Corrections applied.** When a later reply corrects an answer, the entry gives the corrected one.
- **Disagreements flagged, not hidden.** When posts give conflicting answers, the entry says so.
- **Open questions listed,** so you can see what the knowledge base can't answer yet.
- **Kept current cheaply.** As the forum changes, only the threads and entries a change touches go back to the LLM.

Each entry is a short, self-contained section (the question, its answer and links to the posts), so the file chunks cleanly for retrieval, or fits whole in an agent's context when it's small. Here is one entry from a run over the seeded forum, with the link shortened. In that thread, a community member's answer was corrected by the maintainer, and the entry gives the correction:

```markdown title="FAQ.md"
## Storage

### How do I move the photo library to a new drive without re-uploading everything?

For versions 2.0 and later: stop the server, copy the library folder to the new disk, start the server, then set the new library location in Administration > Storage. (Do not rely on LIBRARY_PATH in .env for 2.0+; it is ignored.)

Asked 1×: [How do I move the library to a bigger disk?](https://discord.com/channels/…) (Solved)
```

Questions nobody has answered come last, under `## Open questions`, with the same `Asked N×` line and no answer. The questions and answers are written by an LLM, so their wording differs from run to run.

## Pipeline overview

![CocoIndex flow: Discord threads go through three phases, per-thread LLM extraction of a question and answer, entity resolution of the questions, and one pass that writes FAQ.md to a local folder](https://cocoindex.io/blobs/docs-v1/img/examples/discord-support-faq/flow-v1-dark.svg)

The extract → merge steps follow [Meeting notes to a knowledge graph](/docs/examples/meeting-notes-to-knowledge-graph/), with questions where that example has people; what differs is the source and the output. The pipeline runs in three phases:

1. **Per thread:** render the thread as text, and have an LLM extract the question it asks, the answer that settled it, whether it's resolved, and a topic. A post tagged **Solved** counts as resolved, whatever the LLM says.
2. **Merge questions** asked in other words with [entity resolution](/docs/ops/entity_resolution/): embeddings find each question's close candidates, and an LLM picks the one it matches, if any.
3. **Write `FAQ.md`:** the answered questions by topic, each with how often it was asked and links to its threads, and the unanswered ones last, under **Open questions**.

Messages outside a thread are skipped: there, a question and its answer are separate messages, and pairing them up would be guesswork.

## Shared resources: the lifespan

The lifespan provides the [`DiscordApp`](/docs/connectors/discord/#discordapp), the two LLM models, and the embedder that phase 2 compares questions with. The models are [context keys](/docs/programming_guide/context/) with `detect_change=True`, so switching to another model reruns what used it.

```python title="main.py"
DISCORD = coco.ContextKey[discord.DiscordApp]("discord")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)
RESOLUTION_LLM_MODEL = coco.ContextKey[str]("resolution_llm_model", detect_change=True)
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with discord.DiscordApp(token=os.environ["DISCORD_BOT_TOKEN"]) as app:
        builder.provide(DISCORD, app)
        builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-5-mini"))
        builder.provide(
            RESOLUTION_LLM_MODEL,
            os.environ.get("RESOLUTION_LLM_MODEL", "openai/gpt-5-mini"),
        )
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

Both models default to `openai/gpt-5-mini`. They take any [LiteLLM model](https://docs.litellm.ai/docs/providers), such as `ollama/llama3.2` to run locally without a key.

## Phase 1: one question and answer per thread

![Phase 1: one process_post component per thread; a support post's text goes to a memoized LLM extraction and becomes a Post carried to phases 2 and 3, while a thread that asks nothing gives None](https://cocoindex.io/blobs/docs-v1/img/examples/discord-support-faq/stage-phase1-dark.svg)

The LLM fills a `QA` with [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/). `EXTRACT_PROMPT` tells it to return null for a thread that asks nothing, and the field descriptions do the rest: the question is restated so that someone else with the same problem could search for it, without the asker's setup, which keeps rewordings of one question close for phase 2.

```python title="main.py"
class QA(pydantic.BaseModel):
    question: str = pydantic.Field(
        description=(
            "The question the thread asks, restated as one short standalone "
            "question that someone else with the same problem could search for: "
            "the problem as the asker sees it, without details of their setup "
            "(how they run the server, which device) unless the question is "
            "about them. A cause found in the thread goes in the answer. No names."
        )
    )
    answer: str = pydantic.Field(
        description=(
            "The answer that settled it, as a short standalone paragraph. If a "
            "later message corrects an earlier answer, give the corrected one. "
            "Empty if nobody answered."
        )
    )
    resolved: bool = pydantic.Field(
        description=(
            "Whether someone answered the question and nothing later says the "
            "answer didn't work. False if it's unanswered or the asker is still stuck."
        )
    )
    topic: str = pydantic.Field(
        description="One or two words in Title Case naming the area, such as 'Uploads'."
    )


class ExtractedThread(pydantic.BaseModel):
    qa: QA | None = pydantic.Field(
        description="The thread's question and answer; null if it asks no question."
    )
```

`extract_qa` is memoized by the thread's text, and `process_post`, the [processing component](/docs/programming_guide/processing_component/) for one thread, turns its answer into a `Post`:

```python title="main.py"
@coco.fn(memo=True)
async def extract_qa(thread_text: str) -> QA | None:
    """Extract a thread's question and answer via LiteLLM + instructor."""
    client = cast(
        instructor.AsyncInstructor,
        instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON),
    )
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=ExtractedThread,
        messages=[
            {"role": "system", "content": EXTRACT_PROMPT},
            {"role": "user", "content": thread_text},
        ],
    )
    if result.qa is None:
        return None
    # Re-validate to restore class identity for pickling.
    return QA.model_validate(result.qa.model_dump())


@coco.fn(memo=True)
async def process_post(thread: discord.DiscordThread) -> Post | None:
    text = await thread.read_text()
    tagged_solved = SOLVED_TAG in (thread.tags or ())
    # Memoized by the text, so a thread rerun by an archive or a lock asks the
    # LLM nothing. The text's "Tags:" line is left out, so a retag doesn't
    # either: the Solved tag is read from the thread itself.
    lines = text.split("\n")
    if thread.tags:
        lines.remove(f"Tags: {', '.join(thread.tags)}")
    qa = await extract_qa("\n".join(lines))
    if qa is None:
        return None
    return Post(
        thread_id=thread.id,
        url=f"https://discord.com/channels/{thread.guild_id}/{thread.id}",
        title=thread.name or "",
        text=text,
        question=qa.question,
        answer=qa.answer,
        resolved=tagged_solved or qa.resolved,
        tagged_solved=tagged_solved,
        topic=qa.topic,
    )
```

The two levels of memoization do different jobs:

- **`process_post` reruns when the thread changes:** a reply, a deleted message, a rename, a new tag, an archive or a lock.
- **`extract_qa` is called again only when the text it reads changed.** An archive or a lock leaves the text alone, and the `Tags:` line is taken out of it, so closing, locking or retagging a thread reruns `process_post` but asks the LLM nothing. The **Solved** tag is read from the thread itself.

`Post` keeps the thread's full text too, for phase 3. A thread that asks nothing, such as *Show us your setup* in the seeded forum, returns `None` and gets no entry.

## Phase 2: merge questions asked in other words

![Phase 2: a single _resolve_questions pass; the set of extracted questions from every thread is deduplicated by embedding similarity plus LLM confirmation into a canonical-question map](https://cocoindex.io/blobs/docs-v1/img/examples/discord-support-faq/stage-phase2-dark.svg)

The seeded forum has three posts about long video uploads that never finish behind a proxy, each worded differently. [`resolve_entities`](/docs/ops/entity_resolution/) embeds every extracted question and, for each one, finds the close candidates within `max_distance`; `LlmPairResolver` then picks which candidate it matches, if any. The result maps every question to its group's canonical one.

```python title="main.py"
@coco.fn(memo=True)
async def _resolve_questions(questions: set[str]) -> ResolvedEntities:
    return await resolve_entities(
        entities=questions,
        embedder=coco.use_context(EMBEDDER),
        resolve_pair=LlmPairResolver(
            model=coco.use_context(RESOLUTION_LLM_MODEL),
            entity_type="support question",
            extra_guidance=(
                "Two questions are the same when one answer would answer both, "
                "however differently they're worded: the same symptom is the "
                "same question, even if the askers describe different setups."
            ),
        ),
        # A question reworded sits further from the original than a name
        # written another way does. A wider reach gives the LLM more candidates
        # to check, which costs calls; every merge is still one the LLM chose.
        max_distance=0.55,
    )
```

The library's default `max_distance` is 0.3, which suits a name written different ways; a reworded question sits further from the original than that. A wider reach gives the LLM more candidates to check, which costs LLM calls; every merge is still one the LLM chose. The function is memoized on the set of questions, so a run in which no question changed resolves nothing again.

## Phase 3: write FAQ.md

![Phase 3: a single write_faq pass; the posts from phase 1 and the resolved questions from phase 2 are grouped by question, merged into one answer each, and declared as FAQ.md in a local folder](https://cocoindex.io/blobs/docs-v1/img/examples/discord-support-faq/stage-phase3-dark.svg)

The posts are grouped by their canonical question. A group with no answered post is an open question. A question answered in one thread takes that thread's answer. For a question answered in several, an LLM writes one answer from those threads:

```python title="main.py"
async def faq_entry(question: str, group: list[Post]) -> FaqEntry:
    # Discord's IDs grow with time, so this is oldest first.
    group.sort(key=lambda p: int(p.thread_id))
    answered = [p for p in group if p.answered]
    if not answered:
        return FaqEntry(group[-1].topic, question, None, group)
    if len(answered) == 1:
        answer = answered[0].answer
    else:
        # Memoized by the question and the threads' text, so only an entry
        # whose question or threads changed is written again.
        answer = await merge_answers(question, [p.text for p in answered])
    return FaqEntry(answered[-1].topic, question, answer, group)
```

`merge_answers` reads the threads themselves, not the answers extracted from them, so the merged answer is one step from what people wrote. Its prompt asks it to add nothing of its own, and to say so plainly when the threads disagree, naming the thread tagged **Solved**:

```python title="main.py"
MERGE_PROMPT = f"""\
You write one FAQ answer from several support threads that ask the same
question. Each thread is its title, a "Tags:" line if it has tags, then each
message as "name: text", oldest first. A thread tagged {SOLVED_TAG} was marked
solved by the forum's moderators.

Answer the question using only what the threads say; add nothing of your own.
If the threads disagree, don't pick one silently: say plainly what each says,
naming the thread by its title, and which one is tagged {SOLVED_TAG}. Don't name
people.
"""
```

`render_faq` lays the entries out: answered questions by topic, each followed by `Asked N×:` and a link to every thread in its group. A link to a thread tagged **Solved** is marked `(Solved)`; in an answered entry, a link to a thread that asked but got no answer that worked is marked `(Open)`. `write_faq` declares the file as a [target state](/docs/programming_guide/target_state/) through [`localfs`](/docs/connectors/localfs/), so it's rewritten only when its content changes:

```python title="main.py"
@coco.fn
async def write_faq(posts: list[Post], questions: ResolvedEntities) -> None:
    localfs.declare_file(FAQ_PATH, render_faq(await faq_entries(posts, questions)))
```

## Wire it up: app_main

`app_main` collects the threads from every channel, skipping messages outside a thread, then runs the three phases. Each thread gets its own component under `post/<thread ID>`, so a deleted thread drops out of the FAQ.

```python title="main.py"
@coco.fn
async def app_main() -> None:
    app = coco.use_context(DISCORD)
    channel_ids = [
        cid.strip()
        for cid in os.environ["DISCORD_CHANNEL_IDS"].split(",")
        if cid.strip()
    ]
    threads: list[discord.DiscordThread] = []
    for cid in channel_ids:
        channel = discord.DiscordChannel(
            app=app,
            channel_id=cid,
            message_fields=MESSAGE_FIELDS,
            thread_fields=THREAD_FIELDS,
        )
        async for _key, item in channel.items():
            # Outside a thread, a question and its answer are separate
            # messages, and pairing them up would be guesswork.
            if isinstance(item, discord.DiscordThread):
                threads.append(item)

    # --- Phase 1: per thread ---
    extracted = await asyncio.gather(
        *(
            coco.use_mount(coco.component_subpath("post", t.id), process_post, t)
            for t in threads
        )
    )
    posts = [p for p in extracted if p is not None]

    # --- Phase 2: merge questions ---
    questions = await coco.use_mount(
        coco.component_subpath("resolve_questions"),
        _resolve_questions,
        {p.question for p in posts},
    )

    # --- Phase 3: FAQ.md ---
    await coco.mount(coco.component_subpath("faq"), write_faq, posts, questions)
```

`MESSAGE_FIELDS` and `THREAD_FIELDS` pick only the fields the thread's text and links read, so a reaction or someone joining a thread reruns nothing; a thread keeps its counters, last message, archive and lock fields, since they are the only signs that the messages inside it changed. See [Choosing fields](/docs/connectors/discord/#choosing-fields).

## Setup

- A Discord bot in your server, with the **Message Content** intent turned on, and **View Channels** and **Read Message History** in the channels to read. The connector page's [Setup](/docs/connectors/discord/#setup) walks through it. The bot only reads.
- Threads to read: a forum, or a text or announcement channel with threads. For solved posts to be tagged, give the forum a tag named **Solved**.
- An LLM: an OpenAI API key for the default `openai/gpt-5-mini`, or `LLM_MODEL` and `RESOLUTION_LLM_MODEL` set to another LiteLLM model.
- From the example folder, copy `.env.example` to `.env` and set `OPENAI_API_KEY` (or the two models), `DISCORD_BOT_TOKEN`, and `DISCORD_CHANNEL_IDS` to one or more channel IDs, comma-separated. Then install the dependencies:

  ```sh
  cp .env.example .env
  pip install -e .
  ```

- Optionally, fill a forum with made-up support posts: the help forum of a self-hosted photo library. In the forum's settings, open **Integrations → Webhooks → New Webhook**, and copy the URL into `.env` as `DISCORD_SEED_WEBHOOK_URL`. The script finds the forum's **Solved** tag through the bot, since a webhook can only apply a tag by its ID. It posts some posts untagged, so the forum mustn't require tags. Run it once, since every run posts everything again:

  ```sh
  python seed_server.py
  ```

## Run the pipeline

The Discord source has no live mode, so build the FAQ with a single `cocoindex update`; each run lists every channel and catches up:

```sh
cocoindex update main
```

Then open `FAQ.md`. In the seeded forum:

- the three posts about uploads stalling behind a proxy make one entry, asked 3×;
- the library move gives the corrected answer, not the first one;
- the question about file names, which nobody answered, is under **Open questions**;
- and *Show us your setup*, which asks nothing, isn't there at all.

## Incremental updates

On each run CocoIndex compares what changed and sends only that back to the LLM:

- **A thread goes back to the LLM** only when it gets a reply, loses a message, or is renamed.
- **Closing, locking or retagging a thread** reruns it, but its question and answer aren't extracted again, since the text the LLM reads is the same.
- **An answer written from several threads** is written again only when one of those threads changes (a retag included, since that answer names the thread tagged **Solved**) or the entry's question does. A change to one post leaves every other entry's answer alone.
- **A deleted thread** drops out of the FAQ.

To see it, run `cocoindex update main` again after each change in Discord, then read `FAQ.md` and the run's `process_post` line:

1. Answer the post about file names, for example with "Yes: Settings > Downloads > Keep original file names.": `1 reprocessed`, and the question moves from Open questions into the FAQ, with that answer.
2. Tag that post **Solved**: `1 reprocessed`, but the LLM isn't asked again, since the tags aren't part of the text it reads; only the post's link changes, gaining `(Solved)`.
3. Ask a question again: make a new post asking one of the answered questions in other words, such as "Can my parents see an album without signing up?", and answer it: `1 added`, and that entry now says asked 2×, with a link to each post and an answer written from both.
4. Contradict an answer: in that new post, reply with a correction that disagrees with the other post, such as "That's incorrect, this is not possible yet. Viewing without an account is planned for the next release.": `1 reprocessed`, and that entry's answer is written again and should say that the posts disagree. No other entry changes.
5. Close a post: `1 reprocessed`, but its text is the same, so the LLM isn't asked again and `FAQ.md` stays the same.
6. Delete the post about the phone backup stuck at 99%: `1 deleted`, and the upload entry says asked 2×, without its `(Open)` link. Its question and answer may be reworded.

After steps 3 and 4, the sharing entry read like this in one run:

```markdown title="FAQ.md"
### Can people view a shared album without creating an account?

The threads disagree. "Sharing an album with family who don't have accounts" (tagged Solved) says: Yes — open the album and choose Share > Create link; the link can have a password and expiry date, and you can turn off downloads. "Album for my parents" says: Not possible yet — viewing without an account is not available yet and is planned for the next release.

Asked 2×: [Sharing an album with family who don't have accounts](https://discord.com/channels/…) (Solved), [Album for my parents](https://discord.com/channels/…)
```

To correct an answer, **reply with the correction rather than editing the message**. Discord reports no change on a thread when a message inside it is edited, so the edit is read the next time the thread changes in one of the ways above. See [The gap inside threads](/docs/connectors/discord/#the-gap-inside-threads).

## Limits

- **The answers are written by an LLM,** so they can lose details from individual posts, or be wrong. The links back let anyone check.
- **Merging is the LLM's judgment too,** made on pairs of questions: each question is compared with a group's representative question, so a group held together by one post's wording can split when that post is deleted.
- **One Markdown file suits a small knowledge base.** For a large one, the same pipeline can declare each entry as a row in a vector store or database instead of a section of `FAQ.md`.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/discord_support_faq](https://github.com/cocoindex-io/cocoindex/tree/main/examples/discord_support_faq). The [Discord connector](/docs/connectors/discord/) page covers the rest of the API, and [Entity resolution](/docs/ops/entity_resolution/) the merge step's options.
