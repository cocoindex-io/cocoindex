"""
Discord Support FAQ (v1) - CocoIndex pipeline example.

Turns a Discord server's support threads into FAQ.md, a knowledge base an agent
can use: one current answer per question, kept up to date as the threads change.

Build FAQ.md (one-shot catch-up; the discord source has no live mode):
    cocoindex update main

The pipeline runs in three phases:
  1. Per thread: an LLM reads it and extracts the question it asks, the answer
     that settled it, whether it's resolved, and a topic.
  2. Questions asked in other words are merged with embedding-based entity
     resolution, an LLM confirming each close pair.
  3. FAQ.md lists every answered question by topic, with how often it was asked
     and links to the threads that asked it. A question answered in several
     threads gets one answer, written by an LLM from those threads, which says
     so when they disagree. Questions nobody has answered yet come last, under
     Open questions.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import cast

import instructor
import litellm
import pydantic

import cocoindex as coco
from cocoindex.connectors import discord, localfs
from cocoindex.ops.entity_resolution import ResolvedEntities, resolve_entities
from cocoindex.ops.entity_resolution.llm_resolver import LlmPairResolver
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

litellm.drop_params = True

FAQ_PATH = pathlib.Path("FAQ.md")
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Only the fields the thread's text and links read, so a reaction or someone
# joining a thread reads nothing again. A thread keeps the fields that change
# when it gets a reply, a deletion, an archive or a lock, since those are the
# only signs that the messages inside it changed.
MESSAGE_FIELDS = ["author", "content", "embeds", "mentions", "type"]
THREAD_FIELDS = [
    "guild_id",
    "name",
    "tags",
    "message_count",
    "total_message_sent",
    "last_message_id",
    "archived",
    "archive_timestamp",
    "locked",
]

# A forum tag that marks a thread solved, whatever the LLM makes of it.
SOLVED_TAG = "Solved"

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


# ---------------------------------------------------------------------------
# Phase 1: one question and answer per thread
# ---------------------------------------------------------------------------


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


EXTRACT_PROMPT = """\
You read a support thread from a Discord server: its title, then each message as
"name: text", oldest first.

If the thread asks for help or asks a question, extract the question and the
answer that settled it. If it asks nothing (an announcement, a discussion, people
sharing things), return null.
"""


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


@dataclass
class Post:
    """A thread's question and answer, with what the FAQ links to and the
    thread's text, for writing one answer from several threads."""

    thread_id: str
    url: str
    title: str
    text: str
    question: str
    answer: str
    resolved: bool
    tagged_solved: bool  # resolved may also come from the LLM; this only from the tag
    topic: str

    @property
    def answered(self) -> bool:
        """Whether an answer in the thread worked."""
        return self.resolved and bool(self.answer)


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


# ---------------------------------------------------------------------------
# Phase 2: merge questions asked in other words
# ---------------------------------------------------------------------------


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
        # written another way does. The LLM confirms every pair within reach,
        # so reaching further costs calls, not wrong merges.
        max_distance=0.55,
    )


# ---------------------------------------------------------------------------
# Phase 3: write FAQ.md
# ---------------------------------------------------------------------------


class MergedAnswer(pydantic.BaseModel):
    answer: str = pydantic.Field(
        description="One short standalone answer to the question, from the threads."
    )


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


@coco.fn(memo=True)
async def merge_answers(question: str, thread_texts: list[str]) -> str:
    """Write one answer from the threads that answer ``question``.

    It reads the threads themselves, not the answers extracted from them, so
    the answer is one step from what people wrote."""
    client = cast(
        instructor.AsyncInstructor,
        instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON),
    )
    threads = "\n\n---\n\n".join(thread_texts)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=MergedAnswer,
        messages=[
            {"role": "system", "content": MERGE_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\n{threads}"},
        ],
    )
    return result.answer


@dataclass
class FaqEntry:
    topic: str
    question: str
    answer: str | None  # None until one of its posts is answered
    posts: list[Post]


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


async def faq_entries(posts: list[Post], questions: ResolvedEntities) -> list[FaqEntry]:
    groups: dict[str, list[Post]] = {}
    for post in posts:
        groups.setdefault(questions.canonical_of(post.question), []).append(post)
    return list(await asyncio.gather(*(faq_entry(q, g) for q, g in groups.items())))


def _render_link(post: Post, *, mark_open: bool) -> str:
    link = f"[{post.title}]({post.url})"
    if post.tagged_solved:
        return f"{link} ({SOLVED_TAG})"
    if mark_open and not post.answered:
        return f"{link} (Open)"
    return link


def _render_entry(entry: FaqEntry) -> list[str]:
    # Under Open questions every link is open, so there's nothing to mark.
    mark_open = entry.answer is not None
    links = ", ".join(_render_link(p, mark_open=mark_open) for p in entry.posts)
    answer = [entry.answer, ""] if entry.answer is not None else []
    return [
        "",
        f"### {entry.question}",
        "",
        *answer,
        f"Asked {len(entry.posts)}×: {links}",
    ]


def render_faq(entries: list[FaqEntry]) -> str:
    answered = sorted(
        (e for e in entries if e.answer is not None),
        key=lambda e: (e.topic, -len(e.posts), e.question),
    )
    open_questions = sorted(
        (e for e in entries if e.answer is None),
        key=lambda e: (-len(e.posts), e.question),
    )
    lines = [
        "# Frequently asked questions",
        "",
        "Built from the support threads on Discord. Questions asked in other "
        "words are merged into one entry.",
        "",
        f"Links marked ({SOLVED_TAG}) were tagged {SOLVED_TAG} by the forum's "
        "moderators; links marked (Open) asked the question, but no answer "
        "worked for them yet.",
    ]
    topic = None
    for entry in answered:
        if entry.topic != topic:
            topic = entry.topic
            lines += ["", f"## {topic}"]
        lines += _render_entry(entry)
    if not answered:
        lines += ["", "No answered questions yet."]
    if open_questions:
        lines += ["", "## Open questions", "", "Asked, but not answered yet."]
        for entry in open_questions:
            lines += _render_entry(entry)
    return "\n".join(lines) + "\n"


@coco.fn
async def write_faq(posts: list[Post], questions: ResolvedEntities) -> None:
    localfs.declare_file(FAQ_PATH, render_faq(await faq_entries(posts, questions)))


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


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


app = coco.App(
    coco.AppConfig(name="DiscordSupportFaqV1"),
    app_main,
)
