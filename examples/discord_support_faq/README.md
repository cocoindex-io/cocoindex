# Discord Support FAQ

This example turns the support threads on a Discord server into a knowledge base that an agent, or a person, can use: `FAQ.md`, one entry per question, with how often it was asked and links back to Discord.

A support forum holds the answers, but not in a form that's easy to use. The same question comes up in many posts, worded differently, each buried among thanks and dead ends; a correction posted later sits next to the answer it corrects; and nothing says which questions are still open. An agent reading the raw forum has to read every post and guess which answer is current. `FAQ.md` does that work once:

- **One answer per question.** Posts asking the same thing in other words are merged into one entry.
- **Corrections applied.** When a later reply corrects an answer, the entry gives the corrected one.
- **Disagreements flagged, not hidden.** When posts give conflicting answers, the entry says so.
- **Open questions listed,** so you can see what the knowledge base can't answer yet.
- **Kept current cheaply.** As the forum changes, only the threads and entries a change touches go back to the LLM.

Each entry is a short, self-contained section (the question, its answer and links to the posts), so the file chunks cleanly for retrieval, or fits whole in an agent's context when it's small.

The extract → merge steps follow [Meeting Notes → Knowledge Graph](https://cocoindex.io/docs/examples/meeting-notes-to-knowledge-graph/), with questions where that example has people; what differs is the source and the output:

1. List each channel's threads with the [Discord connector](https://cocoindex.io/docs/connectors/discord/): forum posts, and threads in text and announcement channels.
2. Render each thread as text (its title and every message in it), and have an LLM extract its question, its answer, whether it's resolved, and a topic, with [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/). A post tagged **Solved** counts as resolved, whatever the LLM says.
3. Merge questions asked in other words with [entity resolution](https://cocoindex.io/docs/ops/entity_resolution/): embeddings find the close pairs, and an LLM confirms each one.
4. Write `FAQ.md`: the answered questions by topic, each with how often it was asked and links to its threads: a thread tagged **Solved** is marked `(Solved)`, and one that asked the question but got no answer that worked is marked `(Open)`, so you can tell which threads the answer comes from. A question answered in one thread takes that thread's answer. For a question answered in several, an LLM writes one answer from those threads, reading their messages rather than the answers extracted from them, and says so when they disagree. Questions nobody has answered yet come last, under **Open questions**, with how often they were asked and links, as a to-do list for whoever answers.

Messages outside a thread are skipped: there, a question and its answer are separate messages, and pairing them up would be guesswork.

Runs are incremental. A thread is read again only when it gets a reply, loses a message, or is renamed. Closing, locking or retagging one reruns it, but its question and answer aren't extracted again, since the text the LLM reads is the same (the tags are left out of it; the **Solved** tag is read from the thread itself). An answer written from several threads is written again only when one of those threads changes (a retag included, since that answer names the thread tagged **Solved**) or the entry's question does, so a change to one post leaves every other entry's answer alone. A deleted thread drops out of the FAQ. The channel only picks the fields the thread's text and links read (`MESSAGE_FIELDS` and `THREAD_FIELDS` in [`main.py`](main.py)), so a reaction or someone joining a thread doesn't read anything again.

The answers are written by an LLM, so they can lose details from individual posts, or be wrong; the links back let anyone check. Merging is the LLM's judgment too, made on pairs of questions: each question is compared with a group's representative question, so a group held together by one post's wording can split when that post is deleted. One Markdown file suits a small knowledge base. For a large one, the same pipeline can declare each entry as a row in a vector store or database instead of a section of `FAQ.md`.

To correct an answer, **reply with the correction rather than editing the message**. Discord reports no change on a thread when a message inside it is edited, so the edit is read the next time the thread changes in one of the ways above. See [The gap inside threads](https://cocoindex.io/docs/connectors/discord/#the-gap-inside-threads).

## Requirements

- A Discord bot in your server, with the **Message Content** intent turned on, and **View Channels** and **Read Message History** in the channels to read. The connector page's [Setup](https://cocoindex.io/docs/connectors/discord/#setup) walks through it. The bot only reads.
- Threads to read: a forum, or a text or announcement channel with threads. To fill a forum with made-up support posts, `seed_server.py` posts through a webhook (below). For the solved ones to be tagged, give the forum a tag named **Solved**; the forum mustn't require tags.
- An LLM. By default both steps use `openai/gpt-5-mini`, with an OpenAI API key. `LLM_MODEL` and `RESOLUTION_LLM_MODEL` take any [LiteLLM model](https://docs.litellm.ai/docs/providers), such as `ollama/llama3.2` to run locally without a key.

## Run it

Configure the example:

```bash
cp .env.example .env
```

Edit `.env`: set `OPENAI_API_KEY` (or the two models), `DISCORD_BOT_TOKEN`, and `DISCORD_CHANNEL_IDS` to one or more channel IDs, comma-separated, in any order. To copy a channel's ID, turn on Developer Mode (User Settings > Advanced) and right-click the channel.

Install dependencies:

```bash
pip install -e .
```

Optionally, seed a forum: in its settings, open Integrations > Webhooks > New Webhook, and copy the URL into `.env` as `DISCORD_SEED_WEBHOOK_URL`. The script finds the forum's **Solved** tag through the bot, since a webhook can only apply a tag by its ID. Run it once (every run posts everything again):

```bash
python seed_server.py
```

Build the FAQ:

```bash
cocoindex update main
```

Then open `FAQ.md`. In the seeded forum, the three posts about uploads stalling behind a proxy should make one entry, asked 3×; the library move should give the corrected answer, not the first one; the question about file names, which nobody answered, is under Open questions; and "Show us your setup", which asks nothing, isn't there at all.

## See it update

After each change in Discord, run `cocoindex update main` again, then read `FAQ.md` and the run's `process_post` line:

1. Answer the post about file names, for example with "Yes: Settings > Downloads > Keep original file names.": `1 reprocessed`, and the question moves from Open questions into the FAQ, with that answer.
2. Tag that post **Solved**: `1 reprocessed`, but the LLM isn't asked again, since the tags aren't part of the text it reads; only the post's link changes, gaining `(Solved)`.
3. Ask a question again: make a new post asking one of the answered questions in other words, such as "Can my parents see an album without signing up?", and answer it: `1 added`, and that entry now says asked 2×, with a link to each post and an answer written from both.
4. Contradict an answer: in that new post, reply with a correction that disagrees with the other post, such as "That's incorrect, this is not possible yet. Viewing without an account is planned for the next release.": `1 reprocessed`, and that entry's answer is written again and should say that the posts disagree. No other entry changes.
5. Close a post: `1 reprocessed`, but its text is the same, so the LLM isn't asked again and `FAQ.md` stays the same.
6. Delete the post about the phone backup stuck at 99%: `1 deleted`, and the upload entry says asked 2×, without its `(Open)` link. Its question and answer may be reworded.
