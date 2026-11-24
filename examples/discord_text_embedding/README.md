# Discord Text Embedding

This example indexes Discord channels into Postgres with pgvector, for semantic search: a forum channel's posts, and a text or announcement channel's messages and threads.

The chunk → embed → store half is the same as [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/); what differs is the source:

1. List each channel's items with the [Discord connector](https://cocoindex.io/docs/connectors/discord/): one per forum post or thread, and one per message outside a thread.
2. Render each item as text (a post's title, tags and every message in it) and split it into chunks.
3. Embed each chunk with `sentence-transformers/all-MiniLM-L6-v2`.
4. Store the chunks and vectors in Postgres, each row with a link back to Discord.

Runs are incremental. A post re-embeds when it gets a reply, loses a message, or is renamed or retagged; other posts are left alone. Closing or locking a post reruns it, but its text is the same, and embeddings are cached by text, so nothing is embedded again. A deleted post's rows are removed. The channel only picks the fields its text and links read (`MESSAGE_FIELDS` and `THREAD_FIELDS` in [`main.py`](main.py)), so a reaction, a pin or someone joining a thread doesn't re-embed anything.

One thing isn't picked up right away: **an edit to a message inside a post**. Discord reports no change on the post for it, so it's indexed the next time the post changes in one of the ways above. See [The gap inside threads](https://cocoindex.io/docs/connectors/discord/#the-gap-inside-threads).

## Requirements

- A Discord bot in your server, with the **Message Content** intent turned on, and **View Channels** and **Read Message History** in the channels to index. The connector page's [Setup](https://cocoindex.io/docs/connectors/discord/#setup) walks through it. The bot only reads.
- Channels to index: forum, text or announcement channels. To fill one of each with made-up posts, `seed_server.py` posts through webhooks (below): support posts in the forum, a chat in the text channel, and announcements as rich embeds (a release note and a maintenance notice) in the announcement channel. It creates forum posts without tags, so the forum mustn't require them.
- Postgres with pgvector.

## Run it

Start Postgres:

```bash
docker compose -f ../../dev/postgres.yaml up -d
```

Configure the example:

```bash
cp .env.example .env
```

Edit `.env`: set `DISCORD_BOT_TOKEN`, and `DISCORD_CHANNEL_IDS` to one or more channel IDs, comma-separated, in any order. To copy a channel's ID, turn on Developer Mode (User Settings > Advanced) and right-click the channel.

Install dependencies:

```bash
pip install -e .
```

Optionally, seed the channels. A webhook belongs to one channel, so make one in each: in the channel's settings, open Integrations > Webhooks > New Webhook, and copy its URL into `.env` as `DISCORD_SEED_FORUM_WEBHOOK_URL`, `DISCORD_SEED_TEXT_WEBHOOK_URL` or `DISCORD_SEED_ANNOUNCEMENT_WEBHOOK_URL`. The script fills the channels whose webhook is set. Run it once (every run posts everything again):

```bash
python seed_server.py
```

Build the index:

```bash
cocoindex update main
```

Query the index:

```bash
python main.py "notes stop syncing in the morning"
```

Each result shows a post's or thread's title and its link, or a message's link. A message's embeds are part of its text, so `python main.py "is sync down for maintenance?"` finds the seeded announcement.

## See it update

After each change in Discord, run `cocoindex update main` again and read its `process_item` line:

1. Reply to one of the forum posts: `1 reprocessed`, the rest unchanged. Only that post was re-embedded.
2. React to a message in the text channel: nothing reprocessed, since the example doesn't read reactions.
3. Start a thread on a message in the text channel (hover over it, then **Create Thread**), and post in it: `1 reprocessed`. The message's rows now hold the thread's text, under its title. Discord may also post a "started a thread" notice in the channel, as it does for a thread on a message that isn't brand new; that shows as `1 added`, a new item but a system notice, so it gets no row.
4. Delete a forum post: `1 deleted`, and the post's rows are gone.
