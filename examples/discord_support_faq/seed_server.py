"""
Fill a Discord forum channel with made-up support posts for the example: the
help forum of a self-hosted photo library. Three posts ask the same question in
other words, one has an answer a later reply corrects, one is still unanswered,
and one asks nothing at all.

It posts through a webhook, so the bot that main.py reads with never needs to
write. In the forum's settings, open Integrations -> Webhooks -> New Webhook and
copy its URL into .env as DISCORD_SEED_WEBHOOK_URL. Then run:

    python seed_server.py

If the forum has a tag named Solved, the solved posts get it; the script finds
the tag through the bot (DISCORD_BOT_TOKEN), since a webhook can apply a tag
only by its ID. A webhook can post under any name, so the posts read like
several people talking. Every run posts everything again.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import httpx
from dotenv import load_dotenv

API_BASE_URL = "https://discord.com/api/v10"
USER_AGENT = "DiscordBot (https://github.com/cocoindex-io/cocoindex, 0.1.0)"
SOLVED_TAG = "Solved"

# Each post: its title, whether to tag it Solved, then (name, message) pairs;
# the first message opens it.
FORUM_POSTS: list[tuple[str, bool, list[tuple[str, str]]]] = [
    (
        "Uploads from my phone stall at 99%",
        True,
        [
            (
                "mika",
                "Photos upload fine from the phone, but any video longer than a "
                "minute sits at 99% forever. The server runs behind nginx.",
            ),
            (
                "maintainer",
                "That's nginx's limit on request size: it rejects anything over "
                "1 MB by default, and the app keeps retrying. Set "
                "`client_max_body_size 0;` (or a large value) in the server "
                "block and reload nginx.",
            ),
            ("mika", "Raised it and the videos went through. Thanks!"),
        ],
    ),
    (
        "Large videos never finish uploading",
        False,
        [
            (
                "jon",
                "Short clips back up, but long videos fail every time, with no "
                "error. The server runs in Docker behind a reverse proxy.",
            ),
            (
                "rosa",
                "Had the same thing. It's the proxy capping the request size, "
                "not the server. Raise the proxy's maximum request body size "
                "and they'll go through.",
            ),
            ("jon", "That was it, all caught up now."),
        ],
    ),
    (
        "Phone backup stuck at 99% on long videos",
        False,
        [
            (
                "priya",
                "Long videos from my phone never finish backing up: they get "
                "to 99% and stay there. Short clips are fine. My server is "
                "behind nginx, if that matters.",
            ),
            ("tomas", "Same here, also behind nginx."),
        ],
    ),
    (
        "How do I move the library to a bigger disk?",
        True,
        [
            (
                "lena",
                "My photo library is filling up the system disk. How do I move "
                "it to a new drive without uploading everything again?",
            ),
            (
                "omar",
                "Stop the server, copy the library folder to the new disk, and "
                "point `LIBRARY_PATH` at it in your .env.",
            ),
            (
                "maintainer",
                "Correction for 2.0 and later: the server ignores "
                "`LIBRARY_PATH` now. Stop the server, copy the folder, start "
                "it again, then set the new location under Administration > "
                "Storage.",
            ),
            ("lena", "Worked on 2.1, thanks both."),
        ],
    ),
    (
        "Face recognition doesn't find anyone",
        False,
        [
            (
                "kai",
                "I turned on face recognition, but the People page is still "
                "empty a day later.",
            ),
            (
                "maintainer",
                "Faces are only detected in new uploads. For the photos you "
                "already have, go to Administration > Jobs and run **Detect "
                "faces** on all photos.",
            ),
            ("kai", "Running now, and faces are showing up."),
        ],
    ),
    (
        "Sharing an album with family who don't have accounts",
        True,
        [
            (
                "rosa",
                "Can I share an album with my parents without making accounts "
                "for them?",
            ),
            (
                "maintainer",
                "Yes: open the album and choose Share > Create link. A link can "
                "have a password and an expiry date, and you can turn off "
                "downloads.",
            ),
        ],
    ),
    (
        "Can downloads keep the original file names?",
        False,
        [
            (
                "omar",
                "When I download an album as a zip, every file is renamed to "
                "its date. Is there a way to keep the names my camera gave them?",
            ),
        ],
    ),
    (
        "Show us your setup",
        False,
        [
            (
                "tomas",
                "Mine runs on a small home server with two mirrored disks, and "
                "a nightly backup goes to a friend's NAS.",
            ),
            (
                "lena",
                "An old laptop in a closet with the lid closed. It's been fine "
                "for a year.",
            ),
        ],
    ),
]


def _send(client: httpx.Client, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    while True:
        resp = client.request(method, url, **kwargs)
        if resp.status_code == 429:
            time.sleep(float(resp.json()["retry_after"]))
            continue
        # Not raise_for_status(): its message holds the URL, which for a webhook
        # holds its token.
        if resp.is_error:
            sys.exit(f"Discord answered HTTP {resp.status_code}: {resp.text}")
        if resp.headers.get("X-RateLimit-Remaining") == "0":
            time.sleep(float(resp.headers["X-RateLimit-Reset-After"]))
        result: dict[str, Any] = resp.json()
        return result


def _execute(
    client: httpx.Client, url: str, body: dict[str, Any], thread_id: str | None = None
) -> dict[str, Any]:
    params = {"wait": "true"}
    if thread_id is not None:
        params["thread_id"] = thread_id
    body = {**body, "allowed_mentions": {"parse": []}}
    return _send(client, "POST", url, params=params, json=body)


def solved_tag_id(client: httpx.Client, url: str, bot_token: str) -> str | None:
    # Reading a webhook through its URL needs no other credentials.
    forum_id = _send(client, "GET", url)["channel_id"]
    forum = _send(
        client,
        "GET",
        f"{API_BASE_URL}/channels/{forum_id}",
        headers={"Authorization": f"Bot {bot_token}", "User-Agent": USER_AGENT},
    )
    for tag in forum.get("available_tags", []):
        if tag["name"] == SOLVED_TAG:
            tag_id: str = tag["id"]
            return tag_id
    return None


def seed_forum(client: httpx.Client, url: str, solved_tag: str | None) -> None:
    for title, solved, messages in FORUM_POSTS:
        (name, text), *replies = messages
        body: dict[str, Any] = {"thread_name": title, "username": name, "content": text}
        if solved and solved_tag is not None:
            body["applied_tags"] = [solved_tag]
        opening = _execute(client, url, body)
        # A forum post's opening message is posted in the post itself.
        post_id = opening["channel_id"]
        for name, text in replies:
            _execute(client, url, {"username": name, "content": text}, post_id)
        print(f"Posted {title!r} ({len(messages)} messages)")


def main() -> None:
    load_dotenv()
    url = os.getenv("DISCORD_SEED_WEBHOOK_URL")
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not url or not bot_token:
        sys.exit("Set DISCORD_SEED_WEBHOOK_URL and DISCORD_BOT_TOKEN in .env.")
    with httpx.Client(timeout=30.0) as client:
        solved_tag = solved_tag_id(client, url, bot_token)
        if solved_tag is None:
            print(f"The forum has no {SOLVED_TAG!r} tag, so no post gets one.")
        seed_forum(client, url, solved_tag)


if __name__ == "__main__":
    main()
