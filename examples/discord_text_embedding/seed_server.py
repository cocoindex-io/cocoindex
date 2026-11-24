"""
Fill Discord channels with made-up posts for the example, one channel of each
type main.py reads:

- a forum channel: support posts, each a conversation;
- a text channel: a chat, one message each;
- an announcement channel: release notes, as rich embeds.

It posts through webhooks, so the bot that main.py reads with never needs to
write. A webhook belongs to one channel: in each channel's settings, open
Integrations -> Webhooks -> New Webhook and copy its URL into .env as
DISCORD_SEED_FORUM_WEBHOOK_URL, DISCORD_SEED_TEXT_WEBHOOK_URL or
DISCORD_SEED_ANNOUNCEMENT_WEBHOOK_URL. Then run:

    python seed_server.py

Only the channels whose webhook is set are filled. A webhook can post under any
name, so the posts read like several people talking, but it can't reply to a
message or start a thread outside a forum. Every run posts everything again.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import httpx
from dotenv import load_dotenv

# Each post: its title, then (name, message) pairs; the first message opens it.
FORUM_POSTS: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "Sync stops after my laptop wakes from sleep",
        [
            (
                "ada",
                "Every morning my notes stop syncing until I quit and reopen the "
                "app. The status bar still shows last night's sync time.",
            ),
            (
                "support",
                "Sync pauses while the machine sleeps and should resume on wake. "
                "Check Settings > Sync > **Resume after sleep**: it was off by "
                "default before version 2.4.",
            ),
            ("ada", "That was it, the setting was off. Syncing again, thanks!"),
        ],
    ),
    (
        "Exported PDF cuts off long code blocks",
        [
            (
                "bo",
                "When I export a notebook to PDF, lines in code blocks run off "
                "the right edge of the page instead of wrapping.",
            ),
            (
                "support",
                "Turn on **Wrap long lines** in the export dialog, or pick "
                "landscape pages. Code blocks keep their line breaks by default "
                "so that copied code still runs.",
            ),
        ],
    ),
    (
        "Can I host the sync server myself?",
        [
            (
                "chen",
                "Our company can't store notes on a third-party cloud. Is there "
                "a way to run the sync server on our own machines?",
            ),
            (
                "support",
                "Yes: the sync server ships as a container image. Point the apps "
                "at it under Settings > Sync > **Custom server**, and give it a "
                "volume for its data.",
            ),
            (
                "chen",
                "How do we back it up? Is copying the volume while it runs safe?",
            ),
            (
                "support",
                "Stop the container first, or use its built-in snapshot command, "
                "which writes a consistent copy while it keeps serving.",
            ),
        ],
    ),
    (
        "Password reset email never arrives",
        [
            (
                "dee",
                "I asked for a password reset three times and got nothing, not "
                "even in spam. My address is on a company domain.",
            ),
            (
                "eli",
                "Same at my work. Our mail filter was holding the messages; IT "
                "had to allow the sender.",
            ),
            (
                "support",
                "Company filters are the usual cause. Your workspace admin can "
                "also send you a sign-in link from the admin dashboard, which "
                "skips email altogether.",
            ),
        ],
    ),
    (
        "Dark mode on the phone app?",
        [
            (
                "fay",
                "The desktop app has a dark theme, but the phone app is "
                "blinding at night. Any plans?",
            ),
            (
                "support",
                "It's in the beta now: join the beta from the app store page, "
                "then pick it under Settings > Appearance.",
            ),
        ],
    ),
    (
        "Search doesn't find words inside attached PDFs",
        [
            (
                "bo",
                "Searching finds my notes' text but never words inside the PDFs "
                "I attach to them.",
            ),
            (
                "support",
                "Attachment indexing is a workspace setting, off by default for "
                "storage reasons. An admin can turn on **Index attachments**; the "
                "first pass over existing files can take a few hours.",
            ),
        ],
    ),
]


# (name, message) pairs, in order.
TEXT_MESSAGES: list[tuple[str, str]] = [
    (
        "ada",
        "Does anyone have a good way to keep meeting notes from piling up? "
        "Mine are one long notebook by now.",
    ),
    (
        "chen",
        "I start each meeting from a template with **Decisions** and **Next "
        "steps** headings, then move the next steps into my task list after.",
    ),
    (
        "bo",
        "Same, plus one tag per project. Searching a tag gives me every "
        "meeting about it, so I never sort notebooks by hand.",
    ),
    (
        "dee",
        "Tip: the web clipper saves an article as a note with its link, if you "
        "keep research next to your notes.",
    ),
    (
        "fay",
        "Is the shared workspace on the free plan, or do we need to upgrade "
        "to edit notes together?",
    ),
    (
        "eli",
        "Up to three people can share a workspace for free; past that it's "
        "the team plan.",
    ),
]

# Each announcement: the name it's posted under, and its rich embeds.
ANNOUNCEMENTS: list[tuple[str, list[dict[str, Any]]]] = [
    (
        "release-bot",
        [
            {
                "author": {"name": "Release notes"},
                "title": "Version 2.5 is out",
                "description": "Faster sync on large notebooks, and attachments "
                "you can search.",
                "fields": [
                    {
                        "name": "New",
                        "value": "Index attachments: search the text inside "
                        "attached PDFs.",
                    },
                    {
                        "name": "Fixed",
                        "value": "Sync now resumes on its own after the "
                        "laptop wakes from sleep.",
                    },
                ],
                "footer": {"text": "Update from the app store or the website."},
            }
        ],
    ),
    (
        "status-bot",
        [
            {
                "title": "Scheduled maintenance",
                "description": "Sync will be paused for about 30 minutes while "
                "we upgrade the database. Your notes stay available offline, "
                "and changes sync once it's back.",
                "footer": {"text": "Status page: status.example.com"},
            }
        ],
    ),
]


def _execute(
    client: httpx.Client, url: str, body: dict[str, Any], thread_id: str | None = None
) -> dict[str, Any]:
    params = {"wait": "true"}
    if thread_id is not None:
        params["thread_id"] = thread_id
    body = {**body, "allowed_mentions": {"parse": []}}
    while True:
        resp = client.post(url, params=params, json=body)
        if resp.status_code == 429:
            time.sleep(float(resp.json()["retry_after"]))
            continue
        # Not raise_for_status(): its message holds the URL, which holds the
        # webhook's token.
        if resp.is_error:
            sys.exit(f"Discord answered HTTP {resp.status_code}: {resp.text}")
        if resp.headers.get("X-RateLimit-Remaining") == "0":
            time.sleep(float(resp.headers["X-RateLimit-Reset-After"]))
        message: dict[str, Any] = resp.json()
        return message


def seed_forum(client: httpx.Client, url: str) -> None:
    for title, messages in FORUM_POSTS:
        (name, text), *replies = messages
        opening = _execute(
            client, url, {"thread_name": title, "username": name, "content": text}
        )
        # A forum post's opening message is posted in the post itself.
        post_id = opening["channel_id"]
        for name, text in replies:
            _execute(client, url, {"username": name, "content": text}, post_id)
        print(f"Forum: posted {title!r} ({len(messages)} messages)")


def seed_text(client: httpx.Client, url: str) -> None:
    for name, text in TEXT_MESSAGES:
        _execute(client, url, {"username": name, "content": text})
    print(f"Text channel: posted {len(TEXT_MESSAGES)} messages")


def seed_announcements(client: httpx.Client, url: str) -> None:
    for name, embeds in ANNOUNCEMENTS:
        _execute(client, url, {"username": name, "embeds": embeds})
    print(f"Announcement channel: posted {len(ANNOUNCEMENTS)} announcements")


SEEDERS = {
    "DISCORD_SEED_FORUM_WEBHOOK_URL": seed_forum,
    "DISCORD_SEED_TEXT_WEBHOOK_URL": seed_text,
    "DISCORD_SEED_ANNOUNCEMENT_WEBHOOK_URL": seed_announcements,
}


def main() -> None:
    load_dotenv()
    urls = {var: url for var in SEEDERS if (url := os.getenv(var))}
    if not urls:
        sys.exit(f"Set at least one of {', '.join(SEEDERS)} in .env.")
    with httpx.Client(timeout=30.0) as client:
        for var, url in urls.items():
            SEEDERS[var](client, url)


if __name__ == "__main__":
    main()
