"""Discord source connector.

The public surface is exposed via the package ``cocoindex.connectors.discord``.

The connector reads a server's text, announcement and forum channels through
Discord's REST API with a bot token. A channel's items are its threads (forum
posts included) and the main-stream messages that don't start one.

* **Rate limits** are tracked per route bucket and top-level ID, as Discord
  counts them, under Discord's global limit per bot; a 429's ``retry_after`` is
  always obeyed.
* **Message Content intent.** Without it, Discord answers with messages whose
  text is empty rather than an error, so the application's flags are checked
  before a channel is listed.
"""

from __future__ import annotations

from cocoindex._internal.extras import install_hint

try:
    import httpx  # noqa: F401
except ImportError as e:  # pragma: no cover - import-time guard
    raise ImportError(
        "httpx is required for the Discord source connector. "
        + install_hint("discord")
    ) from e

__all__: list[str] = []

# Unversioned URLs get API v6.
_BASE_URL = "https://discord.com/api/v10"

# Discord's largest page of messages.
_PAGE_SIZE = 100

# Discord's documented 429s ask for waits of 65 s and longer, and a request
# that gives up fails the listing of the whole channel.
_RETRY_TOTAL_CAP_SECS = 300.0
_RETRY_BACKOFF_START_SECS = 1.0
_RETRY_BACKOFF_MAX_SECS = 30.0

# Discord's global limit per bot.
_GLOBAL_REQUESTS_PER_SECOND = 50

# Message types that are conversation: default, reply, slash command and
# context-menu command. Others are notices, such as a member added to a thread.
_CONVERSATION_MESSAGE_TYPES = frozenset({0, 19, 20, 23})

# Application flags granting the Message Content intent: bit 19 is set by the
# toggle on the Developer Portal's Bot page, bit 18 after Discord's review.
_MESSAGE_CONTENT_INTENT_FLAGS = 1 << 18 | 1 << 19
