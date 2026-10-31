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

import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Any, Self

from cocoindex._internal.extras import install_hint

try:
    import httpx
except ImportError as e:  # pragma: no cover - import-time guard
    raise ImportError(
        "httpx is required for the Discord source connector. " + install_hint("discord")
    ) from e

from cocoindex._version import __version__

__all__ = ["DiscordApp"]

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


# ============================================================================
# Discord bot token
# ============================================================================


class _AppState:
    """A ``DiscordApp``'s runtime state: its lazily opened context, and the
    lock that guards opening and closing it. A frozen dataclass can't
    reassign a field, so both sit in this holder."""

    __slots__ = ("context", "lock")

    def __init__(self) -> None:
        self.context: _ApiContext | None = None
        self.lock = asyncio.Lock()


@dataclass(frozen=True)
class DiscordApp:
    """A Discord bot's token.

    The App owns the underlying ``httpx`` client, so use it as an async context
    manager, or call :meth:`aclose`, to release connections::

        async with discord.DiscordApp(token=os.environ["DISCORD_BOT_TOKEN"]) as app:
            ...

    The HTTP client is shared by **object identity**: use one ``DiscordApp``
    per bot token.

    ``ca_bundle_path``, when set, is a PEM CA-bundle file used to verify
    Discord's TLS certificate, for a network behind a private or corporate CA.
    ``None`` (the default) uses the system trust store.
    """

    token: str = field(repr=False)
    ca_bundle_path: str | None = None
    # Runtime state, so excluded from the dataclass's value identity and
    # constructor.
    _state: _AppState = field(
        default_factory=_AppState, init=False, compare=False, repr=False
    )

    async def _get_context(self) -> _ApiContext:
        ctx = self._state.context
        if ctx is not None:
            return ctx
        async with self._state.lock:
            if self._state.context is None:
                self._state.context = _ApiContext(self)
            return self._state.context

    async def aclose(self) -> None:
        """Close the HTTP connections this App opened. Idempotent; a request
        after it opens a new client."""
        async with self._state.lock:
            ctx, self._state.context = self._state.context, None
        if ctx is not None:
            await ctx.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.aclose()


class _ApiContext:
    """Per-App HTTP client, owned by a ``DiscordApp``."""

    __slots__ = ("_client",)

    def __init__(self, app: DiscordApp) -> None:
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            timeout=httpx.Timeout(60.0, connect=30.0),
            # httpx 0.28 deprecates passing the CA bundle's path as `verify`.
            verify=ssl.create_default_context(cafile=app.ca_bundle_path)
            if app.ca_bundle_path is not None
            else True,
            headers={
                "Authorization": f"Bot {app.token}",
                # The form Discord requires of a bot's User-Agent.
                "User-Agent": (
                    "DiscordBot (https://github.com/cocoindex-io/cocoindex, "
                    f"{__version__})"
                ),
            },
        )

    async def aclose(self) -> None:
        await self._client.aclose()
