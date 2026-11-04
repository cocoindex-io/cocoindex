"""Discord source connector.

The public surface is exposed via the package ``cocoindex.connectors.discord``.

The connector reads a server's text, announcement and forum channels through
Discord's REST API with a bot token. A channel's items are its threads (forum
posts included) and the main-stream messages that don't start one.

* **Rate limits** are tracked per route bucket and top-level ID, as Discord
  counts them, under Discord's global limit per bot; a 429's ``retry_after`` is
  waited out, within a cap on the total wait.
* **A rejected token** stops the App from sending more requests, since Discord
  bans an IP that sends too many failing ones.
* **Message Content intent.** Without it, Discord answers with messages whose
  text is empty rather than an error, so the application's flags are checked
  before a channel is listed.
* **A listing raises rather than ending early,** since an item missing from
  it would look deleted, and its outputs would be removed.
"""

from __future__ import annotations

import asyncio
import logging
import re
import ssl
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple, Self, TypeVar

from cocoindex._internal.extras import install_hint

try:
    import httpx
except ImportError as e:  # pragma: no cover - import-time guard
    raise ImportError(
        "httpx is required for the Discord source connector. " + install_hint("discord")
    ) from e

from cocoindex._version import __version__
from cocoindex.resources.rate_limit import RateLimiter

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

__all__ = [
    "DiscordApp",
    "DiscordAttachment",
    "DiscordChannel",
    "DiscordChannelInfo",
    "DiscordEmbed",
    "DiscordMessage",
    "DiscordMessageRef",
    "DiscordReaction",
    "DiscordThread",
    "DiscordUser",
]

_log = logging.getLogger(__name__)

_D = TypeVar("_D", bound="DataclassInstance")

# Unversioned URLs get API v6.
_BASE_URL = "https://discord.com/api/v10"

# Routes, as path templates. ``{id}`` is the top-level ID Discord counts rate
# limits by.
_ROUTE_APPLICATION = "/applications/@me"
_ROUTE_GUILD_CHANNELS = "/guilds/{id}/channels"
_ROUTE_ACTIVE_THREADS = "/guilds/{id}/threads/active"
_ROUTE_CHANNEL = "/channels/{id}"
_ROUTE_CHANNEL_MESSAGES = "/channels/{id}/messages"
_ROUTE_ARCHIVED_PUBLIC_THREADS = "/channels/{id}/threads/archived/public"
_ROUTE_JOINED_PRIVATE_THREADS = "/channels/{id}/users/@me/threads/archived/private"

# Discord's largest page, of messages and of archived threads alike.
_PAGE_SIZE = 100

# The channel types read: text and announcement channels have a main stream
# and threads, a forum only threads (its posts).
_TEXT_CHANNEL_TYPE = 0
_ANNOUNCEMENT_CHANNEL_TYPE = 5
_FORUM_CHANNEL_TYPE = 15

# Only a text channel holds private threads.
_PRIVATE_THREAD_TYPE = 12

# Discord's documented 429s ask for waits of 65 s and longer, and a request
# that gives up fails the listing of the whole channel.
_RETRY_TOTAL_CAP_SECS = 300.0
_RETRY_BACKOFF_START_SECS = 1.0
_RETRY_BACKOFF_MAX_SECS = 30.0

# A single wait this long is logged as a warning, so that it doesn't look like
# a hang.
_LONG_WAIT_SECS = 60.0

# How much of a body that isn't JSON an error message quotes.
_BODY_EXCERPT_CHARS = 200

# Discord's global limit per bot.
_GLOBAL_REQUESTS_PER_SECOND = 50

# Message types that are conversation: default, reply, slash command and
# context-menu command. Others are notices, such as a member added to a thread.
_CONVERSATION_MESSAGE_TYPES = frozenset({0, 19, 20, 23})

# A reply's message type. The type, not the ``message_reference``, marks a
# reply: crossposts, pin and thread notices and thread starters' stubs carry a
# reference too.
_REPLY_MESSAGE_TYPE = 19

# The first message in a thread started from a message: a stub standing for
# the starter, which is in the channel's main stream. It has no content, and
# a thread's ``message_count`` doesn't count it.
_STARTER_STUB_MESSAGE_TYPE = 21

# The notice Discord posts in a channel when a thread is started without a
# message, or on an older message. The first has the thread's ID.
_THREAD_CREATED_MESSAGE_TYPE = 18

# A forward's ``message_reference.type``; 0, the default, is any other
# reference.
_FORWARD_REFERENCE_TYPE = 1

# A thread's flag for a forum post pinned to the top of its forum.
_PINNED_THREAD_FLAG = 1 << 1

# The Discord tokens that a message's ``read_text()`` rewrites: a user
# mention, a custom emoji, a timestamp and a slash command. Channel, role,
# game-profile and server-navigation tokens stay as typed, since naming them
# would take requests. A name is any run of characters but whitespace,
# ``:``, ``<`` and ``>``, and a timestamp's style any letter, so a style
# Discord adds later still renders.
_TOKEN = re.compile(
    r"<(?:@!?(?P<user>\d+)"
    r"|a?:(?P<emoji>[^\s:<>]+):\d+"
    r"|t:(?P<unix>\d+)(?::[A-Za-z])?"
    r"|/(?P<command>[^\s:<>]+(?: [^\s:<>]+){0,2}):\d+)>"
)

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# The latest time a datetime holds, 9999-12-31T23:59:59Z, in Unix seconds.
_MAX_UNIX_SECS = (datetime.max.replace(tzinfo=UTC) - _UNIX_EPOCH) // timedelta(
    seconds=1
)

# Application flags granting the Message Content intent: bit 19 is set by the
# toggle on the Developer Portal's Bot page, bit 18 after Discord's review.
_MESSAGE_CONTENT_INTENT_FLAGS = 1 << 18 | 1 << 19

_TOKEN_HINT = (
    "Check the bot token. This DiscordApp sends no more requests; fix the token "
    "and start again."
)

_INTENT_HINT = (
    "The Message Content intent is off, so Discord would send messages without "
    "their text. Turn it on: Developer Portal → your app → Bot → Message "
    "Content Intent."
)

# Likely fixes for Discord's errors, by HTTP status and Discord's error code.
# Any 401 gets _TOKEN_HINT, whatever its code.
_ERROR_HINTS = {
    (403, 50001): (
        "The bot can't read this channel or server: check that it's still in the "
        "server and has View Channel and Read Message History in the channel."
    ),
    (403, 50013): (
        "The bot is missing a permission in this channel: give it View Channel "
        "and Read Message History."
    ),
    (404, 10003): "Discord doesn't know this channel ID.",
    (404, 10004): "Discord doesn't know this server ID.",
}


class _DiscordError(Exception):
    """Discord answered with an error, or the connector won't go on: Discord
    rejected the token earlier, the Message Content intent is off, or a
    listing can't be read in full. The message names the request, Discord's
    error code and the likely fix."""


def _error_hint(status: int, code: int | None) -> str | None:
    """The likely fix for an error response, if one is known."""
    if status == 401:
        return _TOKEN_HINT
    if code is None:
        return None
    return _ERROR_HINTS.get((status, code))


def _error_message(
    path: str, resp: httpx.Response, *, retries_capped: bool = False
) -> str:
    """The message for an error response: the request, the status, Discord's
    code and message, and the likely fix. A body that isn't JSON, such as a
    Cloudflare page, is quoted instead. ``retries_capped`` says that the
    request was retried until the next wait would pass the cap."""
    msg = f"Discord GET {path} failed"
    if retries_capped:
        msg += f", and retrying would pass the {_RETRY_TOTAL_CAP_SECS:g} s cap on waits"
    msg += f": HTTP {resp.status_code}"
    code: int | None = None
    if resp.content:
        try:
            body = resp.json()
        except ValueError:
            msg += f", and the body isn't JSON: {resp.text[:_BODY_EXCERPT_CHARS]!r}"
        else:
            if isinstance(body, dict):
                code = body.get("code")
                if code is not None:
                    msg += f", code {code}"
                if message := body.get("message"):
                    msg += f" ({message})"
    if (hint := _error_hint(resp.status_code, code)) is not None:
        msg += f". {hint}"
    return msg


def _retry_after(resp: httpx.Response) -> float:
    """Seconds a 429 asks to wait: the body's ``retry_after``, else the
    ``Retry-After`` header, else 1 s. The header rounds the bucket's whole
    reset up to a second: 5 s where the body said 0.345 s, and a retry after
    the body's wait went through."""
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(
        retry_after := body.get("retry_after"), int | float
    ):
        return float(retry_after)
    if (header := resp.headers.get("Retry-After")) is not None:
        try:
            return float(header)
        except ValueError:
            pass
    return 1.0


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
            channels = await app.list_channels(guild_id)
            ...

    The HTTP client, Discord's rate-limit state and the record of a rejected
    token are shared by **object identity**: use one ``DiscordApp`` per bot
    token. Two Apps with the same token pace their requests separately, so
    together they can exceed Discord's limits.

    Requests are paced on the rate-limit headers Discord sends, under
    Discord's global limit of 50 requests a second per bot, with no
    configuration. ``rate_limiter``, when set, adds a budget on top, for a bot
    whose limits other tools share: one token per request, retries included.

    ``ca_bundle_path``, when set, is a PEM CA-bundle file used to verify
    Discord's TLS certificate, for a network behind a private or corporate CA.
    ``None`` (the default) uses the system trust store.
    """

    token: str = field(repr=False)
    rate_limiter: RateLimiter | None = None
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

    async def list_channels(self, guild_id: str) -> list[DiscordChannelInfo]:
        """Every channel in the server ``guild_id``, threads aside, in
        Discord's order. One request; Discord allows about 10 a minute per
        server.

        A pipeline that mounts one component per listed channel deletes a
        channel's outputs once it drops out of this list, and from 2026-11-16
        Discord leaves out channels the bot can't view. To keep a channel's
        outputs when the bot loses access to it, name the channel's ID
        instead."""
        ctx = await self._get_context()
        channels = await ctx.get(_ROUTE_GUILD_CHANNELS, guild_id)
        return [
            DiscordChannelInfo(
                id=channel["id"],
                name=channel["name"],
                type=channel["type"],
                category_id=channel.get("parent_id"),
            )
            for channel in channels
        ]

    async def aclose(self) -> None:
        """Close the HTTP connections this App opened. Idempotent; a request
        after it opens a new client, with no rate-limit state or rejected
        token carried over."""
        async with self._state.lock:
            ctx, self._state.context = self._state.context, None
        if ctx is not None:
            await ctx.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.aclose()


@dataclass(frozen=True)
class DiscordChannelInfo:
    """One channel of a server, as :meth:`DiscordApp.list_channels` lists it."""

    id: str
    name: str
    # Discord's channel type, such as 0 for text, 2 voice, 4 a category, 5
    # announcement and 15 forum.
    type: int
    # The category the channel sits under, if any.
    category_id: str | None


class _BucketState(NamedTuple):
    """What a route's bucket has left for one top-level ID, from the latest
    response's headers."""

    remaining: int
    # When the bucket resets, in ``time.monotonic()`` seconds.
    reset_at: float


class _ApiContext:
    """Per-App HTTP client and rate-limit state, owned by a ``DiscordApp``.

    Every request goes through :meth:`get`, which paces it, retries it and
    turns Discord's errors into :class:`_DiscordError`. Discord counts a rate
    limit per bucket and top-level ID (the channel or server in the path), and
    names a route's bucket only in its responses' headers, so a route is paced
    from its first response on.
    """

    __slots__ = (
        "_bucket_states",
        "_client",
        "_global_limiter",
        "_intent_lock",
        "_intent_ok",
        "_rate_limiter",
        "_route_buckets",
        "_token_rejected",
    )

    def __init__(self, app: DiscordApp) -> None:
        self._rate_limiter = app.rate_limiter
        # Bucket pacing doesn't hold requests under the global limit: every
        # channel, thread included, has buckets of its own.
        self._global_limiter = RateLimiter(
            max_rows_per_second=_GLOBAL_REQUESTS_PER_SECOND
        )
        self._route_buckets: dict[str, str] = {}
        self._bucket_states: dict[tuple[str, str | None], _BucketState] = {}
        # Set by a 401. Discord bans an IP that gets 10,000 responses of 401,
        # 403 or 429 in 10 minutes, so a rejected token stops every request.
        self._token_rejected = False
        self._intent_ok = False
        self._intent_lock = asyncio.Lock()
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

    async def get(
        self,
        route: str,
        major: str | None,
        params: Mapping[str, str | int] | None = None,
    ) -> Any:
        """GET ``route``, a path template, with its top-level ID ``major`` in
        place of ``{id}``, and return the response's JSON.

        Each attempt, retries included, waits for the route's bucket if it's
        used up, then takes a token from the App's ``rate_limiter`` (if set)
        and from the global limiter. A 429 waits for Discord's
        ``retry_after``, and a 5xx or a network error backs off exponentially;
        those waits count toward a cap, and the error is raised once the next
        wait would pass it. Pacing doesn't count. Any other error raises at
        once, and a 401 also stops every later request.
        """
        path = route.format(id=major)
        total_waited = 0.0
        next_backoff = _RETRY_BACKOFF_START_SECS
        while True:
            if self._token_rejected:
                raise _DiscordError(
                    f"Discord GET {path} not sent: Discord rejected the bot token "
                    f"earlier (HTTP 401). {_TOKEN_HINT}"
                )
            await self._wait_for_bucket(route, major, path)
            if self._rate_limiter is not None:
                await self._rate_limiter.acquire()
            await self._global_limiter.acquire()
            wait_secs: float
            try:
                resp = await self._client.get(path, params=params)
            except httpx.TransportError as e:
                wait_secs = next_backoff
                if total_waited + wait_secs > _RETRY_TOTAL_CAP_SECS:
                    raise
                next_backoff = min(next_backoff * 2, _RETRY_BACKOFF_MAX_SECS)
                reason = repr(e)
            else:
                self._record_bucket(route, major, resp)
                status = resp.status_code
                if 200 <= status < 300:
                    try:
                        return resp.json()
                    except ValueError:
                        raise _DiscordError(_error_message(path, resp)) from None
                if status == 429:
                    wait_secs = _retry_after(resp)
                elif status >= 500:
                    wait_secs = next_backoff
                    next_backoff = min(next_backoff * 2, _RETRY_BACKOFF_MAX_SECS)
                else:
                    if status == 401:
                        self._token_rejected = True
                    raise _DiscordError(_error_message(path, resp))
                if total_waited + wait_secs > _RETRY_TOTAL_CAP_SECS:
                    raise _DiscordError(_error_message(path, resp, retries_capped=True))
                reason = f"HTTP {status}"
            _log.log(
                logging.WARNING if wait_secs > _LONG_WAIT_SECS else logging.INFO,
                "Discord GET %s: %s; retrying in %.1fs",
                path,
                reason,
                wait_secs,
            )
            await asyncio.sleep(wait_secs)
            total_waited += wait_secs

    async def _wait_for_bucket(self, route: str, major: str | None, path: str) -> None:
        """If the latest response said the route's bucket had nothing left
        for ``major``, wait until it resets."""
        bucket = self._route_buckets.get(route)
        if bucket is None:
            return
        key = (bucket, major)
        state = self._bucket_states.get(key)
        if state is None or state.remaining > 0:
            return
        wait_secs = state.reset_at - time.monotonic()
        if wait_secs > 0:
            # Unlike a retry's, this wait isn't capped, so a long one is logged.
            if wait_secs > _LONG_WAIT_SECS:
                _log.warning(
                    "Discord GET %s: rate limit reached; waiting %.1fs for it to reset",
                    path,
                    wait_secs,
                )
            await asyncio.sleep(wait_secs)
        # The reset has passed, so the next response says what the bucket has;
        # a state recorded during the wait is newer, and stays.
        if self._bucket_states.get(key) is state:
            del self._bucket_states[key]

    def _record_bucket(
        self, route: str, major: str | None, resp: httpx.Response
    ) -> None:
        """Note the route's bucket and what it has left for ``major``, from
        the response's headers. A route that sends none isn't paced.

        A 429's headers count too. They can say the bucket is used up until a
        reset later than the 429's ``retry_after``, and then the retry waits
        for that reset as well."""
        bucket = resp.headers.get("X-RateLimit-Bucket")
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset_after = resp.headers.get("X-RateLimit-Reset-After")
        if bucket is None or remaining is None or reset_after is None:
            return
        try:
            state = _BucketState(int(remaining), time.monotonic() + float(reset_after))
        except ValueError:
            return
        self._route_buckets[route] = bucket
        self._bucket_states[(bucket, major)] = state

    async def check_message_content_intent(self) -> None:
        """Raise unless the app has the Message Content intent. Without it,
        Discord sends messages with their text empty rather than an error. A
        success is kept for the context's life; a failure isn't, so the next
        check asks again."""
        if self._intent_ok:
            return
        async with self._intent_lock:
            if self._intent_ok:
                return
            application = await self.get(_ROUTE_APPLICATION, None)
            if not application.get("flags", 0) & _MESSAGE_CONTENT_INTENT_FLAGS:
                raise _DiscordError(_INTENT_HINT)
            self._intent_ok = True

    async def iter_messages(self, channel_id: str) -> AsyncIterator[dict[str, Any]]:
        """Every message in the channel ``channel_id``, a thread's too, newest
        first. Each page asks for the messages before the smallest ID on the
        page before, until one comes back empty: Discord doesn't promise that
        a short page is the last."""
        params: dict[str, str | int] = {"limit": _PAGE_SIZE}
        while True:
            page = await self.get(_ROUTE_CHANNEL_MESSAGES, channel_id, params)
            if not page:
                return
            for raw in page:
                yield raw
            # IDs vary in length, so they're compared as numbers.
            params = {
                "limit": _PAGE_SIZE,
                "before": min((raw["id"] for raw in page), key=int),
            }

    async def aclose(self) -> None:
        await self._client.aclose()


# ============================================================================
# Messages
# ============================================================================


@dataclass(frozen=True)
class DiscordUser:
    """A message's author, or a user it mentions. For a webhook's post, the
    webhook, under the name it posted with."""

    id: str
    username: str
    # The display name the user chose; ``None`` if unset.
    global_name: str | None
    # Also true for a webhook's post.
    bot: bool


@dataclass(frozen=True)
class DiscordAttachment:
    """A file attached to a message.

    ``url`` is signed, and a listing after it expires returns it signed anew,
    so it's left out of the memo key. The ID stands for the content:
    replacing a file uploads a new one, with a new ID."""

    id: str
    filename: str
    size: int
    content_type: str | None
    # The alt text. An edit can change it under the same ID.
    description: str | None
    url: str

    def __coco_memo_key__(self) -> object:
        return (self.id, self.filename, self.size, self.content_type, self.description)


@dataclass(frozen=True)
class DiscordReaction:
    """A message's reactions with one emoji."""

    # A custom emoji's ID; ``None`` for a Unicode emoji.
    emoji_id: str | None
    # The Unicode emoji itself, or a custom emoji's name.
    emoji_name: str | None
    # Super reactions included.
    count: int


@dataclass(frozen=True)
class DiscordEmbed:
    """A rich embed on a bot's or a webhook's post."""

    title: str | None
    description: str | None
    # The embed's own link.
    url: str | None
    # The embed author's name.
    author: str | None
    # The footer's text.
    footer: str | None
    # ``(name, value)`` pairs.
    fields: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DiscordMessageRef:
    """The message a forward was made from."""

    # ``None`` if Discord leaves it out.
    guild_id: str | None
    channel_id: str
    message_id: str


@dataclass(frozen=True)
class DiscordMessage:
    """One message: in a channel's main stream, starting a thread, or inside
    one.

    A forward holds the forwarded message's text, mentions, attachments and
    embeds; its other fields are the forward's own. Embeds are kept on bots'
    and webhooks' posts only: on people's posts they're link previews,
    which Discord makes from the linked page.

    A field the channel's ``message_fields`` leaves out holds ``None``, or
    ``()`` for a tuple field; ``id`` is always set. A change to any other
    field reruns a memoized function that takes the message, except a newly
    signed attachment URL.
    """

    id: str
    guild_id: str | None
    # Where it was posted: the channel, or the thread.
    channel_id: str | None
    # Discord's message type: 0 default, 19 reply, 7 a member joined, ...
    type: int | None
    author: DiscordUser | None
    webhook_id: str | None
    # Discord's Markdown, as stored.
    content: str | None
    mentions: tuple[DiscordUser, ...]
    # The IDs of the roles it mentions.
    mention_roles: tuple[str, ...]
    mention_everyone: bool | None
    attachments: tuple[DiscordAttachment, ...]
    embeds: tuple[DiscordEmbed, ...]
    # In Discord's order.
    reactions: tuple[DiscordReaction, ...]
    pinned: bool | None
    timestamp: datetime | None
    edited_timestamp: datetime | None
    # A reply's parent. The ID stays after the parent is deleted.
    reply_to: str | None
    forwarded_from: DiscordMessageRef | None

    async def read_text(self) -> str:
        """The message as plain text, ``"<author>: <text>"``.

        The text is ``content``, then each embed's author, title,
        description, fields (``name: value``) and footer, a line each. User
        mentions show the user's display name, custom emoji ``:name:``,
        timestamps ISO 8601 in UTC, and slash commands ``/name``; other
        tokens and Discord's Markdown stay as typed. ``""`` when there's no
        text, such as a member's join notice or a file without a caption.
        Only the fields the channel exposes are read: without ``author``,
        there's no ``<author>:``.
        """
        parts = [self.content]
        for embed in self.embeds:
            parts += [embed.author, embed.title, embed.description]
            parts += [f"{name}: {value}" for name, value in embed.fields]
            parts.append(embed.footer)
        text = _render_tokens("\n".join(p for p in parts if p), self.mentions)
        if not text or self.author is None:
            return text
        return f"{_display_name(self.author)}: {text}"


def _select_fields(obj: _D, selected: tuple[str, ...] | None) -> _D:
    """``obj`` with every field ``selected`` leaves out set to ``None``, or
    ``()`` for a tuple field. ``selected`` is a channel's normalized
    selection: ``None`` for every field, else the names to keep."""
    if selected is None:
        return obj
    # A field left out holds a constant, so it never reruns anything.
    left_out: dict[str, Any] = {
        f.name: () if isinstance(getattr(obj, f.name), tuple) else None
        for f in fields(obj)
        if f.name not in selected
    }
    return replace(obj, **left_out)


def _optional_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _parse_user(raw: dict[str, Any]) -> DiscordUser:
    return DiscordUser(
        id=raw["id"],
        username=raw["username"],
        global_name=raw.get("global_name"),
        bot=raw.get("bot", False),
    )


def _parse_embed(raw: dict[str, Any]) -> DiscordEmbed:
    return DiscordEmbed(
        title=raw.get("title"),
        description=raw.get("description"),
        url=raw.get("url"),
        author=raw.get("author", {}).get("name"),
        footer=raw.get("footer", {}).get("text"),
        fields=tuple((f["name"], f["value"]) for f in raw.get("fields", [])),
    )


def _parse_message(
    raw: dict[str, Any], guild_id: str, message_fields: tuple[str, ...] | None
) -> DiscordMessage:
    """A message object from Discord as a :class:`DiscordMessage`.

    ``guild_id`` is the channel's, since message objects don't carry it.
    ``message_fields`` is the channel's normalized selection: ``None`` for
    every field, else the names to keep, ``id`` among them. Which message is
    a reply and which embeds are kept are read from ``raw``, whatever the
    selection leaves out. A forward's snapshot has no author, so the
    forwarder decides whether the snapshot's embeds are kept.
    """
    reference = raw.get("message_reference", {})
    forwarded = reference.get("type", 0) == _FORWARD_REFERENCE_TYPE
    source = raw["message_snapshots"][0]["message"] if forwarded else raw
    by_bot = raw["author"].get("bot", False) or raw.get("webhook_id") is not None
    embeds = (
        tuple(
            _parse_embed(e) for e in source.get("embeds", []) if e.get("type") == "rich"
        )
        if by_bot
        else ()
    )
    forwarded_from = (
        DiscordMessageRef(
            guild_id=reference.get("guild_id"),
            channel_id=reference["channel_id"],
            message_id=reference["message_id"],
        )
        if forwarded
        else None
    )
    message = DiscordMessage(
        id=raw["id"],
        guild_id=guild_id,
        channel_id=raw["channel_id"],
        type=raw["type"],
        author=_parse_user(raw["author"]),
        webhook_id=raw.get("webhook_id"),
        content=source.get("content", ""),
        mentions=tuple(_parse_user(u) for u in source.get("mentions", [])),
        mention_roles=tuple(source.get("mention_roles", [])),
        mention_everyone=raw.get("mention_everyone", False),
        attachments=tuple(
            DiscordAttachment(
                id=a["id"],
                filename=a["filename"],
                size=a["size"],
                content_type=a.get("content_type"),
                description=a.get("description"),
                url=a["url"],
            )
            for a in source.get("attachments", [])
        ),
        embeds=embeds,
        reactions=tuple(
            DiscordReaction(
                emoji_id=r["emoji"].get("id"),
                emoji_name=r["emoji"].get("name"),
                count=r["count"],
            )
            for r in raw.get("reactions", [])
        ),
        pinned=raw.get("pinned", False),
        timestamp=datetime.fromisoformat(raw["timestamp"]),
        edited_timestamp=_optional_datetime(raw.get("edited_timestamp")),
        reply_to=(
            reference.get("message_id") if raw["type"] == _REPLY_MESSAGE_TYPE else None
        ),
        forwarded_from=forwarded_from,
    )
    return _select_fields(message, message_fields)


def _display_name(user: DiscordUser) -> str:
    """The name Discord's app shows: the user's display name, else the
    username."""
    return user.global_name or user.username


def _render_tokens(text: str, mentions: tuple[DiscordUser, ...]) -> str:
    """``text`` with Discord's tokens for users, custom emoji, timestamps and
    slash commands rewritten as plain text. A user missing from ``mentions``
    shows as ``@<id>``. A token inside a code span is rewritten too, though
    Discord's app shows it as typed."""
    names = {user.id: _display_name(user) for user in mentions}

    def render(match: re.Match[str]) -> str:
        if (user_id := match["user"]) is not None:
            return f"@{names.get(user_id, user_id)}"
        if (emoji := match["emoji"]) is not None:
            return f":{emoji}:"
        if (command := match["command"]) is not None:
            return f"/{command}"
        secs = int(match["unix"])
        # Anyone can type a time past the year 9999, and raising would fail
        # the item on every run.
        if secs > _MAX_UNIX_SECS:
            return match[0]
        # Not ``datetime.fromtimestamp``, which fails past the year 3000 on
        # Windows.
        return f"{_UNIX_EPOCH + timedelta(seconds=secs):%Y-%m-%dT%H:%M:%SZ}"

    return _TOKEN.sub(render, text)


# ============================================================================
# Threads
# ============================================================================


@dataclass(frozen=True)
class _ThreadData:
    """A thread's fields, which :class:`DiscordThread`'s properties expose,
    in their order."""

    id: str
    guild_id: str | None
    parent_id: str | None
    type: int | None
    name: str | None
    tags: tuple[str, ...]
    archived: bool | None
    archive_timestamp: datetime | None
    locked: bool | None
    pinned: bool | None
    message_count: int | None
    total_message_sent: int | None
    last_message_id: str | None
    member_count: int | None
    owner_id: str | None
    create_timestamp: datetime | None
    auto_archive_duration: int | None
    rate_limit_per_user: int | None
    invitable: bool | None
    last_pin_timestamp: datetime | None


def _parse_thread(
    raw: dict[str, Any],
    guild_id: str,
    tag_names: Mapping[str, str],
    thread_fields: tuple[str, ...] | None,
) -> _ThreadData:
    """A thread object from Discord as a thread's fields.

    ``guild_id`` is the channel's. ``tag_names`` maps the IDs of a forum's
    tags to their names; a tag the forum no longer lists is skipped.
    ``thread_fields`` is the channel's normalized selection, as for
    :func:`_parse_message`. What every thread has is read strictly, so a
    missing key raises rather than inventing a value; the rest defaults.
    """
    metadata = raw["thread_metadata"]
    return _select_fields(
        _ThreadData(
            id=raw["id"],
            guild_id=guild_id,
            parent_id=raw["parent_id"],
            type=raw["type"],
            name=raw["name"],
            tags=tuple(
                tag_names[tag_id]
                for tag_id in raw.get("applied_tags", [])
                if tag_id in tag_names
            ),
            archived=metadata["archived"],
            archive_timestamp=datetime.fromisoformat(metadata["archive_timestamp"]),
            locked=metadata["locked"],
            pinned=bool(raw.get("flags", 0) & _PINNED_THREAD_FLAG),
            message_count=raw.get("message_count", 0),
            total_message_sent=raw.get("total_message_sent", 0),
            last_message_id=raw.get("last_message_id"),
            member_count=raw.get("member_count", 0),
            owner_id=raw["owner_id"],
            create_timestamp=_optional_datetime(metadata.get("create_timestamp")),
            auto_archive_duration=metadata["auto_archive_duration"],
            rate_limit_per_user=raw.get("rate_limit_per_user", 0),
            invitable=metadata.get("invitable"),
            last_pin_timestamp=_optional_datetime(raw.get("last_pin_timestamp")),
        ),
        thread_fields,
    )


class DiscordThread:
    """One thread in a channel, forum posts included.

    Its fields come from Discord's thread listing, and :attr:`starter` from
    the channel's main stream, so neither costs a request. :meth:`messages`
    fetches the messages inside it on first use.

    A memoized function that takes the thread reruns when any of its fields
    or its starter changes: a message posted or deleted (the counters), a
    rename, new tags, an archive or unarchive, a lock or a pin. The messages
    inside aren't part of that, so an edit or a reaction inside the thread,
    or a new display name of a message's author, waits for the thread's next
    change, such as Discord archiving it, which Discord does on its own
    schedule and, on a quiet server, may never do.

    A field the channel's ``thread_fields`` leaves out holds ``None``, or
    ``()`` for ``tags``; ``id`` is always set.
    """

    __slots__ = (
        "_cached_messages",
        "_ctx",
        "_data",
        "_guild_id",
        "_message_fields",
        "_raw_message_count",
        "_starter",
    )

    def __init__(
        self,
        ctx: _ApiContext,
        raw_thread: dict[str, Any],
        guild_id: str,
        tag_names: Mapping[str, str],
        starter: DiscordMessage | None,
        message_fields: tuple[str, ...] | None,
        thread_fields: tuple[str, ...] | None,
    ) -> None:
        self._ctx = ctx
        self._data = _parse_thread(raw_thread, guild_id, tag_names, thread_fields)
        self._starter = starter
        # For the messages that messages() parses, whatever thread_fields
        # leaves out.
        self._guild_id = guild_id
        self._message_fields = message_fields
        # Read whatever thread_fields leaves out; ``None`` if Discord left it
        # out, and then messages() asks.
        self._raw_message_count: int | None = raw_thread.get("message_count")
        self._cached_messages: list[DiscordMessage] | None = None

    @property
    def id(self) -> str:
        return self._data.id

    @property
    def guild_id(self) -> str | None:
        return self._data.guild_id

    @property
    def parent_id(self) -> str | None:
        """The channel the thread is in."""
        return self._data.parent_id

    @property
    def type(self) -> int | None:
        """10 an announcement thread, 11 public, 12 private."""
        return self._data.type

    @property
    def name(self) -> str | None:
        """The thread's name; a forum post's title."""
        return self._data.name

    @property
    def tags(self) -> tuple[str, ...]:
        """A forum post's tags, by name, in Discord's order."""
        return self._data.tags

    @property
    def archived(self) -> bool | None:
        return self._data.archived

    @property
    def archive_timestamp(self) -> datetime | None:
        """When the thread was last archived or unarchived."""
        return self._data.archive_timestamp

    @property
    def locked(self) -> bool | None:
        """Whether only moderators can unarchive it."""
        return self._data.locked

    @property
    def pinned(self) -> bool | None:
        """Whether it's a forum post pinned to the top of its forum."""
        return self._data.pinned

    @property
    def message_count(self) -> int | None:
        """Messages after the first, deleted ones not counted."""
        return self._data.message_count

    @property
    def total_message_sent(self) -> int | None:
        """Messages ever sent after the first; it never goes down."""
        return self._data.total_message_sent

    @property
    def last_message_id(self) -> str | None:
        """The newest message's ID; that message may have been deleted."""
        return self._data.last_message_id

    @property
    def member_count(self) -> int | None:
        """Approximate; it stops counting at 50."""
        return self._data.member_count

    @property
    def owner_id(self) -> str | None:
        """Who started the thread."""
        return self._data.owner_id

    @property
    def create_timestamp(self) -> datetime | None:
        """``None`` for threads created before 2022-01-09."""
        return self._data.create_timestamp

    @property
    def auto_archive_duration(self) -> int | None:
        """How long the thread stays in the channel list after its last
        activity, in minutes: 60, 1440, 4320 or 10080 (*Hide After
        Inactivity* in Discord's app). Discord archives threads on its own
        schedule."""
        return self._data.auto_archive_duration

    @property
    def rate_limit_per_user(self) -> int | None:
        """Slow mode, in seconds."""
        return self._data.rate_limit_per_user

    @property
    def invitable(self) -> bool | None:
        """For a private thread, whether non-moderators can add people."""
        return self._data.invitable

    @property
    def last_pin_timestamp(self) -> datetime | None:
        """When a message in it was last pinned; ``None`` while none is."""
        return self._data.last_pin_timestamp

    @property
    def starter(self) -> DiscordMessage | None:
        """The message the thread was started from, in the channel's main
        stream. ``None`` for a forum post, whose opening message is inside
        it, and for a private thread, a thread started without a message,
        and one whose starter was deleted."""
        return self._starter

    async def messages(self) -> list[DiscordMessage]:
        """The messages in the thread, oldest first, :attr:`starter` first
        when set. Fetched on first use and kept; a thread holding nothing but
        its starter makes no request. An error raises, which fails only the
        processing of this thread, so its outputs are kept."""
        if self._cached_messages is not None:
            return self._cached_messages
        # Started from a message, with nothing posted since: inside is only
        # the stub, which message_count doesn't count.
        if self._starter is not None and self._raw_message_count == 0:
            messages = [self._starter]
        else:
            listed = [
                _parse_message(raw, self._guild_id, self._message_fields)
                async for raw in self._ctx.iter_messages(self._data.id)
                if raw["type"] != _STARTER_STUB_MESSAGE_TYPE
            ]
            # Pages run newest first, and IDs vary in length.
            listed.sort(key=lambda message: int(message.id))
            messages = listed if self._starter is None else [self._starter, *listed]
        self._cached_messages = messages
        return messages

    async def read_text(self) -> str:
        """The thread as plain text: its name, then ``Tags: a, b``, then each
        message's text (see :meth:`DiscordMessage.read_text`), oldest first,
        each starting on a new line.

        Only conversation is kept: notices, such as a member added or a
        rename, and messages without text are left out. Only the fields the
        channel exposes are read: without messages' ``type``, every message
        with text is kept.
        """
        lines = [self._data.name] if self._data.name else []
        if self._data.tags:
            lines.append(f"Tags: {', '.join(self._data.tags)}")
        for message in await self.messages():
            if (
                message.type is not None
                and message.type not in _CONVERSATION_MESSAGE_TYPES
            ):
                continue
            if text := await message.read_text():
                lines.append(text)
        return "\n".join(lines)

    def __coco_memo_key__(self) -> object:
        # Every field, then the starter, then the names of the fields its
        # messages expose: messages() and the text depend on them too, so a
        # change to the channel's message_fields, or a version that adds a
        # message field, reruns the thread. The API context isn't content.
        message_fields = (
            _MESSAGE_FIELDS if self._message_fields is None else self._message_fields
        )
        return (self._data, self._starter, message_fields)


# ============================================================================
# Channels
# ============================================================================


_MESSAGE_FIELDS = tuple(f.name for f in fields(DiscordMessage))
_THREAD_FIELDS = tuple(f.name for f in fields(_ThreadData))


def _normalize_fields(
    param: str, names: Sequence[str] | None, valid: tuple[str, ...], class_name: str
) -> tuple[str, ...] | None:
    """A field selection as the parsers take it: ``None`` for every field,
    which ``None`` and an empty sequence both mean, else the names, sorted,
    with ``id``, so that selections meaning the same give the same memo key.
    An unknown name raises ``ValueError``, listing the valid ones."""
    if not names:
        return None
    if unknown := [name for name in names if name not in valid]:
        raise ValueError(
            f"{param} {unknown} not found in {class_name} fields: {list(valid)}"
        )
    return tuple(sorted({*names, "id"}))


def _archived_before(threads: list[dict[str, Any]]) -> str:
    """The archived public listing's next ``before``: 1 ms after the oldest
    archive time on the page. Archive times have millisecond precision, so
    threads can share one, and ``before`` is strict, so this asks for a tie
    again rather than skipping it; the threads already listed are dropped.
    Ties that fill a whole page can't be got past this way. Written as
    Discord writes its timestamps."""
    oldest = min(
        datetime.fromisoformat(raw["thread_metadata"]["archive_timestamp"])
        for raw in threads
    )
    return (oldest + timedelta(milliseconds=1)).isoformat(timespec="microseconds")


def _id_before(threads: list[dict[str, Any]]) -> str:
    """A listing by ID's next ``before``: the smallest ID on the page."""
    # IDs vary in length, so they're compared as numbers.
    smallest: str = min((raw["id"] for raw in threads), key=int)
    return smallest


class DiscordChannel:
    """Handle for a text, announcement or forum channel accessed via a
    :class:`DiscordApp`.

    A channel is a lightweight handle owning no connections of its own: every
    request goes through the App's. ``channel_id`` is the channel's ID, as
    :meth:`DiscordApp.list_channels` lists it, or as Discord's app copies it
    in Developer Mode.

    ``message_fields`` and ``thread_fields`` choose the fields the items
    expose, by their names on :class:`DiscordMessage` and
    :class:`DiscordThread`. ``None`` or an empty sequence means all of them,
    and ``id`` is always set. ``message_fields`` applies to thread starters
    and the messages inside threads too. A field left out holds ``None``, or
    ``()`` for a tuple field, so a change to it reruns nothing. Discord's
    listings return whole objects, so leaving fields out saves no requests.
    An unknown name raises ``ValueError``.
    """

    __slots__ = ("_app", "_channel_id", "_message_fields", "_thread_fields")

    def __init__(
        self,
        *,
        app: DiscordApp,
        channel_id: str,
        message_fields: Sequence[str] | None = None,
        thread_fields: Sequence[str] | None = None,
    ) -> None:
        self._app = app
        self._channel_id = channel_id
        self._message_fields = _normalize_fields(
            "message_fields", message_fields, _MESSAGE_FIELDS, "DiscordMessage"
        )
        self._thread_fields = _normalize_fields(
            "thread_fields", thread_fields, _THREAD_FIELDS, "DiscordThread"
        )

    @property
    def channel_id(self) -> str:
        return self._channel_id

    def __coco_memo_key__(self) -> object:
        # The App is left out: it holds credentials and connections, not
        # content.
        return (
            "discord_channel",
            self._channel_id,
            self._message_fields,
            self._thread_fields,
        )

    async def items(self) -> AsyncIterator[tuple[str, DiscordMessage | DiscordThread]]:
        """Async iterate over (key, item) pairs for use with ``mount_each()``.

        The items are the channel's main-stream messages, newest first, then
        its threads, forum posts included, newest first. The key is the
        message's or the thread's ID. A message that starts a thread isn't an
        item of its own: it's the thread's :attr:`~DiscordThread.starter`,
        under the same ID, so a message that gains a thread turns into one.
        Discord's notice for a thread started without a message has the
        thread's ID and isn't an item either. A private thread is listed once
        someone adds the bot to it.

        Any error raises rather than ending the listing early, since the
        items missing from a cut-short listing would look deleted, and their
        outputs would be removed. That includes a channel the bot can't read,
        the Message Content intent turned off, and a listing Discord can't
        page through in full.
        """
        ctx = await self._app._get_context()
        await ctx.check_message_content_intent()
        channel = await ctx.get(_ROUTE_CHANNEL, self._channel_id)
        channel_type = channel["type"]
        if channel_type not in (
            _TEXT_CHANNEL_TYPE,
            _ANNOUNCEMENT_CHANNEL_TYPE,
            _FORUM_CHANNEL_TYPE,
        ):
            # Listing nothing would delete the channel's outputs.
            raise ValueError(
                f"Discord channel {self._channel_id} has type {channel_type}; "
                "DiscordChannel reads text (0), announcement (5) and forum (15) "
                "channels, and their threads through them."
            )
        guild_id: str = channel["guild_id"]
        tag_names = {
            tag["id"]: tag["name"] for tag in channel.get("available_tags", [])
        }
        threads = await self._list_threads(ctx, guild_id, channel_type)
        starters: dict[str, DiscordMessage] = {}
        # A forum's messages are all in its posts.
        if channel_type != _FORUM_CHANNEL_TYPE:
            async for raw in ctx.iter_messages(self._channel_id):
                message = _parse_message(raw, guild_id, self._message_fields)
                # The thread listings say which messages start a thread, not
                # the message's flags: a thread started since they ran is
                # listed next time. A thread started without a message has
                # the ID of Discord's notice for it, which starts nothing.
                if raw["id"] not in threads:
                    yield raw["id"], message
                elif raw["type"] != _THREAD_CREATED_MESSAGE_TYPE:
                    starters[raw["id"]] = message
        for thread_id in sorted(threads, key=int, reverse=True):
            yield (
                thread_id,
                DiscordThread(
                    ctx,
                    threads[thread_id],
                    guild_id,
                    tag_names,
                    starters.get(thread_id),
                    self._message_fields,
                    self._thread_fields,
                ),
            )

    async def _list_threads(
        self, ctx: _ApiContext, guild_id: str, channel_type: int
    ) -> dict[str, dict[str, Any]]:
        """The channel's threads, active and archived, as Discord's thread
        objects by ID. A later listing's entry replaces an earlier one's, so
        the newest state wins."""
        threads: dict[str, dict[str, Any]] = {}
        await self._add_active_threads(ctx, guild_id, threads)
        await self._add_archived_threads(
            ctx, _ROUTE_ARCHIVED_PUBLIC_THREADS, _archived_before, threads
        )
        if channel_type == _TEXT_CHANNEL_TYPE:
            await self._add_archived_threads(
                ctx, _ROUTE_JOINED_PRIVATE_THREADS, _id_before, threads
            )
        # A thread unarchived while the archived listings ran is in neither of
        # them. One archived meanwhile was in the first active listing.
        await self._add_active_threads(ctx, guild_id, threads)
        return threads

    async def _add_active_threads(
        self, ctx: _ApiContext, guild_id: str, threads: dict[str, dict[str, Any]]
    ) -> None:
        """Add the channel's active threads, from the server's listing of
        them. A private thread is kept only if the bot has joined it, as the
        archived listing of private threads keeps only those."""
        listing = await ctx.get(_ROUTE_ACTIVE_THREADS, guild_id)
        # Undocumented, and false live; the listing has no way to page.
        if listing.get("has_more", False):
            raise _DiscordError(
                f"Discord GET {_ROUTE_ACTIVE_THREADS.format(id=guild_id)} said more "
                "threads were left, but the listing has no way to page through "
                "them; listing the channel without them would delete their outputs."
            )
        joined = {member["id"] for member in listing["members"]}
        for raw in listing["threads"]:
            if raw["parent_id"] != self._channel_id:
                continue
            if raw["type"] == _PRIVATE_THREAD_TYPE and raw["id"] not in joined:
                continue
            threads[raw["id"]] = raw

    async def _add_archived_threads(
        self,
        ctx: _ApiContext,
        route: str,
        next_before: Callable[[list[dict[str, Any]]], str],
        threads: dict[str, dict[str, Any]],
    ) -> None:
        """Add every thread an archived listing lists, paging while Discord
        says more are left; ``next_before`` gives a page's ``before`` from the
        page before it."""
        params: dict[str, str | int] = {"limit": _PAGE_SIZE}
        listed: set[str] = set()
        while True:
            listing = await ctx.get(route, self._channel_id, params)
            page: list[dict[str, Any]] = listing["threads"]
            for raw in page:
                threads[raw["id"]] = raw
            # New to this listing, that is: a thread the active listing had may
            # have been archived since.
            new_ids = {raw["id"] for raw in page} - listed
            listed |= new_ids
            if not listing["has_more"]:
                return
            if not new_ids:
                # Starting 1 ms after the page before's oldest thread, a page
                # of threads already listed can only be ties.
                ties = page and route == _ROUTE_ARCHIVED_PUBLIC_THREADS
                cause = (
                    ": more threads were archived in the same millisecond than "
                    "fit on a page, and this listing pages by archive time, so it "
                    "can't get past them until some are unarchived or deleted"
                    if ties
                    else ""
                )
                raise _DiscordError(
                    f"Discord GET {route.format(id=self._channel_id)} listed no new "
                    "thread, though it said more were left, so the rest can't be "
                    f"reached{cause}. Listing the channel without them would delete "
                    "their outputs."
                )
            params = {"limit": _PAGE_SIZE, "before": next_before(page)}
