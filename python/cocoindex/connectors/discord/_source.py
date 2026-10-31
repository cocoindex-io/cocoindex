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
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, NamedTuple, Self

from cocoindex._internal.extras import install_hint

try:
    import httpx
except ImportError as e:  # pragma: no cover - import-time guard
    raise ImportError(
        "httpx is required for the Discord source connector. " + install_hint("discord")
    ) from e

from cocoindex._version import __version__
from cocoindex.resources.rate_limit import RateLimiter

__all__ = ["DiscordApp"]

_log = logging.getLogger(__name__)

# Unversioned URLs get API v6.
_BASE_URL = "https://discord.com/api/v10"

# Discord's largest page of messages.
_PAGE_SIZE = 100

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

# Application flags granting the Message Content intent: bit 19 is set by the
# toggle on the Developer Portal's Bot page, bit 18 after Discord's review.
_MESSAGE_CONTENT_INTENT_FLAGS = 1 << 18 | 1 << 19

_TOKEN_HINT = (
    "Check the bot token. This DiscordApp sends no more requests; fix the token "
    "and start again."
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
    """Discord answered with an error, or the connector won't send a request
    because Discord rejected the token earlier. The message names the
    request, Discord's error code and the likely fix."""


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

    async def aclose(self) -> None:
        await self._client.aclose()
