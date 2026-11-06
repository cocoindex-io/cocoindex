"""Tests for the Discord source connector.

Covers the package's public names, ``DiscordApp`` and its internal API client
(the retry loop, rate-limit pacing, errors and the Message Content intent
check), parsing messages and threads and rendering their text, and
``DiscordChannel``'s listing and guards. And end to end, a pipeline of one
component per channel and one per item, run by the engine over a fake server:
a rerun redoes only the items that changed, a channel removed from the
pipeline loses its entries, and errors delete nothing.

HTTP is faked with ``httpx.MockTransport``; the connector's real request,
paging and parsing code runs against it. The end-to-end pipeline creates its
``DiscordApp`` in its main function, as a real one does, and ``_setup`` points
it at the fake there.

Live tests run against a real server and are skipped unless
``DISCORD_BOT_TOKEN``, ``DISCORD_TEST_GUILD`` and ``DISCORD_TEST_CHANNEL`` are
set (and, for the forum and missing-permission tests, ``DISCORD_TEST_FORUM``
and ``DISCORD_TEST_NO_HISTORY_CHANNEL``). They post, edit, archive and delete
the bot's own messages and threads, so point them at a test server.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import os
import re
import ssl
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple, cast
from urllib.parse import quote

import httpx
import pytest

import cocoindex as coco
from cocoindex._internal.memo_fingerprint import memo_fingerprint
from cocoindex.connectors import discord
from cocoindex.connectors.discord import _source
from cocoindex.connectors.discord._source import (
    _ROUTE_ARCHIVED_PUBLIC_THREADS,
    _ROUTE_CHANNEL,
    _ROUTE_CHANNEL_MESSAGES,
    _ROUTE_JOINED_PRIVATE_THREADS,
    _archived_before,
    _DiscordError,
    _error_hint,
    _looks_stripped,
    _parse_message,
    _render_tokens,
)
from cocoindex.resources.rate_limit import RateLimiter
from tests import common
from tests.common.target_states import GlobalDictTarget, Metrics

_GUILD = "1001"
_OTHER_GUILD = "901"
_CHANNEL = "1002"
_OTHER_CHANNEL = "902"
_BOT_ID = "1003"
_PERSON_ID = "1004"

# The Message Content intent as the Developer Portal's toggle grants it.
_INTENT_ON = 1 << 19

# Taken before the ``sleeps`` fixture patches it, for the fake's yields.
_real_sleep = asyncio.sleep


def test_package_reexports_source() -> None:
    assert discord.__all__ == _source.__all__
    for name in discord.__all__:
        assert getattr(discord, name) is getattr(_source, name)


# ---------------------------------------------------------------------------
# Fake Discord
# ---------------------------------------------------------------------------


def _raw_channel(
    channel_id: str, channel_type: int = 0, **fields: Any
) -> dict[str, Any]:
    """A server channel object, in ``_GUILD`` unless ``fields`` say otherwise."""
    return {
        "id": channel_id,
        "type": channel_type,
        "guild_id": _GUILD,
        "name": f"channel-{channel_id[-4:]}",
        "parent_id": None,
        "position": 0,
        "flags": 0,
        **fields,
    }


def _raw_user(
    user_id: str, username: str, global_name: str | None = None, *, bot: bool = False
) -> dict[str, Any]:
    """A user object as a message carries it. Discord sends ``bot`` only
    when it's true."""
    raw: dict[str, Any] = {
        "id": user_id,
        "username": username,
        "global_name": global_name,
        "flags": 0,
    }
    if bot:
        raw["bot"] = True
    return raw


def _raw_message(message_id: str, **fields: Any) -> dict[str, Any]:
    """A message object as Discord lists it: a person's message in
    ``_CHANNEL``, with nothing attached. ``fields`` override or add keys."""
    return {
        "id": message_id,
        "channel_id": _CHANNEL,
        "type": 0,
        "author": _raw_user(_PERSON_ID, "ada", "Ada"),
        "content": f"message {message_id[-4:]}",
        "mentions": [],
        "mention_roles": [],
        "mention_everyone": False,
        "attachments": [],
        "embeds": [],
        "components": [],
        "pinned": False,
        "tts": False,
        "flags": 0,
        "timestamp": "2026-01-15T12:00:00.250000+00:00",
        "edited_timestamp": None,
        **fields,
    }


# Discord's message for each error the fake answers.
_ERROR_MESSAGES = {
    (401, 0): "401: Unauthorized",
    (403, 50001): "Missing Access",
    (404, 0): "404: Not Found",
    (404, 10003): "Unknown Channel",
    (404, 10004): "Unknown Guild",
}


def _discord_error(status: int, code: int) -> httpx.Response:
    return httpx.Response(
        status, json={"code": code, "message": _ERROR_MESSAGES[(status, code)]}
    )


def _rate_limited(retry_after: float) -> httpx.Response:
    """A 429 with Discord's body and no rate-limit headers."""
    return httpx.Response(
        429,
        json={
            "message": "You are being rate limited.",
            "retry_after": retry_after,
            "global": False,
        },
    )


def _bucket_headers(*, remaining: int, reset_after: float) -> dict[str, str]:
    return {
        "X-RateLimit-Bucket": "b",
        "X-RateLimit-Limit": "5",
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Reset-After": str(reset_after),
    }


@dataclasses.dataclass
class _FakeBucket:
    """A rate limit the fake enforces on one path, as Discord does: ``limit``
    requests, until ``reset_after`` seconds after the first of them, then 429s
    until the reset."""

    limit: int
    reset_after: float
    remaining: int = 0
    reset_at: float = float("-inf")


def _archived_at(raw_thread: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(raw_thread["thread_metadata"]["archive_timestamp"])


class _FakeDiscord:
    """An in-memory Discord server. It answers the connector's routes from
    its state as Discord does, with 404 for an unknown channel or server. A
    thread is a channel too, so ``/channels/{id}`` answers a thread's ID.

    Knobs: ``fail[path]`` answers ``(status, code)``; ``script[path]`` answers
    the next requests to ``path`` in order (an exception is raised, as a
    network error), then normal handling resumes; ``before_request[path]`` is
    called once, just before the next request to ``path`` is answered, to
    change the state mid-listing; ``rate_limit_headers[path]`` are added to
    every answer for ``path`` but scripted ones; ``buckets[path]`` enforces a
    rate limit on ``path``, timed by ``clock``, logging each 429 it answers in
    ``rate_limited``; ``page_cap`` answers fewer messages or threads per page
    than asked for, as Discord may. ``requests`` logs every request as
    ``(method, path, params)``.
    """

    def __init__(self) -> None:
        # The application's flags, which grant the Message Content intent.
        self.flags = _INTENT_ON
        self.guild_ids = {_GUILD}
        self.channels: dict[str, dict[str, Any]] = {}
        # Raw thread objects, whose thread_metadata says whether each is
        # archived.
        self.threads: dict[str, dict[str, Any]] = {}
        # The threads the bot is a member of.
        self.joined: set[str] = set()
        # Each channel's messages, a thread's included.
        self.messages: dict[str, list[dict[str, Any]]] = {}
        self.fail: dict[str, tuple[int, int]] = {}
        self.script: dict[str, list[httpx.Response | httpx.HTTPError]] = {}
        self.before_request: dict[str, Callable[[], None]] = {}
        self.rate_limit_headers: dict[str, dict[str, str]] = {}
        self.buckets: dict[str, _FakeBucket] = {}
        self.clock: Callable[[], float] = time.monotonic
        self.rate_limited: list[str] = []
        self.page_cap: int | None = None
        self.requests: list[tuple[str, str, dict[str, str]]] = []

    def add_channel(self, raw_channel: dict[str, Any]) -> None:
        self.channels[raw_channel["id"]] = raw_channel
        self.guild_ids.add(raw_channel["guild_id"])

    def add_thread(self, raw_thread: dict[str, Any], *, joined: bool = False) -> None:
        self.threads[raw_thread["id"]] = raw_thread
        if joined:
            self.joined.add(raw_thread["id"])

    def post(self, channel_id: str, *messages: dict[str, Any]) -> None:
        """Add ``messages`` to the channel or thread ``channel_id``. A thread
        in ``threads`` counts them as Discord does: each but its first
        message (the stub standing for its starter, or a forum post's opening
        message, which has the post's ID) adds to its counters and becomes
        its ``last_message_id``."""
        posted = [{**message, "channel_id": channel_id} for message in messages]
        self.messages.setdefault(channel_id, []).extend(posted)
        if (thread := self.threads.get(channel_id)) is None:
            return
        for message in posted:
            # 21: a starter's stub.
            if message["type"] != 21 and message["id"] != channel_id:
                thread["message_count"] += 1
                thread["total_message_sent"] += 1
                thread["last_message_id"] = message["id"]

    def message(self, channel_id: str, message_id: str) -> dict[str, Any]:
        """The message ``message_id`` in ``channel_id``, to change in place."""
        return next(m for m in self.messages[channel_id] if m["id"] == message_id)

    def count(self, path: str) -> int:
        return sum(1 for _, logged, _ in self.requests if logged == path)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        # Yield, as a real request would, so concurrent callers interleave.
        await _real_sleep(0)
        path = request.url.path.removeprefix("/api/v10")
        params = dict(request.url.params)
        self.requests.append((request.method, path, params))
        if (change := self.before_request.pop(path, None)) is not None:
            change()
        if queue := self.script.get(path):
            answer = queue.pop(0)
            if isinstance(answer, httpx.HTTPError):
                raise answer
            return answer
        if (bucket := self.buckets.get(path)) is not None:
            now = self.clock()
            if now >= bucket.reset_at:
                bucket.remaining = bucket.limit
                bucket.reset_at = now + bucket.reset_after
            if bucket.remaining == 0:
                self.rate_limited.append(path)
                return httpx.Response(
                    429,
                    json={
                        "message": "You are being rate limited.",
                        "retry_after": bucket.reset_at - now,
                        "global": False,
                    },
                    headers=_bucket_headers(
                        remaining=0, reset_after=bucket.reset_at - now
                    ),
                )
            bucket.remaining -= 1
        if (failure := self.fail.get(path)) is not None:
            resp = _discord_error(*failure)
        else:
            resp = self._answer(path, params)
        resp.headers.update(self.rate_limit_headers.get(path, {}))
        if bucket is not None:
            resp.headers.update(
                _bucket_headers(
                    remaining=bucket.remaining, reset_after=bucket.reset_at - now
                )
            )
        return resp

    def _limit(self, params: dict[str, str]) -> int:
        """``limit``, 50 when it's missing (Discord's default for messages),
        and at most ``page_cap``."""
        limit = int(params.get("limit", 50))
        return limit if self.page_cap is None else min(limit, self.page_cap)

    def _message_page(
        self, channel_id: str, params: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Newest first, before the ``before`` ID (strictly, compared as
        numbers), and at most ``limit``."""
        page = sorted(
            self.messages.get(channel_id, []), key=lambda m: int(m["id"]), reverse=True
        )
        if (before := params.get("before")) is not None:
            page = [m for m in page if int(m["id"]) < int(before)]
        return [self._as_sent(m) for m in page[: self._limit(params)]]

    def _as_sent(self, message: dict[str, Any]) -> dict[str, Any]:
        """``message`` as Discord sends it to the bot. Without the Message
        Content intent, Discord empties what a message says, except in the
        bot's own messages and ones that mention it (live)."""
        if self.flags & (1 << 18 | 1 << 19):
            return message
        if message["author"]["id"] == _BOT_ID or any(
            user["id"] == _BOT_ID for user in message.get("mentions", [])
        ):
            return message
        stripped = {
            **message,
            "content": "",
            "embeds": [],
            "attachments": [],
            "components": [],
        }
        stripped.pop("poll", None)
        return stripped

    def _members(self, threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """The bot's memberships of ``threads``, as Discord's thread member
        objects."""
        return [
            {
                "id": thread["id"],
                "user_id": _BOT_ID,
                "join_timestamp": "2026-01-15T12:05:00.500000+00:00",
                "flags": 0,
            }
            for thread in threads
            if thread["id"] in self.joined
        ]

    def _thread_page(
        self, threads: list[dict[str, Any]], params: dict[str, str]
    ) -> httpx.Response:
        """A page of an archived listing: at most ``limit`` of ``threads``,
        and whether more are left."""
        limit = self._limit(params)
        page = threads[:limit]
        return httpx.Response(
            200,
            json={
                "threads": page,
                "members": self._members(page),
                "has_more": len(threads) > limit,
            },
        )

    def _archived(self, channel_id: str) -> list[dict[str, Any]]:
        return [
            thread
            for thread in self.threads.values()
            if thread["parent_id"] == channel_id
            and thread["thread_metadata"]["archived"]
        ]

    def _answer(self, path: str, params: dict[str, str]) -> httpx.Response:
        if path == "/applications/@me":
            return httpx.Response(200, json={"id": _BOT_ID, "flags": self.flags})
        if m := re.fullmatch(r"/guilds/(\d+)/channels", path):
            if m[1] not in self.guild_ids:
                return _discord_error(404, 10004)
            return httpx.Response(
                200, json=[c for c in self.channels.values() if c["guild_id"] == m[1]]
            )
        if m := re.fullmatch(r"/guilds/(\d+)/threads/active", path):
            if m[1] not in self.guild_ids:
                return _discord_error(404, 10004)
            # Every server's, unjoined private threads included, as for a bot
            # with Manage Threads.
            active = [
                thread
                for thread in self.threads.values()
                if thread["guild_id"] == m[1]
                and not thread["thread_metadata"]["archived"]
            ]
            return httpx.Response(
                200,
                json={
                    "threads": active,
                    "members": self._members(active),
                    "has_more": False,
                },
            )
        if m := re.fullmatch(r"/channels/(\d+)", path):
            raw = self.channels.get(m[1]) or self.threads.get(m[1])
            if raw is None:
                return _discord_error(404, 10003)
            return httpx.Response(200, json=raw)
        if m := re.fullmatch(r"/channels/(\d+)/messages", path):
            if (
                m[1] not in self.channels
                and m[1] not in self.threads
                and m[1] not in self.messages
            ):
                return _discord_error(404, 10003)
            return httpx.Response(200, json=self._message_page(m[1], params))
        if m := re.fullmatch(r"/channels/(\d+)/threads/archived/public", path):
            if m[1] not in self.channels:
                return _discord_error(404, 10003)
            # Newest archive first, and before ``before`` strictly, as live.
            # Ties keep the order they were added in.
            public = sorted(
                (t for t in self._archived(m[1]) if t["type"] != 12),
                key=_archived_at,
                reverse=True,
            )
            if (before := params.get("before")) is not None:
                public = [
                    t
                    for t in public
                    if _archived_at(t) < datetime.fromisoformat(before)
                ]
            return self._thread_page(public, params)
        if m := re.fullmatch(
            r"/channels/(\d+)/users/@me/threads/archived/private", path
        ):
            if m[1] not in self.channels:
                return _discord_error(404, 10003)
            # The joined ones only, by ID newest first, before the ``before`` ID.
            private = sorted(
                (
                    t
                    for t in self._archived(m[1])
                    if t["type"] == 12 and t["id"] in self.joined
                ),
                key=lambda t: int(t["id"]),
                reverse=True,
            )
            if (before := params.get("before")) is not None:
                private = [t for t in private if int(t["id"]) < int(before)]
            return self._thread_page(private, params)
        return _discord_error(404, 0)


def _fake_with_channels() -> _FakeDiscord:
    fake = _FakeDiscord()
    fake.add_channel(_raw_channel(_CHANNEL, name="general"))
    fake.add_channel(_raw_channel(_OTHER_CHANNEL, name="random"))
    return fake


async def _attach(app: discord.DiscordApp, fake: _FakeDiscord) -> _source._ApiContext:
    """Open ``app``'s context and point its client at ``fake``."""
    ctx = await app._get_context()
    await ctx._client.aclose()
    ctx._client = httpx.AsyncClient(
        base_url=_source._BASE_URL,
        headers={"Authorization": f"Bot {app.token}"},
        transport=httpx.MockTransport(fake.handler),
    )
    return ctx


async def _setup(
    fake: _FakeDiscord, **app_kwargs: Any
) -> tuple[discord.DiscordApp, _source._ApiContext]:
    """A ``DiscordApp`` whose context talks to ``fake``."""
    app = discord.DiscordApp(token="t", **app_kwargs)
    return app, await _attach(app, fake)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """The waits the connector asks for, recorded instead of slept: real 429
    waits would pass pytest's timeout. ``time.monotonic`` isn't patched, so a
    bucket wait comes out at about the bucket's reset."""
    recorded: list[float] = []

    async def record(delay: float) -> None:
        recorded.append(delay)
        await _real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", record)
    return recorded


class _RecordingLimiter:
    """Stands in for ``RateLimiter``, which can't be subclassed or report how
    often it was acquired: records its rate and counts ``acquire()`` calls,
    without waiting."""

    def __init__(self, max_rows_per_second: float | None = None) -> None:
        self.max_rows_per_second = max_rows_per_second
        self.acquired = 0

    async def acquire(self, n: int = 1) -> None:
        self.acquired += n


# ---------------------------------------------------------------------------
# DiscordApp lifecycle and HTTP client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDiscordAppLifecycle:
    """The App opens one ``_ApiContext`` (and httpx client) on first use and
    closes it with ``aclose()``. Opening a context sends no request."""

    async def test_context_is_lazy_and_shared(self) -> None:
        app = discord.DiscordApp(token="t")
        assert app._state.context is None
        async with app:
            contexts = await asyncio.gather(*(app._get_context() for _ in range(10)))
            assert all(ctx is contexts[0] for ctx in contexts)
            assert await app._get_context() is contexts[0]

    async def test_channels_share_one_context(self) -> None:
        # The intent check is kept per context, so each channel opening its
        # own would check twice.
        fake = _fake_with_channels()
        app, ctx = await _setup(fake)
        async with app:
            for channel_id in (_CHANNEL, _OTHER_CHANNEL):
                await _items(_channel(app, channel_id))
            assert app._state.context is ctx
        assert fake.count("/applications/@me") == 1
        assert fake.count(f"/channels/{_CHANNEL}") == 1
        assert fake.count(f"/channels/{_OTHER_CHANNEL}") == 1

    async def test_aclose(self) -> None:
        fake = _fake_with_channels()
        # Used up, so the bucket's state outlives the request.
        fake.rate_limit_headers[f"/channels/{_CHANNEL}"] = _bucket_headers(
            remaining=0, reset_after=1.0
        )
        fake.fail[f"/channels/{_OTHER_CHANNEL}"] = (401, 0)
        app, ctx = await _setup(fake)
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            with pytest.raises(_DiscordError):
                await ctx.get(_ROUTE_CHANNEL, _OTHER_CHANNEL)
            assert ctx._token_rejected
            assert ctx._bucket_states
        assert ctx._client.is_closed
        assert app._state.context is None
        await app.aclose()

        # The next context shares nothing with the closed one.
        new_ctx = await _attach(app, fake)
        assert new_ctx is not ctx
        assert not new_ctx._token_rejected
        assert new_ctx._route_buckets == {}
        assert new_ctx._bucket_states == {}
        await new_ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        await app.aclose()

    async def test_equality_ignores_runtime_state(self) -> None:
        app_a, app_b = discord.DiscordApp(token="t"), discord.DiscordApp(token="t")
        assert app_a == app_b
        assert hash(app_a) == hash(app_b)
        async with app_a:
            await app_a._get_context()
            assert app_a == app_b
            assert hash(app_a) == hash(app_b)
        assert discord.DiscordApp(token="a") != discord.DiscordApp(token="b")


class TestDiscordAppRepr:
    def test_token_not_in_repr(self) -> None:
        app = discord.DiscordApp(token="secret-token")
        assert "secret-token" not in repr(app)


class TestHttpClient:
    """The client's arguments, captured by a stand-in for
    ``httpx.AsyncClient``."""

    def _capture_client(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
        captured: dict[str, Any] = {}

        class _FakeClient:
            def __init__(self, **kwargs: Any) -> None:
                captured.update(kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
        return captured

    def test_client_arguments(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured = self._capture_client(monkeypatch)
        _source._ApiContext(discord.DiscordApp(token="secret"))
        assert captured["base_url"] == "https://discord.com/api/v10"
        assert captured["headers"] == {
            "Authorization": "Bot secret",
            "User-Agent": (
                "DiscordBot (https://github.com/cocoindex-io/cocoindex, "
                f"{coco.__version__})"
            ),
        }
        assert captured["timeout"] == httpx.Timeout(60.0, connect=30.0)
        # httpx's default, which reads SSL_CERT_FILE and SSL_CERT_DIR.
        assert "verify" not in captured


# ---------------------------------------------------------------------------
# Requests, retries and errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGet:
    """``_ApiContext.get``: what each response leads to, the waits and the
    cap on them."""

    @pytest.mark.parametrize(
        ("body", "headers", "wait"),
        [
            # Discord's header rounds up the bucket's whole reset.
            pytest.param({"retry_after": 0.4}, {"Retry-After": "5"}, 0.4, id="body"),
            pytest.param({}, {"Retry-After": "3"}, 3.0, id="header"),
            pytest.param({}, {}, 1.0, id="neither"),
        ],
    )
    async def test_429_wait(
        self,
        sleeps: list[float],
        body: dict[str, Any],
        headers: dict[str, str],
        wait: float,
    ) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.script[path] = [
            httpx.Response(
                429,
                json={
                    "message": "You are being rate limited.",
                    "global": False,
                    **body,
                },
                headers=headers,
            )
        ]
        app, ctx = await _setup(fake)
        async with app:
            assert await ctx.get(_ROUTE_CHANNEL, _CHANNEL) == fake.channels[_CHANNEL]
        assert sleeps == [wait]
        assert fake.count(path) == 2

    async def test_5xx_and_transport_errors_back_off(self, sleeps: list[float]) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.script[path] = [
            httpx.Response(502),
            httpx.Response(503),
            httpx.ConnectError("connection refused"),
        ]
        app, ctx = await _setup(fake)
        async with app:
            assert await ctx.get(_ROUTE_CHANNEL, _CHANNEL) == fake.channels[_CHANNEL]
            assert sleeps == [1.0, 2.0, 4.0]

            sleeps.clear()
            fake.script[path] = [httpx.Response(503) for _ in range(7)]
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]

            # A 429's wait is Discord's, not a backoff, so it doesn't double
            # the next one.
            sleeps.clear()
            fake.script[path] = [_rate_limited(0.5), httpx.Response(502)]
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert sleeps == [0.5, 1.0]

    async def test_retry_cap(self, sleeps: list[float]) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.script[path] = [_rate_limited(200.0) for _ in range(3)]
        app, ctx = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError,
                match=(
                    rf"^Discord GET {path} failed, and retrying would pass the "
                    r"300 s cap on waits: HTTP 429 \(You are being rate limited\.\)$"
                ),
            ):
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert sleeps == [200.0]
            assert fake.count(path) == 2

            # Network errors re-raise httpx's error once the next backoff
            # would pass the cap: 1 + 2 + 4 + 8 + 16 + 8 * 30 = 271 s, and
            # another 30 s would make 301.
            sleeps.clear()
            fake.script[path] = [httpx.ConnectError("refused") for _ in range(14)]
            with pytest.raises(httpx.ConnectError):
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0] + [30.0] * 8

    async def test_certificate_failure_not_retried(self, sleeps: list[float]) -> None:
        # A certificate that fails verification fails again, so retrying
        # would only delay the error by up to the cap. httpx raises it as a
        # ConnectError caused, through httpcore's, by the ssl error.
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        cause = httpx.ConnectError("certificate verify failed")
        cause.__cause__ = ssl.SSLCertVerificationError(
            1, "certificate verify failed: self-signed certificate"
        )
        error = httpx.ConnectError("certificate verify failed")
        error.__cause__ = cause
        fake.script[path] = [error]
        app, ctx = await _setup(fake)
        async with app:
            with pytest.raises(httpx.ConnectError) as excinfo:
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert excinfo.value is error
        assert sleeps == []
        assert fake.count(path) == 1

    async def test_pacing_waits_not_capped(self, sleeps: list[float]) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.rate_limit_headers[path] = _bucket_headers(remaining=0, reset_after=250.0)
        app, ctx = await _setup(fake)
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            del fake.rate_limit_headers[path]
            fake.script[path] = [_rate_limited(100.0)]
            # Waits 250 s for the bucket, then 100 s for the 429: 350 s in
            # all, but only the 100 s counts toward the cap.
            assert await ctx.get(_ROUTE_CHANNEL, _CHANNEL) == fake.channels[_CHANNEL]
        assert len(sleeps) == 2
        assert 249.0 < sleeps[0] <= 250.0
        assert sleeps[1] == 100.0

    async def test_other_4xx_not_retried(self, sleeps: list[float]) -> None:
        fake = _FakeDiscord()
        unknown = "404"
        app, ctx = await _setup(fake)
        async with app:
            with pytest.raises(_DiscordError) as excinfo:
                await ctx.get(_ROUTE_CHANNEL, unknown)
        assert str(excinfo.value) == (
            f"Discord GET /channels/{unknown} failed: HTTP 404, code 10003 "
            "(Unknown Channel). Discord doesn't know this channel ID."
        )
        assert fake.count(f"/channels/{unknown}") == 1
        assert sleeps == []

    async def test_401_latches(self) -> None:
        fake = _fake_with_channels()
        fake.fail[f"/channels/{_OTHER_CHANNEL}"] = (401, 0)
        app, ctx = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError,
                match=(
                    r"HTTP 401, code 0 \(401: Unauthorized\)\. Check the bot token\. "
                    r"This DiscordApp sends no more requests"
                ),
            ):
                await ctx.get(_ROUTE_CHANNEL, _OTHER_CHANNEL)
            with pytest.raises(
                _DiscordError,
                match=rf"^Discord GET /channels/{_CHANNEL} not sent: .*HTTP 401",
            ):
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert len(fake.requests) == 1

    # A 2xx too: its body would otherwise reach the parsers.
    @pytest.mark.parametrize("status", [403, 200])
    async def test_non_json_body(self, status: int) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.script[path] = [
            httpx.Response(
                status,
                text=(
                    "<!DOCTYPE html><html><head><title>Access denied | discord.com "
                    "used Cloudflare to restrict access</title></head></html>"
                ),
            )
        ]
        app, ctx = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError,
                match=rf"HTTP {status}, and the body isn't JSON: '<!DOCTYPE html><html>",
            ):
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)


class TestErrorHint:
    @pytest.mark.parametrize(
        ("status", "code", "hint"),
        [
            (401, 0, "Check the bot token."),
            (401, 40001, "Check the bot token."),
            (401, None, "Check the bot token."),
            (403, 50001, "The bot can't read this channel or server"),
            (403, 50013, "The bot is missing a permission in this channel"),
            (404, 10003, "Discord doesn't know this channel ID."),
            (404, 10004, "Discord doesn't know this server ID."),
        ],
    )
    def test_known(self, status: int, code: int | None, hint: str) -> None:
        found = _error_hint(status, code)
        assert found is not None
        assert found.startswith(hint)

    @pytest.mark.parametrize(
        ("status", "code"), [(404, 10008), (403, None), (400, 50035), (500, None)]
    )
    def test_unknown(self, status: int, code: int | None) -> None:
        assert _error_hint(status, code) is None


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPacing:
    async def test_bucket_pacing_per_top_level_id(self, sleeps: list[float]) -> None:
        # Live, two channels' message listings shared a bucket ID but were
        # counted separately.
        fake = _fake_with_channels()
        fake.rate_limit_headers[f"/channels/{_CHANNEL}"] = _bucket_headers(
            remaining=0, reset_after=2.5
        )
        fake.rate_limit_headers[f"/channels/{_OTHER_CHANNEL}"] = _bucket_headers(
            remaining=4, reset_after=2.5
        )
        app, ctx = await _setup(fake)
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert sleeps == []
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert len(sleeps) == 1
            assert 2.0 < sleeps[0] <= 2.5
            await ctx.get(_ROUTE_CHANNEL, _OTHER_CHANNEL)
            await ctx.get(_ROUTE_CHANNEL, _OTHER_CHANNEL)
        assert len(sleeps) == 1

    async def test_429_headers_pace_the_retry(self, sleeps: list[float]) -> None:
        # A 429's body can ask for less than its headers say the bucket needs
        # to reset; the retry waits out both, the second wait uncounted.
        fake = _fake_with_channels()
        fake.script[f"/channels/{_CHANNEL}"] = [
            httpx.Response(
                429,
                json={
                    "message": "You are being rate limited.",
                    "retry_after": 0.5,
                    "global": False,
                },
                headers=_bucket_headers(remaining=0, reset_after=4.5),
            )
        ]
        app, ctx = await _setup(fake)
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert len(sleeps) == 2
        assert sleeps[0] == 0.5
        assert 4.0 < sleeps[1] <= 4.5

    async def test_concurrent_requests_share_the_bucket(
        self, sleeps: list[float], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Channels listed together each send the server's active-thread
        # listing twice, all on one bucket and server ID. The bucket's state
        # comes only from responses, so requests sent together would all see
        # what was left before any of them.
        monkeypatch.setattr(_source, "RateLimiter", _RecordingLimiter)
        fake = _FakeDiscord()
        channel_ids = [str(n) for n in range(1099, 1107)]
        for channel_id in channel_ids:
            fake.add_channel(_raw_channel(channel_id))
        path = f"/guilds/{_GUILD}/threads/active"
        fake.buckets[path] = _FakeBucket(limit=5, reset_after=0.5)
        # Time passes as the connector waits.
        fake.clock = lambda: time.monotonic() + sum(sleeps)
        app, _ = await _setup(fake)
        async with app:
            await asyncio.gather(*(_items(_channel(app, c)) for c in channel_ids))
        assert fake.rate_limited == []
        assert fake.count(path) == 2 * len(channel_ids)

    async def test_finished_requests_leave_nothing_behind(
        self, sleeps: list[float]
    ) -> None:
        # Every thread is its own top-level ID, so a long-lived App that kept
        # a lock and a bucket state per ID would keep one per thread it read.
        fake = _fake_with_channels()
        channel_path = f"/channels/{_CHANNEL}"
        fake.rate_limit_headers[channel_path] = _bucket_headers(
            remaining=4, reset_after=2.5
        )
        # Used up, but reset by the time the requests finish.
        fake.rate_limit_headers[f"/channels/{_OTHER_CHANNEL}"] = _bucket_headers(
            remaining=0, reset_after=0.0
        )
        app, ctx = await _setup(fake)
        async with app:
            await asyncio.gather(
                *(
                    ctx.get(_ROUTE_CHANNEL, channel_id)
                    for channel_id in [_CHANNEL, _OTHER_CHANNEL] * 3
                )
            )
            assert ctx._route_locks == {}
            assert ctx._bucket_states == {}

            # A state saying to wait is kept, so the next request still waits.
            fake.rate_limit_headers[channel_path] = _bucket_headers(
                remaining=0, reset_after=2.5
            )
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert ctx._route_locks == {}
            assert list(ctx._bucket_states) == [("b", _CHANNEL)]
        assert sleeps == []

    async def test_global_429_pauses_every_request(self, sleeps: list[float]) -> None:
        # A global limit covers every request the bot sends, and Discord
        # counts each 429 toward its ban on an IP.
        fake = _fake_with_channels()
        fake.script[f"/channels/{_CHANNEL}"] = [
            httpx.Response(
                429,
                json={
                    "message": "You are being rate limited.",
                    "retry_after": 2.0,
                    "global": True,
                },
            )
        ]
        app, ctx = await _setup(fake)

        async def after_the_429() -> Any:
            # Once the first request starts waiting out its 429.
            while not sleeps:
                await _real_sleep(0)
            return await ctx.get(_ROUTE_CHANNEL, _OTHER_CHANNEL)

        async with app:
            await asyncio.gather(ctx.get(_ROUTE_CHANNEL, _CHANNEL), after_the_429())
        # Each request waited out the pause once.
        assert len(sleeps) == 2
        assert all(1.9 < wait <= 2.0 for wait in sleeps)
        assert fake.count(f"/channels/{_CHANNEL}") == 2
        assert fake.count(f"/channels/{_OTHER_CHANNEL}") == 1

    async def test_routes_without_headers_not_paced(self, sleeps: list[float]) -> None:
        app, ctx = await _setup(_fake_with_channels())
        async with app:
            for _ in range(3):
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert sleeps == []
        assert ctx._route_buckets == {}

    async def test_caller_limiter_one_token_per_attempt(
        self, sleeps: list[float]
    ) -> None:
        fake = _fake_with_channels()
        fake.script[f"/channels/{_CHANNEL}"] = [_rate_limited(0.5)]
        limiter = _RecordingLimiter()
        app, ctx = await _setup(fake, rate_limiter=cast(RateLimiter, limiter))
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert limiter.acquired == 2

    async def test_global_limiter(
        self, sleeps: list[float], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        created: list[_RecordingLimiter] = []

        def recording_limiter(max_rows_per_second: float) -> _RecordingLimiter:
            limiter = _RecordingLimiter(max_rows_per_second)
            created.append(limiter)
            return limiter

        monkeypatch.setattr(_source, "RateLimiter", recording_limiter)
        caller = _RecordingLimiter()
        fake = _fake_with_channels()
        for app_kwargs in ({}, {"rate_limiter": cast(RateLimiter, caller)}):
            fake.script[f"/channels/{_CHANNEL}"] = [_rate_limited(0.5)]
            created.clear()
            app, ctx = await _setup(fake, **app_kwargs)
            async with app:
                await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
            assert len(created) == 1
            assert ctx._global_limiter is cast(RateLimiter, created[0])
            assert created[0].max_rows_per_second == 50
            assert created[0].acquired == 2
        assert caller.acquired == 2


# ---------------------------------------------------------------------------
# Message Content intent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestIntentCheck:
    @pytest.mark.parametrize(
        ("flags", "intent_on"),
        [
            pytest.param(1 << 19, True, id="toggle"),
            pytest.param(1 << 18, True, id="reviewed"),
            pytest.param(1 << 12 | 1 << 15, False, id="neither"),
        ],
    )
    async def test_flags(self, flags: int, intent_on: bool) -> None:
        fake = _FakeDiscord()
        fake.flags = flags
        app, ctx = await _setup(fake)
        async with app:
            if intent_on:
                await ctx.check_message_content_intent()
            else:
                with pytest.raises(
                    _DiscordError,
                    match=r"^The Message Content intent is off.*Message Content Intent\.$",
                ):
                    await ctx.check_message_content_intent()

    async def test_success_cached_failure_not(self) -> None:
        fake = _FakeDiscord()
        fake.flags = 0
        app, ctx = await _setup(fake)
        async with app:
            for _ in range(2):
                with pytest.raises(_DiscordError):
                    await ctx.check_message_content_intent_once()
            assert fake.count("/applications/@me") == 2
            fake.flags = _INTENT_ON
            for _ in range(2):
                await ctx.check_message_content_intent_once()
        assert fake.count("/applications/@me") == 3

    async def test_concurrent_first_checks_send_one(self) -> None:
        fake = _FakeDiscord()
        app, ctx = await _setup(fake)
        async with app:
            await asyncio.gather(
                *(ctx.check_message_content_intent_once() for _ in range(5))
            )
        assert fake.count("/applications/@me") == 1

    async def test_concurrent_first_checks_share_a_failure(
        self, sleeps: list[float]
    ) -> None:
        # Behind a lasting network error, each channel waiting on the first
        # check would otherwise retry up to the cap in turn.
        fake = _FakeDiscord()
        fake.script["/applications/@me"] = [
            httpx.ConnectError("refused") for _ in range(14)
        ]
        app, ctx = await _setup(fake)
        async with app:
            results = await asyncio.gather(
                *(ctx.check_message_content_intent_once() for _ in range(5)),
                return_exceptions=True,
            )
            assert all(isinstance(result, httpx.ConnectError) for result in results)
            assert fake.count("/applications/@me") == 14
            assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0] + [30.0] * 8
            # A failure isn't kept: the next check asks again.
            await ctx.check_message_content_intent_once()
        assert fake.count("/applications/@me") == 15

    async def test_cancelled_callers_leave_no_unread_error(
        self, sleeps: list[float], caplog: pytest.LogCaptureFixture
    ) -> None:
        # Callers cancelled while the shared check is under way, as at a
        # shutdown, leave nobody to read its error, which asyncio then logs.
        fake = _FakeDiscord()
        fake.script["/applications/@me"] = [
            httpx.Response(502),
            _discord_error(401, 0),
        ]
        app, ctx = await _setup(fake)
        async with app:
            callers = [
                asyncio.create_task(ctx.check_message_content_intent_once())
                for _ in range(2)
            ]
            # Until the check backs off after the 502.
            while not sleeps:
                await _real_sleep(0)
            check = ctx._intent_check
            assert check is not None
            for caller in callers:
                caller.cancel()
            await asyncio.wait(callers)
            await asyncio.wait([check])
            # Reading its error here would read it for the connector.
            assert check.done() and not check.cancelled()
            del check
            gc.collect()
        assert "never retrieved" not in caplog.text


# ---------------------------------------------------------------------------
# DiscordApp.list_channels
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestListChannels:
    async def test_every_channel(self) -> None:
        category, voice, forum, news = (
            "1010",
            "904",
            "1011",
            "1012",
        )
        fake = _FakeDiscord()
        fake.add_channel(_raw_channel(category, 4, name="Text Channels"))
        fake.add_channel(_raw_channel(_CHANNEL, 0, name="general", parent_id=category))
        fake.add_channel(_raw_channel(voice, 2, name="Lounge"))
        fake.add_channel(_raw_channel(forum, 15, name="help", available_tags=[]))
        fake.add_channel(_raw_channel(news, 5, name="news"))
        fake.add_channel(
            _raw_channel("905", 0, name="elsewhere", guild_id=_OTHER_GUILD)
        )
        app, _ = await _setup(fake)
        async with app:
            channels = await app.list_channels(_GUILD)
        info = discord.DiscordChannelInfo
        assert channels == [
            info(id=category, name="Text Channels", type=4, category_id=None),
            info(id=_CHANNEL, name="general", type=0, category_id=category),
            info(id=voice, name="Lounge", type=2, category_id=None),
            info(id=forum, name="help", type=15, category_id=None),
            info(id=news, name="news", type=5, category_id=None),
        ]
        assert fake.requests == [("GET", f"/guilds/{_GUILD}/channels", {})]

    async def test_error_raises(self) -> None:
        fake = _fake_with_channels()
        fake.fail[f"/guilds/{_GUILD}/channels"] = (403, 50001)
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError, match=r"HTTP 403, code 50001 \(Missing Access\)"
            ):
                await app.list_channels(_GUILD)


# ---------------------------------------------------------------------------
# Parsing messages
# ---------------------------------------------------------------------------

_PARENT_ID = "1005"
_MESSAGE_ID = "1006"
_OTHER_PERSON_ID = "903"
_WEBHOOK_ID = "1007"
_ROLE_ID = "1008"
_ATTACHMENT_ID = "1009"

_BOT_USER = _raw_user(_BOT_ID, "example-bot", bot=True)
_WEBHOOK_USER = _raw_user(_WEBHOOK_ID, "example-hook", bot=True)

_ADA = discord.DiscordUser(id=_PERSON_ID, username="ada", global_name="Ada", bot=False)
_EMBED = discord.DiscordEmbed(
    title="Release 1.2",
    description="Listings are faster.",
    url="https://example.com/releases/1.2",
    author="release-bot",
    footer="cocoindex",
    fields=(("Breaking", "none"), ("Docs", "updated")),
)
_ATTACHMENT = discord.DiscordAttachment(
    id=_ATTACHMENT_ID,
    filename="notes.txt",
    size=10,
    content_type="text/plain; charset=utf-8",
    description="meeting notes",
    url=(
        f"https://cdn.discordapp.com/attachments/{_CHANNEL}/{_ATTACHMENT_ID}/notes.txt"
        "?ex=1&is=0&hm=a"
    ),
)

_MESSAGE_FIELD_NAMES = frozenset(
    f.name for f in dataclasses.fields(discord.DiscordMessage)
)


def _raw_embed(**fields: Any) -> dict[str, Any]:
    """A rich embed as Discord lists a bot's, with parts the connector
    drops."""
    return {
        "type": "rich",
        "title": "Release 1.2",
        "description": "Listings are faster.",
        "url": "https://example.com/releases/1.2",
        "author": {"name": "release-bot", "icon_url": "https://example.com/bot.png"},
        "footer": {"text": "cocoindex", "icon_url": "https://example.com/logo.png"},
        "fields": [
            {"name": "Breaking", "value": "none", "inline": False},
            {"name": "Docs", "value": "updated", "inline": True},
        ],
        "color": 5814783,
        "content_scan_version": 0,
        **fields,
    }


def _raw_attachment(**fields: Any) -> dict[str, Any]:
    """``_ATTACHMENT`` as Discord lists it, with parts the connector drops."""
    return {
        "id": _ATTACHMENT.id,
        "filename": _ATTACHMENT.filename,
        "size": _ATTACHMENT.size,
        "content_type": _ATTACHMENT.content_type,
        "description": _ATTACHMENT.description,
        "url": _ATTACHMENT.url,
        "proxy_url": _ATTACHMENT.url.replace(
            "cdn.discordapp.com", "media.discordapp.net"
        ),
        "title": "notes",
        **fields,
    }


def _raw_reaction(
    emoji_name: str, count: int, emoji_id: str | None = None
) -> dict[str, Any]:
    """A reaction as listed, with the bot's own state and the super-reaction
    details, which the connector drops."""
    return {
        "emoji": {"id": emoji_id, "name": emoji_name},
        "count": count,
        "count_details": {"burst": 0, "normal": count},
        "burst_colors": [],
        "burst_count": 0,
        "burst_me": False,
        "me": False,
        "me_burst": False,
    }


def _raw_reference(
    message_id: str,
    *,
    channel_id: str = _CHANNEL,
    guild_id: str = _GUILD,
    reference_type: int = 0,
) -> dict[str, Any]:
    return {
        "channel_id": channel_id,
        "guild_id": guild_id,
        "message_id": message_id,
        "type": reference_type,
    }


def _raw_forward(snapshot: dict[str, Any], **fields: Any) -> dict[str, Any]:
    """A forward of the message ``_PARENT_ID`` in ``_OTHER_CHANNEL``, as
    listed: no content of its own, and ``snapshot`` as the forwarded
    message."""
    return _raw_message(
        _MESSAGE_ID,
        content="",
        flags=1 << 14,
        message_reference=_raw_reference(
            _PARENT_ID, channel_id=_OTHER_CHANNEL, reference_type=1
        ),
        message_snapshots=[{"message": snapshot}],
        **fields,
    )


class TestParseMessage:
    def test_message(self) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            content=f"<@{_OTHER_PERSON_ID}> the notes, <@&{_ROLE_ID}> @everyone",
            mentions=[_raw_user(_OTHER_PERSON_ID, "bo")],
            mention_roles=[_ROLE_ID],
            mention_everyone=True,
            attachments=[_raw_attachment()],
            reactions=[_raw_reaction("👍", 2)],
            edited_timestamp="2026-01-15T12:00:05.500000+00:00",
        )
        message = _parse_message(raw, _GUILD, None)
        assert message == discord.DiscordMessage(
            id=_MESSAGE_ID,
            guild_id=_GUILD,
            channel_id=_CHANNEL,
            type=0,
            author=_ADA,
            webhook_id=None,
            content=f"<@{_OTHER_PERSON_ID}> the notes, <@&{_ROLE_ID}> @everyone",
            mentions=(
                discord.DiscordUser(
                    id=_OTHER_PERSON_ID, username="bo", global_name=None, bot=False
                ),
            ),
            mention_roles=(_ROLE_ID,),
            mention_everyone=True,
            attachments=(_ATTACHMENT,),
            embeds=(),
            reactions=(
                discord.DiscordReaction(emoji_id=None, emoji_name="👍", count=2),
            ),
            pinned=False,
            timestamp=datetime(2026, 1, 15, 12, 0, 0, 250000, tzinfo=UTC),
            edited_timestamp=datetime(2026, 1, 15, 12, 0, 5, 500000, tzinfo=UTC),
            reply_to=None,
            forwarded_from=None,
        )
        assert message.timestamp is not None
        assert message.timestamp.utcoffset() == timedelta(0)

    @pytest.mark.parametrize(
        ("message_type", "fields", "reply_to"),
        [
            pytest.param(19, {}, _PARENT_ID, id="reply"),
            pytest.param(6, {}, None, id="pin-notice"),
            pytest.param(18, {}, None, id="thread-notice"),
            pytest.param(21, {}, None, id="thread-starter-stub"),
            pytest.param(
                0,
                {
                    "flags": 1 << 1,
                    "message_reference": _raw_reference(
                        _PARENT_ID, channel_id=_OTHER_CHANNEL, guild_id=_OTHER_GUILD
                    ),
                },
                None,
                id="crosspost",
            ),
        ],
    )
    def test_reply_to_only_for_type_19(
        self, message_type: int, fields: dict[str, Any], reply_to: str | None
    ) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            **{
                "type": message_type,
                "message_reference": _raw_reference(_PARENT_ID),
                **fields,
            },
        )
        message = _parse_message(raw, _GUILD, None)
        assert message.reply_to == reply_to
        assert message.forwarded_from is None

    def test_forward(self) -> None:
        raw = _raw_forward(
            {
                "attachments": [_raw_attachment()],
                "components": [],
                "content": "the notes from Monday",
                "edited_timestamp": "2026-01-14T09:30:00.000000+00:00",
                "embeds": [_raw_embed()],
                "flags": 0,
                "mention_roles": [_ROLE_ID],
                "mentions": [_raw_user(_PERSON_ID, "ada", "Ada")],
                "timestamp": "2026-01-14T09:00:00.000000+00:00",
                "type": 0,
            },
            # A bot's forward, so that the snapshot's embed is kept.
            author=_BOT_USER,
            reactions=[_raw_reaction("👀", 1)],
            pinned=True,
            # A forward can't mention everyone; set here to show which
            # object is read.
            mention_everyone=True,
        )
        message = _parse_message(raw, _GUILD, None)
        # From the snapshot.
        assert message.content == "the notes from Monday"
        assert message.mentions == (_ADA,)
        assert message.mention_roles == (_ROLE_ID,)
        assert message.attachments == (_ATTACHMENT,)
        assert message.embeds == (_EMBED,)
        # The forward's own.
        assert message.id == _MESSAGE_ID
        assert message.author is not None
        assert message.author.id == _BOT_ID
        assert message.timestamp == datetime(2026, 1, 15, 12, 0, 0, 250000, tzinfo=UTC)
        assert message.edited_timestamp is None
        assert message.reactions == (
            discord.DiscordReaction(emoji_id=None, emoji_name="👀", count=1),
        )
        assert message.pinned is True
        assert message.mention_everyone is True
        assert message.forwarded_from == discord.DiscordMessageRef(
            guild_id=_GUILD, channel_id=_OTHER_CHANNEL, message_id=_PARENT_ID
        )
        assert message.reply_to is None

    @pytest.mark.parametrize(
        ("fields", "embeds"),
        [
            # A link preview can be rich too: X's was.
            pytest.param({"embeds": [_raw_embed()]}, (), id="person"),
            pytest.param(
                {"author": _BOT_USER, "embeds": [_raw_embed()]}, (_EMBED,), id="bot"
            ),
            pytest.param(
                {"author": _BOT_USER, "embeds": [_raw_embed(type="article")]},
                (),
                id="bot-non-rich",
            ),
            pytest.param(
                {
                    "author": _WEBHOOK_USER,
                    "webhook_id": _WEBHOOK_ID,
                    "embeds": [_raw_embed()],
                },
                (_EMBED,),
                id="webhook",
            ),
            pytest.param(
                {
                    "author": _raw_user(_WEBHOOK_ID, "example-hook"),
                    "webhook_id": _WEBHOOK_ID,
                    "embeds": [_raw_embed()],
                },
                (_EMBED,),
                id="webhook-id-alone",
            ),
        ],
    )
    def test_embeds_only_on_bot_and_webhook_posts(
        self, fields: dict[str, Any], embeds: tuple[discord.DiscordEmbed, ...]
    ) -> None:
        raw = _raw_message(_MESSAGE_ID, **fields)
        assert _parse_message(raw, _GUILD, None).embeds == embeds

    def test_person_forwarding_a_bot_post_keeps_no_embeds(self) -> None:
        # A snapshot carries no author, so the forwarder decides.
        raw = _raw_forward({"content": "", "embeds": [_raw_embed()]})
        assert _parse_message(raw, _GUILD, None).embeds == ()

    def test_attachments_and_reactions(self) -> None:
        photo = _raw_attachment(
            id="1013",
            filename="photo.png",
            size=48213,
            content_type="image/png",
            url=f"https://cdn.discordapp.com/attachments/{_CHANNEL}/1013/photo.png?ex=1&is=0&hm=b",
        )
        del photo["description"]
        raw = _raw_message(
            _MESSAGE_ID,
            content="",
            attachments=[_raw_attachment(), photo],
            reactions=[
                _raw_reaction("👍", 3),
                _raw_reaction("example_emoji", 1, "1014"),
            ],
        )
        message = _parse_message(raw, _GUILD, None)
        assert message.attachments == (
            _ATTACHMENT,
            discord.DiscordAttachment(
                id="1013",
                filename="photo.png",
                size=48213,
                content_type="image/png",
                description=None,
                url=f"https://cdn.discordapp.com/attachments/{_CHANNEL}/1013/photo.png?ex=1&is=0&hm=b",
            ),
        )
        assert message.reactions == (
            discord.DiscordReaction(emoji_id=None, emoji_name="👍", count=3),
            discord.DiscordReaction(
                emoji_id="1014",
                emoji_name="example_emoji",
                count=1,
            ),
        )

    def test_missing_keys(self) -> None:
        raw = _raw_message(_MESSAGE_ID, author=_BOT_USER)
        for key in ("mention_roles", "attachments", "embeds", "edited_timestamp"):
            del raw[key]
        assert "reactions" not in raw
        assert "webhook_id" not in raw
        message = _parse_message(raw, _GUILD, None)
        assert message.reactions == ()
        assert message.mention_roles == ()
        assert message.attachments == ()
        assert message.embeds == ()
        assert message.edited_timestamp is None
        assert message.webhook_id is None


class TestMessageFields:
    """``message_fields``, written as ``DiscordChannel`` normalizes it."""

    def test_left_out_fields(self) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            type=19,
            content="agreed",
            mentions=[_raw_user(_OTHER_PERSON_ID, "bo")],
            mention_roles=[_ROLE_ID],
            mention_everyone=True,
            attachments=[_raw_attachment()],
            reactions=[_raw_reaction("👍", 2)],
            pinned=True,
            edited_timestamp="2026-01-15T12:00:05.500000+00:00",
            message_reference=_raw_reference(_PARENT_ID),
        )
        assert _parse_message(raw, _GUILD, ("content", "id")) == discord.DiscordMessage(
            id=_MESSAGE_ID,
            guild_id=None,
            channel_id=None,
            type=None,
            author=None,
            webhook_id=None,
            content="agreed",
            mentions=(),
            mention_roles=(),
            mention_everyone=None,
            attachments=(),
            embeds=(),
            reactions=(),
            pinned=None,
            timestamp=None,
            edited_timestamp=None,
            reply_to=None,
            forwarded_from=None,
        )

    def test_logic_reads_the_raw_object(self) -> None:
        # The embed filter reads the author, though it's left out.
        embeds_only = ("embeds", "id")
        person = _parse_message(
            _raw_message(_MESSAGE_ID, embeds=[_raw_embed()]), _GUILD, embeds_only
        )
        bot = _parse_message(
            _raw_message(_MESSAGE_ID, author=_BOT_USER, embeds=[_raw_embed()]),
            _GUILD,
            embeds_only,
        )
        assert (person.author, person.embeds) == (None, ())
        assert (bot.author, bot.embeds) == (None, (_EMBED,))

        # The reply check reads the type, though it's left out.
        reply = _parse_message(
            _raw_message(
                _MESSAGE_ID, type=19, message_reference=_raw_reference(_PARENT_ID)
            ),
            _GUILD,
            ("id", "reply_to"),
        )
        assert (reply.type, reply.reply_to) == (None, _PARENT_ID)


# ---------------------------------------------------------------------------
# Message fingerprints
# ---------------------------------------------------------------------------


def _message_fingerprint(
    raw: dict[str, Any], message_fields: tuple[str, ...] | None = None
) -> object:
    return memo_fingerprint(_parse_message(raw, _GUILD, message_fields))


class TestAttachmentMemoKey:
    def test_url_not_in_key(self) -> None:
        resigned_url = _ATTACHMENT.url.replace("ex=1&is=0&hm=a", "ex=2&is=1&hm=c")
        resigned = _raw_attachment(url=resigned_url)
        assert memo_fingerprint(_ATTACHMENT) == memo_fingerprint(
            dataclasses.replace(_ATTACHMENT, url=resigned_url)
        )
        assert _message_fingerprint(
            _raw_message(_MESSAGE_ID, attachments=[_raw_attachment()])
        ) == _message_fingerprint(_raw_message(_MESSAGE_ID, attachments=[resigned]))

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("id", "1013"),
            ("filename", "notes-v2.txt"),
            ("size", 18),
            ("content_type", "text/markdown"),
            ("description", "minutes"),
        ],
    )
    def test_every_other_field_in_key(self, key: str, value: object) -> None:
        assert _message_fingerprint(
            _raw_message(_MESSAGE_ID, attachments=[_raw_attachment()])
        ) != _message_fingerprint(
            _raw_message(_MESSAGE_ID, attachments=[_raw_attachment(**{key: value})])
        )


class TestMessageFingerprint:
    @pytest.mark.parametrize(
        ("changed", "change"),
        [
            pytest.param(
                ("content", "edited_timestamp"),
                {
                    "content": "release notes, edited",
                    "edited_timestamp": "2026-01-15T12:00:05.500000+00:00",
                },
                id="edit",
            ),
            pytest.param(
                ("reactions",), {"reactions": [_raw_reaction("👍", 2)]}, id="reaction"
            ),
            pytest.param(("pinned",), {"pinned": True}, id="pin"),
            pytest.param(
                ("author",),
                {"author": _raw_user(_BOT_ID, "example-bot", "Coco", bot=True)},
                id="author-renamed",
            ),
            pytest.param(
                ("embeds",),
                {"embeds": [_raw_embed(footer={"text": "cocoindex 1.2"})]},
                id="embed-footer",
            ),
            pytest.param(
                ("mention_everyone",), {"mention_everyone": True}, id="mention-everyone"
            ),
        ],
    )
    def test_exposed_fields_count(
        self, changed: tuple[str, ...], change: dict[str, Any]
    ) -> None:
        before = _raw_message(
            _MESSAGE_ID,
            author=_BOT_USER,
            content="release notes",
            embeds=[_raw_embed()],
            reactions=[_raw_reaction("👍", 1)],
        )
        after = {**before, **change}
        assert _message_fingerprint(before) == _message_fingerprint(before)
        assert _message_fingerprint(before) != _message_fingerprint(after)

        others = tuple(sorted(_MESSAGE_FIELD_NAMES - set(changed)))
        assert _message_fingerprint(before, others) == _message_fingerprint(
            after, others
        )


# ---------------------------------------------------------------------------
# Message text
# ---------------------------------------------------------------------------

_MENTIONS = (
    _ADA,
    discord.DiscordUser(
        id=_OTHER_PERSON_ID, username="bo", global_name=None, bot=False
    ),
)


async def _message_text(
    raw: dict[str, Any], message_fields: tuple[str, ...] | None = None
) -> str:
    return await _parse_message(raw, _GUILD, message_fields).read_text()


class TestRenderTokens:
    @pytest.mark.parametrize(
        ("token", "rendered"),
        [
            pytest.param(f"<@{_PERSON_ID}>", "@Ada", id="user"),
            pytest.param(f"<@!{_PERSON_ID}>", "@Ada", id="user-old-form"),
            pytest.param(
                f"<@{_OTHER_PERSON_ID}>", "@bo", id="user-without-display-name"
            ),
            pytest.param("<@906>", "@906", id="user-not-in-mentions"),
            pytest.param("<:example_emoji:1014>", ":example_emoji:", id="emoji"),
            pytest.param(
                "<a:example_wave:1015>", ":example_wave:", id="animated-emoji"
            ),
            pytest.param("<t:1768478400>", "2026-01-15T12:00:00Z", id="timestamp"),
            pytest.param(
                "<t:1768478400:R>", "2026-01-15T12:00:00Z", id="timestamp-style"
            ),
            pytest.param(
                "<t:1768478400:X>", "2026-01-15T12:00:00Z", id="timestamp-unknown-style"
            ),
            pytest.param(
                "<t:253402300799>", "9999-12-31T23:59:59Z", id="latest-timestamp"
            ),
            pytest.param("</remind:1016>", "/remind", id="command"),
            pytest.param("</remind list:1016>", "/remind list", id="subcommand"),
            pytest.param(
                "</remind admin clear:1016>",
                "/remind admin clear",
                id="subcommand-in-group",
            ),
        ],
    )
    def test_rewritten(self, token: str, rendered: str) -> None:
        assert _render_tokens(f"see {token} now", _MENTIONS) == f"see {rendered} now"

    @pytest.mark.parametrize(
        "text",
        [
            # Naming a channel or a role would take a request.
            pytest.param(f"<#{_CHANNEL}>", id="channel"),
            pytest.param(f"<@&{_ROLE_ID}>", id="role"),
            pytest.param("<id:customize>", id="server-navigation"),
            pytest.param("<@$1017>", id="game-profile"),
            # Past what a datetime holds. Anyone can type one, and raising
            # would fail the item on every run.
            pytest.param("<t:99999999999999>", id="timestamp-past-9999"),
            pytest.param("<t:253402300800>", id="timestamp-just-past-9999"),
            # Discord escapes nothing, so nothing is unescaped.
            pytest.param(
                "**bold** [the guide](https://example.com/guide) `code`", id="markdown"
            ),
            pytest.param("&lt; &amp;", id="entities"),
        ],
    )
    def test_left_as_typed(self, text: str) -> None:
        assert _render_tokens(f"see {text} now", _MENTIONS) == f"see {text} now"

    def test_message_as_typed(self) -> None:
        # Made up, after a message a person typed in Discord's app, as the
        # listing returned it: tokens picked from the composer, and Markdown,
        # `&`, `<` and `>` left raw.
        content = (
            f"<@{_PERSON_ID}> the notes are in <#{_CHANNEL}>, <@&{_ROLE_ID}> "
            "<:example_emoji:1014> https://example.com/notes?week=2&lang=en\n"
            "[agenda](https://example.com/agenda) & 1 < 2 > 0 &amp; `a <i>` "
            "<t:1768478400:R>"
        )
        assert _render_tokens(content, (_ADA,)) == (
            f"@Ada the notes are in <#{_CHANNEL}>, <@&{_ROLE_ID}> "
            ":example_emoji: https://example.com/notes?week=2&lang=en\n"
            "[agenda](https://example.com/agenda) & 1 < 2 > 0 &amp; `a <i>` "
            "2026-01-15T12:00:00Z"
        )


@pytest.mark.asyncio
class TestMessageReadText:
    async def test_content_and_embeds(self) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            author=_BOT_USER,
            content="release notes",
            embeds=[_raw_embed(), {"type": "rich", "title": "Changelog"}],
        )
        # The embed's url isn't text.
        assert await _message_text(raw) == (
            "example-bot: release notes\n"
            "release-bot\n"
            "Release 1.2\n"
            "Listings are faster.\n"
            "Breaking: none\n"
            "Docs: updated\n"
            "cocoindex\n"
            "Changelog"
        )

    @pytest.mark.parametrize(
        ("fields", "name"),
        [
            pytest.param({}, "Ada", id="display-name"),
            pytest.param({"author": _BOT_USER}, "example-bot", id="username"),
            pytest.param(
                {"author": _WEBHOOK_USER, "webhook_id": _WEBHOOK_ID},
                "example-hook",
                id="webhook",
            ),
        ],
    )
    async def test_author_display_name(self, fields: dict[str, Any], name: str) -> None:
        raw = _raw_message(_MESSAGE_ID, content="hi all", **fields)
        assert await _message_text(raw) == f"{name}: hi all"

    async def test_tokens_in_embeds_rendered(self) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            author=_BOT_USER,
            content="",
            embeds=[{"type": "rich", "description": "Starts <t:1768478400:F>"}],
        )
        assert await _message_text(raw) == "example-bot: Starts 2026-01-15T12:00:00Z"

    @pytest.mark.parametrize(
        "fields",
        [
            pytest.param({"type": 7, "content": ""}, id="member-joined"),
            pytest.param(
                {"content": "", "attachments": [_raw_attachment()]},
                id="file-without-caption",
            ),
            # Its text is in its components, which aren't exposed.
            pytest.param(
                {
                    "author": _BOT_USER,
                    "content": "",
                    "flags": 1 << 15,
                    "components": [{"type": 10, "id": 1, "content": "Build passed"}],
                },
                id="components-v2",
            ),
            # Its question and answers aren't exposed.
            pytest.param(
                {
                    "content": "",
                    "poll": {
                        "question": {"text": "Lunch at noon?"},
                        "answers": [
                            {"answer_id": 1, "poll_media": {"text": "yes"}},
                            {"answer_id": 2, "poll_media": {"text": "no"}},
                        ],
                        "expiry": "2026-01-15T20:00:00.000000+00:00",
                        "allow_multiselect": False,
                        "layout_type": 1,
                        "results": {"answer_counts": [], "is_finalized": False},
                    },
                },
                id="poll",
            ),
        ],
    )
    async def test_empty_text(self, fields: dict[str, Any]) -> None:
        assert await _message_text(_raw_message(_MESSAGE_ID, **fields)) == ""

    @pytest.mark.parametrize(
        ("message_fields", "text"),
        [
            pytest.param(
                ("content", "embeds", "id", "mentions"),
                "@bo release notes\nRelease 1.2",
                id="author",
            ),
            pytest.param(
                ("author", "content", "embeds", "id"),
                f"example-bot: @{_OTHER_PERSON_ID} release notes\nRelease 1.2",
                id="mentions",
            ),
            pytest.param(
                ("author", "content", "id", "mentions"),
                "example-bot: @bo release notes",
                id="embeds",
            ),
            pytest.param(
                ("author", "embeds", "id", "mentions"),
                "example-bot: Release 1.2",
                id="content",
            ),
        ],
    )
    async def test_left_out_fields(
        self, message_fields: tuple[str, ...], text: str
    ) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            author=_BOT_USER,
            content=f"<@{_OTHER_PERSON_ID}> release notes",
            mentions=[_raw_user(_OTHER_PERSON_ID, "bo")],
            embeds=[{"type": "rich", "title": "Release 1.2"}],
        )
        assert await _message_text(raw, message_fields) == text

    async def test_not_rendered(self) -> None:
        raw = _raw_message(
            _MESSAGE_ID,
            content="hi all",
            attachments=[_raw_attachment()],
            reactions=[_raw_reaction("👍", 2)],
            pinned=True,
            mention_everyone=True,
        )
        assert await _message_text(raw) == "Ada: hi all"


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------

# A thread started from the main-stream message with the same ID.
_THREAD_ID = "1018"
_FORUM_ID = "1019"
# A post in _FORUM_ID.
_POST_ID = "1020"
_TAG_NAMES = {"1021": "question", "1022": "solved"}

# DiscordThread's fields, in their order; the starter aside.
_THREAD_FIELD_NAMES = (
    "id",
    "guild_id",
    "parent_id",
    "type",
    "name",
    "tags",
    "archived",
    "archive_timestamp",
    "locked",
    "pinned",
    "message_count",
    "total_message_sent",
    "last_message_id",
    "member_count",
    "owner_id",
    "create_timestamp",
    "auto_archive_duration",
    "rate_limit_per_user",
    "invitable",
    "last_pin_timestamp",
)

# For threads whose messages() a test doesn't call: parsing and the memo key
# never use the context.
_UNUSED_CONTEXT = cast(_source._ApiContext, None)


def _raw_thread(
    thread_id: str,
    parent_id: str = _CHANNEL,
    *,
    metadata: dict[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """A thread object as Discord lists it: an open public thread in
    ``parent_id`` with nothing posted since it started, untagged, and with
    the voice settings Discord attaches to every thread. ``metadata``
    overrides keys of its ``thread_metadata``; ``fields`` override or add
    other keys."""
    return {
        "id": thread_id,
        "type": 11,
        "guild_id": _GUILD,
        "parent_id": parent_id,
        "name": f"thread-{thread_id[-4:]}",
        "owner_id": _PERSON_ID,
        "last_message_id": thread_id,
        "message_count": 0,
        "total_message_sent": 0,
        "member_count": 1,
        "rate_limit_per_user": 0,
        "flags": 0,
        "bitrate": 64000,
        "rtc_region": None,
        "user_limit": 0,
        "thread_metadata": {
            "archived": False,
            "archive_timestamp": "2026-01-15T12:05:00.500000+00:00",
            "auto_archive_duration": 4320,
            "locked": False,
            "create_timestamp": "2026-01-15T12:05:00.500000+00:00",
            **(metadata or {}),
        },
        **fields,
    }


def _starter(**fields: Any) -> discord.DiscordMessage:
    """The message ``_THREAD_ID`` was started from, as the main stream lists
    it."""
    raw = _raw_message(
        _THREAD_ID,
        **{
            "content": "Release planning?",
            # HAS_THREAD
            "flags": 1 << 5,
            "thread": _raw_thread(_THREAD_ID),
            **fields,
        },
    )
    return _parse_message(raw, _GUILD, None)


def _thread(
    raw_thread: dict[str, Any],
    *,
    ctx: _source._ApiContext = _UNUSED_CONTEXT,
    guild_id: str = _GUILD,
    tag_names: Mapping[str, str] = _TAG_NAMES,
    starter: discord.DiscordMessage | None = None,
    message_fields: tuple[str, ...] | None = None,
    thread_fields: tuple[str, ...] | None = None,
) -> discord.DiscordThread:
    return discord.DiscordThread(
        ctx, raw_thread, guild_id, tag_names, starter, message_fields, thread_fields
    )


def _properties(thread: discord.DiscordThread) -> dict[str, object]:
    return {name: getattr(thread, name) for name in _THREAD_FIELD_NAMES}


class TestThread:
    def test_fields(self) -> None:
        starter = _starter()
        raw = _raw_thread(
            _THREAD_ID,
            name="Release planning",
            owner_id=_OTHER_PERSON_ID,
            last_message_id="1023",
            message_count=3,
            # One message was deleted.
            total_message_sent=4,
            member_count=2,
            rate_limit_per_user=30,
            last_pin_timestamp="2026-01-15T12:20:00+00:00",
            metadata={
                "archived": True,
                "archive_timestamp": "2026-01-15T13:00:00.750000+00:00",
                "auto_archive_duration": 60,
                "locked": True,
            },
        )
        thread = _thread(raw, starter=starter)
        assert _properties(thread) == {
            "id": _THREAD_ID,
            "guild_id": _GUILD,
            "parent_id": _CHANNEL,
            "type": 11,
            "name": "Release planning",
            "tags": (),
            "archived": True,
            "archive_timestamp": datetime(2026, 1, 15, 13, 0, 0, 750000, tzinfo=UTC),
            "locked": True,
            "pinned": False,
            "message_count": 3,
            "total_message_sent": 4,
            "last_message_id": "1023",
            "member_count": 2,
            "owner_id": _OTHER_PERSON_ID,
            "create_timestamp": datetime(2026, 1, 15, 12, 5, 0, 500000, tzinfo=UTC),
            "auto_archive_duration": 60,
            "rate_limit_per_user": 30,
            "invitable": None,
            "last_pin_timestamp": datetime(2026, 1, 15, 12, 20, tzinfo=UTC),
        }
        assert thread.starter == starter

    @pytest.mark.parametrize(
        ("thread_type", "metadata", "invitable"),
        [
            pytest.param(10, {}, None, id="announcement"),
            pytest.param(12, {"invitable": False}, False, id="private"),
        ],
    )
    def test_kinds(
        self, thread_type: int, metadata: dict[str, Any], invitable: bool | None
    ) -> None:
        thread = _thread(_raw_thread(_THREAD_ID, type=thread_type, metadata=metadata))
        assert (thread.type, thread.invitable) == (thread_type, invitable)

    def test_forum_post_tags_and_pinned(self) -> None:
        raw = _raw_thread(
            _POST_ID, _FORUM_ID, flags=1 << 1, applied_tags=["1022", "907", "1021"]
        )
        thread = _thread(raw)
        # In Discord's order, without the tag the forum no longer lists.
        assert thread.tags == ("solved", "question")
        assert thread.pinned is True
        assert thread.parent_id == _FORUM_ID
        # Only bit 1 is the pin; Discord documents other channel flags.
        assert _thread(_raw_thread(_POST_ID, _FORUM_ID, flags=1 << 15)).pinned is False

    def test_missing_keys(self) -> None:
        raw = _raw_thread(_THREAD_ID)
        for key in (
            "message_count",
            "total_message_sent",
            "member_count",
            "rate_limit_per_user",
            "last_message_id",
            "flags",
        ):
            del raw[key]
        del raw["thread_metadata"]["create_timestamp"]
        assert "last_pin_timestamp" not in raw
        assert "applied_tags" not in raw
        assert "invitable" not in raw["thread_metadata"]
        thread = _thread(raw)
        assert thread.message_count == 0
        assert thread.total_message_sent == 0
        assert thread.member_count == 0
        assert thread.rate_limit_per_user == 0
        assert thread.last_message_id is None
        assert thread.create_timestamp is None
        assert thread.last_pin_timestamp is None
        assert thread.invitable is None
        assert thread.pinned is False
        assert thread.tags == ()

    # Every thread has these. A missing one fails the channel, which keeps
    # its outputs, rather than inventing a value.
    @pytest.mark.parametrize("key", ["id", "type", "parent_id", "name", "owner_id"])
    def test_required_keys(self, key: str) -> None:
        raw = _raw_thread(_THREAD_ID)
        del raw[key]
        with pytest.raises(KeyError):
            _thread(raw)

    @pytest.mark.parametrize(
        "key", ["archived", "archive_timestamp", "locked", "auto_archive_duration"]
    )
    def test_required_metadata_keys(self, key: str) -> None:
        raw = _raw_thread(_THREAD_ID)
        del raw["thread_metadata"][key]
        with pytest.raises(KeyError):
            _thread(raw)

    def test_properties_are_the_thread_fields(self) -> None:
        properties = {
            name
            for name, value in vars(discord.DiscordThread).items()
            if isinstance(value, property)
        }
        assert properties == {*_THREAD_FIELD_NAMES, "starter"}
        # The memo key holds the fields in this order.
        assert (
            tuple(f.name for f in dataclasses.fields(_source._ThreadData))
            == _THREAD_FIELD_NAMES
        )


class TestThreadFields:
    """``thread_fields``, written as ``DiscordChannel`` normalizes it."""

    def test_left_out_fields(self) -> None:
        starter = _starter()
        # Tags and a pin, as a forum post has, so that every field has a value
        # to leave out.
        raw = _raw_thread(
            _THREAD_ID,
            type=12,
            name="Release planning",
            applied_tags=["1021"],
            flags=1 << 1,
            message_count=3,
            total_message_sent=4,
            member_count=2,
            rate_limit_per_user=30,
            last_pin_timestamp="2026-01-15T12:20:00+00:00",
            metadata={"locked": True, "invitable": False},
        )
        thread = _thread(raw, starter=starter, thread_fields=("id", "name"))
        assert _properties(thread) == {
            **dict.fromkeys(_THREAD_FIELD_NAMES),
            "id": _THREAD_ID,
            "name": "Release planning",
            "tags": (),
        }
        # The starter isn't a thread field.
        assert thread.starter == starter


def _thread_fingerprint(raw_thread: dict[str, Any], **thread_kwargs: Any) -> object:
    return memo_fingerprint(_thread(raw_thread, **thread_kwargs))


class TestThreadMemoKey:
    @pytest.mark.parametrize(
        ("name", "change"),
        [
            pytest.param(name, change, id=name)
            for name, change in [
                ("parent_id", {"parent_id": _OTHER_CHANNEL}),
                ("type", {"type": 12}),
                ("name", {"name": "Release planning, moved"}),
                ("tags", {"applied_tags": ["1021"]}),
                ("archived", {"metadata": {"archived": True}}),
                (
                    "archive_timestamp",
                    {
                        "metadata": {
                            "archive_timestamp": "2026-01-15T13:00:00.750000+00:00"
                        }
                    },
                ),
                ("locked", {"metadata": {"locked": True}}),
                ("pinned", {"flags": 1 << 1}),
                ("message_count", {"message_count": 2}),
                ("total_message_sent", {"total_message_sent": 3}),
                ("last_message_id", {"last_message_id": "1023"}),
                ("member_count", {"member_count": 3}),
                ("owner_id", {"owner_id": _OTHER_PERSON_ID}),
                (
                    "create_timestamp",
                    {
                        "metadata": {
                            "create_timestamp": "2026-01-15T12:06:00.000000+00:00"
                        }
                    },
                ),
                ("auto_archive_duration", {"metadata": {"auto_archive_duration": 60}}),
                ("rate_limit_per_user", {"rate_limit_per_user": 10}),
                ("invitable", {"metadata": {"invitable": True}}),
                (
                    "last_pin_timestamp",
                    {"last_pin_timestamp": "2026-01-15T12:20:00+00:00"},
                ),
            ]
        ],
    )
    def test_every_exposed_property_counts(
        self, name: str, change: dict[str, Any]
    ) -> None:
        starter = _starter()
        before, after = _raw_thread(_THREAD_ID), _raw_thread(_THREAD_ID, **change)
        assert _thread_fingerprint(before, starter=starter) == _thread_fingerprint(
            before, starter=starter
        )
        assert _thread_fingerprint(before, starter=starter) != _thread_fingerprint(
            after, starter=starter
        )

        others = tuple(sorted(set(_THREAD_FIELD_NAMES) - {name}))
        assert _thread_fingerprint(
            before, starter=starter, thread_fields=others
        ) == _thread_fingerprint(after, starter=starter, thread_fields=others)

    def test_guild_id_counts(self) -> None:
        raw = _raw_thread(_THREAD_ID)
        assert _thread_fingerprint(raw) != _thread_fingerprint(
            raw, guild_id=_OTHER_GUILD
        )
        others = tuple(sorted(set(_THREAD_FIELD_NAMES) - {"guild_id"}))
        assert _thread_fingerprint(raw, thread_fields=others) == _thread_fingerprint(
            raw, guild_id=_OTHER_GUILD, thread_fields=others
        )

    # Editing or deleting the starter changes nothing on the thread object.
    @pytest.mark.parametrize(
        "starter",
        [
            pytest.param(
                _starter(
                    content="Release planning, edited",
                    edited_timestamp="2026-01-15T12:10:00.000000+00:00",
                ),
                id="edited",
            ),
            pytest.param(None, id="deleted"),
        ],
    )
    def test_starter_counts(self, starter: discord.DiscordMessage | None) -> None:
        raw = _raw_thread(_THREAD_ID)
        assert _thread_fingerprint(raw, starter=_starter()) != _thread_fingerprint(
            raw, starter=starter
        )

    def test_message_fields_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A forum post has no starter, so only the message fields its
        # messages are parsed with can tell the two apart.
        raw = _raw_thread(_POST_ID, _FORUM_ID)
        content_only = ("content", "id")
        default = _thread_fingerprint(raw)
        assert _thread_fingerprint(raw, message_fields=content_only) != default
        assert _thread_fingerprint(
            raw, message_fields=content_only
        ) == _thread_fingerprint(raw, message_fields=content_only)
        # Every field, listed, exposes what the default does.
        every_field = tuple(sorted(_source._MESSAGE_FIELDS))
        assert _thread_fingerprint(raw, message_fields=every_field) == default
        # A later version that adds a message field changes the default too.
        monkeypatch.setattr(
            _source, "_MESSAGE_FIELDS", (*_source._MESSAGE_FIELDS, "poll")
        )
        assert _thread_fingerprint(raw) != default

    @pytest.mark.asyncio
    async def test_context_not_in_key(self) -> None:
        # Two Apps' contexts differ, and fingerprinting one would raise.
        fingerprints = []
        for _ in range(2):
            app, ctx = await _setup(_FakeDiscord())
            async with app:
                fingerprints.append(
                    _thread_fingerprint(
                        _raw_thread(_THREAD_ID), ctx=ctx, starter=_starter()
                    )
                )
        assert fingerprints[0] == fingerprints[1]


# ---------------------------------------------------------------------------
# Thread messages and text
# ---------------------------------------------------------------------------


def _stub(thread_id: str = _THREAD_ID, stub_id: str = "1024") -> dict[str, Any]:
    """The first message in the thread ``thread_id``: a stub standing for the
    starter, which is in the main stream. It has an ID of its own and no
    content."""
    return _raw_message(
        stub_id, type=21, content="", message_reference=_raw_reference(thread_id)
    )


@pytest.mark.asyncio
class TestThreadMessages:
    async def test_starter_only_needs_no_request(self) -> None:
        fake = _FakeDiscord()
        fake.post(_THREAD_ID, _stub())
        starter = _starter()
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(_raw_thread(_THREAD_ID), ctx=ctx, starter=starter)
            assert await thread.messages() == [starter]
        assert fake.requests == []

    async def test_missing_message_count_fetches(self) -> None:
        # Only the listing can tell whether more than the stub is inside.
        fake = _FakeDiscord()
        fake.post(_THREAD_ID, _stub(), _raw_message("1025"))
        raw = _raw_thread(_THREAD_ID)
        del raw["message_count"]
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(raw, ctx=ctx, starter=_starter())
            assert [m.id for m in await thread.messages()] == [_THREAD_ID, "1025"]

    async def test_pages_sorts_and_drops_the_stub(self) -> None:
        # IDs from 4 digits to 5: sorted as text, "10000" would come before
        # "9999".
        ids = [str(n) for n in range(9901, 10151)]
        fake = _FakeDiscord()
        fake.page_cap = 70
        fake.post(_THREAD_ID, _stub(), *(_raw_message(i) for i in ids))
        starter = _starter()
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(
                _raw_thread(_THREAD_ID, message_count=250, total_message_sent=250),
                ctx=ctx,
                starter=starter,
            )
            messages = await thread.messages()
        assert [m.id for m in messages] == [_THREAD_ID, *ids]
        assert messages[0] is starter
        # Three pages of 70, short of the 100 asked for; one of 41, the stub
        # among them; then an empty one. Each asks for the messages before
        # the smallest ID on the page before.
        newest_first = [*reversed(ids), "1024"]
        assert [
            params
            for _, path, params in fake.requests
            if path == f"/channels/{_THREAD_ID}/messages"
        ] == [
            {"limit": "100"},
            *({"limit": "100", "before": newest_first[i]} for i in (69, 139, 209, 250)),
        ]

    async def test_forum_post(self) -> None:
        # The opening message is inside the post, with the post's ID.
        fake = _FakeDiscord()
        fake.post(
            _POST_ID,
            _raw_message(_POST_ID, content="Which day works?"),
            _raw_message("1026"),
            _raw_message("1027"),
        )
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(_raw_thread(_POST_ID, _FORUM_ID, message_count=2), ctx=ctx)
            messages = await thread.messages()
        assert thread.starter is None
        assert [m.id for m in messages] == [_POST_ID, "1026", "1027"]
        assert messages[0].content == "Which day works?"

    async def test_cached_and_errors(self) -> None:
        fake = _FakeDiscord()
        fake.post(_POST_ID, _raw_message(_POST_ID))
        path = f"/channels/{_POST_ID}/messages"
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(_raw_thread(_POST_ID, _FORUM_ID), ctx=ctx)
            messages = await thread.messages()
            assert await thread.messages() is messages
            # One page, then an empty one.
            assert fake.count(path) == 2

            # Such as a private thread the bot was removed from.
            fake.fail[path] = (403, 50001)
            failing = _thread(_raw_thread(_POST_ID, _FORUM_ID), ctx=ctx)
            with pytest.raises(
                _DiscordError,
                match=r"HTTP 403, code 50001 \(Missing Access\)\. The bot can't read",
            ):
                await failing.messages()
            # A failure isn't kept: the next call asks again.
            del fake.fail[path]
            assert [m.id for m in await failing.messages()] == [_POST_ID]

    async def test_logic_reads_the_raw_object(self) -> None:
        fake = _FakeDiscord()
        fake.post(_THREAD_ID, _stub(), _raw_message("1028"))
        app, ctx = await _setup(fake)
        async with app:
            # The shortcut reads message_count, though it's left out.
            starter_only = _thread(
                _raw_thread(_THREAD_ID),
                ctx=ctx,
                starter=_starter(),
                thread_fields=("id", "name"),
            )
            assert [m.id for m in await starter_only.messages()] == [_THREAD_ID]
            assert fake.requests == []

            # The stub is dropped by its type, and the messages carry the
            # server's ID, though both are left out.
            thread = _thread(
                _raw_thread(_THREAD_ID, message_count=1),
                ctx=ctx,
                starter=_starter(),
                message_fields=("content", "guild_id", "id"),
                thread_fields=("id", "name"),
            )
            messages = await thread.messages()
        assert [m.id for m in messages] == [_THREAD_ID, "1028"]
        assert (messages[1].type, messages[1].guild_id) == (None, _GUILD)


# A forum post's messages, oldest first: conversation, with notices and a
# file without a caption in between.
_POST_MESSAGES = [
    _raw_message(_POST_ID, content="Which day works?"),
    # A member added.
    _raw_message(
        "1029", type=1, content="", mentions=[_raw_user(_OTHER_PERSON_ID, "bo")]
    ),
    _raw_message(
        "1030",
        type=19,
        author=_raw_user(_OTHER_PERSON_ID, "bo"),
        content="Thursday",
        message_reference=_raw_reference(_POST_ID, channel_id=_POST_ID),
    ),
    # A slash command's reply, with an embed.
    _raw_message(
        "1031",
        type=20,
        author=_BOT_USER,
        content="Scheduled for Thursday",
        embeds=[{"type": "rich", "title": "Release 1.3"}],
    ),
    # A rename: its content is the new name.
    _raw_message("1032", type=4, content="Release planning"),
    # A pin.
    _raw_message(
        "1033",
        type=6,
        content="",
        message_reference=_raw_reference("1030", channel_id=_POST_ID),
    ),
    _raw_message("1034", content="", attachments=[_raw_attachment()]),
    # A context-menu command's reply.
    _raw_message(
        "1035", type=23, author=_BOT_USER, content="Translated: Thursday works"
    ),
    # A member removed.
    _raw_message(
        "1036", type=2, content="", mentions=[_raw_user(_OTHER_PERSON_ID, "bo")]
    ),
]


@pytest.mark.asyncio
class TestThreadReadText:
    async def _post_text(
        self,
        *,
        message_fields: tuple[str, ...] | None = None,
        thread_fields: tuple[str, ...] | None = None,
    ) -> str:
        fake = _FakeDiscord()
        fake.post(_POST_ID, *_POST_MESSAGES)
        raw = _raw_thread(
            _POST_ID,
            _FORUM_ID,
            name="Release planning",
            applied_tags=["1022", "1021"],
            message_count=8,
        )
        app, ctx = await _setup(fake)
        async with app:
            thread = _thread(
                raw,
                ctx=ctx,
                message_fields=message_fields,
                thread_fields=thread_fields,
            )
            return await thread.read_text()

    async def test_name_tags_and_conversation(self) -> None:
        # A message with an embed spans two lines.
        assert await self._post_text() == (
            "Release planning\n"
            "Tags: solved, question\n"
            "Ada: Which day works?\n"
            "bo: Thursday\n"
            "example-bot: Scheduled for Thursday\n"
            "Release 1.3\n"
            "example-bot: Translated: Thursday works"
        )

    @pytest.mark.parametrize(
        ("message_fields", "thread_fields", "lines"),
        [
            pytest.param(
                None,
                ("id", "tags"),
                [
                    "Tags: solved, question",
                    "Ada: Which day works?",
                    "bo: Thursday",
                    "example-bot: Scheduled for Thursday",
                    "Release 1.3",
                    "example-bot: Translated: Thursday works",
                ],
                id="name",
            ),
            pytest.param(
                None,
                ("id", "name"),
                [
                    "Release planning",
                    "Ada: Which day works?",
                    "bo: Thursday",
                    "example-bot: Scheduled for Thursday",
                    "Release 1.3",
                    "example-bot: Translated: Thursday works",
                ],
                id="tags",
            ),
            # Every message with text is kept, the rename notice too.
            pytest.param(
                ("author", "content", "embeds", "id"),
                None,
                [
                    "Release planning",
                    "Tags: solved, question",
                    "Ada: Which day works?",
                    "bo: Thursday",
                    "example-bot: Scheduled for Thursday",
                    "Release 1.3",
                    "Ada: Release planning",
                    "example-bot: Translated: Thursday works",
                ],
                id="type",
            ),
        ],
    )
    async def test_left_out_fields(
        self,
        message_fields: tuple[str, ...] | None,
        thread_fields: tuple[str, ...] | None,
        lines: list[str],
    ) -> None:
        text = await self._post_text(
            message_fields=message_fields, thread_fields=thread_fields
        )
        assert text == "\n".join(lines)


# ---------------------------------------------------------------------------
# DiscordChannel
# ---------------------------------------------------------------------------


def _channel(
    app: discord.DiscordApp, channel_id: str = _CHANNEL, **selection: Any
) -> discord.DiscordChannel:
    return discord.DiscordChannel(app=app, channel_id=channel_id, **selection)


class TestDiscordChannel:
    @pytest.mark.parametrize(
        ("selection", "message"),
        [
            pytest.param(
                {"message_fields": ["content", "contnet"]},
                "message_fields ['contnet'] not found in DiscordMessage fields: "
                f"{[f.name for f in dataclasses.fields(discord.DiscordMessage)]}",
                id="message_fields",
            ),
            pytest.param(
                {"thread_fields": ["nmae", "name"]},
                "thread_fields ['nmae'] not found in DiscordThread fields: "
                f"{list(_THREAD_FIELD_NAMES)}",
                id="thread_fields",
            ),
            # The starter isn't a thread field: it's always attached.
            pytest.param(
                {"thread_fields": ["starter"]},
                "thread_fields ['starter'] not found in DiscordThread fields: "
                f"{list(_THREAD_FIELD_NAMES)}",
                id="starter",
            ),
        ],
    )
    def test_unknown_field_names(self, selection: dict[str, Any], message: str) -> None:
        app = discord.DiscordApp(token="t")
        with pytest.raises(ValueError) as exc_info:
            _channel(app, **selection)
        assert str(exc_info.value) == message
        # Checked before any request: the App's context isn't even opened.
        assert app._state.context is None

    def test_normalized_selection_and_memo_key(self) -> None:
        app = discord.DiscordApp(token="t")
        channel = _channel(app)
        assert channel.channel_id == _CHANNEL
        # The key holds the names of the fields exposed, sorted.
        every_message_field = tuple(sorted(_source._MESSAGE_FIELDS))
        every_thread_field = tuple(sorted(_source._THREAD_FIELDS))
        assert channel.__coco_memo_key__() == (
            "discord_channel",
            _CHANNEL,
            every_message_field,
            every_thread_field,
        )

        def key(**selection: Any) -> object:
            return _channel(app, **selection).__coco_memo_key__()

        # Empty means every field, as None does, and so does every field
        # listed.
        assert key(message_fields=[], thread_fields=[]) == key()
        assert (
            key(
                message_fields=list(_source._MESSAGE_FIELDS),
                thread_fields=list(_source._THREAD_FIELDS),
            )
            == key()
        )
        # Sorted, without duplicates, with id: selections that mean the same
        # give the same key.
        assert key(message_fields=["content", "author", "content"]) == (
            "discord_channel",
            _CHANNEL,
            ("author", "content", "id"),
            every_thread_field,
        )
        assert key(message_fields=["content", "author"]) == key(
            message_fields=["author", "content", "id"]
        )
        assert key(thread_fields=("name",)) == (
            "discord_channel",
            _CHANNEL,
            every_message_field,
            ("id", "name"),
        )
        assert key(message_fields=["content"]) != key(
            message_fields=["content", "author"]
        )
        assert key(message_fields=["type"]) != key(thread_fields=["type"])

        # The App is left out: credentials and connections aren't content, and
        # its lock can't be fingerprinted.
        other_app = discord.DiscordApp(token="other")
        assert _channel(other_app).__coco_memo_key__() == key()
        assert memo_fingerprint(_channel(app)) == memo_fingerprint(_channel(other_app))
        assert memo_fingerprint(_channel(app)) != memo_fingerprint(
            _channel(app, _OTHER_CHANNEL)
        )


# ---------------------------------------------------------------------------
# DiscordChannel.items()
# ---------------------------------------------------------------------------

_ACTIVE_PATH = f"/guilds/{_GUILD}/threads/active"
_PUBLIC_PATH = f"/channels/{_CHANNEL}/threads/archived/public"
_PRIVATE_PATH = f"/channels/{_CHANNEL}/users/@me/threads/archived/private"
_MESSAGES_PATH = f"/channels/{_CHANNEL}/messages"
# The parameters of a listing's first page.
_PAGE = {"limit": "100"}

# _FORUM_ID's tags, as its channel object lists them.
_AVAILABLE_TAGS = [
    {
        "id": tag_id,
        "name": name,
        "moderated": False,
        "emoji_id": None,
        "emoji_name": None,
    }
    for tag_id, name in _TAG_NAMES.items()
]

_Item = discord.DiscordMessage | discord.DiscordThread


def _archived_thread(
    thread_id: str, archived_at: str, parent_id: str = _CHANNEL, **fields: Any
) -> dict[str, Any]:
    """A thread archived at ``archived_at``, otherwise as ``_raw_thread``."""
    return _raw_thread(
        thread_id,
        parent_id,
        metadata={"archived": True, "archive_timestamp": archived_at},
        **fields,
    )


async def _items(channel: discord.DiscordChannel) -> list[tuple[str, _Item]]:
    return [pair async for pair in channel.items()]


def _keys(items: list[tuple[str, _Item]]) -> list[str]:
    return [key for key, _ in items]


def _threads(items: list[tuple[str, _Item]]) -> dict[str, discord.DiscordThread]:
    """The thread items by key, in the order listed."""
    return {key: item for key, item in items if isinstance(item, discord.DiscordThread)}


def _requests(fake: _FakeDiscord) -> list[tuple[str, dict[str, str]]]:
    """Every request's path and parameters; the connector only GETs."""
    return [(path, params) for _, path, params in fake.requests]


def _params(fake: _FakeDiscord, path: str) -> list[dict[str, str]]:
    """The parameters of each request to ``path``, in order."""
    return [params for logged, params in _requests(fake) if logged == path]


def _set_fields(message: discord.DiscordMessage) -> dict[str, object]:
    """The message's fields that hold something."""
    values = {f.name: getattr(message, f.name) for f in dataclasses.fields(message)}
    return {name: v for name, v in values.items() if v is not None and v != ()}


@pytest.mark.asyncio
class TestItems:
    async def test_text_channel(self) -> None:
        fake = _fake_with_channels()
        fake.post(
            _CHANNEL,
            _raw_message("1037"),
            _raw_message("1038", content="Release planning?", flags=1 << 5),
            _raw_message("1039"),
        )
        # Started from 1038.
        fake.add_thread(_raw_thread("1038"))
        fake.add_thread(_archived_thread("1040", "2026-01-15T12:30:00.250000+00:00"))
        fake.add_thread(
            _raw_thread("1041", type=12, metadata={"invitable": True}), joined=True
        )
        fake.add_thread(
            _archived_thread("1042", "2026-01-15T12:40:00.500000+00:00", type=12),
            joined=True,
        )
        fake.add_thread(_raw_thread("908", _OTHER_CHANNEL))
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        # Messages as Discord lists them, newest first, then threads, newest
        # first; the starter isn't a message item of its own.
        assert [(key, type(item)) for key, item in items] == [
            ("1039", discord.DiscordMessage),
            ("1037", discord.DiscordMessage),
            ("1042", discord.DiscordThread),
            ("1041", discord.DiscordThread),
            ("1040", discord.DiscordThread),
            ("1038", discord.DiscordThread),
        ]
        assert all(key == item.id for key, item in items)
        threads = _threads(items)
        assert threads["1038"].starter == _parse_message(
            fake.messages[_CHANNEL][1], _GUILD, None
        )
        assert [threads[key].starter for key in ("1040", "1041", "1042")] == [None] * 3
        assert {thread.guild_id for thread in threads.values()} == {_GUILD}
        assert _requests(fake) == [
            ("/applications/@me", {}),
            (f"/channels/{_CHANNEL}", {}),
            (_ACTIVE_PATH, {}),
            (_PUBLIC_PATH, _PAGE),
            (_PRIVATE_PATH, _PAGE),
            (_ACTIVE_PATH, {}),
            (_MESSAGES_PATH, _PAGE),
            (_MESSAGES_PATH, {**_PAGE, "before": "1037"}),
        ]

    async def test_forum_channel(self) -> None:
        fake = _FakeDiscord()
        fake.add_channel(_raw_channel(_FORUM_ID, 15, available_tags=_AVAILABLE_TAGS))
        fake.add_thread(_raw_thread(_POST_ID, _FORUM_ID, applied_tags=["1022", "1021"]))
        fake.add_thread(
            _archived_thread(
                "1043",
                "2026-01-15T12:30:00.250000+00:00",
                _FORUM_ID,
                applied_tags=["1021"],
            )
        )
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app, _FORUM_ID))
        threads = _threads(items)
        assert len(threads) == len(items)
        assert [
            (key, thread.parent_id, thread.tags, thread.starter)
            for key, thread in threads.items()
        ] == [
            ("1043", _FORUM_ID, ("question",), None),
            (_POST_ID, _FORUM_ID, ("solved", "question"), None),
        ]
        # A forum has no main stream and no private threads.
        assert _requests(fake) == [
            ("/applications/@me", {}),
            (f"/channels/{_FORUM_ID}", {}),
            (_ACTIVE_PATH, {}),
            (f"/channels/{_FORUM_ID}/threads/archived/public", _PAGE),
            (_ACTIVE_PATH, {}),
        ]

    async def test_announcement_channel(self) -> None:
        fake = _FakeDiscord()
        fake.add_channel(_raw_channel("1044", 5))
        fake.add_thread(_raw_thread("1045", "1044", type=10))
        fake.post("1044", _raw_message("1045", flags=1 << 5), _raw_message("1046"))
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app, "1044"))
        assert [(key, type(item)) for key, item in items] == [
            ("1046", discord.DiscordMessage),
            ("1045", discord.DiscordThread),
        ]
        thread = _threads(items)["1045"]
        assert thread.type == 10
        assert thread.starter is not None
        # No private threads in an announcement channel.
        assert _requests(fake) == [
            ("/applications/@me", {}),
            ("/channels/1044", {}),
            (_ACTIVE_PATH, {}),
            ("/channels/1044/threads/archived/public", _PAGE),
            (_ACTIVE_PATH, {}),
            ("/channels/1044/messages", _PAGE),
            ("/channels/1044/messages", {**_PAGE, "before": "1045"}),
        ]

    @pytest.mark.parametrize(
        "channel_type",
        [
            pytest.param(2, id="voice"),
            pytest.param(13, id="stage"),
            pytest.param(16, id="media"),
            pytest.param(4, id="category"),
            pytest.param(11, id="thread"),
        ],
    )
    async def test_unsupported_type(self, channel_type: int) -> None:
        fake = _fake_with_channels()
        if channel_type == 11:
            # A thread is a channel too, but is listed through its parent.
            channel_id = _THREAD_ID
            fake.add_thread(_raw_thread(_THREAD_ID))
        else:
            channel_id = "1047"
            fake.add_channel(_raw_channel(channel_id, channel_type))
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                ValueError,
                match=rf"^Discord channel {channel_id} has type {channel_type}; ",
            ):
                await _items(_channel(app, channel_id))
        assert _requests(fake) == [
            ("/applications/@me", {}),
            (f"/channels/{channel_id}", {}),
        ]

    async def test_archived_public_paging_with_tied_timestamps(self) -> None:
        fake = _fake_with_channels()
        fake.page_cap = 3
        # 1050 and 1051 were archived in the same millisecond, and the first
        # page ends between them.
        for thread_id, archived_at in [
            ("1048", "2026-01-15T12:40:00.500000+00:00"),
            ("1049", "2026-01-15T12:30:00.250000+00:00"),
            ("1050", "2026-01-15T12:20:00.750000+00:00"),
            ("1051", "2026-01-15T12:20:00.750000+00:00"),
            ("1052", "2026-01-15T12:10:00+00:00"),
        ]:
            fake.add_thread(_archived_thread(thread_id, archived_at))
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        assert _keys(items) == ["1052", "1051", "1050", "1049", "1048"]
        # ``before`` is strict, so the next page starts 1 ms after the oldest
        # thread on this one: the tie is asked for again, and the thread
        # already listed is skipped.
        assert _params(fake, _PUBLIC_PATH) == [
            _PAGE,
            {**_PAGE, "before": "2026-01-15T12:20:00.751000+00:00"},
        ]

    async def test_archived_public_without_progress(self) -> None:
        # Ties filling a whole page: starting 1 ms after them asks for the
        # same page again, so ``before`` can't get past them.
        fake = _fake_with_channels()
        fake.page_cap = 2
        for thread_id, archived_at in [
            ("1053", "2026-01-15T12:40:00.500000+00:00"),
            ("1054", "2026-01-15T12:30:00.250000+00:00"),
            ("1055", "2026-01-15T12:30:00.250000+00:00"),
            ("1056", "2026-01-15T12:20:00.750000+00:00"),
        ]:
            fake.add_thread(_archived_thread(thread_id, archived_at))
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError,
                match=(
                    rf"^Discord GET {_PUBLIC_PATH} listed no new thread.*: more "
                    r"threads were archived in the same millisecond than fit on a "
                    r"page, .* until some are unarchived or deleted\. "
                ),
            ):
                await _items(_channel(app))
        # The second page added 1055; the third was the second again.
        assert fake.count(_PUBLIC_PATH) == 3

    @pytest.mark.parametrize(
        ("path", "pages"),
        [
            pytest.param(_PUBLIC_PATH, [[]], id="public-empty"),
            pytest.param(_PRIVATE_PATH, [[]], id="private-empty"),
            # This listing pages by ID, so a page answered twice isn't ties.
            pytest.param(_PRIVATE_PATH, [["1076"], ["1076"]], id="private-repeated"),
        ],
    )
    async def test_archived_stall_without_ties(
        self, path: str, pages: list[list[str]]
    ) -> None:
        fake = _fake_with_channels()
        fake.script[path] = [
            httpx.Response(
                200,
                json={
                    "threads": [
                        _archived_thread(
                            thread_id, "2026-01-15T12:30:00.250000+00:00", type=12
                        )
                        for thread_id in page
                    ],
                    "members": [],
                    "has_more": True,
                },
            )
            for page in pages
        ]
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError, match=rf"^Discord GET {path} listed no new thread"
            ) as exc_info:
                await _items(_channel(app))
        assert "same millisecond" not in str(exc_info.value)

    async def test_archived_private_paging_by_id(self) -> None:
        # IDs from 4 digits to 5: compared as text, "10000" would be smaller
        # than "9999".
        ids = ["9998", "9999", "10000", "10001", "10002"]
        fake = _fake_with_channels()
        fake.page_cap = 2
        for thread_id in ids:
            fake.add_thread(
                _archived_thread(
                    thread_id, "2026-01-15T12:30:00.250000+00:00", type=12
                ),
                joined=True,
            )
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        assert _keys(items) == ids[::-1]
        assert _params(fake, _PRIVATE_PATH) == [
            _PAGE,
            {**_PAGE, "before": "10001"},
            {**_PAGE, "before": "9999"},
        ]

    async def test_active_threads_filtered(self) -> None:
        fake = _fake_with_channels()
        fake.add_thread(_raw_thread("1057"))
        fake.add_thread(_raw_thread("909", _OTHER_CHANNEL))
        fake.add_thread(_raw_thread("1058", type=12), joined=True)
        # Listed, since the bot has Manage Threads, but nobody added the bot,
        # or it was removed: item-level loss, so the thread drops out and the
        # listing goes on.
        fake.add_thread(_raw_thread("1059", type=12))
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        assert _keys(items) == ["1058", "1057"]

    @pytest.mark.parametrize(
        "listing", [pytest.param(0, id="first"), pytest.param(1, id="second")]
    )
    async def test_active_has_more(self, listing: int) -> None:
        def answer(has_more: bool) -> httpx.Response:
            return httpx.Response(
                200, json={"threads": [], "members": [], "has_more": has_more}
            )

        fake = _fake_with_channels()
        fake.script[_ACTIVE_PATH] = [
            *(answer(False) for _ in range(listing)),
            answer(True),
        ]
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError, match=rf"^Discord GET {_ACTIVE_PATH} said more threads"
            ):
                await _items(_channel(app))
        assert fake.count(_ACTIVE_PATH) == listing + 1

    async def test_thread_unarchived_during_the_listing(self) -> None:
        # Unarchived after the first active listing, before the archived one,
        # so only the second active listing has it.
        fake = _fake_with_channels()
        raw = _archived_thread("1060", "2026-01-15T12:30:00.250000+00:00")
        fake.add_thread(raw)

        def unarchive() -> None:
            raw["thread_metadata"].update(
                archived=False, archive_timestamp="2026-01-15T12:50:00.750000+00:00"
            )

        fake.before_request[_PUBLIC_PATH] = unarchive
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        assert _keys(items) == ["1060"]
        thread = _threads(items)["1060"]
        assert (thread.archived, thread.archive_timestamp) == (
            False,
            datetime(2026, 1, 15, 12, 50, 0, 750000, tzinfo=UTC),
        )

    async def test_threads_archived_during_the_listing(self) -> None:
        # Archived after the first active listing listed them. The archived
        # listing's newer state wins, and its first page, holding only these,
        # counts as progress though the active listing had them.
        fake = _fake_with_channels()
        fake.page_cap = 2
        open_threads = [_raw_thread("1061"), _raw_thread("1062")]
        for raw in open_threads:
            fake.add_thread(raw)
        fake.add_thread(_archived_thread("1063", "2026-01-15T12:30:00.250000+00:00"))

        def archive() -> None:
            for raw, archived_at in zip(
                open_threads,
                [
                    "2026-01-15T12:50:00.750000+00:00",
                    "2026-01-15T12:40:00.500000+00:00",
                ],
                strict=True,
            ):
                raw["thread_metadata"].update(
                    archived=True, archive_timestamp=archived_at
                )

        fake.before_request[_PUBLIC_PATH] = archive
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        threads = _threads(items)
        assert list(threads) == ["1063", "1062", "1061"]
        assert [thread.archived for thread in threads.values()] == [True] * 3
        assert threads["1061"].archive_timestamp == datetime(
            2026, 1, 15, 12, 50, 0, 750000, tzinfo=UTC
        )
        assert _params(fake, _PUBLIC_PATH) == [
            _PAGE,
            {**_PAGE, "before": "2026-01-15T12:40:00.501000+00:00"},
        ]

    async def test_starters(self) -> None:
        fake = _fake_with_channels()
        # Started on a message posted well before it.
        fake.add_thread(_raw_thread("1063", name="Roadmap"))
        fake.add_thread(_raw_thread("1064"))
        # Its starter was deleted.
        fake.add_thread(_raw_thread("1065"))
        # Started without a message.
        fake.add_thread(_raw_thread("1066", name="Release notes"))
        fake.post(
            _CHANNEL,
            _raw_message("1063", content="What's next?", flags=1 << 5),
            _raw_message("1064", content="Release planning?", flags=1 << 5),
            # The notice Discord posts for a thread started without a
            # message: its ID is the thread's, and its content the thread's
            # name.
            _raw_message(
                "1066",
                type=18,
                content="Release notes",
                message_reference=_raw_reference("1066", channel_id="1066"),
            ),
            # The notice for a thread started on an older message has an ID
            # of its own.
            _raw_message(
                "1067",
                type=18,
                content="Roadmap",
                message_reference=_raw_reference("1063", channel_id="1063"),
            ),
            # Flagged as starting a thread, but the thread was started after
            # the thread listings ran: a message item this time.
            _raw_message("1068", flags=1 << 5),
        )
        app, _ = await _setup(fake)
        async with app:
            items = await _items(_channel(app))
        assert _keys(items) == ["1068", "1067", "1066", "1065", "1064", "1063"]
        notice = items[1][1]
        assert isinstance(notice, discord.DiscordMessage)
        assert (notice.type, notice.content) == (18, "Roadmap")
        threads = _threads(items)
        assert threads["1063"].starter == _parse_message(
            fake.messages[_CHANNEL][0], _GUILD, None
        )
        assert threads["1064"].starter == _parse_message(
            fake.messages[_CHANNEL][1], _GUILD, None
        )
        assert threads["1065"].starter is None
        # The notice sharing its ID isn't the thread's starter, nor an item.
        assert threads["1066"].starter is None

    async def test_selection(self) -> None:
        # message_fields applies to message items and starters alike, and
        # thread_fields to the threads; id is set whatever the selection.
        fake = _fake_with_channels()
        fake.add_thread(_raw_thread("1069", name="Release planning"))
        fake.post(
            _CHANNEL,
            _raw_message("1069", content="Release planning?", flags=1 << 5),
            _raw_message("1070", content="Shipped"),
        )
        app, _ = await _setup(fake)
        async with app:
            channel = _channel(app, message_fields=["content"], thread_fields=["name"])
            items = await _items(channel)
        assert _keys(items) == ["1070", "1069"]
        (_, message), (_, thread) = items
        assert isinstance(message, discord.DiscordMessage)
        assert isinstance(thread, discord.DiscordThread)
        assert thread.starter is not None
        assert _set_fields(message) == {"id": "1070", "content": "Shipped"}
        assert _set_fields(thread.starter) == {
            "id": "1069",
            "content": "Release planning?",
        }
        assert _properties(thread) == {
            **dict.fromkeys(_THREAD_FIELD_NAMES),
            "id": "1069",
            "name": "Release planning",
            "tags": (),
        }


@pytest.mark.asyncio
class TestGuards:
    """A misconfiguration or lost access raises, so the channel's outputs
    are kept, rather than listing fewer items, whose outputs would be
    deleted."""

    async def test_intent_off(self) -> None:
        fake = _fake_with_channels()
        fake.flags = 0
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(
                _DiscordError, match=r"^The Message Content intent is off"
            ):
                await _items(_channel(app))
        assert _requests(fake) == [("/applications/@me", {})]

    async def test_no_read_message_history(self) -> None:
        # Without it the message listing answers 200 and nothing, as for an
        # empty channel; the archived listing needs it too, and comes first.
        fake = _fake_with_channels()
        fake.fail[_PUBLIC_PATH] = (403, 50001)
        app, _ = await _setup(fake)
        seen: list[str] = []
        async with app:
            with pytest.raises(
                _DiscordError,
                match=r"HTTP 403, code 50001 \(Missing Access\)\. The bot can't read",
            ):
                async for key, _ in _channel(app).items():
                    seen.append(key)
        assert seen == []
        assert _MESSAGES_PATH not in (path for path, _ in _requests(fake))

    async def test_no_view_channel(self) -> None:
        fake = _fake_with_channels()
        fake.fail[f"/channels/{_CHANNEL}"] = (403, 50001)
        app, _ = await _setup(fake)
        async with app:
            with pytest.raises(_DiscordError, match=r"HTTP 403, code 50001"):
                await _items(_channel(app))
        assert _requests(fake) == [
            ("/applications/@me", {}),
            (f"/channels/{_CHANNEL}", {}),
        ]

    async def test_error_mid_listing_raises(self) -> None:
        fake = _fake_with_channels()
        fake.post(_CHANNEL, *(_raw_message(i) for i in ("1071", "1072", "1073")))
        fake.page_cap = 2
        # The first page as the fake lists it, then the second fails.
        fake.script[_MESSAGES_PATH] = [
            fake._answer(_MESSAGES_PATH, _PAGE),
            _discord_error(403, 50001),
        ]
        app, _ = await _setup(fake)
        seen: list[str] = []
        async with app:
            with pytest.raises(_DiscordError, match=r"HTTP 403, code 50001"):
                async for key, _ in _channel(app).items():
                    seen.append(key)
        assert seen == ["1073", "1072"]
        assert _params(fake, _MESSAGES_PATH) == [_PAGE, {**_PAGE, "before": "1072"}]


class TestLooksStripped:
    """A conversation message with none of the fields that say something
    looks stripped: Discord creates no message without one of them."""

    @pytest.mark.parametrize(
        ("fields", "stripped"),
        [
            pytest.param({"content": ""}, True, id="nothing"),
            pytest.param({"content": "", "type": 19}, True, id="empty-reply"),
            pytest.param({}, False, id="content"),
            pytest.param({"content": "", "embeds": [_raw_embed()]}, False, id="embed"),
            pytest.param(
                {"content": "", "attachments": [_raw_attachment()]},
                False,
                id="attachment",
            ),
            pytest.param(
                {"content": "", "components": [{"type": 10, "content": "Hi"}]},
                False,
                id="components",
            ),
            pytest.param(
                {
                    "content": "",
                    "sticker_items": [{"id": "1107", "name": "wave", "format_type": 1}],
                },
                False,
                id="sticker",
            ),
            pytest.param(
                {
                    "content": "",
                    "poll": {
                        "question": {"text": "Lunch?"},
                        "answers": [{"answer_id": 1, "poll_media": {"text": "Yes"}}],
                    },
                },
                False,
                id="poll",
            ),
            # Only that it's there counts.
            pytest.param(
                {"content": "", "shared_client_theme": {"colors": ["5865f2"]}},
                False,
                id="theme",
            ),
            # Notices have nothing to say.
            pytest.param({"content": "", "type": 7}, False, id="join-notice"),
            pytest.param({"content": "", "type": 21}, False, id="starter-stub"),
        ],
    )
    def test_message(self, fields: dict[str, Any], stripped: bool) -> None:
        assert _looks_stripped(_raw_message("1108", **fields)) is stripped

    @pytest.mark.parametrize(
        ("snapshot", "stripped"),
        [
            pytest.param({"content": "", "attachments": []}, True, id="empty"),
            pytest.param({"content": "Hello"}, False, id="content"),
        ],
    )
    def test_forward_judged_by_its_snapshot(
        self, snapshot: dict[str, Any], stripped: bool
    ) -> None:
        assert _looks_stripped(_raw_forward(snapshot)) is stripped


@pytest.mark.asyncio
class TestStrippedMessages:
    """With the Message Content intent turned off after an App's first check,
    Discord sends people's messages with nothing they say, and their outputs
    would be rewritten or deleted. A page holding such a message raises
    before any of it is yielded."""

    async def test_intent_turned_off_later(self) -> None:
        fake = _fake_with_channels()
        fake.post(_CHANNEL, _raw_message("1109"), _raw_message("1110"))
        app, _ = await _setup(fake)
        seen: list[str] = []
        async with app:
            assert _keys(await _items(_channel(app))) == ["1110", "1109"]
            fake.flags = 0
            with pytest.raises(
                _DiscordError, match=r"^The Message Content intent is off"
            ):
                async for key, _ in _channel(app).items():
                    seen.append(key)
        assert seen == []
        # The first listing's check is kept; the stripped page asked again.
        assert fake.count("/applications/@me") == 2

    async def test_failed_page_check_asks_up_front_again(self) -> None:
        # A forum's listing reads no message, so with the up-front check
        # still passed, each of its posts would fail on its own, run after
        # run, each asking again.
        fake = _fake_with_channels()
        fake.post(_CHANNEL, _raw_message("1117"))
        app, _ = await _setup(fake)
        intent_off = r"^The Message Content intent is off"
        async with app:
            await _items(_channel(app))
            fake.flags = 0
            with pytest.raises(_DiscordError, match=intent_off):
                await _items(_channel(app))
            fake.requests.clear()
            with pytest.raises(_DiscordError, match=intent_off):
                await _items(_channel(app))
        # The next listing fails up front, before fetching the channel.
        assert _requests(fake) == [("/applications/@me", {})]

    async def test_thread_messages(self) -> None:
        fake = _FakeDiscord()
        fake.flags = 0
        fake.post(_POST_ID, _raw_message(_POST_ID), _raw_message("1111"))
        app, ctx = await _setup(fake)
        async with app:
            post = _thread(_raw_thread(_POST_ID, parent_id=_FORUM_ID), ctx=ctx)
            with pytest.raises(
                _DiscordError, match=r"^The Message Content intent is off"
            ):
                await post.messages()

    async def test_messages_that_keep_what_they_say(self) -> None:
        # The bot's own messages and ones that mention it keep their text
        # without the intent, and a notice has none to lose.
        fake = _fake_with_channels()
        fake.post(
            _CHANNEL,
            _raw_message("1112", author=_BOT_USER),
            _raw_message(
                "1113", content=f"<@{_BOT_ID}> are you there?", mentions=[_BOT_USER]
            ),
            _raw_message("1114", type=7, content=""),
        )
        app, ctx = await _setup(fake)
        async with app:
            await ctx.check_message_content_intent_once()
            fake.flags = 0
            assert _keys(await _items(_channel(app))) == ["1114", "1113", "1112"]
        assert fake.count("/applications/@me") == 1

    async def test_empty_message_with_the_intent_on(self) -> None:
        # A message can be empty with the intent on, such as a webhook's post
        # of only rich embeds once they're suppressed (flag 1 << 2).
        fake = _fake_with_channels()
        fake.post(
            _CHANNEL,
            _raw_message(
                "1115",
                author=_WEBHOOK_USER,
                webhook_id=_WEBHOOK_ID,
                content="",
                flags=1 << 2,
            ),
            _raw_message("1116"),
        )
        app, _ = await _setup(fake)
        async with app:
            assert _keys(await _items(_channel(app))) == ["1116", "1115"]
        # The first check, then the page's.
        assert fake.count("/applications/@me") == 2


# ---------------------------------------------------------------------------
# End to end, through the engine
# ---------------------------------------------------------------------------

# What the pipeline did; a new _Pipeline clears them.
_metrics = Metrics()
_failures: list[BaseException] = []


def _record_failure(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    # Returning carries on with the run, as the engine's default (logging the
    # failure) does.
    _failures.append(exc)


# On the environment, since each component's context starts from its handler.
coco_env = common.create_test_env(__file__, exception_handler=_record_failure)


@coco.fn(memo=True)
async def _export_item(item: _Item, channel_id: str) -> None:
    entry = f"{channel_id}/{item.id}"
    _metrics.increment(entry)
    coco.declare_target_state(
        GlobalDictTarget.target_state(entry, await item.read_text())
    )


@coco.fn
async def _sync_channel(channel: discord.DiscordChannel) -> None:
    # Entries are named by the channel's ID, not by the item's channel_id or
    # parent_id, which a selection can leave out.
    await coco.mount_each(_export_item, channel.items(), channel.channel_id)


async def _sync_channels(
    app: discord.DiscordApp,
    channel_ids: Sequence[str],
    message_fields: Sequence[str] | None = None,
) -> None:
    """Mounts ``_sync_channel`` for each channel in ``channel_ids``."""
    async with app:
        channels = [
            (
                channel_id,
                discord.DiscordChannel(
                    app=app, channel_id=channel_id, message_fields=message_fields
                ),
            )
            for channel_id in channel_ids
        ]
        handle = await coco.mount_each(_sync_channel, channels)
        # mount_each returns once the channels are scheduled; the App has to
        # stay open until they and their items are done.
        await handle.ready()


def _clear_outputs() -> None:
    """An empty target, and nothing recorded of earlier runs."""
    GlobalDictTarget.store.clear()
    _metrics.clear()
    _failures.clear()


class _Run(NamedTuple):
    """What one run of a ``_Pipeline`` did."""

    # How often _export_item ran, by entry.
    exported: dict[str, int]
    # The failed components' errors, sorted, since components run
    # concurrently.
    failures: list[str]
    # The IDs of the threads whose messages were listed.
    threads_listed: set[str]


class _Pipeline:
    """A pipeline run by the engine over ``fake``: one component per channel
    in ``channel_ids``, each mounting one per item, which exports the item's
    text to ``GlobalDictTarget`` as entry ``"<channel_id>/<item_id>"``.
    Tests change ``fake``, ``channel_ids`` and ``message_fields`` between
    runs."""

    def __init__(
        self, name: str, fake: _FakeDiscord, channel_ids: Sequence[str]
    ) -> None:
        _clear_outputs()
        self.fake = fake
        self.channel_ids = list(channel_ids)
        self.message_fields: list[str] | None = None
        self._app = coco.App(
            coco.AppConfig(name=name, environment=coco_env), self._main
        )

    async def _main(self) -> None:
        app, _ = await _setup(self.fake)
        await _sync_channels(app, self.channel_ids, self.message_fields)

    def run(self) -> _Run:
        self.fake.requests.clear()
        self._app.update_blocking()
        failures = sorted(str(e) for e in _failures)
        _failures.clear()
        threads_listed = {
            m[1]
            for _, path, _ in self.fake.requests
            if (m := re.fullmatch(r"/channels/(\d+)/messages", path))
            and m[1] in self.fake.threads
        }
        return _Run(_metrics.collect(), failures, threads_listed)


def _entries() -> dict[str, str]:
    return {key: entry.data for key, entry in GlobalDictTarget.store.data.items()}


def _missing_access(path: str) -> str:
    """The error that a 403 with code 50001 on ``path`` raises."""
    return (
        f"Discord GET {path} failed: HTTP 403, code 50001 (Missing Access). "
        f"{_source._ERROR_HINTS[(403, 50001)]}"
    )


def _start_thread(fake: _FakeDiscord, message_id: str, stub_id: str) -> None:
    """Start a thread on the message ``message_id`` in ``_CHANNEL``, as
    Discord does: the thread has the message's ID and holds a stub standing
    for it, and the message is flagged ``HAS_THREAD``."""
    fake.add_thread(_raw_thread(message_id))
    fake.message(_CHANNEL, message_id)["flags"] = 1 << 5
    fake.post(message_id, _stub(message_id, stub_id))


_SERVER_CHANNELS = (_CHANNEL, _OTHER_CHANNEL, _FORUM_ID)


def _server() -> _FakeDiscord:
    """The server the end-to-end tests start from, whose first run exports
    ``_SERVER_ENTRIES``: in ``_CHANNEL``, messages (a reply, one with a
    reaction, one with a file) and two threads started from messages; in
    ``_OTHER_CHANNEL``, a message and an archived thread whose starter is
    gone; in ``_FORUM_ID``, a post."""
    bo = _raw_user(_OTHER_PERSON_ID, "bo")
    fake = _fake_with_channels()
    fake.add_channel(_raw_channel(_FORUM_ID, 15, available_tags=_AVAILABLE_TAGS))
    fake.post(
        _CHANNEL,
        _raw_message("1077"),
        _raw_message(
            "1078",
            type=19,
            author=bo,
            content="Thursday",
            message_reference=_raw_reference("1077"),
        ),
        _raw_message("1079", reactions=[_raw_reaction("👍", 1)]),
        _raw_message("1080", content="Release planning?"),
        _raw_message("1083"),
        _raw_message("1084", content="Notes attached", attachments=[_raw_attachment()]),
        _raw_message("1085", content="Retro?"),
    )
    _start_thread(fake, "1080", "1081")
    fake.post("1080", _raw_message("1082", author=bo, content="Thursday works"))
    _start_thread(fake, "1085", "1086")
    fake.post("1085", _raw_message("1087"))
    fake.post(_OTHER_CHANNEL, _raw_message("1088"))
    fake.add_thread(
        _archived_thread("1089", "2026-01-15T12:30:00.250000+00:00", _OTHER_CHANNEL)
    )
    fake.post("1089", _raw_message("1090"))
    fake.add_thread(
        _raw_thread("1091", _FORUM_ID, name="Release planning", applied_tags=["1021"])
    )
    fake.post(
        "1091",
        _raw_message("1091", content="Which day works?"),
        _raw_message("1092", author=bo, content="Thursday"),
    )
    return fake


_SERVER_ENTRIES = {
    "1002/1077": "Ada: message 1077",
    "1002/1078": "bo: Thursday",
    "1002/1079": "Ada: message 1079",
    "1002/1080": "thread-1080\nAda: Release planning?\nbo: Thursday works",
    "1002/1083": "Ada: message 1083",
    "1002/1084": "Ada: Notes attached",
    "1002/1085": "thread-1085\nAda: Retro?\nAda: message 1087",
    "902/1088": "Ada: message 1088",
    "902/1089": "thread-1089\nAda: message 1090",
    "1019/1091": "Release planning\nTags: question\nAda: Which day works?\nbo: Thursday",
}


class TestEndToEnd:
    """The connector in a pipeline run by the engine, over the server
    ``_server`` builds, changed between runs. The engine doesn't raise a
    component's failure out of the run, so the tests check the failures the
    run recorded."""

    def test_reruns_only_changed_items(self) -> None:
        fake = _server()
        pipeline = _Pipeline(
            "test_discord_reruns_only_changed_items", fake, _SERVER_CHANNELS
        )
        run = pipeline.run()
        assert run == _Run(
            dict.fromkeys(_SERVER_ENTRIES, 1), [], {"1080", "1085", "1089", "1091"}
        )
        assert _entries() == _SERVER_ENTRIES

        # In the main streams: an edit, a reaction, a new message, a deleted one.
        fake.message(_CHANNEL, "1077").update(
            content="message 1077, edited",
            edited_timestamp="2026-01-15T13:00:00.250000+00:00",
        )
        fake.message(_CHANNEL, "1079")["reactions"] = [_raw_reaction("👍", 2)]
        fake.post(_OTHER_CHANNEL, _raw_message("1093"))
        fake.messages[_OTHER_CHANNEL].remove(fake.message(_OTHER_CHANNEL, "1088"))
        # A thread started on 1083, a post in thread 1080, and forum post 1091
        # archived.
        _start_thread(fake, "1083", "1094")
        fake.post("1080", _raw_message("1095"))
        fake.threads["1091"]["thread_metadata"].update(
            archived=True, archive_timestamp="2026-01-15T14:00:00.500000+00:00"
        )
        # Changes Discord reports nothing for: an edit inside open thread
        # 1085, and 1084's attachment URL signed anew.
        fake.message("1085", "1087").update(
            content="message 1087, edited",
            edited_timestamp="2026-01-15T13:00:00.250000+00:00",
        )
        fake.message(_CHANNEL, "1084")["attachments"] = [
            _raw_attachment(
                url=_ATTACHMENT.url.replace("ex=1&is=0&hm=a", "ex=2&is=1&hm=c")
            )
        ]
        run = pipeline.run()
        assert run.failures == []
        # Not the reply 1078, though its parent was edited.
        assert run.exported == {
            "1002/1077": 1,
            "1002/1079": 1,
            "1002/1080": 1,
            "1002/1083": 1,
            "902/1093": 1,
            "1019/1091": 1,
        }
        # The new thread holds only its starter, so it needs no request.
        assert run.threads_listed == {"1080", "1091"}
        entries = {
            **_SERVER_ENTRIES,
            "1002/1077": "Ada: message 1077, edited",
            "1002/1080": (
                "thread-1080\nAda: Release planning?\nbo: Thursday works\n"
                "Ada: message 1095"
            ),
            # The message turned into the thread, under the same key.
            "1002/1083": "thread-1083\nAda: message 1083",
            "902/1093": "Ada: message 1093",
        }
        del entries["902/1088"]
        # 1085 keeps the text from before the edit inside it, on purpose:
        # nothing Discord reports for the thread changed.
        assert _entries() == entries

        # Nothing changed: every channel is listed again, but no item runs,
        # and no thread's messages are listed.
        run = pipeline.run()
        assert run == _Run({}, [], set())
        assert {f"/channels/{channel_id}" for channel_id in _SERVER_CHANNELS} <= {
            path for _, path, _ in fake.requests
        }
        assert _entries() == entries

    def test_changing_the_selection(self) -> None:
        fake = _server()
        pipeline = _Pipeline(
            "test_discord_changing_the_selection", fake, _SERVER_CHANNELS
        )
        assert pipeline.run().failures == []

        # Leaving reactions out reruns the one message that had any, and
        # every thread, since a thread's messages are parsed with the
        # selection. The other messages expose the same as before.
        pipeline.message_fields = sorted(_MESSAGE_FIELD_NAMES - {"reactions"})
        run = pipeline.run()
        assert run.failures == []
        assert run.exported == {
            "1002/1079": 1,
            "1002/1080": 1,
            "1002/1085": 1,
            "902/1089": 1,
            "1019/1091": 1,
        }
        assert _entries() == _SERVER_ENTRIES

        # Then a reaction reruns nothing, on a message or on a thread's
        # starter.
        fake.message(_CHANNEL, "1077")["reactions"] = [_raw_reaction("👍", 1)]
        fake.message(_CHANNEL, "1080")["reactions"] = [_raw_reaction("👀", 1)]
        assert pipeline.run() == _Run({}, [], set())

    def test_removing_a_channel(self) -> None:
        fake = _server()
        pipeline = _Pipeline("test_discord_removing_a_channel", fake, _SERVER_CHANNELS)
        assert pipeline.run().failures == []

        pipeline.channel_ids.remove(_OTHER_CHANNEL)
        assert pipeline.run() == _Run({}, [], set())
        # Its message and its thread lose their entries; nothing of it is
        # requested.
        assert _entries() == {
            entry: text
            for entry, text in _SERVER_ENTRIES.items()
            if not entry.startswith(f"{_OTHER_CHANNEL}/")
        }
        assert not any(
            path.startswith(f"/channels/{_OTHER_CHANNEL}")
            for _, path, _ in fake.requests
        )

    def test_errors_delete_nothing(self) -> None:
        fake = _server()
        pipeline = _Pipeline(
            "test_discord_errors_delete_nothing", fake, _SERVER_CHANNELS
        )
        assert pipeline.run().failures == []

        # _CHANNEL loses Read Message History: its message listing answers
        # 200 and nothing, and its archived listing 403. _OTHER_CHANNEL, where
        # a message is posted meanwhile, still syncs.
        listed = fake.messages[_CHANNEL]
        fake.messages[_CHANNEL] = []
        fake.fail[_PUBLIC_PATH] = (403, 50001)
        fake.post(_OTHER_CHANNEL, _raw_message("1096"))
        run = pipeline.run()
        assert run.failures == [_missing_access(_PUBLIC_PATH)]
        assert run.exported == {"902/1096": 1}
        entries = {**_SERVER_ENTRIES, "902/1096": "Ada: message 1096"}
        assert _entries() == entries
        fake.messages[_CHANNEL] = listed
        del fake.fail[_PUBLIC_PATH]

        # _CHANNEL's main stream fails on its second page: the messages and
        # threads never reached keep their entries. A 403, which isn't
        # retried: a 5xx's backoff would really wait here.
        fake.page_cap = 2
        fake.script[_MESSAGES_PATH] = [
            fake._answer(_MESSAGES_PATH, _PAGE),
            _discord_error(403, 50001),
        ]
        run = pipeline.run()
        assert run.failures == [_missing_access(_MESSAGES_PATH)]
        assert run.exported == {}
        assert _entries() == entries
        fake.page_cap = None

        # Thread 1080 gets a message but its messages fail to list: only its
        # component fails, and it keeps its entry, while 1098, posted
        # meanwhile, is exported.
        thread_path = "/channels/1080/messages"
        fake.post("1080", _raw_message("1097"))
        fake.post(_CHANNEL, _raw_message("1098"))
        fake.fail[thread_path] = (403, 50001)
        run = pipeline.run()
        assert run.failures == [_missing_access(thread_path)]
        assert run.exported == {"1002/1080": 1, "1002/1098": 1}
        entries["1002/1098"] = "Ada: message 1098"
        assert _entries() == entries

        # Fixed: the thread runs again, since a failed component's memo isn't
        # saved; nothing else does.
        del fake.fail[thread_path]
        run = pipeline.run()
        assert run == _Run({"1002/1080": 1}, [], {"1080"})
        entries["1002/1080"] += "\nAda: message 1097"
        assert _entries() == entries


# ---------------------------------------------------------------------------
# Live tests against a real server
# ---------------------------------------------------------------------------

_HAS_LIVE = all(
    os.environ.get(name)
    for name in ("DISCORD_BOT_TOKEN", "DISCORD_TEST_GUILD", "DISCORD_TEST_CHANNEL")
)
requires_live = pytest.mark.skipif(
    not _HAS_LIVE,
    reason="DISCORD_BOT_TOKEN, DISCORD_TEST_GUILD or DISCORD_TEST_CHANNEL not set",
)
requires_live_forum = pytest.mark.skipif(
    not (_HAS_LIVE and os.environ.get("DISCORD_TEST_FORUM")),
    reason="the live variables or DISCORD_TEST_FORUM not set",
)
requires_live_no_history = pytest.mark.skipif(
    not (_HAS_LIVE and os.environ.get("DISCORD_TEST_NO_HISTORY_CHANNEL")),
    reason="the live variables or DISCORD_TEST_NO_HISTORY_CHANNEL not set",
)


class _LiveFixtures:
    """Posts, edits, reacts to and archives the bot's own messages and
    threads on the real server; a forum post is a thread. On exit, deletes
    the threads it started, which deletes the messages inside them, then the
    messages it posted elsewhere, newest first.

    Needs more of the bot than the connector does: Send Messages (and in
    Threads), Create Public Threads, Embed Links, Add Reactions, and Manage
    Threads, to archive and delete threads."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=_source._BASE_URL,
            timeout=60.0,
            headers={
                "Authorization": f"Bot {os.environ['DISCORD_BOT_TOKEN']}",
                "User-Agent": (
                    "DiscordBot (https://github.com/cocoindex-io/cocoindex, "
                    f"{coco.__version__})"
                ),
            },
        )
        # (channel ID, message ID), in the order posted.
        self._messages: list[tuple[str, str]] = []
        self._threads: list[str] = []

    async def _send(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        missing_ok: bool = False,
    ) -> Any:
        """One request, waiting out a 429, then for the bucket's reset when
        the response says it's used up."""
        while True:
            resp = await self._client.request(method, path, json=body)
            if resp.headers.get("X-RateLimit-Remaining") == "0":
                reset_after = resp.headers.get("X-RateLimit-Reset-After", "1")
                await asyncio.sleep(float(reset_after))
            if resp.status_code != 429:
                break
            await asyncio.sleep(float(resp.json().get("retry_after", 1)))
        if missing_ok and resp.status_code == 404:
            return None
        assert resp.is_success, (
            f"Discord {method} {path} failed: HTTP {resp.status_code}, {resp.text[:200]}"
        )
        return None if resp.status_code == 204 else resp.json()

    async def post(
        self, channel_id: str, content: str = "", **fields: Any
    ) -> dict[str, Any]:
        """Posts a message in ``channel_id``, a thread's ID included; returns
        it as Discord does."""
        body = {"content": content, **fields} if content else fields
        message: dict[str, Any] = await self._send(
            "POST", f"/channels/{channel_id}/messages", body
        )
        self._messages.append((channel_id, message["id"]))
        return message

    async def edit(self, channel_id: str, message_id: str, content: str) -> None:
        await self._send(
            "PATCH",
            f"/channels/{channel_id}/messages/{message_id}",
            {"content": content},
        )

    async def react(self, channel_id: str, message_id: str, emoji: str) -> None:
        await self._send(
            "PUT",
            f"/channels/{channel_id}/messages/{message_id}/reactions/{quote(emoji)}/@me",
        )

    async def start_thread(
        self, channel_id: str, message_id: str, name: str
    ) -> dict[str, Any]:
        """Starts a thread on a message; returns the thread object."""
        thread: dict[str, Any] = await self._send(
            "POST",
            f"/channels/{channel_id}/messages/{message_id}/threads",
            {"name": name},
        )
        self._threads.append(thread["id"])
        return thread

    async def start_thread_without_message(
        self, channel_id: str, name: str
    ) -> dict[str, Any]:
        """Starts a public thread with no message; returns the thread object.
        The notice Discord posts in the channel for it has the thread's ID
        and outlives the thread, so it's deleted as a message."""
        thread: dict[str, Any] = await self._send(
            "POST", f"/channels/{channel_id}/threads", {"name": name, "type": 11}
        )
        self._threads.append(thread["id"])
        self._messages.append((channel_id, thread["id"]))
        return thread

    async def forum_post(
        self, forum_id: str, name: str, content: str
    ) -> dict[str, Any]:
        """Posts in a forum; returns the post's thread object, whose ID is
        its opening message's too."""
        thread: dict[str, Any] = await self._send(
            "POST",
            f"/channels/{forum_id}/threads",
            {"name": name, "message": {"content": content}},
        )
        self._threads.append(thread["id"])
        return thread

    async def archive(self, thread_id: str) -> None:
        await self._send("PATCH", f"/channels/{thread_id}", {"archived": True})

    async def __aenter__(self) -> _LiveFixtures:
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        try:
            for thread_id in reversed(self._threads):
                await self._send("DELETE", f"/channels/{thread_id}", missing_ok=True)
            for channel_id, message_id in reversed(self._messages):
                if channel_id not in self._threads:
                    await self._send(
                        "DELETE",
                        f"/channels/{channel_id}/messages/{message_id}",
                        missing_ok=True,
                    )
        finally:
            await self._client.aclose()


def _live_app() -> discord.DiscordApp:
    return discord.DiscordApp(token=os.environ["DISCORD_BOT_TOKEN"])


def _author_name(raw_message: dict[str, Any]) -> str:
    """The display name of a message's author, as Discord returned it: the
    test server's own, so never written in this file."""
    author = raw_message["author"]
    name: str = author.get("global_name") or author["username"]
    return name


# Each test posts up to seven messages, at about five per 5 s in a channel,
# deletes them, and lists the whole test channel up to five times, each a few
# pages and paced by Discord: more than pytest's 30 s.
@requires_live
@pytest.mark.timeout(120)
@pytest.mark.asyncio
class TestLive:
    async def test_intent_and_list_channels(self) -> None:
        async with _live_app() as app:
            ctx = await app._get_context()
            await ctx.check_message_content_intent()
            listed = await app.list_channels(os.environ["DISCORD_TEST_GUILD"])
        types = {channel.id: channel.type for channel in listed}
        assert types[os.environ["DISCORD_TEST_CHANNEL"]] == 0
        if forum := os.environ.get("DISCORD_TEST_FORUM"):
            assert types[forum] == 15
        # The test server has channels under categories, so this checks that
        # category_id is the category's ID.
        category_ids = {c.category_id for c in listed if c.category_id is not None}
        assert category_ids
        assert {types.get(category_id) for category_id in category_ids} == {4}

    async def test_text_channel_items(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        async with _LiveFixtures() as fx, _live_app() as app:
            first = await fx.post(channel_id, "live: a message")
            reply = await fx.post(
                channel_id,
                "live: a reply",
                message_reference={"message_id": first["id"]},
            )
            starter = await fx.post(channel_id, "live: a thread's first message")
            thread_id = (
                await fx.start_thread(channel_id, starter["id"], "live: a thread")
            )["id"]
            inside = [
                await fx.post(thread_id, f"live: in the thread, {n}") for n in (1, 2)
            ]
            embed = await fx.post(
                channel_id,
                embeds=[
                    {
                        "title": "live: an embed",
                        "description": "Its description",
                        "fields": [{"name": "Field", "value": "Value"}],
                        "footer": {"text": "Its footer"},
                    }
                ],
            )
            bot = _author_name(first)

            listed = await _items(
                discord.DiscordChannel(app=app, channel_id=channel_id)
            )
            # Newest first, and nothing has been posted since.
            assert _keys(listed)[:3] == [embed["id"], reply["id"], first["id"]]
            items = dict(listed)
            message = items[first["id"]]
            assert isinstance(message, discord.DiscordMessage)
            assert await message.read_text() == f"{bot}: live: a message"
            replying = items[reply["id"]]
            assert isinstance(replying, discord.DiscordMessage)
            assert (replying.type, replying.reply_to) == (19, first["id"])
            embedding = items[embed["id"]]
            assert isinstance(embedding, discord.DiscordMessage)
            assert embedding.embeds == (
                discord.DiscordEmbed(
                    title="live: an embed",
                    description="Its description",
                    url=None,
                    author=None,
                    footer="Its footer",
                    fields=(("Field", "Value"),),
                ),
            )
            assert await embedding.read_text() == (
                f"{bot}: live: an embed\nIts description\nField: Value\nIts footer"
            )
            # The starter is the thread's, not an item of its own.
            thread = items[thread_id]
            assert isinstance(thread, discord.DiscordThread)
            assert thread_id == starter["id"]
            assert thread.starter is not None and thread.starter.id == starter["id"]
            assert [m.id for m in await thread.messages()] == [
                starter["id"],
                *(m["id"] for m in inside),
            ]
            assert await thread.read_text() == (
                f"live: a thread\n{bot}: live: a thread's first message\n"
                f"{bot}: live: in the thread, 1\n{bot}: live: in the thread, 2"
            )

    async def test_thread_without_a_message(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        async with _LiveFixtures() as fx, _live_app() as app:
            thread_id = (
                await fx.start_thread_without_message(
                    channel_id, "live: a thread without a message"
                )
            )["id"]
            inside = await fx.post(thread_id, "live: in the thread")
            bot = _author_name(inside)

            ctx = await app._get_context()
            newest = await ctx.get(_ROUTE_CHANNEL_MESSAGES, channel_id, {"limit": 10})
            # Discord's notice for the thread has the thread's ID.
            assert any(m["id"] == thread_id and m["type"] == 18 for m in newest)
            listed = await _items(
                discord.DiscordChannel(app=app, channel_id=channel_id)
            )
            # So the ID is listed once, for the thread, which has no starter.
            assert _keys(listed).count(thread_id) == 1
            thread = dict(listed)[thread_id]
            assert isinstance(thread, discord.DiscordThread)
            assert thread.starter is None
            assert await thread.read_text() == (
                f"live: a thread without a message\n{bot}: live: in the thread"
            )

    @requires_live_forum
    async def test_forum_items(self) -> None:
        forum_id = os.environ["DISCORD_TEST_FORUM"]
        async with _LiveFixtures() as fx, _live_app() as app:
            post_id = (
                await fx.forum_post(
                    forum_id, "live: a forum post", "live: its opening message"
                )
            )["id"]
            reply = await fx.post(post_id, "live: a reply in the post")

            items = dict(
                await _items(discord.DiscordChannel(app=app, channel_id=forum_id))
            )
            thread = items[post_id]
            assert isinstance(thread, discord.DiscordThread)
            assert thread.starter is None
            assert [m.id for m in await thread.messages()] == [post_id, reply["id"]]

    async def test_change_signals(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        async with _LiveFixtures() as fx, _live_app() as app:
            message_id = (await fx.post(channel_id, "live: a message"))["id"]
            starter = await fx.post(channel_id, "live: a thread's first message")
            thread_id = (
                await fx.start_thread(channel_id, starter["id"], "live: a thread")
            )["id"]
            inside_id = (await fx.post(thread_id, "live: in the thread"))["id"]
            channel = discord.DiscordChannel(app=app, channel_id=channel_id)
            fingerprints: dict[str, object] = {}

            async def changed() -> set[str]:
                """Which of the two items' fingerprints changed since the
                last listing."""
                before = dict(fingerprints)
                fingerprints.update(
                    (key, memo_fingerprint(item))
                    for key, item in await _items(channel)
                    if key in (message_id, thread_id)
                )
                return {key for key in before if before[key] != fingerprints[key]}

            await changed()
            assert fingerprints.keys() == {message_id, thread_id}
            await fx.edit(channel_id, message_id, "live: a message, edited")
            assert await changed() == {message_id}
            await fx.react(channel_id, message_id, "👍")
            assert await changed() == {message_id}
            # Nothing Discord reports for the thread changes: the gap inside
            # threads.
            await fx.edit(thread_id, inside_id, "live: in the thread, edited")
            assert await changed() == set()
            await fx.post(thread_id, "live: in the thread, again")
            assert await changed() == {thread_id}

    @requires_live_no_history
    async def test_no_read_message_history(self) -> None:
        channel_id = os.environ["DISCORD_TEST_NO_HISTORY_CHANNEL"]
        async with _live_app() as app:
            with pytest.raises(_DiscordError, match="HTTP 403, code 50001"):
                await _items(discord.DiscordChannel(app=app, channel_id=channel_id))

    async def test_archived_paging(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        async with _LiveFixtures() as fx, _live_app() as app:
            starter = await fx.post(channel_id, "live: a thread to archive")
            thread_id = (
                await fx.start_thread(channel_id, starter["id"], "live: archived")
            )["id"]
            await fx.archive(thread_id)
            ctx = await app._get_context()

            async def listed(before: str | None = None) -> list[dict[str, Any]]:
                """The archived public listing's first page, of 100."""
                params: dict[str, str | int] = {"limit": 100}
                if before is not None:
                    params["before"] = before
                listing = await ctx.get(
                    _ROUTE_ARCHIVED_PUBLIC_THREADS, channel_id, params
                )
                threads: list[dict[str, Any]] = listing["threads"]
                return threads

            # A page of 100 is accepted by both archived listings; get()
            # raises on an error.
            await ctx.get(_ROUTE_JOINED_PRIVATE_THREADS, channel_id, {"limit": 100})
            thread = next(t for t in await listed() if t["id"] == thread_id)
            # `before` is strict, and 1 ms later lists the thread again.
            archived_at = thread["thread_metadata"]["archive_timestamp"]
            assert thread_id not in {t["id"] for t in await listed(archived_at)}
            assert thread_id in {
                t["id"] for t in await listed(_archived_before([thread]))
            }


async def _live_main(channel_ids: list[str]) -> None:
    await _sync_channels(_live_app(), channel_ids)


@requires_live
@pytest.mark.asyncio
class TestLiveEndToEnd:
    # The first run exports every item in the test channels, not only the
    # ones posted here, and lists each thread's messages.
    @pytest.mark.timeout(180)
    async def test_two_runs(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        forum_id = os.environ.get("DISCORD_TEST_FORUM")
        async with _LiveFixtures() as fx:
            message = await fx.post(channel_id, "live: a message")
            starter = await fx.post(channel_id, "live: a thread's first message")
            thread_id = (
                await fx.start_thread(channel_id, starter["id"], "live: a thread")
            )["id"]
            await fx.post(thread_id, "live: in the thread")
            bot = _author_name(message)
            expected = {
                f"{channel_id}/{message['id']}": f"{bot}: live: a message",
                f"{channel_id}/{thread_id}": (
                    f"live: a thread\n{bot}: live: a thread's first message\n"
                    f"{bot}: live: in the thread"
                ),
            }
            channel_ids = [channel_id]
            if forum_id:
                post_id = (
                    await fx.forum_post(
                        forum_id, "live: a forum post", "live: its opening message"
                    )
                )["id"]
                expected[f"{forum_id}/{post_id}"] = (
                    f"live: a forum post\n{bot}: live: its opening message"
                )
                channel_ids.append(forum_id)

            _clear_outputs()
            app = coco.App(
                coco.AppConfig(name="test_discord_live_two_runs", environment=coco_env),
                _live_main,
                channel_ids=channel_ids,
            )
            await app.update()
            assert _failures == []
            entries = _entries()
            assert {entry: entries.get(entry) for entry in expected} == expected

            # Nothing changed: the channels are listed again, but no item runs.
            _metrics.clear()
            await app.update()
            assert _failures == []
            assert _metrics.collect() == {}
            assert _entries() == entries


@requires_live
@pytest.mark.asyncio
class TestLivePacing:
    # Discord has let the message listing take 5 requests per 1 to 6 s per
    # channel, live, so 25 requests take up to about 30 s.
    @pytest.mark.timeout(60)
    async def test_bucket_pacing_avoids_429(self) -> None:
        channel_id = os.environ["DISCORD_TEST_CHANNEL"]
        statuses: list[int] = []

        async def on_response(response: httpx.Response) -> None:
            statuses.append(response.status_code)

        async with _live_app() as app:
            ctx = await app._get_context()
            ctx._client.event_hooks = {"response": [on_response]}
            for _ in range(25):
                await ctx.get(_ROUTE_CHANNEL_MESSAGES, channel_id, {"limit": 1})
        # One response per request: no 429, and nothing retried.
        assert statuses == [200] * 25
