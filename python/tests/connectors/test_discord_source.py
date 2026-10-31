"""Tests for the Discord source connector.

Covers the package's public names, ``DiscordApp`` and its internal API client
(the retry loop, rate-limit pacing, errors and the Message Content intent
check), parsing messages and threads and rendering their text, and
``DiscordChannel``'s listing and guards. And end to end, a pipeline run by the
engine over a fake server: a rerun redoes only the items that changed, and
errors delete nothing.

HTTP is faked with ``httpx.MockTransport``; the connector's real request,
paging and parsing code runs against it.

Live tests run against a real server and are skipped unless a bot token is
set.
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
import ssl
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import certifi
import httpx
import pytest

import cocoindex
from cocoindex._internal.memo_fingerprint import memo_fingerprint
from cocoindex.connectors import discord
from cocoindex.connectors.discord import _source
from cocoindex.connectors.discord._source import (
    _DiscordError,
    _error_hint,
    _parse_message,
)
from cocoindex.resources.rate_limit import RateLimiter

# IDs of different lengths, so that comparing them as strings would show.
_GUILD = "1001"
_OTHER_GUILD = "901"
_CHANNEL = "1002"
_OTHER_CHANNEL = "902"
_BOT_ID = "1003"
_PERSON_ID = "1004"

# The Message Content intent as the Developer Portal's toggle grants it.
_INTENT_ON = 1 << 19

_ROUTE_CHANNEL = "/channels/{id}"

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


class _FakeDiscord:
    """An in-memory Discord server. It answers the connector's routes from
    its state as Discord does, with 404 for an unknown channel or server.

    Knobs: ``fail[path]`` answers ``(status, code)``; ``script[path]`` answers
    the next requests to ``path`` in order (an exception is raised, as a
    network error), then normal handling resumes; ``rate_limit_headers[path]``
    are added to every answer for ``path`` but scripted ones. ``requests``
    logs every request as ``(method, path, params)``.
    """

    def __init__(self) -> None:
        # The application's flags, which grant the Message Content intent.
        self.flags = _INTENT_ON
        self.guild_ids = {_GUILD}
        self.channels: dict[str, dict[str, Any]] = {}
        self.fail: dict[str, tuple[int, int]] = {}
        self.script: dict[str, list[httpx.Response | httpx.HTTPError]] = {}
        self.rate_limit_headers: dict[str, dict[str, str]] = {}
        self.requests: list[tuple[str, str, dict[str, str]]] = []

    def add_channel(self, raw_channel: dict[str, Any]) -> None:
        self.channels[raw_channel["id"]] = raw_channel
        self.guild_ids.add(raw_channel["guild_id"])

    def count(self, path: str) -> int:
        return sum(1 for _, logged, _ in self.requests if logged == path)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        # Yield, as a real request would, so concurrent callers interleave.
        await _real_sleep(0)
        path = request.url.path.removeprefix("/api/v10")
        self.requests.append((request.method, path, dict(request.url.params)))
        if queue := self.script.get(path):
            answer = queue.pop(0)
            if isinstance(answer, httpx.HTTPError):
                raise answer
            return answer
        if (failure := self.fail.get(path)) is not None:
            resp = _discord_error(*failure)
        else:
            resp = self._answer(path)
        resp.headers.update(self.rate_limit_headers.get(path, {}))
        return resp

    def _answer(self, path: str) -> httpx.Response:
        if path == "/applications/@me":
            return httpx.Response(200, json={"id": _BOT_ID, "flags": self.flags})
        if m := re.fullmatch(r"/guilds/(\d+)/channels", path):
            if m[1] not in self.guild_ids:
                return _discord_error(404, 10004)
            return httpx.Response(
                200, json=[c for c in self.channels.values() if c["guild_id"] == m[1]]
            )
        if m := re.fullmatch(r"/channels/(\d+)", path):
            raw = self.channels.get(m[1])
            if raw is None:
                return _discord_error(404, 10003)
            return httpx.Response(200, json=raw)
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

    async def test_aclose(self) -> None:
        fake = _fake_with_channels()
        fake.rate_limit_headers[f"/channels/{_CHANNEL}"] = _bucket_headers(
            remaining=4, reset_after=1.0
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
        app = discord.DiscordApp(token="secret-token", ca_bundle_path="/etc/ca.pem")
        assert "secret-token" not in repr(app)
        assert "/etc/ca.pem" in repr(app)


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
                f"{cocoindex.__version__})"
            ),
        }
        assert captured["timeout"] == httpx.Timeout(60.0, connect=30.0)

    def test_ca_bundle_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured = self._capture_client(monkeypatch)
        _source._ApiContext(
            discord.DiscordApp(token="t", ca_bundle_path=certifi.where())
        )
        with_bundle = captured["verify"]
        _source._ApiContext(discord.DiscordApp(token="t"))
        assert isinstance(with_bundle, ssl.SSLContext)
        assert captured["verify"] is True


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

        ctx = await _attach(app, fake)
        async with app:
            await ctx.get(_ROUTE_CHANNEL, _CHANNEL)
        assert len(fake.requests) == 2

    async def test_non_json_body(self) -> None:
        fake = _fake_with_channels()
        path = f"/channels/{_CHANNEL}"
        fake.script[path] = [
            httpx.Response(
                403,
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
                match=r"HTTP 403, and the body isn't JSON: '<!DOCTYPE html><html>",
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
                    await ctx.check_message_content_intent()
            assert fake.count("/applications/@me") == 2
            fake.flags = _INTENT_ON
            for _ in range(2):
                await ctx.check_message_content_intent()
        assert fake.count("/applications/@me") == 3

    async def test_concurrent_first_checks_send_one(self) -> None:
        fake = _FakeDiscord()
        app, ctx = await _setup(fake)
        async with app:
            await asyncio.gather(
                *(ctx.check_message_content_intent() for _ in range(5))
            )
        assert fake.count("/applications/@me") == 1


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
