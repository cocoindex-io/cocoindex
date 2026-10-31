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
import re
import ssl
from typing import Any, cast

import certifi
import httpx
import pytest

import cocoindex
from cocoindex.connectors import discord
from cocoindex.connectors.discord import _source
from cocoindex.connectors.discord._source import _DiscordError, _error_hint
from cocoindex.resources.rate_limit import RateLimiter

# IDs of different lengths, so that comparing them as strings would show.
_GUILD = "1001"
_CHANNEL = "1002"
_OTHER_CHANNEL = "902"

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


# Discord's message for each error the fake answers.
_ERROR_MESSAGES = {
    (401, 0): "401: Unauthorized",
    (404, 0): "404: Not Found",
    (404, 10003): "Unknown Channel",
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
    its state as Discord does, with 404 for an unknown channel.

    Knobs: ``fail[path]`` answers ``(status, code)``; ``script[path]`` answers
    the next requests to ``path`` in order (an exception is raised, as a
    network error), then normal handling resumes; ``rate_limit_headers[path]``
    are added to every answer for ``path`` but scripted ones. ``requests``
    logs every request as ``(method, path, params)``.
    """

    def __init__(self) -> None:
        self.channels: dict[str, dict[str, Any]] = {}
        self.fail: dict[str, tuple[int, int]] = {}
        self.script: dict[str, list[httpx.Response | httpx.HTTPError]] = {}
        self.rate_limit_headers: dict[str, dict[str, str]] = {}
        self.requests: list[tuple[str, str, dict[str, str]]] = []

    def add_channel(self, raw_channel: dict[str, Any]) -> None:
        self.channels[raw_channel["id"]] = raw_channel

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
