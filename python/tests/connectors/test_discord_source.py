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
import ssl
from typing import Any

import certifi
import httpx
import pytest

import cocoindex
from cocoindex.connectors import discord
from cocoindex.connectors.discord import _source


def test_package_reexports_source() -> None:
    assert discord.__all__ == _source.__all__
    for name in discord.__all__:
        assert getattr(discord, name) is getattr(_source, name)


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
        app = discord.DiscordApp(token="t")
        async with app:
            ctx = await app._get_context()
            assert not ctx._client.is_closed
        assert ctx._client.is_closed
        assert app._state.context is None
        await app.aclose()

        new_ctx = await app._get_context()
        assert new_ctx is not ctx
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
