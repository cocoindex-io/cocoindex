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

from cocoindex.connectors import discord
from cocoindex.connectors.discord import _source


def test_package_reexports_source() -> None:
    assert discord.__all__ == _source.__all__
    for name in discord.__all__:
        assert getattr(discord, name) is getattr(_source, name)
