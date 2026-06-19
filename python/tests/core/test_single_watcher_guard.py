"""Unit tests for :class:`SingleWatcherGuard`.

The guard enforces the single-active-subscriber contract shared by the
``LiveStream`` / ``LiveMapFeed`` / ``LiveMapView`` ``watch()`` implementations.
"""

from __future__ import annotations

import pytest

from cocoindex.connectorkits import SingleWatcherGuard


def test_reentry_raises() -> None:
    """A second concurrent acquisition raises with the labeled message."""
    guard = SingleWatcherGuard("Feed")
    with guard:
        with pytest.raises(
            RuntimeError, match="Feed supports a single active watch\\(\\) at a time."
        ):
            with guard:
                pass


def test_resets_after_normal_exit() -> None:
    """The flag clears on normal exit, so sequential re-watch is allowed."""
    guard = SingleWatcherGuard("Feed")
    with guard:
        pass
    with guard:  # would raise if the flag had not been reset
        pass


def test_resets_after_exception() -> None:
    """The flag clears even when the guarded body raises."""
    guard = SingleWatcherGuard("Feed")
    with pytest.raises(ValueError):
        with guard:
            raise ValueError("boom")
    with guard:  # re-entry works because __exit__ ran despite the exception
        pass
