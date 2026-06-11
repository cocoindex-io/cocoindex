"""Shared helpers for CocoIndex connector implementations."""

from __future__ import annotations

from typing import Any

__all__ = ["SingleWatcherGuard", "default_subpath_name"]


class SingleWatcherGuard:
    """Enforces the single-active-subscriber contract for a ``watch()`` feed.

    The live-source protocols consumed by ``mount_each`` — ``coco.LiveStream`` /
    ``coco.LiveMapFeed`` / ``coco.LiveMapView`` — are **single-subscriber**: a
    ``watch()`` typically owns an exclusive underlying resource (a broker consumer
    subscription, an OS file watch, a single in-memory change channel) that cannot
    be safely fanned out to two concurrent callers — e.g. two subscriptions would
    race one consumer's offset commits. Fan-out to multiple subscribers, if ever
    needed, belongs in a layer above the feed, not in the feed itself.

    A feed holds one guard (constructed in ``__init__``) and wraps its ``watch()``
    body with it, so a second concurrent ``watch()`` raises ``RuntimeError``
    immediately instead of silently corrupting the first::

        class MyStream:
            def __init__(self, ...):
                self._watch_guard = SingleWatcherGuard("MyStream")

            async def watch(self, subscriber):
                with self._watch_guard:
                    ...  # body

    The flag resets on every exit — normal return, exception, or cancellation (a
    cancelled ``await`` inside the ``with`` unwinds through ``__exit__``) — so the
    feed can be re-watched sequentially after a prior ``watch()`` finishes.

    A plain flag (no lock) suffices when ``watch()`` runs entirely on one event
    loop, as the framework's live consumer does. A feed that already carries
    equivalent "is being watched" state can guard on that instead.
    """

    __slots__ = ("_label", "_active")

    def __init__(self, label: str) -> None:
        self._label = label
        self._active = False

    def __enter__(self) -> None:
        if self._active:
            raise RuntimeError(
                f"{self._label} supports a single active watch() at a time."
            )
        self._active = True

    def __exit__(self, *exc: object) -> None:
        self._active = False


def default_subpath_name(processor_fn: Any) -> str | None:
    """Resolve the default subpath name for a mount target.

    Honors an explicit ``__coco_subpath_name__`` attribute (set by wrappers
    like ``coco.auto_refresh`` so the wrapper class can keep an honest
    ``__name__`` while still mounting under the wrapped function's name),
    falling back to ``__name__``.
    """
    name = getattr(processor_fn, "__coco_subpath_name__", None)
    if isinstance(name, str):
        return name
    name = getattr(processor_fn, "__name__", None)
    if isinstance(name, str):
        return name
    return None
