from __future__ import annotations

import enum as _enum
from collections.abc import AsyncIterator, Coroutine
from datetime import timedelta
from typing import Any, Generic, NamedTuple, Protocol, TypeVar

R = TypeVar("R")

_TERMINATED_VERSION = 2**64 - 1  # u64::MAX


def _resolve_report_to_stdout(
    report_to_stdout: bool | timedelta,
) -> tuple[bool, float | None]:
    """Normalize a ``bool | timedelta`` progress flag into
    ``(enabled, refresh_interval_secs)`` for the core boundary.

    - ``False`` → ``(False, None)`` (no report)
    - ``True`` → ``(True, None)`` (report at the default interval)
    - ``timedelta`` → ``(True, secs)`` (report at that interval; must be positive)
    """
    if isinstance(report_to_stdout, timedelta):
        secs = report_to_stdout.total_seconds()
        if secs <= 0:
            raise ValueError("report_to_stdout interval must be a positive duration")
        return True, secs
    return bool(report_to_stdout), None


class _CoreStatsHandle(Protocol):
    """Structural interface shared by the core update / drop / stats-group
    handles — everything `_StatsView` needs to read progress."""

    def stats_snapshot(self) -> tuple[int, bool, dict[str, dict[str, int]]]: ...
    def changed(self) -> Coroutine[Any, Any, int]: ...


H = TypeVar("H", bound=_CoreStatsHandle)


def _decode_update_stats(raw: dict[str, dict[str, int]]) -> UpdateStats:
    """Decode the raw `{processor: {field: value}}` snapshot from core."""
    return UpdateStats(
        by_component={name: ComponentStats(**group) for name, group in raw.items()}
    )


class UpdateStatus(_enum.StrEnum):
    RUNNING = "running"
    READY = "ready"


class ComponentStats(NamedTuple):
    """Per-processor stats group, mirroring Rust's ProcessingStatsGroup."""

    num_execution_starts: int
    num_unchanged: int
    num_adds: int
    num_deletes: int
    num_reprocesses: int
    num_errors: int

    @property
    def num_processed(self) -> int:
        return (
            self.num_unchanged + self.num_adds + self.num_deletes + self.num_reprocesses
        )

    @property
    def num_finished(self) -> int:
        return self.num_processed + self.num_errors

    @property
    def num_in_progress(self) -> int:
        return max(0, self.num_execution_starts - self.num_finished)


class UpdateStats(NamedTuple):
    """Mirrors Rust's ProcessingStats snapshot."""

    by_component: dict[str, ComponentStats]

    @property
    def total(self) -> ComponentStats:
        """Aggregate stats across all processors."""
        return ComponentStats(
            num_execution_starts=sum(
                s.num_execution_starts for s in self.by_component.values()
            ),
            num_unchanged=sum(s.num_unchanged for s in self.by_component.values()),
            num_adds=sum(s.num_adds for s in self.by_component.values()),
            num_deletes=sum(s.num_deletes for s in self.by_component.values()),
            num_reprocesses=sum(s.num_reprocesses for s in self.by_component.values()),
            num_errors=sum(s.num_errors for s in self.by_component.values()),
        )


class UpdateSnapshot(NamedTuple, Generic[R]):
    stats: UpdateStats
    status: UpdateStatus
    result: R | None


class _StatsView(Generic[H]):
    """Shared ``stats()`` / ``watch()`` over a core handle exposing
    ``stats_snapshot()`` and ``changed()``.

    Yields snapshots without a result and ends at READY — for handles that have
    no return value (e.g. a stats group). ``UpdateHandle`` has its own
    result-bearing ``watch()`` but shares :func:`_decode_update_stats`.
    """

    _core_handle: H

    def stats(self) -> UpdateStats | None:
        """Returns a snapshot of the latest stats, or None if none yet."""
        _version, _ready, raw = self._core_handle.stats_snapshot()
        return _decode_update_stats(raw) if raw else None

    async def watch(self) -> AsyncIterator[UpdateSnapshot[None]]:
        """Yields RUNNING snapshots until ready, then a final READY snapshot."""
        last_version = 0
        while True:
            version = await self._core_handle.changed()

            # Termination is signalled by TERMINATED_VERSION on the watch
            # channel without bumping the stats version, so check it first.
            if version >= _TERMINATED_VERSION:
                _v, _ready, raw = self._core_handle.stats_snapshot()
                if raw:
                    yield UpdateSnapshot(
                        stats=_decode_update_stats(raw),
                        status=UpdateStatus.READY,
                        result=None,
                    )
                return

            snap_version, ready, raw = self._core_handle.stats_snapshot()
            if snap_version == last_version:
                continue  # no actual change since last yield
            last_version = snap_version

            if raw:
                yield UpdateSnapshot(
                    stats=_decode_update_stats(raw),
                    status=UpdateStatus.READY if ready else UpdateStatus.RUNNING,
                    result=None,
                )


class StatsGroupHandle(_StatsView[_CoreStatsHandle]):
    """Read handle for a ``coco.stats_group(...)`` scope.

    Exposes the same ``stats()`` / ``watch()`` surface as ``UpdateHandle`` over
    the group's separately-aggregated stats. No ``result()`` — a group has no
    return value; its lifecycle is bounded by the ``with`` block.
    """

    def __init__(self, core_handle: _CoreStatsHandle) -> None:
        self._core_handle = core_handle
