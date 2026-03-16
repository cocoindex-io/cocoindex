from __future__ import annotations

import enum as _enum
from typing import Generic, NamedTuple, TypeVar

R = TypeVar("R")


class UpdateStatus(_enum.StrEnum):
    RUNNING = "running"
    DONE = "done"


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
