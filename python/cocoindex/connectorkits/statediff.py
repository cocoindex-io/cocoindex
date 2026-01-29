"""Utilities for computing tracking record diffs for connector outputs.

This module provides small, opinionated helpers that decide what write action to
take ("insert", "upsert", "replace", "delete") by comparing:

These inputs are bundled in `StateTransition[T]`:

- **desired**: the tracking record we want to exist now
- **prev**: previously observed tracking record(s) for the same identity
- **prev_may_be_missing**: whether `prev` is potentially missing

The distinction between "replace" and "upsert" is:

- **replace**: observed tracking record differs from desired, so we must overwrite to make
  it match.
- **upsert**: observed tracking record matches desired (or no prev), but prev might be
  missing so we still write to ensure eventual convergence.
"""

from __future__ import annotations

__all__ = [
    "CompositeTrackingRecord",
    "DiffAction",
    "ManagedBy",
    "MutualTrackingRecord",
    "TrackingRecordTransition",
    "diff",
    "diff_composite",
    "resolve_system_transition",
]

import dataclasses as _dataclasses
from typing import (
    Collection as _Collection,
    Generic as _Generic,
    Hashable as _Hashable,
    Literal as _Literal,
    NamedTuple as _NamedTuple,
)
from typing_extensions import TypeVar as _TypeVar

import cocoindex as _coco

_TrackingRecordT = _TypeVar("_TrackingRecordT")
_MainTrackingRecordT = _TypeVar("_MainTrackingRecordT")
_SubKeyT = _TypeVar("_SubKeyT", bound=_Hashable)
_SubTrackingRecordT = _TypeVar("_SubTrackingRecordT")

DiffAction = _Literal["insert", "upsert", "replace", "delete"]


class CompositeTrackingRecord(
    _Generic[_MainTrackingRecordT, _SubKeyT, _SubTrackingRecordT], _NamedTuple
):
    """A state with a main component and a set of keyed sub-states.

    This is useful when a single identity produces:

    - a **main** record (single state), and
    - multiple **sub** records keyed by some hashable `SubKeyT`.

    `diff_composite()` computes the main action plus grouped sub-state diffs.
    """

    main: _MainTrackingRecordT
    sub: dict[_SubKeyT, _SubTrackingRecordT]


@_dataclasses.dataclass(slots=True)
class _GroupedStates(_Generic[_SubKeyT, _SubTrackingRecordT]):
    """Internal mutable accumulator used by `diff_composite()`."""

    desired: _SubTrackingRecordT | _coco.NonExistenceType = _dataclasses.field(
        default=_coco.NON_EXISTENCE
    )
    prev: list[_SubTrackingRecordT] = _dataclasses.field(default_factory=list)


class TrackingRecordTransition(_Generic[_TrackingRecordT], _NamedTuple):
    """A bundle of desired vs previously observed state, with completeness info.

    `diff()` takes a `TrackingRecordTransition[T]` and returns the action needed to make
    state converge to `desired` (given the observed `prev` and whether it might
    be incomplete).
    """

    desired: _TrackingRecordT | _coco.NonExistenceType
    prev: _Collection[_TrackingRecordT]
    prev_may_be_missing: bool


ManagedBy = _Literal["system", "user"]


class MutualTrackingRecord(_Generic[_TrackingRecordT], _NamedTuple):
    """A tracking record tagged with ownership/management information.

    This is useful when a resource can be managed by either the system
    (CocoIndex-controlled) or the user (externally controlled).
    """

    tracking_record: _TrackingRecordT
    managed_by: ManagedBy


def resolve_system_transition(
    t: TrackingRecordTransition[MutualTrackingRecord[_TrackingRecordT]], /
) -> TrackingRecordTransition[_TrackingRecordT] | None:
    """Resolve a transition to the system-managed subset, or return None.

    Rules:
        - If desired is user-managed: return None.
        - If desired is NON_EXISTENCE and (no prev, or any prev is user-managed): return None.
        - Otherwise: return a `StateTransition[TrackingRecordT]` where:
          - desired is `desired.state` (or NON_EXISTENCE)
          - prev keeps only system-managed states
          - prev_may_be_missing is preserved from input
    """

    if not _coco.is_non_existence(t.desired) and t.desired.managed_by == "user":
        return None

    if _coco.is_non_existence(t.desired):
        if len(t.prev) == 0:
            return None
        if any(p.managed_by == "user" for p in t.prev):
            return None
        return TrackingRecordTransition(
            desired=_coco.NON_EXISTENCE,
            prev=[p.tracking_record for p in t.prev if p.managed_by == "system"],
            prev_may_be_missing=t.prev_may_be_missing,
        )

    return TrackingRecordTransition(
        desired=t.desired.tracking_record,
        prev=[p.tracking_record for p in t.prev if p.managed_by == "system"],
        prev_may_be_missing=t.prev_may_be_missing,
    )


def diff(t: TrackingRecordTransition[_TrackingRecordT] | None, /) -> DiffAction | None:
    """Determine the write action needed to make state converge.

    Args:
        t: The desired/previous state bundle. Use `coco.NON_EXISTENCE` for
            `t.desired` to indicate the state should not exist.

            If `t.prev_may_be_missing` is true, `t.prev` may be incomplete; in
            that case we may return "insert"/"upsert" even when `t.desired`
            matches known values, to ensure eventual convergence.

    Returns:
        One of:
        - "delete": `t.desired` is NON_EXISTENCE and we observed any previous state
        - "replace": at least one observed previous state differs from `t.desired`
        - "insert": no prev observed, prev may be missing, and `t.desired` exists
        - "upsert": `t.desired` matches observed prev, but prev may be missing
        - None: no action needed (already converged and prev not missing)
    """

    if t is None:
        return None

    if _coco.is_non_existence(t.desired):
        if len(t.prev) == 0:
            return None
        return "delete"

    if any(p != t.desired for p in t.prev):
        return "replace"

    if not t.prev_may_be_missing:
        return None

    if len(t.prev) == 0:
        return "insert"

    return "upsert"


def diff_composite(
    t: TrackingRecordTransition[
        CompositeTrackingRecord[_TrackingRecordT, _SubKeyT, _SubTrackingRecordT]
    ]
    | None,
    /,
) -> tuple[
    DiffAction | None,
    dict[_SubKeyT, TrackingRecordTransition[_SubTrackingRecordT]],
]:
    """Compute a diff for a composite state and group sub-state transitions.

    Args:
        t: A `StateTransition` whose desired/prev values are `CompositeState`.

    Returns:
        A pair of:
        - the main diff action (via `diff()` on the `.main` field), and
        - a mapping from each observed or desired `sub_key` to a
          `StateTransition[SubStateT]` for that key.

    Notes:
        If the main action is "replace" or "delete", we treat sub-state
        observations as potentially missing because a main-level rewrite can
        imply sub-state churn.
    """

    if t is None:
        return (None, {})

    if _coco.is_non_existence(t.desired):
        if len(t.prev) == 0:
            return (None, {})
        return ("delete", {})

    main_action = diff(
        TrackingRecordTransition(
            t.desired.main, [p.main for p in t.prev], t.prev_may_be_missing
        )
    )

    sub_prev_may_be_missing = t.prev_may_be_missing or (
        main_action is not None and main_action in ("replace", "delete")
    )

    grouped_states: dict[_SubKeyT, _GroupedStates[_SubKeyT, _SubTrackingRecordT]] = {}
    for p in t.prev:
        for sub_key, sub_state in p.sub.items():
            grouped_states.setdefault(sub_key, _GroupedStates()).prev.append(sub_state)
    for sub_key, desired_state in t.desired.sub.items():
        grouped_states.setdefault(sub_key, _GroupedStates()).desired = desired_state

    groups = {
        k: TrackingRecordTransition(
            desired=grouped_state.desired,
            prev=grouped_state.prev,
            prev_may_be_missing=(
                sub_prev_may_be_missing or len(grouped_state.prev) < len(t.prev)
            ),
        )
        for k, grouped_state in grouped_states.items()
    }
    return (main_action, groups)
