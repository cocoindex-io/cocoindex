"""
Memory benchmark for a component declaring many target states.

One processing component declares `BENCH_N` rows — dicts of a few hundred
bytes — against a recording target whose sink only counts and digests what it
is given. Each run is one `update` against `BENCH_DB` and prints a JSON line
with the process's peak RSS, the number of actions applied and sink calls
made, and a digest of the applied actions (independent of the order the sink
got them in), so that runs with and without spilling
(`COCOINDEX_TARGET_STATE_SPILL_THRESHOLD`) can be compared for memory and
checked for identical results. Run it twice against the same `BENCH_DB` for
a cold pass (every row inserted) and a warm one (nothing changed: the
fingerprints kept in memory make the pass skip every row).

Environment knobs:
    BENCH_N         — rows to declare (default 300000).
    BENCH_ROW_BYTES — bytes of text in each row (default 200).
    BENCH_DB        — state store directory (default: a fresh temporary one).
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import resource
import sys
import tempfile
import time
from typing import Any, Collection

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_object

_N: int = int(os.environ.get("BENCH_N", "300000"))
_ROW_BYTES: int = int(os.environ.get("BENCH_ROW_BYTES", "200"))

_Action = tuple[str, Any]


class _RecordingHandler(coco.TargetHandler[dict[str, Any], bytes, None]):
    """Tracks each row by its fingerprint; the sink digests what it applies."""

    tracks_value_fingerprint = True

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_fn(self._apply)
        self.applied = 0
        self.sink_calls = 0
        # Sum of per-action digests: the same whatever order the sink gets
        # the actions in, which spilling changes.
        self.digest = 0

    def _apply(
        self, context_provider: coco.ContextProvider, actions: Collection[_Action], /
    ) -> None:
        self.sink_calls += 1
        for key, value in actions:
            self.applied += 1
            action_digest = hashlib.blake2b(
                repr((key, value)).encode(), digest_size=16
            ).digest()
            self.digest = (self.digest + int.from_bytes(action_digest, "big")) % (
                1 << 128
            )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: dict[str, Any] | coco.NonExistenceType,
        prev_possible_records: Collection[bytes],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, bytes] | None:
        if coco.is_non_existence(desired_state):
            if not prev_possible_records:
                return None
            return coco.TargetReconcileOutput(
                action=(str(key), coco.NON_EXISTENCE),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        record = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == record for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(str(key), desired_state),
            sink=self._sink,
            tracking_record=record,
        )


_handler = _RecordingHandler()
_provider = coco.register_root_target_states_provider(
    "cocoindex/bench/target_state_spill", _handler
)


@coco.fn
def declare_rows() -> None:
    text = "x" * _ROW_BYTES
    for i in range(_N):
        coco.declare_target_state(
            _provider.target_state(
                f"row-{i}",
                {"id": i, "text": text, "tags": ["a", "b", "c"], "score": i * 0.5},
            )
        )


def _peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kibibytes.
    return peak / (1 << 20) if sys.platform == "darwin" else peak / 1024


def main() -> None:
    db_path = pathlib.Path(
        os.environ.get("BENCH_DB")
        or tempfile.mkdtemp(prefix="coco-target-state-spill-")
    )
    env = coco.Environment(coco.Settings.from_env(db_path=db_path))
    app = coco.App(
        coco.AppConfig(name="TargetStateSpillBench", environment=env), declare_rows
    )
    started = time.perf_counter()
    app.update_blocking()
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "n": _N,
                "row_bytes": _ROW_BYTES,
                "spill_threshold": os.environ.get(
                    "COCOINDEX_TARGET_STATE_SPILL_THRESHOLD"
                ),
                "db": str(db_path),
                "seconds": round(elapsed, 2),
                "peak_rss_mib": round(_peak_rss_mib(), 1),
                "applied": _handler.applied,
                "sink_calls": _handler.sink_calls,
                "digest": f"{_handler.digest:032x}",
            }
        )
    )


if __name__ == "__main__":
    main()
