//! Pipeline run statistics.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Statistics returned by `App::run()`.
///
/// `processed` is the **total** number of target states handled this run; it is
/// the sum of the three disjoint outcome buckets `written + skipped + deleted`
/// (so `processed == written + skipped + deleted` always holds).
#[derive(Debug, Clone)]
pub struct RunStats {
    /// Total target states handled this run (= `written + skipped + deleted`).
    pub processed: u64,
    /// Of those, the count left unchanged due to memoization / no-change tracking.
    pub skipped: u64,
    /// Of those, the count created or updated (inserts + reprocesses).
    pub written: u64,
    /// Of those, the count deleted during reconciliation (orphaned states).
    pub deleted: u64,
    /// Total elapsed time of the pipeline execution.
    pub elapsed: Duration,
}

impl Default for RunStats {
    fn default() -> Self {
        Self {
            processed: 0,
            skipped: 0,
            written: 0,
            deleted: 0,
            elapsed: Duration::ZERO,
        }
    }
}

impl fmt::Display for RunStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "processed {}, wrote {}, skipped {}, deleted {} in {:.1}s",
            self.processed,
            self.written,
            self.skipped,
            self.deleted,
            self.elapsed.as_secs_f64()
        )
    }
}

/// Per-component processing statistics — the engine's per-operation
/// `ProcessingStatsGroup`, mirroring Python's `cocoindex.ComponentStats`.
///
/// Unlike the aggregate [`RunStats`], this keeps the outcome buckets distinct
/// (reprocesses separate from adds) and additionally tracks executions started,
/// in-flight executions, and errors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComponentStats {
    /// Times a processor for this component started executing.
    pub num_execution_starts: u64,
    /// Items found unchanged (memoized / no-change skip).
    pub num_unchanged: u64,
    /// Target states created.
    pub num_adds: u64,
    /// Target states deleted during reconciliation.
    pub num_deletes: u64,
    /// Target states re-processed because their input changed.
    pub num_reprocesses: u64,
    /// Executions that ended in an error.
    pub num_errors: u64,
}

impl ComponentStats {
    /// Successfully processed items (excludes errors):
    /// `unchanged + adds + deletes + reprocesses`.
    pub fn num_processed(&self) -> u64 {
        self.num_unchanged + self.num_adds + self.num_deletes + self.num_reprocesses
    }

    /// Items that have finished, including errors (`num_processed + num_errors`).
    pub fn num_finished(&self) -> u64 {
        self.num_processed() + self.num_errors
    }

    /// Executions started but not yet finished.
    pub fn num_in_progress(&self) -> u64 {
        self.num_execution_starts
            .saturating_sub(self.num_finished())
    }

    /// Whether any execution for this component errored.
    pub fn has_errors(&self) -> bool {
        self.num_errors > 0
    }
}

/// Whether an update has caught up its initial processing — mirrors Python's
/// `cocoindex.UpdateStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    /// Initial catch-up processing is still running.
    Running,
    /// Initial processing is caught up. Live components may still update stats.
    Ready,
}

/// A detailed, per-component snapshot of update statistics — mirrors Python's
/// `cocoindex.UpdateStats`. The aggregate [`RunStats`] is essentially
/// [`UpdateStats::total`] collapsed into four buckets; this retains the
/// per-component breakdown plus error and in-flight counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateStats {
    /// Stats keyed by component/operation name (sorted by name).
    pub by_component: BTreeMap<String, ComponentStats>,
    /// Whether initial processing has caught up.
    pub status: UpdateStatus,
}

impl Default for UpdateStatus {
    fn default() -> Self {
        UpdateStatus::Running
    }
}

impl UpdateStats {
    /// The coarse [`RunStats`] aggregate derived from *this* snapshot.
    ///
    /// Prefer this over a separate `stats_snapshot()` call when you need both the
    /// detailed and coarse views together: each handle snapshot is taken
    /// independently, so on a live pipeline two separate calls can observe
    /// different engine versions. Deriving from one [`UpdateStats`] keeps them
    /// consistent. `elapsed` is zero here (it is only set by `App::run`).
    pub fn run_stats(&self) -> RunStats {
        let total = self.total();
        RunStats {
            processed: total.num_processed(),
            skipped: total.num_unchanged,
            written: total.num_adds + total.num_reprocesses,
            deleted: total.num_deletes,
            elapsed: Duration::ZERO,
        }
    }

    /// Aggregate stats summed across every component.
    pub fn total(&self) -> ComponentStats {
        let mut total = ComponentStats::default();
        for s in self.by_component.values() {
            total.num_execution_starts += s.num_execution_starts;
            total.num_unchanged += s.num_unchanged;
            total.num_adds += s.num_adds;
            total.num_deletes += s.num_deletes;
            total.num_reprocesses += s.num_reprocesses;
            total.num_errors += s.num_errors;
        }
        total
    }
}
