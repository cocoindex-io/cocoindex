//! Pipeline run statistics.

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
