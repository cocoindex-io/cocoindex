//! Pipeline run statistics.

use std::fmt;
use std::time::Duration;

/// Statistics returned by `App::run()`.
///
/// Contains information about the number of items processed, skipped (due to
/// memoization), or written/deleted from the filesystem.
#[derive(Debug, Clone)]
pub struct RunStats {
    /// Number of items explicitly processed by the pipeline logic.
    pub processed: u64,
    /// Number of items skipped due to memoization (cache hit).
    pub skipped: u64,
    /// Number of new or modified files written to the output directory.
    pub written: u64,
    /// Number of stale files deleted from the output directory during sync.
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
