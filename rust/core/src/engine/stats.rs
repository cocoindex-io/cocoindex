use std::fmt::Write;
use std::future::Future;
use std::time::Duration;

use crate::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::watch;

const BAR_WIDTH: u64 = 40;
const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// Sentinel version sent on the watch channel when processing is fully terminated
/// (ready + all descendants done). Consumers check this to exit their watch loop.
pub const TERMINATED_VERSION: u64 = u64::MAX;

#[derive(Default, Clone)]
pub struct ProcessingStatsGroup {
    pub num_execution_starts: u64,
    pub num_unchanged: u64,
    pub num_adds: u64,
    pub num_deletes: u64,
    pub num_reprocesses: u64,
    pub num_errors: u64,
}

impl ProcessingStatsGroup {
    /// Number of successfully processed items (excludes errors).
    pub fn num_processed(&self) -> u64 {
        self.num_unchanged + self.num_adds + self.num_deletes + self.num_reprocesses
    }

    /// Number of items that have finished (including errors).
    pub fn num_finished(&self) -> u64 {
        self.num_processed() + self.num_errors
    }

    pub fn num_in_progress(&self) -> u64 {
        self.num_execution_starts
            .saturating_sub(self.num_finished())
    }

    pub fn has_errors(&self) -> bool {
        self.num_errors > 0
    }
}

impl std::fmt::Display for ProcessingStatsGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let processed = self.num_processed();
        let total = self.num_execution_starts;

        if total == 0 {
            return write!(f, "No activity");
        }

        // Progress bar shows processed (successful) items, not including errors
        let bar_filled = if total > 0 {
            (processed * BAR_WIDTH) / total
        } else {
            0
        };

        write!(f, "▕")?;
        for _ in 0..bar_filled {
            write!(f, "█")?;
        }
        for _ in bar_filled..BAR_WIDTH {
            write!(f, " ")?;
        }
        // Show processed/total (errors are not counted as processed)
        write!(f, "▏{processed}/{total}")?;

        let finished = self.num_finished();
        if finished > 0 {
            let mut delimiter = ':';
            if self.num_adds > 0 {
                write!(f, "{delimiter} {} added", self.num_adds)?;
                delimiter = ',';
            }
            if self.num_reprocesses > 0 {
                write!(f, "{delimiter} {} reprocessed", self.num_reprocesses)?;
                delimiter = ',';
            }
            if self.num_deletes > 0 {
                write!(f, "{delimiter} {} deleted", self.num_deletes)?;
                delimiter = ',';
            }
            if self.num_unchanged > 0 {
                write!(f, "{delimiter} {} unchanged", self.num_unchanged)?;
                delimiter = ',';
            }
            if self.num_errors > 0 {
                write!(f, "{delimiter} {} errors", self.num_errors)?;
            }
        }

        Ok(())
    }
}

/// A versioned snapshot of processing stats, combining the stats map with a version counter.
#[derive(Default, Clone)]
pub struct VersionedProcessingStats {
    pub stats: IndexMap<String, ProcessingStatsGroup>,
    pub version: u64,
    /// True once the root processing component is ready (initial processing caught up).
    /// Stats may continue to update after this (live components).
    pub ready: bool,
}

/// Thread-safe container for processing stats with version tracking and change notification.
#[derive(Clone)]
pub struct ProcessingStats {
    inner: Arc<Mutex<VersionedProcessingStats>>,
    version_tx: watch::Sender<u64>,
    version_rx: watch::Receiver<u64>,
}

impl Default for ProcessingStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessingStats {
    pub fn new() -> Self {
        let (version_tx, version_rx) = watch::channel(0u64);
        Self {
            inner: Arc::new(Mutex::new(VersionedProcessingStats::default())),
            version_tx,
            version_rx,
        }
    }

    pub fn update(&self, operation_name: &str, mutator: impl FnOnce(&mut ProcessingStatsGroup)) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(group) = guard.stats.get_mut(operation_name) {
            mutator(group);
        } else {
            let mut group = ProcessingStatsGroup::default();
            mutator(&mut group);
            guard.stats.insert(operation_name.to_string(), group);
        }
        guard.version += 1;
        let version = guard.version;
        drop(guard);
        let _ = self.version_tx.send(version);
    }

    /// Returns an atomic snapshot of (version, stats).
    pub fn snapshot(&self) -> VersionedProcessingStats {
        self.inner.lock().unwrap().clone()
    }

    /// Signal that the root processing component is ready (initial processing caught up).
    /// Stats may continue to update after this (live components).
    pub fn notify_ready(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.ready = true;
        guard.version += 1;
        let version = guard.version;
        drop(guard);
        let _ = self.version_tx.send(version);
    }

    /// Signal that the processing task has fully terminated.
    pub fn notify_terminated(&self) {
        let _ = self.version_tx.send(TERMINATED_VERSION);
    }

    /// Subscribe to version change notifications.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.version_rx.clone()
    }

    pub fn format_stats(&self, start_time: Option<std::time::Instant>) -> String {
        let guard = self.inner.lock().unwrap();
        let mut result = String::new();
        for (name, group) in guard.stats.iter() {
            if !result.is_empty() {
                result.push('\n');
            }
            write!(&mut result, "{name}: {group}").expect("write to string should not fail");
        }
        if let Some(start_time) = start_time {
            if !result.is_empty() {
                write!(
                    &mut result,
                    " [elapsed: {:.1}s]",
                    start_time.elapsed().as_secs_f64()
                )
                .expect("write to string should not fail");
            }
        }
        result
    }
}

impl std::fmt::Display for ProcessingStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_stats(None))
    }
}

/// Progress reporter that periodically reports processing stats to stdout using indicatif.
/// This does not spawn a separate task - instead, use `run_with_progress` to wrap a future.
pub struct ProgressReporter {
    multi_progress: MultiProgress,
    progress_bars: Mutex<IndexMap<String, ProgressBar>>,
    elapsed_bar: ProgressBar,
    stats: ProcessingStats,
    start_time: std::time::Instant,
}

impl ProgressReporter {
    /// Create a new progress reporter for the given stats.
    pub fn new(stats: ProcessingStats) -> Self {
        let multi_progress = MultiProgress::new();

        // Create the elapsed time progress bar
        let elapsed_style = ProgressStyle::default_spinner()
            .template("{msg}")
            .expect("invalid progress style template");
        let elapsed_bar = ProgressBar::new_spinner();
        elapsed_bar.set_style(elapsed_style);
        let elapsed_bar = multi_progress.add(elapsed_bar);

        Self {
            multi_progress,
            progress_bars: Mutex::new(IndexMap::new()),
            elapsed_bar,
            stats,
            start_time: std::time::Instant::now(),
        }
    }

    /// Run a future while periodically reporting progress.
    /// Progress is reported inline using `tokio::select!`, no separate task is spawned.
    pub async fn run_with_progress<T>(&self, fut: impl Future<Output = T>) -> T {
        let mut pinned_fut = Box::pin(fut);
        let mut interval = tokio::time::interval(PROGRESS_REPORT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Skip the first immediate tick
        let mut first_tick = true;

        loop {
            tokio::select! {
                result = &mut pinned_fut => {
                    self.print_final_stats();
                    return result;
                }
                _ = interval.tick() => {
                    if first_tick {
                        first_tick = false;
                        continue;
                    }
                    self.update_progress_bars();
                }
            }
        }
    }

    fn update_progress_bars(&self) {
        let versioned = self.stats.snapshot();
        let mut progress_bars = self.progress_bars.lock().unwrap();

        for (name, group) in versioned.stats.iter() {
            // Check if this processor is complete (all started items have finished)
            let is_complete = group.num_execution_starts > 0 && group.num_in_progress() == 0;

            let pb = progress_bars.entry(name.clone()).or_insert_with(|| {
                let pb = ProgressBar::new_spinner();
                // Insert before the elapsed bar so elapsed is always last
                self.multi_progress.insert_before(&self.elapsed_bar, pb)
            });

            if is_complete {
                // Static style without spinner, with completion icon
                let style = ProgressStyle::default_spinner()
                    .template("{msg}")
                    .expect("invalid progress style template");
                pb.set_style(style);
                let icon = if group.has_errors() { "⚠️" } else { "✅" };
                pb.set_message(format!("{icon} {name}: {group}"));
            } else {
                // Spinner style for in-progress
                let style = ProgressStyle::default_spinner()
                    .template("{spinner:.green}{spinner:.green} {msg}")
                    .expect("invalid progress style template");
                pb.set_style(style);
                pb.set_message(format!("{name}: {group}"));
                pb.tick();
            }
        }

        // Update elapsed time bar
        self.elapsed_bar.set_message(self.format_elapsed_message());
    }

    fn format_elapsed_message(&self) -> String {
        format!(
            "⏳ Elapsed: {:.1}s",
            self.start_time.elapsed().as_secs_f64()
        )
    }

    fn print_final_stats(&self) {
        let versioned = self.stats.snapshot();
        let progress_bars = self.progress_bars.lock().unwrap();

        // Clear all progress bars
        for pb in progress_bars.values() {
            pb.finish_and_clear();
        }
        self.elapsed_bar.finish_and_clear();

        // Print final stats
        self.multi_progress.suspend(|| {
            for (name, group) in versioned.stats.iter() {
                let icon = if group.has_errors() { "⚠️" } else { "✅" };
                println!("{icon} {name}: {group}");
            }
            println!("{}", self.format_elapsed_message());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_increments_on_update() {
        let stats = ProcessingStats::new();
        assert_eq!(stats.snapshot().version, 0);

        stats.update("proc_a", |g| g.num_adds += 1);
        assert_eq!(stats.snapshot().version, 1);

        stats.update("proc_a", |g| g.num_adds += 1);
        stats.update("proc_b", |g| g.num_unchanged += 1);
        let snap = stats.snapshot();
        assert_eq!(snap.version, 3);
        assert_eq!(snap.stats["proc_a"].num_adds, 2);
        assert_eq!(snap.stats["proc_b"].num_unchanged, 1);
    }

    #[test]
    fn test_snapshot_version_and_stats_consistent() {
        let stats = ProcessingStats::new();
        stats.update("a", |g| g.num_adds += 1);
        let snap1 = stats.snapshot();
        assert_eq!(snap1.version, 1);
        assert_eq!(snap1.stats.len(), 1);

        stats.update("b", |g| g.num_deletes += 1);
        let snap2 = stats.snapshot();
        assert_eq!(snap2.version, 2);
        assert_eq!(snap2.stats.len(), 2);

        // snap1 is still the old snapshot
        assert_eq!(snap1.version, 1);
        assert_eq!(snap1.stats.len(), 1);
    }

    #[tokio::test]
    async fn test_watch_receives_version_notifications() {
        let stats = ProcessingStats::new();
        let mut rx = stats.subscribe();

        stats.update("proc", |g| g.num_adds += 1);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 1);

        stats.update("proc", |g| g.num_adds += 1);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 2);
    }

    #[tokio::test]
    async fn test_notify_ready_sets_ready_flag() {
        let stats = ProcessingStats::new();
        let mut rx = stats.subscribe();

        stats.update("proc", |g| g.num_adds += 1);
        rx.changed().await.unwrap();
        assert!(!stats.snapshot().ready);

        stats.notify_ready();
        rx.changed().await.unwrap();
        assert!(stats.snapshot().ready);
        // Version is a normal value, not the sentinel
        assert_ne!(*rx.borrow(), TERMINATED_VERSION);
    }

    #[tokio::test]
    async fn test_notify_terminated_sends_max_version() {
        let stats = ProcessingStats::new();
        let mut rx = stats.subscribe();

        stats.notify_terminated();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), TERMINATED_VERSION);
    }

    #[tokio::test]
    async fn test_subscribe_multiple_receivers() {
        let stats = ProcessingStats::new();
        let mut rx1 = stats.subscribe();
        let mut rx2 = stats.subscribe();

        stats.update("proc", |g| g.num_adds += 1);

        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();
        assert_eq!(*rx1.borrow(), 1);
        assert_eq!(*rx2.borrow(), 1);
    }
}
