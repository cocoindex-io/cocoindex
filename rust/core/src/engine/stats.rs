use crate::prelude::*;
use tokio::sync::watch;

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

    fn add(&mut self, other: &ProcessingStatsGroup) {
        self.num_execution_starts += other.num_execution_starts;
        self.num_unchanged += other.num_unchanged;
        self.num_adds += other.num_adds;
        self.num_deletes += other.num_deletes;
        self.num_reprocesses += other.num_reprocesses;
        self.num_errors += other.num_errors;
    }
}

/// Failures kept for the progress summary; the counts in the stats groups
/// cover every failure.
pub const MAX_RECORDED_FAILURES: usize = 5;

/// One failed component run, for the progress summary.
#[derive(Clone, Debug)]
pub struct FailureRecord {
    /// The processor name the failure is counted under in the stats.
    pub processor: String,
    /// The component's path.
    pub component: crate::state::stable_path::StablePath,
    /// The first line of the error.
    pub message: String,
}

/// The message a failure row shows: the first line of the underlying error.
/// Context layers stay out, as an error's `Display` puts them first, under a
/// bare `Context:` line.
pub(crate) fn failure_message(err: &Error) -> String {
    let text = err.without_contexts().to_string();
    let line = text.lines().find(|l| !l.trim().is_empty());
    line.unwrap_or_default().trim().to_string()
}

/// A versioned snapshot of processing stats, combining the stats map with a version counter.
#[derive(Default, Clone)]
pub struct VersionedProcessingStats {
    pub stats: IndexMap<String, ProcessingStatsGroup>,
    pub version: u64,
    /// True once the root processing component is ready (initial processing caught up).
    /// Stats may continue to update after this (live components).
    pub ready: bool,
    /// The first `MAX_RECORDED_FAILURES` failures, in the order they happened.
    pub failures: Vec<FailureRecord>,
    /// What the stats groups under this scope count, at any depth. Their rows
    /// stay out of this scope's report, but its final status covers them.
    pub in_groups: ProcessingStatsGroup,
}

/// Thread-safe container for processing stats with version tracking and change notification.
#[derive(Clone)]
pub struct ProcessingStats {
    inner: Arc<Mutex<VersionedProcessingStats>>,
    version_tx: watch::Sender<u64>,
    version_rx: watch::Receiver<u64>,
    /// Fires exactly once, on termination. Separate from `version_*` so that
    /// terminated-only waiters aren't woken on every stats update.
    terminated_tx: watch::Sender<bool>,
    terminated_rx: watch::Receiver<bool>,
    /// The enclosing stats, for a stats group.
    parent: Option<Arc<ProcessingStats>>,
}

impl Default for ProcessingStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessingStats {
    pub fn new() -> Self {
        let (version_tx, version_rx) = watch::channel(0u64);
        let (terminated_tx, terminated_rx) = watch::channel(false);
        Self {
            inner: Arc::new(Mutex::new(VersionedProcessingStats::default())),
            version_tx,
            version_rx,
            terminated_tx,
            terminated_rx,
            parent: None,
        }
    }

    /// The stats of a group opened under `parent`.
    pub(crate) fn new_in(parent: &ProcessingStats) -> Self {
        Self {
            parent: Some(Arc::new(parent.clone())),
            ..Self::new()
        }
    }

    /// Count what happened: `count` increments a zeroed group, which is added
    /// to this operation's counts and to `in_groups` of every enclosing stats.
    /// Those keep their version: their own rows did not change.
    pub fn update(&self, operation_name: &str, count: impl FnOnce(&mut ProcessingStatsGroup)) {
        let mut delta = ProcessingStatsGroup::default();
        count(&mut delta);
        let mut guard = self.inner.lock().unwrap();
        if let Some(group) = guard.stats.get_mut(operation_name) {
            group.add(&delta);
        } else {
            guard
                .stats
                .insert(operation_name.to_string(), delta.clone());
        }
        guard.version += 1;
        let version = guard.version;
        drop(guard);
        let _ = self.version_tx.send(version);
        let mut parent = self.parent.as_deref();
        while let Some(stats) = parent {
            stats.inner.lock().unwrap().in_groups.add(&delta);
            parent = stats.parent.as_deref();
        }
    }

    /// Keep a failure for the summary, up to `MAX_RECORDED_FAILURES`. Does not
    /// bump the version: the `update` that counts the failure does.
    pub fn record_failure(&self, failure: FailureRecord) {
        let mut guard = self.inner.lock().unwrap();
        if guard.failures.len() < MAX_RECORDED_FAILURES {
            guard.failures.push(failure);
        }
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
        // The version channel carries the sentinel for change-tracking
        // consumers (e.g. the TTY display, which wants every update anyway);
        // the dedicated terminated channel is for terminated-only waiters.
        let _ = self.version_tx.send(TERMINATED_VERSION);
        let _ = self.terminated_tx.send(true);
    }

    /// Resolves only when the operation has terminated. Because the underlying
    /// channel fires exactly once, the waiter isn't woken on intermediate
    /// stats updates (unlike the version channel).
    pub async fn wait_terminated(&self) {
        let mut rx = self.terminated_rx.clone();
        // Err means the sender was dropped, which we also treat as terminated.
        let _ = rx.wait_for(|terminated| *terminated).await;
    }

    /// True once the operation has terminated.
    pub fn is_terminated(&self) -> bool {
        *self.terminated_rx.borrow()
    }

    /// Subscribe to version change notifications.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.version_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_message_is_the_first_line_of_the_underlying_error() {
        let err = Error::internal_msg("  boom\nsecond line").context("while doing x");
        assert_eq!(failure_message(&err), "boom");
    }

    #[test]
    fn test_group_counts_reach_every_enclosing_stats_in_flight_and_failed() {
        let root = ProcessingStats::new();
        let group = ProcessingStats::new_in(&root);
        let nested = ProcessingStats::new_in(&group);
        nested.update("p", |g| {
            g.num_execution_starts += 2;
            g.num_errors += 1;
        });

        assert_eq!(nested.snapshot().stats["p"].num_errors, 1);
        assert_eq!(nested.snapshot().in_groups.num_errors, 0);
        for stats in [&group, &root] {
            let snapshot = stats.snapshot();
            assert!(snapshot.stats.is_empty(), "the rows stay split out");
            assert_eq!(snapshot.in_groups.num_errors, 1);
            assert_eq!(snapshot.in_groups.num_in_progress(), 1);
        }
        assert_eq!(root.snapshot().version, 0, "its own rows did not change");
    }

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
