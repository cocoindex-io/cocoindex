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
