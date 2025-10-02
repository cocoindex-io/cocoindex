use crate::prelude::*;

use std::{
    ops::AddAssign,
    sync::atomic::{AtomicI64, Ordering::Relaxed},
};

#[derive(Default, Serialize)]
pub struct Counter(pub AtomicI64);

impl Counter {
    pub fn inc(&self, by: i64) {
        self.0.fetch_add(by, Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Relaxed)
    }

    pub fn delta(&self, base: &Self) -> Counter {
        Counter(AtomicI64::new(self.get() - base.get()))
    }

    pub fn into_inner(self) -> i64 {
        self.0.into_inner()
    }

    pub fn merge(&self, delta: &Self) {
        self.0.fetch_add(delta.get(), Relaxed);
    }
}

impl AddAssign for Counter {
    fn add_assign(&mut self, rhs: Self) {
        self.0.fetch_add(rhs.into_inner(), Relaxed);
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self(AtomicI64::new(self.get()))
    }
}

impl std::fmt::Display for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct UpdateStats {
    pub num_no_change: Counter,
    pub num_insertions: Counter,
    pub num_deletions: Counter,
    /// Number of source rows that were updated.
    pub num_updates: Counter,
    /// Number of source rows that were reprocessed because of logic change.
    pub num_reprocesses: Counter,
    pub num_errors: Counter,
    /// Number of source rows currently being processed.
    pub num_in_process: Counter,
}

impl UpdateStats {
    pub fn delta(&self, base: &Self) -> Self {
        UpdateStats {
            num_no_change: self.num_no_change.delta(&base.num_no_change),
            num_insertions: self.num_insertions.delta(&base.num_insertions),
            num_deletions: self.num_deletions.delta(&base.num_deletions),
            num_updates: self.num_updates.delta(&base.num_updates),
            num_reprocesses: self.num_reprocesses.delta(&base.num_reprocesses),
            num_errors: self.num_errors.delta(&base.num_errors),
            num_in_process: self.num_in_process.delta(&base.num_in_process),
        }
    }

    pub fn merge(&self, delta: &Self) {
        self.num_no_change.merge(&delta.num_no_change);
        self.num_insertions.merge(&delta.num_insertions);
        self.num_deletions.merge(&delta.num_deletions);
        self.num_updates.merge(&delta.num_updates);
        self.num_reprocesses.merge(&delta.num_reprocesses);
        self.num_errors.merge(&delta.num_errors);
        self.num_in_process.merge(&delta.num_in_process);
    }

    pub fn has_any_change(&self) -> bool {
        self.num_insertions.get() > 0
            || self.num_deletions.get() > 0
            || self.num_updates.get() > 0
            || self.num_reprocesses.get() > 0
            || self.num_errors.get() > 0
    }

    /// Start processing the specified number of rows.
    /// Increments the in-process counter and is called when beginning row processing.
    pub fn start_processing(&self, count: i64) {
        self.num_in_process.inc(count);
    }

    /// Finish processing the specified number of rows.
    /// Decrements the in-process counter and is called when row processing completes.
    pub fn finish_processing(&self, count: i64) {
        self.num_in_process.inc(-count);
    }

    /// Get the current number of rows being processed.
    pub fn get_in_process_count(&self) -> i64 {
        self.num_in_process.get()
    }
}

/// Per-operation tracking of in-process row counts.
#[derive(Debug, Default)]
pub struct OperationInProcessStats {
    /// Maps operation names to their current in-process row counts.
    operation_counters: std::sync::RwLock<std::collections::HashMap<String, Counter>>,
}

impl OperationInProcessStats {
    /// Start processing rows for the specified operation.
    pub fn start_processing(&self, operation_name: &str, count: i64) {
        let mut counters = self.operation_counters.write().unwrap();
        let counter = counters.entry(operation_name.to_string()).or_default();
        counter.inc(count);
    }

    /// Finish processing rows for the specified operation.
    pub fn finish_processing(&self, operation_name: &str, count: i64) {
        let counters = self.operation_counters.write().unwrap();
        if let Some(counter) = counters.get(operation_name) {
            counter.inc(-count);
        }
    }

    /// Get the current in-process count for a specific operation.
    pub fn get_operation_in_process_count(&self, operation_name: &str) -> i64 {
        let counters = self.operation_counters.read().unwrap();
        counters
            .get(operation_name)
            .map_or(0, |counter| counter.get())
    }

    /// Get a snapshot of all operation in-process counts.
    pub fn get_all_operations_in_process(&self) -> std::collections::HashMap<String, i64> {
        let counters = self.operation_counters.read().unwrap();
        counters
            .iter()
            .map(|(name, counter)| (name.clone(), counter.get()))
            .collect()
    }

    /// Get the total in-process count across all operations.
    pub fn get_total_in_process_count(&self) -> i64 {
        let counters = self.operation_counters.read().unwrap();
        counters.values().map(|counter| counter.get()).sum()
    }
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut messages = Vec::new();
        let num_errors = self.num_errors.get();
        if num_errors > 0 {
            messages.push(format!("{num_errors} source rows FAILED"));
        }

        let num_skipped = self.num_no_change.get();
        if num_skipped > 0 {
            messages.push(format!("{num_skipped} source rows NO CHANGE"));
        }

        let num_insertions = self.num_insertions.get();
        let num_deletions = self.num_deletions.get();
        let num_updates = self.num_updates.get();
        let num_reprocesses = self.num_reprocesses.get();
        let num_source_rows = num_insertions + num_deletions + num_updates + num_reprocesses;
        if num_source_rows > 0 {
            let mut sub_messages = Vec::new();
            if num_insertions > 0 {
                sub_messages.push(format!("{num_insertions} ADDED"));
            }
            if num_deletions > 0 {
                sub_messages.push(format!("{num_deletions} REMOVED"));
            }
            if num_reprocesses > 0 {
                sub_messages.push(format!(
                    "{num_reprocesses} REPROCESSED on flow/logic changes or reexport"
                ));
            }
            if num_updates > 0 {
                sub_messages.push(format!("{num_updates} UPDATED in source content only"));
            }
            messages.push(format!(
                "{num_source_rows} source rows processed ({})",
                sub_messages.join(", "),
            ));
        }

        let num_in_process = self.num_in_process.get();
        if num_in_process > 0 {
            messages.push(format!("{num_in_process} source rows IN PROCESS"));
        }

        if !messages.is_empty() {
            write!(f, "{}", messages.join("; "))?;
        } else {
            write!(f, "No changes")?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct SourceUpdateInfo {
    pub source_name: String,
    pub stats: UpdateStats,
}

impl std::fmt::Display for SourceUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source_name, self.stats)
    }
}

#[derive(Debug, Serialize)]
pub struct IndexUpdateInfo {
    pub sources: Vec<SourceUpdateInfo>,
}

impl std::fmt::Display for IndexUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for source in self.sources.iter() {
            writeln!(f, "{source}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_update_stats_in_process_tracking() {
        let stats = UpdateStats::default();

        // Initially should be zero
        assert_eq!(stats.get_in_process_count(), 0);

        // Start processing some rows
        stats.start_processing(5);
        assert_eq!(stats.get_in_process_count(), 5);

        // Start processing more rows
        stats.start_processing(3);
        assert_eq!(stats.get_in_process_count(), 8);

        // Finish processing some rows
        stats.finish_processing(2);
        assert_eq!(stats.get_in_process_count(), 6);

        // Finish processing remaining rows
        stats.finish_processing(6);
        assert_eq!(stats.get_in_process_count(), 0);
    }

    #[test]
    fn test_update_stats_thread_safety() {
        let stats = Arc::new(UpdateStats::default());
        let mut handles = Vec::new();

        // Spawn multiple threads that concurrently increment and decrement
        for i in 0..10 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                // Each thread processes 100 rows
                stats_clone.start_processing(100);

                // Simulate some work
                thread::sleep(std::time::Duration::from_millis(i * 10));

                // Finish processing
                stats_clone.finish_processing(100);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Should be back to zero
        assert_eq!(stats.get_in_process_count(), 0);
    }

    #[test]
    fn test_operation_in_process_stats() {
        let op_stats = OperationInProcessStats::default();

        // Initially should be zero for all operations
        assert_eq!(op_stats.get_operation_in_process_count("op1"), 0);
        assert_eq!(op_stats.get_total_in_process_count(), 0);

        // Start processing rows for different operations
        op_stats.start_processing("op1", 5);
        op_stats.start_processing("op2", 3);

        assert_eq!(op_stats.get_operation_in_process_count("op1"), 5);
        assert_eq!(op_stats.get_operation_in_process_count("op2"), 3);
        assert_eq!(op_stats.get_total_in_process_count(), 8);

        // Get all operations snapshot
        let all_ops = op_stats.get_all_operations_in_process();
        assert_eq!(all_ops.len(), 2);
        assert_eq!(all_ops.get("op1"), Some(&5));
        assert_eq!(all_ops.get("op2"), Some(&3));

        // Finish processing some rows
        op_stats.finish_processing("op1", 2);
        assert_eq!(op_stats.get_operation_in_process_count("op1"), 3);
        assert_eq!(op_stats.get_total_in_process_count(), 6);

        // Finish processing all remaining rows
        op_stats.finish_processing("op1", 3);
        op_stats.finish_processing("op2", 3);
        assert_eq!(op_stats.get_total_in_process_count(), 0);
    }

    #[test]
    fn test_operation_in_process_stats_thread_safety() {
        let op_stats = Arc::new(OperationInProcessStats::default());
        let mut handles = Vec::new();

        // Spawn threads for different operations
        for i in 0..5 {
            let op_stats_clone = Arc::clone(&op_stats);
            let op_name = format!("operation_{}", i);

            let handle = thread::spawn(move || {
                // Each operation processes 50 rows
                op_stats_clone.start_processing(&op_name, 50);

                // Simulate some work
                thread::sleep(std::time::Duration::from_millis(i * 20));

                // Finish processing
                op_stats_clone.finish_processing(&op_name, 50);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Should be back to zero
        assert_eq!(op_stats.get_total_in_process_count(), 0);
    }

    #[test]
    fn test_update_stats_merge_with_in_process() {
        let stats1 = UpdateStats::default();
        let stats2 = UpdateStats::default();

        // Set up different counts
        stats1.start_processing(10);
        stats1.num_insertions.inc(5);

        stats2.start_processing(15);
        stats2.num_updates.inc(3);

        // Merge stats2 into stats1
        stats1.merge(&stats2);

        // Check that all counters were merged correctly
        assert_eq!(stats1.get_in_process_count(), 25); // 10 + 15
        assert_eq!(stats1.num_insertions.get(), 5);
        assert_eq!(stats1.num_updates.get(), 3);
    }

    #[test]
    fn test_update_stats_delta_with_in_process() {
        let base = UpdateStats::default();
        let current = UpdateStats::default();

        // Set up base state
        base.start_processing(5);
        base.num_insertions.inc(2);

        // Set up current state
        current.start_processing(12);
        current.num_insertions.inc(7);
        current.num_updates.inc(3);

        // Calculate delta
        let delta = current.delta(&base);

        // Check that delta contains the differences
        assert_eq!(delta.get_in_process_count(), 7); // 12 - 5
        assert_eq!(delta.num_insertions.get(), 5); // 7 - 2
        assert_eq!(delta.num_updates.get(), 3); // 3 - 0
    }

    #[test]
    fn test_update_stats_display_with_in_process() {
        let stats = UpdateStats::default();

        // Test with no activity
        assert_eq!(format!("{}", stats), "No changes");

        // Test with in-process rows
        stats.start_processing(5);
        assert!(format!("{}", stats).contains("5 source rows IN PROCESS"));

        // Test with mixed activity
        stats.num_insertions.inc(3);
        stats.num_errors.inc(1);
        let display = format!("{}", stats);
        assert!(display.contains("1 source rows FAILED"));
        assert!(display.contains("3 source rows processed"));
        assert!(display.contains("5 source rows IN PROCESS"));
    }
}
