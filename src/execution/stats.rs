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
pub struct ProcessingCounters {
    /// Total number of processing operations started.
    pub num_starts: Counter,
    /// Total number of processing operations ended.
    pub num_ends: Counter,
}

impl ProcessingCounters {
    /// Start processing the specified number of items.
    pub fn start(&self, count: i64) {
        self.num_starts.inc(count);
    }

    /// End processing the specified number of items.
    pub fn end(&self, count: i64) {
        self.num_ends.inc(count);
    }

    /// Get the current number of items being processed (starts - ends).
    pub fn get_in_process(&self) -> i64 {
        let ends = self.num_ends.get();
        let starts = self.num_starts.get();
        starts - ends
    }

    /// Calculate the delta between this and a base ProcessingCounters.
    pub fn delta(&self, base: &Self) -> Self {
        ProcessingCounters {
            num_starts: self.num_starts.delta(&base.num_starts),
            num_ends: self.num_ends.delta(&base.num_ends),
        }
    }

    /// Merge a delta into this ProcessingCounters.
    pub fn merge(&self, delta: &Self) {
        self.num_starts.merge(&delta.num_starts);
        self.num_ends.merge(&delta.num_ends);
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
    /// Processing counters for tracking in-process rows.
    pub processing: ProcessingCounters,
    /// Cumulative total count of items (for display purposes)
    /// This represents the actual total after applying additions and deletions
    #[serde(skip)]
    pub cumulative_total: Counter,
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
            processing: self.processing.delta(&base.processing),
            cumulative_total: self.cumulative_total.clone(),
        }
    }

    pub fn merge(&self, delta: &Self) {
        self.num_no_change.merge(&delta.num_no_change);
        self.num_insertions.merge(&delta.num_insertions);
        self.num_deletions.merge(&delta.num_deletions);
        self.num_updates.merge(&delta.num_updates);
        self.num_reprocesses.merge(&delta.num_reprocesses);
        self.num_errors.merge(&delta.num_errors);
        self.processing.merge(&delta.processing);
        // Update cumulative total: add insertions, subtract deletions
        let net_change = delta.num_insertions.get() - delta.num_deletions.get();
        self.cumulative_total.inc(net_change);
    }

    pub fn has_any_change(&self) -> bool {
        self.num_insertions.get() > 0
            || self.num_deletions.get() > 0
            || self.num_updates.get() > 0
            || self.num_reprocesses.get() > 0
            || self.num_errors.get() > 0
    }
}

/// Per-operation tracking of in-process row counts.
#[derive(Debug, Default)]
pub struct OperationInProcessStats {
    /// Maps operation names to their processing counters.
    operation_counters: std::sync::RwLock<std::collections::HashMap<String, ProcessingCounters>>,
}

impl OperationInProcessStats {
    /// Start processing rows for the specified operation.
    pub fn start_processing(&self, operation_name: &str, count: i64) {
        let mut counters = self.operation_counters.write().unwrap();
        let counter = counters.entry(operation_name.to_string()).or_default();
        counter.start(count);
    }

    /// Finish processing rows for the specified operation.
    pub fn finish_processing(&self, operation_name: &str, count: i64) {
        let counters = self.operation_counters.write().unwrap();
        if let Some(counter) = counters.get(operation_name) {
            counter.end(count);
        }
    }

    /// Get the current in-process count for a specific operation.
    pub fn get_operation_in_process_count(&self, operation_name: &str) -> i64 {
        let counters = self.operation_counters.read().unwrap();
        counters
            .get(operation_name)
            .map_or(0, |counter| counter.get_in_process())
    }

    /// Get a snapshot of all operation in-process counts.
    pub fn get_all_operations_in_process(&self) -> std::collections::HashMap<String, i64> {
        let counters = self.operation_counters.read().unwrap();
        counters
            .iter()
            .map(|(name, counter)| (name.clone(), counter.get_in_process()))
            .collect()
    }

    /// Get the total in-process count across all operations.
    pub fn get_total_in_process_count(&self) -> i64 {
        let counters = self.operation_counters.read().unwrap();
        counters
            .values()
            .map(|counter| counter.get_in_process())
            .sum()
    }
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut segments = Vec::new();
        let num_insertions = self.num_insertions.get();
        let num_deletions = self.num_deletions.get();
        let num_updates = self.num_updates.get();
        let num_no_change = self.num_no_change.get();
        let num_errors = self.num_errors.get();
        let num_in_process = self.processing.get_in_process();
        let num_reprocesses = self.num_reprocesses.get();
        let total = num_insertions + num_deletions + num_updates + num_no_change + num_reprocesses;

        // Progress bar segments
        if total > 0 {
            if num_insertions > 0 {
                segments.push((num_insertions, "+", format!("(+{} added)", num_insertions)));
            }
            if num_deletions > 0 {
                segments.push((num_deletions, "-", format!("(-{} removed)", num_deletions)));
            }
            if num_updates > 0 {
                segments.push((num_updates, "~", format!("(~{} updated)", num_updates)));
            }
            if num_no_change > 0 {
                segments.push((num_no_change, " ", "".to_string()));
            }
        }

        // Error handling
        if num_errors > 0 {
            write!(f, "{} rows failed", num_errors)?;
            if !segments.is_empty() {
                write!(f, "; ")?;
            }
        }

        // Progress bar
        if !segments.is_empty() {
            let mut sorted_segments = segments.clone();
            sorted_segments.sort_by_key(|s| s.0);
            sorted_segments.reverse();

            let bar_width = 40;
            let mut bar = String::new();

            let mut remaining_width = bar_width;

            for (count, segment_type, _) in sorted_segments.iter() {
                let segment_width = (*count * bar_width as i64 / total as i64) as usize;
                let width = std::cmp::min(segment_width, remaining_width);
                if width > 0 {
                    // Calculate completed and remaining portions
                    let completed_portion =
                        (width as f64 * (total - num_in_process) as f64 / total as f64) as usize;
                    let remaining_portion = width - completed_portion;

                    // Add segment with appropriate characters based on type
                    if completed_portion > 0 {
                        let completed_char = match *segment_type {
                            "+" => "█",
                            "-" => "▓",
                            "~" => "▒",
                            _ => "░",
                        };
                        bar.push_str(&completed_char.repeat(completed_portion));
                    }

                    if remaining_portion > 0 {
                        let remaining_char = match *segment_type {
                            "+" => "▒",
                            "-" => "░",
                            "~" => "░",
                            _ => " ",
                        };
                        bar.push_str(&remaining_char.repeat(remaining_portion));
                    }

                    remaining_width = remaining_width.saturating_sub(width);
                }
            }
            if remaining_width > 0 {
                bar.push_str(&" ".repeat(remaining_width));
            }
            // Use total from current operations - this represents the actual record count
            write!(f, "[{}] {}/{} records ", bar, total - num_in_process, total)?;

            // Add segment labels with different grey shades for each segment type
            let mut first = true;
            for (_, segment_type, label) in segments.iter() {
                if !label.is_empty() {
                    if !first {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", label)?;
                    first = false;
                }
            }
        } else {
            write!(f, "No changes")?;
        }

        // In-process info with grey coloring
        if num_in_process > 0 {
            if !segments.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "({} in process)", num_in_process)?;
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
    fn test_processing_counters() {
        let counters = ProcessingCounters::default();

        // Initially should be zero
        assert_eq!(counters.get_in_process(), 0);
        assert_eq!(counters.num_starts.get(), 0);
        assert_eq!(counters.num_ends.get(), 0);

        // Start processing some items
        counters.start(5);
        assert_eq!(counters.get_in_process(), 5);
        assert_eq!(counters.num_starts.get(), 5);
        assert_eq!(counters.num_ends.get(), 0);

        // Start processing more items
        counters.start(3);
        assert_eq!(counters.get_in_process(), 8);
        assert_eq!(counters.num_starts.get(), 8);
        assert_eq!(counters.num_ends.get(), 0);

        // End processing some items
        counters.end(2);
        assert_eq!(counters.get_in_process(), 6);
        assert_eq!(counters.num_starts.get(), 8);
        assert_eq!(counters.num_ends.get(), 2);

        // End processing remaining items
        counters.end(6);
        assert_eq!(counters.get_in_process(), 0);
        assert_eq!(counters.num_starts.get(), 8);
        assert_eq!(counters.num_ends.get(), 8);
    }

    #[test]
    fn test_processing_counters_delta_and_merge() {
        let base = ProcessingCounters::default();
        let current = ProcessingCounters::default();

        // Set up base state
        base.start(5);
        base.end(2);

        // Set up current state
        current.start(12);
        current.end(4);

        // Calculate delta
        let delta = current.delta(&base);
        assert_eq!(delta.num_starts.get(), 7); // 12 - 5
        assert_eq!(delta.num_ends.get(), 2); // 4 - 2
        assert_eq!(delta.get_in_process(), 5); // 7 - 2

        // Test merge
        let merged = ProcessingCounters::default();
        merged.start(10);
        merged.end(3);
        merged.merge(&delta);
        assert_eq!(merged.num_starts.get(), 17); // 10 + 7
        assert_eq!(merged.num_ends.get(), 5); // 3 + 2
        assert_eq!(merged.get_in_process(), 12); // 17 - 5
    }

    #[test]
    fn test_update_stats_in_process_tracking() {
        let stats = UpdateStats::default();

        // Initially should be zero
        assert_eq!(stats.processing.get_in_process(), 0);

        // Start processing some rows
        stats.processing.start(5);
        assert_eq!(stats.processing.get_in_process(), 5);

        // Start processing more rows
        stats.processing.start(3);
        assert_eq!(stats.processing.get_in_process(), 8);

        // Finish processing some rows
        stats.processing.end(2);
        assert_eq!(stats.processing.get_in_process(), 6);

        // Finish processing remaining rows
        stats.processing.end(6);
        assert_eq!(stats.processing.get_in_process(), 0);
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
                stats_clone.processing.start(100);

                // Simulate some work
                thread::sleep(std::time::Duration::from_millis(i * 10));

                // Finish processing
                stats_clone.processing.end(100);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Should be back to zero
        assert_eq!(stats.processing.get_in_process(), 0);
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
        stats1.processing.start(10);
        stats1.num_insertions.inc(5);

        stats2.processing.start(15);
        stats2.num_updates.inc(3);

        // Merge stats2 into stats1
        stats1.merge(&stats2);

        // Check that all counters were merged correctly
        assert_eq!(stats1.processing.get_in_process(), 25); // 10 + 15
        assert_eq!(stats1.num_insertions.get(), 5);
        assert_eq!(stats1.num_updates.get(), 3);
    }

    #[test]
    fn test_update_stats_delta_with_in_process() {
        let base = UpdateStats::default();
        let current = UpdateStats::default();

        // Set up base state
        base.processing.start(5);
        base.num_insertions.inc(2);

        // Set up current state
        current.processing.start(12);
        current.num_insertions.inc(7);
        current.num_updates.inc(3);

        // Calculate delta
        let delta = current.delta(&base);

        // Check that delta contains the differences
        assert_eq!(delta.processing.get_in_process(), 7); // 12 - 5
        assert_eq!(delta.num_insertions.get(), 5); // 7 - 2
        assert_eq!(delta.num_updates.get(), 3); // 3 - 0
    }

    #[test]
    fn test_update_stats_display_with_in_process() {
        let stats = UpdateStats::default();

        // Test with no activity
        assert_eq!(format!("{}", stats), "No changes");

        // Test with in-process rows (no segments yet, so just shows in-process)
        stats.processing.start(5);
        let display = format!("{}", stats);
        assert!(display.contains("5 in process"));

        // Test with mixed activity
        stats.num_insertions.inc(3);
        stats.num_errors.inc(1);
        stats.cumulative_total.inc(3);
        let display = format!("{}", stats);
        assert!(display.contains("1 rows failed"));
        assert!(display.contains("(+3 added)"));
        assert!(display.contains("5 in process"));
    }

    #[test]
    fn test_granular_operation_tracking_integration() {
        let op_stats = OperationInProcessStats::default();

        // Simulate import operations
        op_stats.start_processing("import_users", 5);
        op_stats.start_processing("import_orders", 3);

        // Simulate transform operations
        op_stats.start_processing("transform_user_data", 4);
        op_stats.start_processing("transform_order_data", 2);

        // Simulate export operations
        op_stats.start_processing("export_to_postgres", 3);
        op_stats.start_processing("export_to_elasticsearch", 2);

        // Check individual operation counts
        assert_eq!(op_stats.get_operation_in_process_count("import_users"), 5);
        assert_eq!(
            op_stats.get_operation_in_process_count("transform_user_data"),
            4
        );
        assert_eq!(
            op_stats.get_operation_in_process_count("export_to_postgres"),
            3
        );

        // Check total count across all operations
        assert_eq!(op_stats.get_total_in_process_count(), 19); // 5+3+4+2+3+2

        // Check snapshot of all operations
        let all_ops = op_stats.get_all_operations_in_process();
        assert_eq!(all_ops.len(), 6);
        assert_eq!(all_ops.get("import_users"), Some(&5));
        assert_eq!(all_ops.get("transform_user_data"), Some(&4));
        assert_eq!(all_ops.get("export_to_postgres"), Some(&3));

        // Finish some operations
        op_stats.finish_processing("import_users", 2);
        op_stats.finish_processing("transform_user_data", 4);
        op_stats.finish_processing("export_to_postgres", 1);

        // Verify counts after completion
        assert_eq!(op_stats.get_operation_in_process_count("import_users"), 3); // 5-2
        assert_eq!(
            op_stats.get_operation_in_process_count("transform_user_data"),
            0
        ); // 4-4
        assert_eq!(
            op_stats.get_operation_in_process_count("export_to_postgres"),
            2
        ); // 3-1
        assert_eq!(op_stats.get_total_in_process_count(), 12); // 3+3+0+2+2+2
    }

    #[test]
    fn test_operation_tracking_with_realistic_pipeline() {
        let op_stats = OperationInProcessStats::default();

        // Simulate a realistic processing pipeline scenario
        // Import phase: Start processing 100 rows
        op_stats.start_processing("users_import", 100);
        assert_eq!(op_stats.get_total_in_process_count(), 100);

        // Transform phase: As import finishes, transform starts
        for i in 0..100 {
            // Each imported row triggers a transform
            if i % 10 == 0 {
                // Complete import batch every 10 items
                op_stats.finish_processing("users_import", 10);
            }

            // Start transform for each item
            op_stats.start_processing("user_transform", 1);

            // Some transforms complete quickly
            if i % 5 == 0 {
                op_stats.finish_processing("user_transform", 1);
            }
        }

        // Verify intermediate state
        assert_eq!(op_stats.get_operation_in_process_count("users_import"), 0); // All imports finished
        assert_eq!(
            op_stats.get_operation_in_process_count("user_transform"),
            80
        ); // 100 started - 20 finished

        // Export phase: As transforms finish, exports start
        for i in 0..80 {
            op_stats.finish_processing("user_transform", 1);
            op_stats.start_processing("user_export", 1);

            // Some exports complete
            if i % 3 == 0 {
                op_stats.finish_processing("user_export", 1);
            }
        }

        // Final verification
        assert_eq!(op_stats.get_operation_in_process_count("users_import"), 0);
        assert_eq!(op_stats.get_operation_in_process_count("user_transform"), 0);
        assert_eq!(op_stats.get_operation_in_process_count("user_export"), 53); // 80 - 27 (80/3 rounded down)
        assert_eq!(op_stats.get_total_in_process_count(), 53);
    }

    #[test]
    fn test_operation_tracking_cumulative_behavior() {
        let op_stats = OperationInProcessStats::default();

        // Test that operation tracking maintains cumulative behavior for delta calculations
        let snapshot1 = OperationInProcessStats::default();

        // Initial state
        op_stats.start_processing("test_op", 10);
        op_stats.finish_processing("test_op", 3);

        // Simulate taking a snapshot (in real code, this would involve cloning counters)
        // For testing, will manually create the "previous" state
        snapshot1.start_processing("test_op", 10);
        snapshot1.finish_processing("test_op", 3);

        // Continue processing
        op_stats.start_processing("test_op", 5);
        op_stats.finish_processing("test_op", 2);

        // Verify cumulative nature
        // op_stats should have: starts=15, ends=5, in_process=10
        // snapshot1 should have: starts=10, ends=3, in_process=7
        // Delta would be: starts=5, ends=2, net_change=3

        assert_eq!(op_stats.get_operation_in_process_count("test_op"), 10); // 15-5
    }
}
