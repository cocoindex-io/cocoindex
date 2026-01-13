use std::fmt::Write;
use std::future::Future;
use std::time::Duration;

use crate::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

const BAR_WIDTH: u64 = 40;
const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

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

#[derive(Default, Clone)]
pub struct ProcessingStats {
    pub stats: Arc<Mutex<IndexMap<String, ProcessingStatsGroup>>>,
}

impl ProcessingStats {
    pub fn update(&self, operation_name: &str, mutator: impl FnOnce(&mut ProcessingStatsGroup)) {
        let mut stats = self.stats.lock().unwrap();
        if let Some(group) = stats.get_mut(operation_name) {
            mutator(group);
        } else {
            let mut group = ProcessingStatsGroup::default();
            mutator(&mut group);
            stats.insert(operation_name.to_string(), group);
        }
    }

    pub fn snapshot(&self) -> IndexMap<String, ProcessingStatsGroup> {
        self.stats.lock().unwrap().clone()
    }

    pub fn format_stats(&self, start_time: Option<std::time::Instant>) -> String {
        let stats = self.stats.lock().unwrap();
        let mut result = String::new();
        for (name, group) in stats.iter() {
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
        let stats_snapshot = self.stats.snapshot();
        let mut progress_bars = self.progress_bars.lock().unwrap();

        for (name, group) in stats_snapshot.iter() {
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
        let stats_snapshot = self.stats.snapshot();
        let progress_bars = self.progress_bars.lock().unwrap();

        // Clear all progress bars
        for pb in progress_bars.values() {
            pb.finish_and_clear();
        }
        self.elapsed_bar.finish_and_clear();

        // Print final stats
        self.multi_progress.suspend(|| {
            for (name, group) in stats_snapshot.iter() {
                let icon = if group.has_errors() { "⚠️" } else { "✅" };
                println!("{icon} {name}: {group}");
            }
            println!("{}", self.format_elapsed_message());
        });
    }
}
