//! Progress display for `update`: per-component lines and a status line,
//! redrawn on stderr when it is a terminal. The line formats are the ones the
//! engine's display used before it moved to its in-place region; the region is
//! private to the engine, so this binary keeps its own copy of the two lines.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use cocoindex::cli::protocol::ComponentStatsWire;
use cocoindex_core::engine::stats::ProcessingStatsGroup;

const MIN_REDRAW: Duration = Duration::from_millis(100);
const SPINNER_CHARS: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// One component's line: `✅ name: 3 total | 2 added, 1 unchanged`, with a
/// spinner and the in-flight count while runs are in progress.
fn format_component_line(name: &str, group: &ProcessingStatsGroup, spinner_idx: usize) -> String {
    let total = group.num_execution_starts;
    let in_flight = group.num_in_progress();
    let icon = if in_flight > 0 {
        format!("{} ", SPINNER_CHARS[spinner_idx % SPINNER_CHARS.len()])
    } else {
        "✅".to_string()
    };
    let mut line = format!("{icon} {name}: {total} total");
    if in_flight > 0 {
        line.push_str(&format!(", {in_flight} in-flight"));
    }
    let parts: Vec<String> = [
        (group.num_adds, "added"),
        (group.num_reprocesses, "reprocessed"),
        (group.num_deletes, "deleted"),
        (group.num_unchanged, "unchanged"),
        (group.num_errors, "⚠️ errors"),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, what)| format!("{count} {what}"))
    .collect();
    if !parts.is_empty() {
        line.push_str(&format!(" | {}", parts.join(", ")));
    }
    line
}

/// The status line under the components: elapsed time, then readiness in
/// live mode.
fn format_status_line(
    live: bool,
    ready: bool,
    elapsed: Duration,
    ready_elapsed: Option<Duration>,
) -> String {
    let elapsed_secs = elapsed.as_secs_f64();
    if !live {
        format!("⏳ Elapsed: {elapsed_secs:.1}s")
    } else if ready {
        let ready_secs = ready_elapsed.map(|d| d.as_secs_f64()).unwrap_or(0.0);
        format!("⏳ Ready (took {ready_secs:.1}s) | Watching for changes...")
    } else {
        format!("⏳ Elapsed: {elapsed_secs:.1}s | Catching up...")
    }
}

pub struct Renderer {
    enabled: bool,
    tty: bool,
    live: bool,
    started: Instant,
    ready_after: Option<Duration>,
    components: BTreeMap<String, ComponentStatsWire>,
    drawn_lines: usize,
    last_draw: Option<Instant>,
    spinner: usize,
}

fn group(stats: &ComponentStatsWire) -> ProcessingStatsGroup {
    ProcessingStatsGroup {
        num_execution_starts: stats.num_execution_starts,
        num_unchanged: stats.num_unchanged,
        num_adds: stats.num_adds,
        num_deletes: stats.num_deletes,
        num_reprocesses: stats.num_reprocesses,
        num_errors: stats.num_errors,
    }
}

impl Renderer {
    pub fn new(quiet: bool, live: bool) -> Self {
        Self {
            enabled: !quiet,
            tty: std::io::stderr().is_terminal(),
            live,
            started: Instant::now(),
            ready_after: None,
            components: BTreeMap::new(),
            drawn_lines: 0,
            last_draw: None,
            spinner: 0,
        }
    }

    /// Forward a line of the child's own output, keeping the progress block
    /// below it.
    pub fn text(&mut self, text: &str) {
        self.clear();
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{text}");
        let _ = out.flush();
        drop(out);
        self.draw();
    }

    pub fn progress(&mut self, ready: bool, components: BTreeMap<String, ComponentStatsWire>) {
        let became_ready = ready && self.ready_after.is_none();
        if became_ready {
            self.ready_after = Some(self.started.elapsed());
        }
        self.components = components;
        let due = self.last_draw.is_none_or(|t| t.elapsed() >= MIN_REDRAW);
        if due || became_ready {
            self.clear();
            self.draw();
        }
    }

    /// Remove the live block (before printing results).
    pub fn clear(&mut self) {
        if self.drawn_lines > 0 {
            let mut err = std::io::stderr().lock();
            let _ = write!(err, "\x1b[{}A\x1b[J", self.drawn_lines);
            let _ = err.flush();
            self.drawn_lines = 0;
        }
    }

    fn draw(&mut self) {
        if !self.enabled || !self.tty || self.components.is_empty() {
            return;
        }
        self.spinner = self.spinner.wrapping_add(1);
        let mut lines: Vec<String> = self
            .components
            .iter()
            .map(|(name, stats)| format_component_line(name, &group(stats), self.spinner))
            .collect();
        lines.push(format_status_line(
            self.live,
            self.ready_after.is_some(),
            self.started.elapsed(),
            self.ready_after,
        ));
        let mut err = std::io::stderr().lock();
        for line in &lines {
            let _ = writeln!(err, "\x1b[2K{line}");
        }
        let _ = err.flush();
        self.drawn_lines = lines.len();
        self.last_draw = Some(Instant::now());
    }

    /// The final per-component lines, for stdout.
    pub fn final_lines(&self) -> Vec<String> {
        self.components
            .iter()
            .map(|(name, stats)| format_component_line(name, &group(stats), 0))
            .collect()
    }
}
