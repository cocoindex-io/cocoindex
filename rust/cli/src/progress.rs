//! Progress display for `update`: per-component lines and a status line,
//! redrawn on stderr when it is a terminal.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use cocoindex::cli::protocol::ComponentStatsWire;
use cocoindex_core::engine::stats::ProcessingStatsGroup;

const MIN_REDRAW: Duration = Duration::from_millis(100);

/// Spinner characters (braille pattern), cycled on each redraw.
const SPINNER_CHARS: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

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

/// One component's line: its counts, and a spinner while work is in flight.
fn format_component_line(name: &str, group: &ProcessingStatsGroup, spinner_idx: usize) -> String {
    let total = group.num_execution_starts;
    let in_flight = group.num_in_progress();
    let mut line = if in_flight > 0 {
        let spinner = SPINNER_CHARS[spinner_idx % SPINNER_CHARS.len()];
        format!("{spinner}  {name}: {total} total, {in_flight} in-flight")
    } else {
        format!("✅ {name}: {total} total")
    };
    let parts: Vec<String> = [
        (group.num_adds, "added"),
        (group.num_reprocesses, "reprocessed"),
        (group.num_deletes, "deleted"),
        (group.num_unchanged, "unchanged"),
        (group.num_errors, "⚠️ errors"),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, label)| format!("{count} {label}"))
    .collect();
    if !parts.is_empty() {
        line.push_str(" | ");
        line.push_str(&parts.join(", "));
    }
    line
}

/// The elapsed line, and for a live update whether it has caught up.
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
        let ready_secs = ready_elapsed.map_or(0.0, |d| d.as_secs_f64());
        format!("⏳ Ready (took {ready_secs:.1}s) | Watching for changes...")
    } else {
        format!("⏳ Elapsed: {elapsed_secs:.1}s | Catching up...")
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
