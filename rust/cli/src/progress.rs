//! Progress display for `update`: per-component lines and a status line,
//! redrawn on stderr when it is a terminal, using the engine's own line
//! formats (`cocoindex_core::engine::progress_display`).

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use cocoindex::cli::protocol::ComponentStatsWire;
use cocoindex_core::engine::progress_display::{format_component_line, format_status_line};
use cocoindex_core::engine::stats::ProcessingStatsGroup;

const MIN_REDRAW: Duration = Duration::from_millis(100);

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
