//! Progress display for `update`: a status line and one row per component,
//! redrawn on stderr when it is a terminal, in the engine's own format
//! (`cocoindex_core::engine::progress_display::render_block`).

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use cocoindex::cli::protocol::ComponentStatsWire;
use cocoindex_core::engine::progress_display::{
    BlockRequest, render_block, terminal_width_from_fd,
};
use cocoindex_core::engine::stats::{ProcessingStatsGroup, VersionedProcessingStats};

const MIN_REDRAW: Duration = Duration::from_millis(100);

const STDERR_FD: i32 = 2;

pub struct Renderer {
    enabled: bool,
    tty: bool,
    live: bool,
    started: Instant,
    ready_at: Option<Instant>,
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
            ready_at: None,
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
        let became_ready = ready && self.ready_at.is_none();
        if became_ready {
            self.ready_at = Some(Instant::now());
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

    fn snapshot(&self) -> VersionedProcessingStats {
        VersionedProcessingStats {
            stats: self
                .components
                .iter()
                .map(|(name, stats)| (name.clone(), group(stats)))
                .collect(),
            ready: self.ready_at.is_some(),
            ..Default::default()
        }
    }

    fn lines(
        &self,
        finished: bool,
        spinner_idx: Option<usize>,
        terminal_width: Option<usize>,
    ) -> Vec<String> {
        render_block(&BlockRequest {
            snapshot: &self.snapshot(),
            live: self.live,
            finished,
            start_time: self.started,
            ready_time: self.ready_at,
            spinner_idx,
            terminal_width,
        })
    }

    fn draw(&mut self) {
        if !self.enabled || !self.tty || self.components.is_empty() {
            return;
        }
        self.spinner = self.spinner.wrapping_add(1);
        let lines = self.lines(
            false,
            Some(self.spinner),
            Some(terminal_width_from_fd(STDERR_FD)),
        );
        let mut err = std::io::stderr().lock();
        for line in &lines {
            let _ = writeln!(err, "\x1b[2K{line}");
        }
        let _ = err.flush();
        self.drawn_lines = lines.len();
        self.last_draw = Some(Instant::now());
    }

    /// The final block, for stdout: fit and colored when stdout is a terminal.
    pub fn final_lines(&self) -> Vec<String> {
        let width = std::io::stdout()
            .is_terminal()
            .then(|| terminal_width_from_fd(1));
        self.lines(true, None, width)
    }
}
