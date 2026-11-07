use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::engine::app::AppOpHandle;
use crate::engine::runtime::get_runtime;
use crate::engine::stats::{
    ProcessingStats, ProcessingStatsGroup, TERMINATED_VERSION, VersionedProcessingStats,
};
use crate::prelude::*;
use crate::state::stable_path::{StableKey, StablePath};

/// Spinner characters (braille pattern), cycled on each redraw.
const SPINNER_CHARS: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// Default refresh interval of the in-place display on a terminal.
const DEFAULT_TTY_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Default interval between blocks of the scrolling display (not a terminal,
/// e.g. a CI log): each block adds lines that stay, so print them rarely.
const DEFAULT_PLAIN_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Global flag ensuring only one show_progress runs at a time.
static PROGRESS_ACTIVE: AtomicBool = AtomicBool::new(false);

/// A stats group drawn as a section of the in-place region, while a root
/// display runs on a terminal.
#[cfg(unix)]
struct GroupSection {
    title: String,
    stats: ProcessingStats,
    live: bool,
    start_time: Instant,
    ready_time: Option<Instant>,
}

/// The groups the in-place region draws, and whether a region runs at all.
/// `None` when no root display runs on a terminal: groups then print plain
/// blocks. The region removes a group, printing its final block above the
/// region, once the group terminates.
#[cfg(unix)]
static GROUP_SECTIONS: std::sync::Mutex<Option<Vec<GroupSection>>> = std::sync::Mutex::new(None);

/// Options for the progress display.
#[derive(Default)]
pub struct ProgressDisplayOptions {
    /// Minimum interval between progress refreshes. `None` uses the default
    /// for the output: frequent on a terminal, rare for scrolling output.
    refresh_interval: Option<Duration>,
}

impl ProgressDisplayOptions {
    /// Build options from an optional refresh interval in seconds — `None`
    /// (or a non-positive value) falls back to the default interval.
    pub fn from_refresh_secs(refresh_interval_secs: Option<f64>) -> Self {
        Self {
            refresh_interval: refresh_interval_secs
                .filter(|secs| *secs > 0.0)
                .map(Duration::from_secs_f64),
        }
    }

    fn tty_refresh_interval(&self) -> Duration {
        self.refresh_interval
            .unwrap_or(DEFAULT_TTY_REFRESH_INTERVAL)
    }

    fn plain_refresh_interval(&self) -> Duration {
        self.refresh_interval
            .unwrap_or(DEFAULT_PLAIN_REFRESH_INTERVAL)
    }
}

/// Cells of the progress bar of a running component, at most.
const BAR_CELLS: usize = 20;

/// Longest name column; longer names are cut at the end.
const MAX_NAME_CELLS: usize = 32;

/// Shortest name column, however narrow the terminal.
const MIN_NAME_CELLS: usize = 6;

/// Failures listed under their components: on the final block, and fewer in
/// the in-place region, which must stay short.
const MAX_FAILURES_FINAL: usize = 5;
const MAX_FAILURES_LIVE: usize = 3;

/// ANSI SGR codes. Secondary text is bright black (gray), not the faint
/// attribute: some terminals ignore faint, and gray reads on dark and light
/// themes alike.
const BOLD: &str = "1";
const GRAY: &str = "90";
const RED: &str = "31";
const GREEN: &str = "32";
const YELLOW: &str = "33";
const CYAN: &str = "36";

/// How a block is drawn.
#[derive(Clone, Copy)]
struct Style {
    /// ANSI colors: a terminal, with `NO_COLOR` unset.
    color: bool,
    /// Spinner frame for the status line. `None` draws a static marker: in
    /// scrolling output a spinner frame would never move.
    spinner_idx: Option<usize>,
    /// Terminal width to fit lines to. `None` keeps lines whole.
    width: Option<usize>,
}

impl Style {
    /// Scrolling output: no color, no spinner, whole lines.
    const PLAIN: Style = Style {
        color: false,
        spinner_idx: None,
        width: None,
    };

    /// A terminal of `width` cells.
    fn terminal(width: usize, spinner_idx: Option<usize>) -> Self {
        Style {
            color: color_enabled(),
            spinner_idx,
            width: Some(width),
        }
    }

    /// Cells a line may take: one stays spare, as some terminals wrap on the
    /// last column, and a wrapped line breaks the in-place redraw.
    fn limit(&self) -> Option<usize> {
        self.width.map(|w| w.saturating_sub(1))
    }
}

/// Colors are on unless `NO_COLOR` is set to a non-empty value (no-color.org).
pub(crate) fn color_enabled() -> bool {
    !matches!(std::env::var_os("NO_COLOR"), Some(v) if !v.is_empty())
}

/// Display cells of `s`.
fn cells(s: &str) -> usize {
    unicode_width::UnicodeWidthStr::width(s)
}

/// `text` with control characters as spaces. A terminal acts on them: a tab or
/// a carriage return moves its cursor past the cells counted here, which breaks
/// the in-place redraw.
fn printable(text: String) -> String {
    if text.contains(char::is_control) {
        text.chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect()
    } else {
        text
    }
}

/// One output line, built from segments that each carry their own color, so
/// width is measured on the text alone and a cut never splits an escape.
#[derive(Default, Clone)]
struct Line(Vec<(String, Option<&'static str>)>);

impl Line {
    fn text(mut self, text: impl Into<String>) -> Self {
        self.0.push((printable(text.into()), None));
        self
    }

    fn styled(mut self, code: &'static str, text: impl Into<String>) -> Self {
        self.0.push((printable(text.into()), Some(code)));
        self
    }

    fn cells(&self) -> usize {
        self.0.iter().map(|(t, _)| cells(t)).sum()
    }

    /// Render for `style`: colored when it says so, and fit to its width with
    /// a trailing `…`.
    fn render(&self, style: &Style) -> String {
        use unicode_width::UnicodeWidthChar;
        let cut_at = match style.limit() {
            Some(limit) if self.cells() > limit => Some(limit.saturating_sub(1)),
            _ => None,
        };
        let mut out = String::new();
        let mut used = 0;
        for (text, code) in &self.0 {
            let mut piece = String::new();
            let mut full = false;
            for ch in text.chars() {
                let w = ch.width().unwrap_or(0);
                if cut_at.is_some_and(|cut| used + w > cut) {
                    full = true;
                    break;
                }
                used += w;
                piece.push(ch);
            }
            match code {
                Some(code) if style.color && !piece.is_empty() => {
                    write!(&mut out, "\x1b[{code}m{piece}\x1b[0m").unwrap()
                }
                _ => out.push_str(&piece),
            }
            if full {
                break;
            }
        }
        if cut_at.is_some() {
            out.push('…');
        }
        out
    }
}

/// Fit `text` in `max` cells, cutting the end: `process_item_wi…`. The start
/// of a name carries its meaning.
fn fit_end(text: &str, max: usize) -> String {
    if cells(text) <= max {
        return text.to_string();
    }
    let mut out = String::new();
    let mut used = 0;
    for ch in text.chars() {
        let w = cells(ch.encode_utf8(&mut [0; 4]));
        if used + w + 1 > max {
            break;
        }
        used += w;
        out.push(ch);
    }
    out.push('…');
    out
}

/// Fit `text` in `max` cells, cutting the start: `…guide/page_a.md`. The end
/// of a path tells its items apart.
fn fit_start(text: &str, max: usize) -> String {
    if cells(text) <= max {
        return text.to_string();
    }
    let mut kept = Vec::new();
    let mut used = 0;
    for ch in text.chars().rev() {
        let w = cells(ch.encode_utf8(&mut [0; 4]));
        if used + w + 1 > max {
            break;
        }
        used += w;
        kept.push(ch);
    }
    std::iter::once('…').chain(kept.into_iter().rev()).collect()
}

/// `<0.1s`, `12.3s`, `2m 05s`, `1h 02m`.
fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 0.05 {
        "<0.1s".to_string()
    } else if secs < 60.0 {
        format!("{secs:.1}s")
    } else if secs < 3600.0 {
        let s = secs as u64;
        format!("{}m {:02}s", s / 60, s % 60)
    } else {
        let s = secs as u64;
        format!("{}h {:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// `singular` for exactly 1, `plural` otherwise.
fn plural(n: u64, singular: &'static str, plural: &'static str) -> &'static str {
    if n == 1 { singular } else { plural }
}

/// Runs per second: one decimal below 10, whole above.
fn format_rate(per_sec: f64) -> String {
    if per_sec < 10.0 {
        format!("{per_sec:.1}/s")
    } else {
        format!("{per_sec:.0}/s")
    }
}

/// A run that fails before its processor name is known is counted under the
/// store's internal placeholder; show it in words.
fn display_name(name: &str) -> &str {
    if name == crate::state::db_schema::UNKNOWN_PROCESSOR_NAME {
        "(unnamed component)"
    } else {
        name
    }
}

/// A component path as people read it: `item/3` for `/"item"/"3"`, and
/// `process_doc/guide/page-7.md` for `/@process_doc/"guide/page-7.md"`.
/// Only when every key is an integer, a symbol or a string without quotes,
/// backslashes or control characters; any other key keeps the exact form
/// `cocoindex show` takes.
fn readable_path(path: &StablePath) -> String {
    if path.0.is_empty() {
        return "(root)".to_string();
    }
    let plain: Option<Vec<String>> = path
        .0
        .iter()
        .map(|key| match key {
            StableKey::Int(i) => Some(i.to_string()),
            StableKey::Symbol(s) => Some(s.to_string()),
            StableKey::Str(s)
                if !s.is_empty()
                    && !s.chars().any(|c| matches!(c, '"' | '\\') || c.is_control()) =>
            {
                Some(s.to_string())
            }
            _ => None,
        })
        .collect();
    match plain {
        Some(parts) => parts.join("/"),
        None => path.to_string(),
    }
}

/// The bar of a running component, `cells` wide: a heavy cyan fill with a
/// half-cell tip over a thin gray track, so the fill always reads louder than
/// the track and moves in half-cell steps.
fn progress_bar(line: Line, finished: u64, started: u64, cells: usize) -> Line {
    let exact = finished as f64 / started as f64 * cells as f64;
    let full = (exact.floor() as usize).min(cells);
    let half = full < cells && exact - full as f64 >= 0.5;
    let track = cells - full - usize::from(half);
    let mut line = line.styled(CYAN, "━".repeat(full));
    if half {
        line = line.styled(CYAN, "╸");
    }
    line.styled(GRAY, "─".repeat(track))
}

/// The outcome counts of finished runs, in a fixed order, as `(text, color)`.
/// The run count is not shown: it is their sum.
fn outcome_parts(group: &ProcessingStatsGroup, unfinished: u64) -> Vec<(String, &'static str)> {
    let mut parts = Vec::new();
    for (n, label) in [
        (group.num_adds, "added"),
        (group.num_reprocesses, "reprocessed"),
        (group.num_deletes, "deleted"),
        (group.num_unchanged, "unchanged"),
    ] {
        if n > 0 {
            parts.push((format!("{n} {label}"), GRAY));
        }
    }
    if group.has_errors() {
        parts.push((format!("{} failed", group.num_errors), RED));
    }
    if unfinished > 0 {
        parts.push((format!("{unfinished} unfinished"), YELLOW));
    }
    parts
}

fn push_parts(mut line: Line, parts: Vec<(String, &'static str)>) -> Line {
    for (i, (text, code)) in parts.into_iter().enumerate() {
        if i > 0 {
            line = line.styled(GRAY, " · ");
        }
        line = line.styled(code, text);
    }
    line
}

/// The cells a row needs after its name, without the optional parts (bar,
/// rate): what the name column must leave.
fn row_detail_cells(group: &ProcessingStatsGroup, block_finished: bool) -> usize {
    let in_flight = group.num_in_progress();
    if in_flight > 0 && !block_finished {
        let mut n = cells(&format!(
            "{}/{}",
            group.num_finished(),
            group.num_execution_starts
        ));
        if group.has_errors() {
            n += cells(&format!(" · {} failed", group.num_errors));
        }
        n
    } else {
        push_parts(Line::default(), outcome_parts(group, in_flight)).cells()
    }
}

/// One component's row: `indent`, a mark, the name padded to `name_cells`,
/// then progress (running) or the outcomes of its runs. On the final block
/// (`block_finished`), runs still in flight were cut off, as by Ctrl+C.
///
/// Every count is a count of component runs: in live mode one item runs again
/// each time it changes, so runs can exceed the item count. Progress is runs
/// finished out of runs started so far; the engine cannot know how many runs
/// are still to start, so the bar can move back when new runs start.
///
/// When space is short the rate goes first, then the bar; the counts and the
/// failures always stay.
#[allow(clippy::too_many_arguments)]
fn component_line(
    indent: usize,
    name: &str,
    group: &ProcessingStatsGroup,
    name_cells: usize,
    elapsed: Duration,
    block_finished: bool,
    style: &Style,
) -> Line {
    let started = group.num_execution_starts;
    let finished = group.num_finished();
    let in_flight = group.num_in_progress();
    let running = in_flight > 0 && !block_finished;

    let line = Line::default().text(" ".repeat(indent));
    let line = if running {
        line.styled(GRAY, "·")
    } else if in_flight > 0 {
        line.styled(YELLOW, "○")
    } else if group.has_errors() {
        line.styled(RED, "✗")
    } else {
        line.styled(GREEN, "✓")
    };
    let name = fit_end(display_name(name), name_cells);
    let pad = " ".repeat(name_cells.saturating_sub(cells(&name)));
    let line = line.text(" ").text(name).text(pad).text("  ");

    if !running {
        return push_parts(line, outcome_parts(group, in_flight));
    }
    if started <= 1 {
        let line = line.styled(GRAY, "running");
        return if group.has_errors() {
            line.styled(GRAY, " · ")
                .styled(RED, format!("{} failed", group.num_errors))
        } else {
            line
        };
    }

    let counts = format!("{finished}/{started}");
    let failed = group
        .has_errors()
        .then(|| format!("{} failed", group.num_errors));
    let secs = elapsed.as_secs_f64();
    let rate = (finished > 0 && secs >= 1.0)
        .then(|| format!(" · {}", format_rate(finished as f64 / secs)));

    let essential = cells(&counts) + failed.as_ref().map_or(0, |f| 3 + cells(f));
    let mut room = style
        .limit()
        .map_or(usize::MAX, |l| l.saturating_sub(line.cells() + essential));
    let rate = rate.filter(|r| {
        let fits = cells(r) <= room;
        if fits {
            room -= cells(r);
        }
        fits
    });
    let bar_cells = room.saturating_sub(1).min(BAR_CELLS);
    let mut line = line;
    if bar_cells >= 6 {
        line = progress_bar(line, finished, started, bar_cells).text(" ");
    }
    line = line.text(counts);
    if let Some(rate) = rate {
        line = line.styled(GRAY, rate);
    }
    if let Some(failed) = failed {
        line = line.styled(GRAY, " · ").styled(RED, failed);
    }
    line
}

/// Counts the status line reports for the whole run.
#[derive(Clone, Copy, Default)]
struct Totals {
    failed: u64,
    /// Runs still in flight. On the final block these were cut off.
    unfinished: u64,
    /// What stats groups under this block count: they have their own rows, and
    /// only the final status adds these. Final blocks have one section, so
    /// nothing counts twice with a nested group's section.
    failed_in_groups: u64,
    unfinished_in_groups: u64,
}

impl Totals {
    fn of(snapshots: &[&VersionedProcessingStats]) -> Self {
        let groups = || snapshots.iter().flat_map(|s| s.stats.values());
        Totals {
            failed: groups().map(|g| g.num_errors).sum(),
            unfinished: groups().map(|g| g.num_in_progress()).sum(),
            failed_in_groups: snapshots.iter().map(|s| s.in_groups.num_errors).sum(),
            unfinished_in_groups: snapshots
                .iter()
                .map(|s| s.in_groups.num_in_progress())
                .sum(),
        }
    }
}

/// What a stats block shows, apart from the stats themselves.
struct BlockSpec<'a> {
    /// `Some` for a stats group: its status line starts with the title.
    title: Option<&'a str>,
    live: bool,
    /// The last block of the run: final status, and the failures listed.
    finished: bool,
    start_time: Instant,
    ready_time: Option<Instant>,
}

/// The status line that heads a block: one mark (the block's only spinner
/// while running), what is happening, and details in gray parentheses.
///
/// A run that ends with runs in flight was cut off (Ctrl+C), so it gets no
/// success mark. A one-shot run then says `Stopped`, not `Done`.
///
/// Failures inside stats groups have their own rows, but they count here: the
/// status of the block's run covers them.
fn status_line(spec: &BlockSpec<'_>, ready: bool, totals: Totals, style: &Style) -> Line {
    let elapsed = format_duration(spec.start_time.elapsed());
    let ready_in = spec
        .ready_time
        .map(|t| format_duration(t.duration_since(spec.start_time)));
    let say = |mark: Line, text: &str| {
        let line = mark.text(" ");
        match spec.title {
            Some(title) => line
                .styled(BOLD, title)
                .styled(GRAY, " · ")
                .styled(BOLD, text.to_string()),
            None => line.styled(BOLD, text.to_string()),
        }
    };
    let spinner = match style.spinner_idx {
        Some(idx) => {
            Line::default().styled(CYAN, SPINNER_CHARS[idx % SPINNER_CHARS.len()].to_string())
        }
        None => Line::default().styled(GRAY, "○"),
    };

    if spec.finished {
        let failed = totals.failed + totals.failed_in_groups;
        let unfinished = totals.unfinished + totals.unfinished_in_groups;
        let interrupted = unfinished > 0;
        let mark = if interrupted {
            Line::default().styled(YELLOW, "!")
        } else if failed > 0 {
            Line::default().styled(RED, "✗")
        } else {
            Line::default().styled(GREEN, "✓")
        };
        let verb = if spec.live || interrupted {
            "Stopped after"
        } else {
            "Done in"
        };
        let mut line = say(mark, &format!("{verb} {elapsed}"));
        if failed > 0 {
            line = line
                .styled(GRAY, " · ")
                .styled(RED, format!("{failed} failed"));
        }
        if unfinished > 0 {
            line = line.styled(GRAY, " · ").styled(
                YELLOW,
                format!(
                    "{unfinished} {} unfinished",
                    plural(unfinished, "run", "runs")
                ),
            );
        }
        match (spec.live, ready_in) {
            (true, Some(r)) => line.styled(GRAY, format!(" (ready in {r})")),
            _ => line,
        }
    } else if let (true, true, Some(r)) = (spec.live, ready, &ready_in) {
        say(Line::default().styled(GREEN, "●"), "Watching for changes")
            .styled(GRAY, format!(" (ready in {r} · ctrl+c to stop)"))
    } else if spec.live {
        say(spinner, "Catching up…").styled(GRAY, format!(" ({elapsed} · ctrl+c to stop)"))
    } else {
        say(spinner, "Running…").styled(GRAY, format!(" ({elapsed})"))
    }
}

/// One set of rows to draw: a block's own components, or a nested group's.
struct Section<'a> {
    /// A nested group's title, drawn as a heading over its rows.
    title: Option<&'a str>,
    snapshot: &'a VersionedProcessingStats,
}

/// The name column shared by every row of `sections`, so values line up
/// across them. In a narrow terminal it shrinks, down to `MIN_NAME_CELLS`, to
/// leave each row its essential counts.
fn name_column(sections: &[Section<'_>], finished: bool, style: &Style) -> usize {
    let mut widest = 0;
    let mut room = usize::MAX;
    for section in sections {
        let indent = if section.title.is_some() { 4 } else { 2 };
        for (name, group) in &section.snapshot.stats {
            widest = widest.max(cells(display_name(name)));
            if let Some(limit) = style.limit() {
                // Indent, mark and its space, the gap, then the counts.
                let fixed = indent + 2 + 2 + row_detail_cells(group, finished);
                room = room.min(limit.saturating_sub(fixed));
            }
        }
    }
    widest.min(MAX_NAME_CELLS).min(room.max(MIN_NAME_CELLS))
}

/// The lines of one block: the status line, the rows of `sections` indented
/// under it (a nested group under its title), and under each row the failures
/// of that component, up to `max_failures` in all.
fn block_lines(
    spec: &BlockSpec<'_>,
    sections: &[Section<'_>],
    max_failures: usize,
    style: &Style,
) -> Vec<String> {
    let snapshots: Vec<&VersionedProcessingStats> = sections.iter().map(|s| s.snapshot).collect();
    let totals = Totals::of(&snapshots);
    let ready = sections.first().is_some_and(|s| s.snapshot.ready);
    let mut lines = vec![status_line(spec, ready, totals, style)];

    let name_cells = name_column(sections, spec.finished, style);
    let elapsed = spec.start_time.elapsed();
    let mut listed = 0;
    for section in sections {
        let indent = match section.title {
            Some(title) => {
                lines.push(Line::default().text("  ").styled(BOLD, title));
                4
            }
            None => 2,
        };
        let failures = &section.snapshot.failures;
        let path_cells = failures
            .iter()
            .map(|f| cells(&readable_path(&f.component)))
            .max()
            .unwrap_or(0)
            .min(if style.width.is_some() {
                MAX_NAME_CELLS
            } else {
                usize::MAX
            });
        for (name, group) in &section.snapshot.stats {
            lines.push(component_line(
                indent,
                name,
                group,
                name_cells,
                elapsed,
                spec.finished,
                style,
            ));
            for failure in failures.iter().filter(|f| &f.processor == name) {
                if listed == max_failures {
                    break;
                }
                listed += 1;
                let path = fit_start(&readable_path(&failure.component), path_cells);
                let pad = " ".repeat(path_cells.saturating_sub(cells(&path)) + 2);
                lines.push(
                    Line::default()
                        .text(" ".repeat(indent + 4))
                        .styled(GRAY, path)
                        .text(pad)
                        .styled(RED, failure.message.clone()),
                );
            }
        }
    }
    let unlisted = (totals.failed as usize).saturating_sub(listed);
    if listed > 0 && unlisted > 0 {
        lines.push(
            Line::default()
                .text("      ")
                .styled(GRAY, format!("… and {unlisted} more failed")),
        );
    }
    lines.iter().map(|l| l.render(style)).collect()
}

/// A block with only its own components: the final block of a run or group,
/// and each block of scrolling output.
fn stats_block_lines(
    spec: &BlockSpec<'_>,
    snapshot: &VersionedProcessingStats,
    style: &Style,
) -> Vec<String> {
    let max_failures = if spec.finished { MAX_FAILURES_FINAL } else { 0 };
    block_lines(
        spec,
        &[Section {
            title: None,
            snapshot,
        }],
        max_failures,
        style,
    )
}

/// A stats block to render outside this module, in the format of the
/// engine's own display: for a front end that draws progress itself, such as
/// the Rust CLI, which gets the stats from its child process.
pub struct BlockRequest<'a> {
    pub snapshot: &'a VersionedProcessingStats,
    pub live: bool,
    /// The last block of the run: final status, and the failures listed.
    pub finished: bool,
    pub start_time: Instant,
    /// When the run became ready (live mode).
    pub ready_time: Option<Instant>,
    /// Spinner frame for the status line; `None` draws a static mark.
    pub spinner_idx: Option<usize>,
    /// The terminal's width when the block is drawn on one: lines then fit
    /// it, colored unless `NO_COLOR` is set. `None` renders plain, whole lines.
    pub terminal_width: Option<usize>,
}

/// The lines of the block `request` describes: a status line and one row per
/// component.
pub fn render_block(request: &BlockRequest<'_>) -> Vec<String> {
    let spec = BlockSpec {
        title: None,
        live: request.live,
        finished: request.finished,
        start_time: request.start_time,
        ready_time: request.ready_time,
    };
    let style = match request.terminal_width {
        Some(width) => Style::terminal(width, request.spinner_idx),
        None => Style {
            spinner_idx: request.spinner_idx,
            ..Style::PLAIN
        },
    };
    stats_block_lines(&spec, request.snapshot, &style)
}

/// The stdout file descriptor.
const STDOUT_FD: i32 = 1;

/// The width of the terminal on `fd`, or 80 when `fd` is not a terminal.
#[cfg(unix)]
pub fn terminal_width_from_fd(fd: i32) -> usize {
    unsafe {
        let mut ws: nix::libc::winsize = std::mem::zeroed();
        if nix::libc::ioctl(fd, nix::libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 {
            return ws.ws_col as usize;
        }
    }
    80
}

#[cfg(not(unix))]
pub fn terminal_width_from_fd(_fd: i32) -> usize {
    80
}

/// The in-place progress region at the bottom of the terminal. The display
/// loop and the PTY reader thread both redraw it, so they share one `Region`
/// behind a mutex: each redraw (cursor moves, captured output, new lines) is
/// built and written while holding the lock, so redraws never interleave, and
/// both draw from the same ready time and spinner position.
#[cfg(unix)]
struct Region {
    live: bool,
    start_time: Instant,
    /// Lines the region takes on screen now.
    num_lines: usize,
    /// When the stats were first seen ready (live mode).
    ready_time: Option<Instant>,
    spinner_idx: usize,
    /// Set once the display ends. Output captured after that point is still
    /// passed through, but the region is not drawn again: nothing would clear
    /// it, and it would stay on screen above the final block.
    closed: bool,
}

#[cfg(unix)]
impl Region {
    /// Append escapes that clear the region and leave the cursor at its top.
    fn clear_into(&mut self, output: &mut Vec<u8>) {
        use std::io::Write;
        let n = self.num_lines;
        if n > 0 {
            write!(output, "\x1b[{n}A").unwrap();
            for _ in 0..n {
                write!(output, "\r\x1b[2K\n").unwrap();
            }
            write!(output, "\x1b[{n}A").unwrap();
        }
        self.num_lines = 0;
    }

    /// Print `captured` output above the region, then redraw the region on
    /// `fd`: the root block, with each running stats group nested under it. A
    /// group that terminated prints its final block above the region once.
    fn redraw(&mut self, fd: i32, captured: &[u8], snapshot: &VersionedProcessingStats) {
        use std::io::Write;
        let width = terminal_width_from_fd(fd);
        let (finished, running) = self.take_groups(width);
        let mut output = Vec::new();
        self.clear_into(&mut output);
        output.extend_from_slice(captured);
        for line in &finished {
            write!(&mut output, "\r\x1b[2K{line}\n").unwrap();
        }
        if self.closed {
            write_to_fd(fd, &output);
            return;
        }

        if snapshot.ready && self.ready_time.is_none() {
            self.ready_time = Some(Instant::now());
        }
        let spec = BlockSpec {
            title: None,
            live: self.live,
            finished: false,
            start_time: self.start_time,
            ready_time: self.ready_time,
        };
        let mut sections = vec![Section {
            title: None,
            snapshot,
        }];
        sections.extend(running.iter().map(|(title, snapshot)| Section {
            title: Some(title),
            snapshot,
        }));
        let style = Style::terminal(width, Some(self.spinner_idx));
        let lines = block_lines(&spec, &sections, MAX_FAILURES_LIVE, &style);
        for line in &lines {
            write!(&mut output, "\r\x1b[2K{line}\n").unwrap();
        }
        self.num_lines = lines.len();
        write_to_fd(fd, &output);
    }

    /// Sort the stats groups: the final blocks of groups that terminated (all
    /// of them once the region is closed), which leave the region, each
    /// followed by a blank line; and the title and stats of the others. A
    /// group with no stats yet shows nothing.
    fn take_groups(&self, width: usize) -> (Vec<String>, Vec<(String, VersionedProcessingStats)>) {
        let mut finished = Vec::new();
        let mut running = Vec::new();
        let mut groups = GROUP_SECTIONS.lock().unwrap();
        let Some(groups) = groups.as_mut() else {
            return (finished, running);
        };
        let closed = self.closed;
        groups.retain_mut(|group| {
            let done = closed || group.stats.is_terminated();
            let snapshot = group.stats.snapshot();
            if snapshot.ready && group.ready_time.is_none() {
                group.ready_time = Some(Instant::now());
            }
            if !done {
                if snapshot.version > 0 {
                    running.push((group.title.clone(), snapshot));
                }
                return true;
            }
            if snapshot.version > 0 {
                let spec = BlockSpec {
                    title: Some(&group.title),
                    live: group.live,
                    finished: true,
                    start_time: group.start_time,
                    ready_time: group.ready_time,
                };
                finished.extend(stats_block_lines(
                    &spec,
                    &snapshot,
                    &Style::terminal(width, None),
                ));
                finished.push(String::new());
            }
            false
        });
        (finished, running)
    }

    /// Clear the region on `fd` for good, printing the final blocks of the
    /// stats groups still in it.
    fn close(&mut self, fd: i32) {
        self.closed = true;
        let (finished, _) = self.take_groups(terminal_width_from_fd(fd));
        *GROUP_SECTIONS.lock().unwrap() = None;
        let mut output = Vec::new();
        self.clear_into(&mut output);
        for line in &finished {
            use std::io::Write;
            write!(&mut output, "\r\x1b[2K{line}\n").unwrap();
        }
        write_to_fd(fd, &output);
    }
}

/// Write bytes to a raw fd.
#[cfg(unix)]
fn write_to_fd(fd: i32, buf: &[u8]) {
    unsafe {
        let mut written = 0;
        while written < buf.len() {
            let n = nix::libc::write(
                fd,
                buf[written..].as_ptr() as *const nix::libc::c_void,
                buf.len() - written,
            );
            if n <= 0 {
                break;
            }
            written += n as usize;
        }
    }
}

/// Internal RAII guard for PTY fd cleanup.
/// Restores original fds, ends the stats groups' sections and clears the global
/// flag on drop, so a cancelled display leaves no region behind for later groups.
#[cfg(unix)]
struct PtyGuard {
    saved_stdout: i32,
    saved_stderr: i32,
}

#[cfg(unix)]
impl Drop for PtyGuard {
    fn drop(&mut self) {
        unsafe {
            nix::libc::dup2(self.saved_stdout, nix::libc::STDOUT_FILENO);
            nix::libc::dup2(self.saved_stderr, nix::libc::STDERR_FILENO);
            nix::libc::close(self.saved_stdout);
            nix::libc::close(self.saved_stderr);
        }
        if let Ok(mut groups) = GROUP_SECTIONS.lock() {
            *groups = None;
        }
        PROGRESS_ACTIVE.store(false, Ordering::SeqCst);
    }
}

/// Run the operation with progress display.
///
/// Consumes the handle and returns the operation result.
/// Sets up PTY capture for conflict-free progress display.
/// Returns an error if another `show_progress` is already running.
pub async fn show_progress<T: Send + 'static>(
    handle: AppOpHandle<T>,
    options: ProgressDisplayOptions,
) -> Result<T> {
    // Check exclusive access
    if PROGRESS_ACTIVE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(internal_error!("Another show_progress is already running"));
    }

    let live = handle.live;
    let start_time = Instant::now();

    // Check if stdout is a TTY
    #[cfg(unix)]
    let is_tty = unsafe { nix::libc::isatty(nix::libc::STDOUT_FILENO) == 1 };
    #[cfg(not(unix))]
    let is_tty = false;

    if is_tty {
        #[cfg(unix)]
        {
            show_progress_pty(handle, options, live, start_time).await
        }
        #[cfg(not(unix))]
        {
            show_progress_plain(handle, options, live, start_time).await
        }
    } else {
        show_progress_plain(handle, options, live, start_time).await
    }
}

/// PTY-based progress display (Unix TTY).
#[cfg(unix)]
async fn show_progress_pty<T: Send + 'static>(
    mut handle: AppOpHandle<T>,
    options: ProgressDisplayOptions,
    live: bool,
    start_time: Instant,
) -> Result<T> {
    use nix::pty::openpty;
    use std::os::unix::io::IntoRawFd;

    // Open PTY
    let pty = openpty(None, None).map_err(|e| internal_error!("openpty failed: {e}"))?;
    let master_fd = pty.master.into_raw_fd();
    let slave_fd = pty.slave.into_raw_fd();

    // Save original fds
    let saved_stdout = unsafe { nix::libc::dup(nix::libc::STDOUT_FILENO) };
    let saved_stderr = unsafe { nix::libc::dup(nix::libc::STDERR_FILENO) };

    // Redirect stdout/stderr to PTY slave
    unsafe {
        nix::libc::dup2(slave_fd, nix::libc::STDOUT_FILENO);
        nix::libc::dup2(slave_fd, nix::libc::STDERR_FILENO);
        nix::libc::close(slave_fd);
    }

    // Create RAII guard — restores fds on drop (normal or panic/cancel)
    let _guard = PtyGuard {
        saved_stdout,
        saved_stderr,
    };

    // From here, stats groups draw inside the region.
    *GROUP_SECTIONS.lock().unwrap() = Some(Vec::new());
    let region = Arc::new(std::sync::Mutex::new(Region {
        live,
        start_time,
        num_lines: 0,
        ready_time: None,
        spinner_idx: 0,
        closed: false,
    }));

    // Set master fd to non-blocking so we can poll for readability with a
    // short timeout — required for a reliable shutdown signal (see the
    // shutdown flag below).
    unsafe {
        let flags = nix::libc::fcntl(master_fd, nix::libc::F_GETFL);
        nix::libc::fcntl(master_fd, nix::libc::F_SETFL, flags | nix::libc::O_NONBLOCK);
    }

    // Dup saved_stdout for the reader thread (guard will close the original)
    let reader_stdout_fd = unsafe { nix::libc::dup(saved_stdout) };

    // Shutdown flag: set when the display loop ends so the reader exits
    // promptly even if the PTY slave hasn't fully closed in the kernel.
    //
    // Why this matters: on macOS, the master's blocking `read()` only returns
    // EOF once *every* fd referencing the slave is closed. If Python (or any
    // dependency) forks a helper between `dup2(slave, STDOUT_FILENO)` and the
    // guard drop — `multiprocessing.resource_tracker` is one such helper —
    // that helper inherits fd 1/fd 2 pointing at our slave, and even after
    // we restore the parent's fds the slave's kernel refcount stays > 0.
    // Without an out-of-band exit signal, the reader's `read()` would hang
    // forever and `reader_handle.join()` below would deadlock the cleanup
    // path. Polling with a short timeout lets us notice the flag promptly.
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_for_reader = shutdown.clone();

    let stats_clone = handle.stats().clone();
    let reader_region = region.clone();

    // Spawn reader on a dedicated OS thread, *not* the Tokio runtime.
    //
    // The reader's job is to drain the PTY master so that anyone (tracing
    // subscribers, println!, etc.) writing to the slave can make progress.
    // If the reader ran as a Tokio task, a flood of concurrent writes from
    // worker threads could fill the slave→master kernel buffer, block every
    // worker in write(2) while holding StdoutLock, and leave the reader with
    // no thread to be scheduled on — a classic runtime-starvation deadlock.
    // An OS thread is always schedulable independent of the Tokio runtime.
    let reader_handle = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            if shutdown_for_reader.load(Ordering::Relaxed) {
                break;
            }
            let mut pfd = nix::libc::pollfd {
                fd: master_fd,
                events: nix::libc::POLLIN,
                revents: 0,
            };
            // 100 ms timeout: bounds shutdown latency without busy-looping.
            let rc = unsafe { nix::libc::poll(&mut pfd, 1, 100) };
            if rc <= 0 {
                continue; // timeout (0) or EINTR/error (<0) — loop & re-check flag
            }
            let n = unsafe {
                nix::libc::read(
                    master_fd,
                    buf.as_mut_ptr() as *mut nix::libc::c_void,
                    buf.len(),
                )
            };
            if n < 0 {
                let errno = std::io::Error::last_os_error().raw_os_error();
                if errno == Some(nix::libc::EAGAIN) || errno == Some(nix::libc::EWOULDBLOCK) {
                    continue;
                }
                break; // EIO (slave closed) or other terminal error
            }
            if n == 0 {
                break; // EOF
            }
            let captured = &buf[..n as usize];
            let snapshot = stats_clone.snapshot();
            reader_region
                .lock()
                .unwrap()
                .redraw(reader_stdout_fd, captured, &snapshot);
        }
        // Close our dup of saved_stdout and the master fd.
        unsafe {
            nix::libc::close(reader_stdout_fd);
            nix::libc::close(master_fd);
        }
    });

    // Display loop
    let refresh_interval = options.tty_refresh_interval();
    let mut sleep_fut = std::pin::pin!(tokio::time::sleep(refresh_interval));

    loop {
        // Wake on stats change OR refresh interval, whichever comes first.
        // This ensures the elapsed timer and spinner update regularly even when
        // no stats changes occur (e.g. slow components with no progress for a while).
        tokio::select! {
            result = handle.changed() => {
                let version = result?;
                if version == TERMINATED_VERSION {
                    break;
                }
            }
            () = &mut sleep_fut => {}
        }

        // Reset the sleep future for the next iteration.
        sleep_fut.set(tokio::time::sleep(refresh_interval));

        let snapshot = handle.stats_snapshot();
        let mut region = region.lock().unwrap();
        region.redraw(saved_stdout, &[], &snapshot);
        region.spinner_idx += 1;
    }

    // Clear progress region before restoring fds
    region.lock().unwrap().close(saved_stdout);

    // Drop guard first: restores stdout/stderr (closing the parent's
    // slave-side refs). The slave may still be referenced by an inherited
    // child fd (e.g. multiprocessing.resource_tracker), so we don't rely on
    // EOF — we set the shutdown flag and let the reader's poll loop notice.
    drop(_guard);
    shutdown.store(true, Ordering::Relaxed);

    // Joining the OS thread is blocking; offload to spawn_blocking so we
    // don't stall the Tokio worker we're running on.
    let _ = tokio::task::spawn_blocking(move || reader_handle.join()).await;

    // Print final stats to restored stdout
    let snapshot = handle.stats_snapshot();
    let spec = BlockSpec {
        title: None,
        live,
        finished: true,
        start_time,
        ready_time: region.lock().unwrap().ready_time,
    };
    let style = Style::terminal(terminal_width_from_fd(STDOUT_FD), None);
    for line in stats_block_lines(&spec, &snapshot, &style) {
        println!("{line}");
    }

    handle.result().await
}

/// Plain text fallback (non-TTY, Windows).
async fn show_progress_plain<T: Send + 'static>(
    handle: AppOpHandle<T>,
    options: ProgressDisplayOptions,
    live: bool,
    start_time: Instant,
) -> Result<T> {
    render_plain(handle.stats(), None, live, &options, start_time).await;
    PROGRESS_ACTIVE.store(false, Ordering::SeqCst);
    handle.result().await
}

/// Render `stats` as plain scrolling text until it terminates. Decoupled from
/// `AppOpHandle` and from the `PROGRESS_ACTIVE` singleton, so a stats group can
/// run this concurrently with the root reporter — its writes are captured and
/// interleaved by the root's PTY reader when one is active (see
/// specs/progress_report), and scroll directly otherwise.
///
/// A block is emitted only when the stats changed since the last one (skipping
/// idle refreshes), plus a final block on termination. `title`, when set,
/// starts each status line (groups). A blank line follows each block but the
/// root's final one, so blocks stay apart and the output ends on its summary.
async fn render_plain(
    stats: &ProcessingStats,
    title: Option<&str>,
    live: bool,
    options: &ProgressDisplayOptions,
    start_time: Instant,
) {
    let mut ready_time: Option<Instant> = None;
    // `ProcessingStats` starts at version 0; first real change bumps it, so an
    // idle group emits nothing until it has something to show.
    let mut last_printed_version: u64 = 0;

    let on_terminal = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let refresh_interval = options.plain_refresh_interval();
    let mut sleep_fut = std::pin::pin!(tokio::time::sleep(refresh_interval));
    loop {
        let terminated = tokio::select! {
            // Wakes only on termination, not on every stats change; the refresh
            // timer drives rendering.
            () = stats.wait_terminated() => true,
            () = &mut sleep_fut => {
                sleep_fut.set(tokio::time::sleep(refresh_interval));
                false
            }
        };

        let snapshot = stats.snapshot();
        if snapshot.ready && ready_time.is_none() {
            ready_time = Some(Instant::now());
        }

        // Skip idle refreshes (no change since the last block); always emit the
        // final terminated block.
        if !terminated && snapshot.version == last_printed_version {
            continue;
        }
        last_printed_version = snapshot.version;

        let style = if on_terminal {
            Style::terminal(terminal_width_from_fd(STDOUT_FD), None)
        } else {
            Style::PLAIN
        };
        let spec = BlockSpec {
            title,
            live,
            finished: terminated,
            start_time,
            ready_time,
        };
        // The root's summary stands apart from the log lines above it.
        if terminated && title.is_none() && !on_terminal {
            println!();
        }
        for line in stats_block_lines(&spec, &snapshot, &style) {
            println!("{line}");
        }
        if !terminated || title.is_some() {
            println!();
        }
        if terminated {
            break;
        }
    }
}

/// Report a stats group's `ProcessingStats`. While a root display runs on a
/// terminal, the group becomes a section of its in-place region, and prints
/// its final block once when it terminates. Otherwise a standalone plain
/// reporter prints scrolling blocks, and ends when the group terminates.
/// `refresh_interval_secs` sets that reporter's interval; a section redraws
/// with its region.
pub(crate) fn report_group_stats(
    stats: ProcessingStats,
    title: String,
    live: bool,
    refresh_interval_secs: Option<f64>,
) {
    #[cfg(unix)]
    if let Some(groups) = GROUP_SECTIONS.lock().unwrap().as_mut() {
        groups.push(GroupSection {
            title,
            stats,
            live,
            start_time: Instant::now(),
            ready_time: None,
        });
        return;
    }
    get_runtime().spawn(async move {
        let options = ProgressDisplayOptions::from_refresh_secs(refresh_interval_secs);
        render_plain(&stats, Some(&title), live, &options, Instant::now()).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::stats::FailureRecord;

    fn group(starts: u64, adds: u64, unchanged: u64, errors: u64) -> ProcessingStatsGroup {
        ProcessingStatsGroup {
            num_execution_starts: starts,
            num_adds: adds,
            num_unchanged: unchanged,
            num_errors: errors,
            ..Default::default()
        }
    }

    fn narrow(width: usize) -> Style {
        Style {
            width: Some(width),
            ..Style::PLAIN
        }
    }

    fn row(name: &str, g: &ProcessingStatsGroup, elapsed: f64, style: &Style) -> String {
        let name_cells = cells(display_name(name));
        component_line(
            0,
            name,
            g,
            name_cells,
            Duration::from_secs_f64(elapsed),
            false,
            style,
        )
        .render(style)
    }

    fn spec(
        live: bool,
        finished: bool,
        elapsed: f64,
        ready_after: Option<f64>,
    ) -> BlockSpec<'static> {
        let start_time = Instant::now() - Duration::from_secs_f64(elapsed);
        BlockSpec {
            title: None,
            live,
            finished,
            start_time,
            ready_time: ready_after.map(|r| start_time + Duration::from_secs_f64(r)),
        }
    }

    fn status(spec: &BlockSpec<'_>, ready: bool, failed: u64) -> String {
        let totals = Totals {
            failed,
            ..Totals::default()
        };
        status_line(spec, ready, totals, &Style::PLAIN).render(&Style::PLAIN)
    }

    fn path(keys: &[StableKey]) -> StablePath {
        StablePath(keys.to_vec().into())
    }

    fn s(text: &str) -> StableKey {
        StableKey::Str(text.into())
    }

    fn item(n: &str) -> StablePath {
        path(&[s("item"), s(n)])
    }

    fn failure(processor: &str, component: StablePath, message: &str) -> FailureRecord {
        FailureRecord {
            processor: processor.to_string(),
            component,
            message: message.to_string(),
        }
    }

    #[test]
    fn test_row_running_shows_progress() {
        let line = row("proc", &group(10, 4, 1, 0), 2.0, &Style::PLAIN);
        assert_eq!(line, "· proc  ━━━━━━━━━━────────── 5/10 · 2.5/s");
        assert_eq!(
            row("proc", &group(1, 0, 0, 0), 2.0, &Style::PLAIN),
            "· proc  running"
        );
        // No rate in the first second.
        assert_eq!(
            row("proc", &group(4, 1, 0, 1), 0.5, &Style::PLAIN),
            "· proc  ━━━━━━━━━━────────── 2/4 · 1 failed"
        );
    }

    #[test]
    fn test_progress_bar_half_cell_tip() {
        let bar = |f, s, c| progress_bar(Line::default(), f, s, c).render(&Style::PLAIN);
        // 7/10 of 5 cells is 3.5: three full cells and a half-cell tip.
        assert_eq!(bar(7, 10, 5), "━━━╸─");
        assert_eq!(bar(10, 10, 5), "━━━━━");
        assert_eq!(bar(0, 10, 3), "───");
    }

    #[test]
    fn test_row_outcomes_in_fixed_order_without_run_count() {
        assert_eq!(
            row("proc", &group(10, 10, 0, 0), 1.0, &Style::PLAIN),
            "✓ proc  10 added"
        );
        assert_eq!(
            row("proc", &group(6, 5, 0, 1), 1.0, &Style::PLAIN),
            "✗ proc  5 added · 1 failed"
        );
        let g = ProcessingStatsGroup {
            num_execution_starts: 45,
            num_adds: 12,
            num_reprocesses: 3,
            num_deletes: 8,
            num_unchanged: 22,
            ..Default::default()
        };
        assert_eq!(
            row("proc", &g, 1.0, &Style::PLAIN),
            "✓ proc  12 added · 3 reprocessed · 8 deleted · 22 unchanged"
        );
        let name = crate::state::db_schema::UNKNOWN_PROCESSOR_NAME;
        assert_eq!(
            row(name, &group(1, 0, 0, 1), 0.0, &Style::PLAIN),
            "✗ (unnamed component)  1 failed"
        );
    }

    #[test]
    fn test_row_narrow_keeps_counts_and_failures() {
        let g = group(10, 4, 0, 1);
        // Rate goes first, then the bar; counts and failures stay.
        assert_eq!(
            row("proc", &g, 2.0, &narrow(31)),
            "· proc  ━━━─── 5/10 · 1 failed"
        );
        assert_eq!(row("proc", &g, 2.0, &narrow(26)), "· proc  5/10 · 1 failed");
        assert_eq!(
            row("proc", &g, 2.0, &narrow(60)),
            "· proc  ━━━━━━━━━━────────── 5/10 · 2.5/s · 1 failed"
        );
    }

    #[test]
    fn test_name_column_shrinks_and_cuts_the_end() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert(
            "process_item_with_a_very_long_name".to_string(),
            group(10, 4, 0, 1),
        );
        let lines = stats_block_lines(&spec(false, false, 2.0, None), &snapshot, &narrow(40));
        assert_eq!(lines[1], "  · process_item_with…  5/10 · 1 failed");
        assert!(lines.iter().all(|l| cells(l) <= 39));
    }

    #[test]
    fn test_status_lines() {
        assert_eq!(
            status(&spec(false, false, 12.3, None), false, 0),
            "○ Running… (12.3s)"
        );
        assert_eq!(
            status(&spec(false, true, 12.3, None), true, 0),
            "✓ Done in 12.3s"
        );
        assert_eq!(
            status(&spec(false, true, 0.01, None), true, 0),
            "✓ Done in <0.1s"
        );
        assert_eq!(
            status(&spec(false, true, 12.3, None), true, 2),
            "✗ Done in 12.3s · 2 failed"
        );
        assert_eq!(
            status(&spec(true, false, 8.2, None), false, 0),
            "○ Catching up… (8.2s · ctrl+c to stop)"
        );
        assert_eq!(
            status(&spec(true, false, 45.2, Some(12.3)), true, 0),
            "● Watching for changes (ready in 12.3s · ctrl+c to stop)"
        );
        assert_eq!(
            status(&spec(true, true, 45.2, Some(12.3)), true, 1),
            "✗ Stopped after 45.2s · 1 failed (ready in 12.3s)"
        );
        let titled = BlockSpec {
            title: Some("Docs"),
            ..spec(false, true, 2.0, None)
        };
        assert_eq!(status(&titled, true, 0), "✓ Docs · Done in 2.0s");
    }

    #[test]
    fn test_status_line_spinner_and_color() {
        let style = Style {
            color: true,
            spinner_idx: Some(1),
            width: None,
        };
        let line = status_line(
            &spec(false, false, 1.0, None),
            false,
            Totals::default(),
            &style,
        )
        .render(&style);
        assert_eq!(
            line,
            "\x1b[36m⠙\x1b[0m \x1b[1mRunning…\x1b[0m\x1b[90m (1.0s)\x1b[0m"
        );
    }

    #[test]
    fn test_interrupted_run_says_stopped() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("main".to_string(), group(1, 0, 0, 0));
        snapshot
            .stats
            .insert("process_item".to_string(), group(12, 1, 0, 0));
        let lines = stats_block_lines(&spec(false, true, 7.8, None), &snapshot, &Style::PLAIN);
        assert_eq!(
            lines,
            vec![
                "! Stopped after 7.8s · 12 runs unfinished",
                "  ○ main          1 unfinished",
                "  ○ process_item  1 added · 11 unfinished",
            ]
        );
    }

    #[test]
    fn test_interrupted_live_run_gets_no_success_mark() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("main".to_string(), group(1, 0, 0, 0));
        let lines = stats_block_lines(&spec(true, true, 2.4, None), &snapshot, &Style::PLAIN);
        assert_eq!(lines[0], "! Stopped after 2.4s · 1 run unfinished");
    }

    #[test]
    fn test_final_status_counts_failures_in_groups() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("main".to_string(), group(1, 1, 0, 0));
        snapshot.in_groups.num_execution_starts = 3;
        snapshot.in_groups.num_errors = 3;
        let lines = stats_block_lines(&spec(false, true, 2.0, None), &snapshot, &Style::PLAIN);
        assert_eq!(
            lines,
            vec!["✗ Done in 2.0s · 3 failed", "  ✓ main  1 added"]
        );
    }

    #[test]
    fn test_final_status_counts_runs_cut_off_in_groups() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("main".to_string(), group(1, 1, 0, 0));
        snapshot.in_groups.num_execution_starts = 2;
        let lines = stats_block_lines(&spec(true, true, 2.0, None), &snapshot, &Style::PLAIN);
        assert_eq!(lines[0], "! Stopped after 2.0s · 2 runs unfinished");
    }

    #[test]
    fn test_control_characters_never_reach_the_terminal() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot
            .stats
            .insert("name\twith\rcontrols".to_string(), group(1, 0, 0, 1));
        snapshot.failures.push(failure(
            "name\twith\rcontrols",
            item("3"),
            "col1\tcol2\r\x1b[31mred",
        ));
        let spec = BlockSpec {
            title: Some("Title\there"),
            ..spec(false, true, 2.0, None)
        };
        for line in stats_block_lines(&spec, &snapshot, &narrow(60)) {
            assert!(!line.contains(char::is_control), "{line:?}");
        }
    }

    #[test]
    fn test_failure_paths_keep_the_end_that_tells_them_apart() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot
            .stats
            .insert("process_item".to_string(), group(2, 0, 0, 2));
        for name in ["page_a.md", "page_b.md"] {
            let key = format!("docs/src/content/docs/programming_guide/{name}");
            snapshot
                .failures
                .push(failure("process_item", path(&[s(&key)]), "boom"));
        }
        let spec = spec(false, true, 2.0, None);
        let terminal = stats_block_lines(&spec, &snapshot, &narrow(100));
        assert_eq!(terminal[2], "      …ocs/programming_guide/page_a.md  boom");
        assert_eq!(terminal[3], "      …ocs/programming_guide/page_b.md  boom");
        // Scrolling output keeps lines whole.
        let plain = stats_block_lines(&spec, &snapshot, &Style::PLAIN);
        assert_eq!(
            plain[2],
            "      docs/src/content/docs/programming_guide/page_a.md  boom"
        );
    }

    #[test]
    fn test_one_unfinished_run_is_singular() {
        let totals = Totals {
            unfinished: 1,
            ..Totals::default()
        };
        let line = status_line(&spec(false, true, 1.0, None), true, totals, &Style::PLAIN)
            .render(&Style::PLAIN);
        assert_eq!(line, "! Stopped after 1.0s · 1 run unfinished");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs_f64(9.94)), "9.9s");
        assert_eq!(format_duration(Duration::from_secs(125)), "2m 05s");
        assert_eq!(format_duration(Duration::from_secs(3720)), "1h 02m");
    }

    #[test]
    fn test_render_fits_width_with_ellipsis() {
        let style = Style {
            color: true,
            spinner_idx: None,
            width: Some(8),
        };
        // 7 usable cells: 6 of text, then the ellipsis.
        let line = Line::default().styled(GREEN, "✓").text(" abcdefgh");
        assert_eq!(line.render(&style), "\x1b[32m✓\x1b[0m abcd…");
        assert_eq!(Line::default().text("abc").render(&style), "abc");
        // Wide characters count 2 cells.
        let wide = Line::default().text("日本語テキスト");
        assert_eq!(
            wide.render(&Style {
                color: false,
                ..style
            }),
            "日本語…"
        );
    }

    #[test]
    fn test_fit_end() {
        assert_eq!(fit_end("short", 10), "short");
        assert_eq!(fit_end("process_item_with_a_long_name", 11), "process_it…");
    }

    #[test]
    fn test_readable_path() {
        assert_eq!(readable_path(&item("3")), "item/3");
        assert_eq!(
            readable_path(&path(&[s("item"), StableKey::Int(3)])),
            "item/3"
        );
        assert_eq!(readable_path(&path(&[])), "(root)");
        // `mount_each` paths start with a symbol, and keys can hold a `/`.
        assert_eq!(
            readable_path(&path(&[
                StableKey::Symbol("process_doc".into()),
                s("guide/page-7.md")
            ])),
            "process_doc/guide/page-7.md"
        );
        // A key that is not plain keeps the exact form.
        assert_eq!(readable_path(&path(&[s("a\"b"), s("3")])), r#"/"a\"b"/"3""#);
        assert_eq!(
            readable_path(&path(&[s("x"), StableKey::Bool(true)])),
            r#"/"x"/true"#
        );
    }

    #[test]
    fn test_final_block_lists_failures_under_their_component() {
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("main".to_string(), group(1, 1, 0, 0));
        snapshot
            .stats
            .insert("process_item".to_string(), group(40, 31, 0, 9));
        snapshot
            .failures
            .push(failure("process_item", item("3"), "ValueError: boom"));
        snapshot
            .failures
            .push(failure("process_item", item("10"), "KeyError: x"));
        let titled = BlockSpec {
            title: Some("Docs"),
            ..spec(false, true, 2.0, None)
        };
        assert_eq!(
            stats_block_lines(&titled, &snapshot, &Style::PLAIN),
            vec![
                "✗ Docs · Done in 2.0s · 9 failed",
                "  ✓ main          1 added",
                "  ✗ process_item  31 added · 9 failed",
                "      item/3   ValueError: boom",
                "      item/10  KeyError: x",
                "      … and 7 more failed",
            ]
        );
        let running = stats_block_lines(&spec(false, false, 0.5, None), &snapshot, &Style::PLAIN);
        assert_eq!(running.len(), 3, "scrolling blocks list no failures");
    }

    /// A log line that arrives after the display ended must pass through, and
    /// the region must not be drawn again: nothing would clear it, and it
    /// would stay on screen above the final block (seen with `drop`).
    #[cfg(unix)]
    #[test]
    fn test_closed_region_passes_output_through_without_redrawing() {
        use std::io::Read;
        use std::os::fd::FromRawFd;
        let mut fds = [0; 2];
        assert_eq!(unsafe { nix::libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read_fd, write_fd) = (fds[0], fds[1]);

        let mut region = Region {
            live: false,
            start_time: Instant::now(),
            num_lines: 0,
            ready_time: None,
            spinner_idx: 0,
            closed: false,
        };
        let mut snapshot = VersionedProcessingStats::default();
        snapshot.stats.insert("proc".to_string(), group(2, 1, 0, 0));
        region.redraw(write_fd, b"", &snapshot);
        assert!(region.num_lines > 0);
        region.close(write_fd);
        region.redraw(write_fd, b"late log\n", &snapshot);
        assert_eq!(region.num_lines, 0);
        unsafe { nix::libc::close(write_fd) };

        let mut out = String::new();
        unsafe { std::fs::File::from_raw_fd(read_fd) }
            .read_to_string(&mut out)
            .unwrap();
        let (_, after) = out.rsplit_once("late log\n").unwrap();
        assert_eq!(after, "", "the region was drawn again after it closed");
    }

    #[test]
    fn test_region_nests_groups_under_the_root() {
        let mut root = VersionedProcessingStats::default();
        root.stats.insert("app_main".to_string(), group(1, 0, 0, 0));
        let mut docs = VersionedProcessingStats::default();
        docs.stats
            .insert("process_item".to_string(), group(10, 3, 0, 1));
        docs.failures
            .push(failure("process_item", item("3"), "ValueError: boom"));
        let sections = [
            Section {
                title: None,
                snapshot: &root,
            },
            Section {
                title: Some("Docs"),
                snapshot: &docs,
            },
        ];
        let lines = block_lines(
            &spec(false, false, 0.5, None),
            &sections,
            MAX_FAILURES_LIVE,
            &Style::PLAIN,
        );
        assert_eq!(
            lines,
            vec![
                "○ Running… (0.5s)",
                "  · app_main      running",
                "  Docs",
                "    · process_item  ━━━━━━━━──────────── 4/10 · 1 failed",
                "        item/3  ValueError: boom",
            ]
        );
    }
}
