use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::engine::app::AppOpHandle;
use crate::engine::runtime::get_runtime;
use crate::engine::stats::{ProcessingStats, ProcessingStatsGroup, TERMINATED_VERSION};
use crate::prelude::*;

/// Spinner characters (braille pattern), cycled on each redraw.
const SPINNER_CHARS: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// Default refresh interval.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Global flag ensuring only one show_progress runs at a time.
static PROGRESS_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Options for the progress display.
pub struct ProgressDisplayOptions {
    /// Minimum interval between progress refreshes.
    pub refresh_interval: Duration,
}

impl Default for ProgressDisplayOptions {
    fn default() -> Self {
        Self {
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl ProgressDisplayOptions {
    /// Build options from an optional refresh interval in seconds — `None`
    /// (or a non-positive value) falls back to the default interval.
    pub fn from_refresh_secs(refresh_interval_secs: Option<f64>) -> Self {
        match refresh_interval_secs {
            Some(secs) if secs > 0.0 => Self {
                refresh_interval: Duration::from_secs_f64(secs),
            },
            _ => Self::default(),
        }
    }
}

/// Format a single component stats line.
pub fn format_component_line(
    name: &str,
    group: &ProcessingStatsGroup,
    spinner_idx: usize,
) -> String {
    let total = group.num_execution_starts;
    let in_flight = group.num_in_progress();

    let icon = if in_flight > 0 {
        let ch = SPINNER_CHARS[spinner_idx % SPINNER_CHARS.len()];
        format!("{ch} ")
    } else {
        "✅".to_string()
    };

    let mut line = String::new();
    write!(&mut line, "{icon} {name}: {total} total").unwrap();

    if in_flight > 0 {
        write!(&mut line, ", {in_flight} in-flight").unwrap();
    }

    // Build breakdown
    let mut parts: Vec<String> = Vec::new();
    if group.num_adds > 0 {
        parts.push(format!("{} added", group.num_adds));
    }
    if group.num_reprocesses > 0 {
        parts.push(format!("{} reprocessed", group.num_reprocesses));
    }
    if group.num_deletes > 0 {
        parts.push(format!("{} deleted", group.num_deletes));
    }
    if group.num_unchanged > 0 {
        parts.push(format!("{} unchanged", group.num_unchanged));
    }
    if group.num_errors > 0 {
        parts.push(format!("{} ⚠️ errors", group.num_errors));
    }

    if !parts.is_empty() {
        write!(&mut line, " | {}", parts.join(", ")).unwrap();
    }

    line
}

/// Format the status/elapsed line.
pub fn format_status_line(
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

/// Truncate a string to fit within `width` terminal cells.
fn truncate_to_width(s: &str, width: usize) -> String {
    if s.len() <= width {
        return s.to_string();
    }
    s.chars().take(width).collect()
}

/// Get terminal width from a given fd, defaulting to 80.
#[cfg(unix)]
fn terminal_width_from_fd(fd: i32) -> usize {
    unsafe {
        let mut ws: nix::libc::winsize = std::mem::zeroed();
        if nix::libc::ioctl(fd, nix::libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 {
            return ws.ws_col as usize;
        }
    }
    80
}

#[cfg(not(unix))]
fn terminal_width_from_fd(_fd: i32) -> usize {
    80
}

/// Build the progress lines for rendering.
fn build_progress_lines(
    stats: &IndexMap<String, ProcessingStatsGroup>,
    live: bool,
    ready: bool,
    start_time: Instant,
    ready_time: &Option<Instant>,
    spinner_idx: usize,
    max_width: usize,
) -> Vec<String> {
    // Header labels the region so it's identifiable among interleaved group
    // log blocks ([Stats: <title>]) the PTY reader scrolls above it.
    let mut lines = vec![truncate_to_width("[Stats]", max_width)];
    for (name, group) in stats.iter() {
        let line = format_component_line(name, group, spinner_idx);
        lines.push(truncate_to_width(&line, max_width));
    }
    let status = format_status_line(
        live,
        ready,
        start_time.elapsed(),
        ready_time.map(|t| t.duration_since(start_time)),
    );
    lines.push(truncate_to_width(&status, max_width));
    lines
}

/// Print final stats summary to stdout.
fn print_final_stats(
    stats: &IndexMap<String, ProcessingStatsGroup>,
    live: bool,
    start_time: Instant,
    ready_time: &Option<Instant>,
) {
    println!("[Stats] (terminated)");
    for (name, group) in stats.iter() {
        // Use static checkmark for final output
        let line = format_component_line(name, group, 0);
        println!("{line}");
    }
    println!(
        "{}",
        format_status_line(
            live,
            true,
            start_time.elapsed(),
            ready_time.map(|t| t.duration_since(start_time))
        )
    );
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
/// Restores original fds and clears the global flag on drop.
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

    // Number of progress lines currently displayed
    let num_lines = Arc::new(std::sync::atomic::AtomicUsize::new(0));

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
    let reader_num_lines = num_lines.clone();

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
            let cur_lines = reader_num_lines.load(Ordering::Relaxed);

            let mut output = Vec::new();
            // Clear progress region
            if cur_lines > 0 {
                use std::io::Write;
                write!(&mut output, "\x1b[{}A", cur_lines).unwrap();
                for _ in 0..cur_lines {
                    write!(&mut output, "\r\x1b[2K\n").unwrap();
                }
                write!(&mut output, "\x1b[{}A", cur_lines).unwrap();
            }
            // Write captured output
            output.extend_from_slice(captured);
            // Redraw progress with current stats
            let snapshot = stats_clone.snapshot();
            let width = terminal_width_from_fd(reader_stdout_fd);
            let lines = build_progress_lines(
                &snapshot.stats,
                live,
                snapshot.ready,
                start_time,
                &None,
                0,
                width,
            );
            {
                use std::io::Write;
                for line in &lines {
                    write!(&mut output, "\r\x1b[2K{line}\n").unwrap();
                }
            }
            reader_num_lines.store(lines.len(), Ordering::Relaxed);
            write_to_fd(reader_stdout_fd, &output);
        }
        // Close our dup of saved_stdout and the master fd.
        unsafe {
            nix::libc::close(reader_stdout_fd);
            nix::libc::close(master_fd);
        }
    });

    // Display loop
    let mut spinner_idx: usize = 0;
    let mut ready_time: Option<Instant> = None;
    let mut sleep_fut = std::pin::pin!(tokio::time::sleep(options.refresh_interval));

    loop {
        // Wake on stats change OR refresh interval, whichever comes first.
        // This ensures the elapsed timer and spinner update regularly even when
        // no stats changes occur (e.g. slow components with no progress for a while).
        tokio::select! {
            result = handle.changed() => {
                let version = result?;
                if version >= TERMINATED_VERSION {
                    break;
                }
            }
            () = &mut sleep_fut => {}
        }

        // Reset the sleep future for the next iteration.
        sleep_fut.set(tokio::time::sleep(options.refresh_interval));

        let snapshot = handle.stats_snapshot();
        if snapshot.ready && ready_time.is_none() {
            ready_time = Some(Instant::now());
        }

        let width = terminal_width_from_fd(saved_stdout);
        let lines = build_progress_lines(
            &snapshot.stats,
            live,
            snapshot.ready,
            start_time,
            &ready_time,
            spinner_idx,
            width,
        );

        // Redraw progress region
        let cur_lines = num_lines.load(Ordering::Relaxed);
        let mut output = Vec::new();
        {
            use std::io::Write;
            if cur_lines > 0 {
                write!(&mut output, "\x1b[{}A", cur_lines).unwrap();
            }
            for line in &lines {
                write!(&mut output, "\r\x1b[2K{line}\n").unwrap();
            }
        }
        num_lines.store(lines.len(), Ordering::Relaxed);
        write_to_fd(saved_stdout, &output);

        spinner_idx += 1;
    }

    // Clear progress region before restoring fds
    let cur_lines = num_lines.load(Ordering::Relaxed);
    if cur_lines > 0 {
        let mut output = Vec::new();
        {
            use std::io::Write;
            write!(&mut output, "\x1b[{}A", cur_lines).unwrap();
            for _ in 0..cur_lines {
                write!(&mut output, "\r\x1b[2K\n").unwrap();
            }
            write!(&mut output, "\x1b[{}A", cur_lines).unwrap();
        }
        write_to_fd(saved_stdout, &output);
    }

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
    print_final_stats(&snapshot.stats, live, start_time, &ready_time);

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
/// headers each block (groups).
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

    let mut sleep_fut = std::pin::pin!(tokio::time::sleep(options.refresh_interval));
    loop {
        let terminated = tokio::select! {
            // Wakes only on termination, not on every stats change; the refresh
            // timer drives rendering.
            () = stats.wait_terminated() => true,
            () = &mut sleep_fut => {
                sleep_fut.set(tokio::time::sleep(options.refresh_interval));
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

        render_plain_block(title, terminated, &snapshot, live, start_time, &ready_time);
        if terminated {
            break;
        }
    }
}

/// Render one plain block: a `[Stats]` header for the root or `[Stats: <title>]`
/// for a group (with a `(terminated)` marker on the final one), the per-component
/// lines, the status line, and a trailing blank line.
fn render_plain_block(
    title: Option<&str>,
    terminated: bool,
    snapshot: &crate::engine::stats::VersionedProcessingStats,
    live: bool,
    start_time: Instant,
    ready_time: &Option<Instant>,
) {
    let marker = if terminated { " (terminated)" } else { "" };
    match title {
        Some(title) => println!("[Stats: {title}]{marker}"),
        None => println!("[Stats]{marker}"),
    }
    for (name, group) in snapshot.stats.iter() {
        println!("{}", format_component_line(name, group, 0));
    }
    println!(
        "{}",
        format_status_line(
            live,
            snapshot.ready,
            start_time.elapsed(),
            ready_time.map(|t| t.duration_since(start_time))
        )
    );
    println!();
}

/// Spawn a standalone plain reporter for a stats group's `ProcessingStats`.
/// Always plain scrolling output: when a root PTY display is active its reader
/// captures and interleaves these writes above the progress region; otherwise
/// they scroll directly. Self-terminates when the group terminates.
pub(crate) fn spawn_group_plain_report(
    stats: ProcessingStats,
    title: String,
    live: bool,
    refresh_interval_secs: Option<f64>,
) {
    get_runtime().spawn(async move {
        let options = ProgressDisplayOptions::from_refresh_secs(refresh_interval_secs);
        render_plain(&stats, Some(&title), live, &options, Instant::now()).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_component_line_in_progress() {
        let group = ProcessingStatsGroup {
            num_execution_starts: 42,
            num_adds: 12,
            num_unchanged: 22,
            ..Default::default()
        };
        let line = format_component_line("proc", &group, 0);
        assert!(line.contains("42 total"));
        assert!(line.contains("8 in-flight"));
        assert!(line.contains("12 added"));
        assert!(line.contains("22 unchanged"));
        assert!(line.starts_with('⠋'));
    }

    #[test]
    fn test_format_component_line_complete() {
        let group = ProcessingStatsGroup {
            num_execution_starts: 39,
            num_adds: 12,
            num_reprocesses: 5,
            num_unchanged: 22,
            ..Default::default()
        };
        let line = format_component_line("proc", &group, 0);
        assert!(line.starts_with("✅"));
        assert!(!line.contains("in-flight"));
        assert!(line.contains("39 total"));
    }

    #[test]
    fn test_format_component_line_with_errors() {
        let group = ProcessingStatsGroup {
            num_execution_starts: 37,
            num_adds: 12,
            num_unchanged: 22,
            num_errors: 3,
            ..Default::default()
        };
        let line = format_component_line("proc", &group, 0);
        assert!(line.starts_with("✅"));
        assert!(line.contains("3 ⚠️ errors"));
    }

    #[test]
    fn test_format_component_line_with_deletions() {
        let group = ProcessingStatsGroup {
            num_execution_starts: 45,
            num_adds: 12,
            num_unchanged: 22,
            num_deletes: 8,
            num_reprocesses: 3,
            ..Default::default()
        };
        let line = format_component_line("proc", &group, 0);
        assert!(line.contains("8 deleted"));
    }

    #[test]
    fn test_format_component_line_zero_counts_omitted() {
        let group = ProcessingStatsGroup {
            num_execution_starts: 5,
            num_adds: 5,
            ..Default::default()
        };
        let line = format_component_line("proc", &group, 0);
        assert!(line.contains("5 added"));
        assert!(!line.contains("unchanged"));
        assert!(!line.contains("deleted"));
        assert!(!line.contains("reprocessed"));
        assert!(!line.contains("errors"));
    }

    #[test]
    fn test_format_status_line_one_shot() {
        let line = format_status_line(false, false, Duration::from_secs_f64(12.3), None);
        assert_eq!(line, "⏳ Elapsed: 12.3s");
    }

    #[test]
    fn test_format_status_line_live_catching_up() {
        let line = format_status_line(true, false, Duration::from_secs_f64(12.3), None);
        assert_eq!(line, "⏳ Elapsed: 12.3s | Catching up...");
    }

    #[test]
    fn test_format_status_line_live_ready() {
        let line = format_status_line(
            true,
            true,
            Duration::from_secs_f64(45.2),
            Some(Duration::from_secs_f64(12.3)),
        );
        assert_eq!(line, "⏳ Ready (took 12.3s) | Watching for changes...");
    }

    #[test]
    fn test_format_line_truncation() {
        let s = "This is a very long line that should be truncated at some point";
        let truncated = truncate_to_width(s, 20);
        assert_eq!(truncated.len(), 20);
    }
}
