//! Run a user binary as a `__coco` protocol child and read its records.

use std::io::Write as _;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use cocoindex::cli::protocol::{ConfirmAction, Event, Line, MARKER, PROTOCOL_VERSION, decode_line};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::error::{CliError, CliResult, fail};

/// How long to wait for the rest of the child's stderr after it exited. A
/// process it left behind can hold the pipe open, so end of file may never come.
const STDERR_DRAIN: Duration = Duration::from_millis(200);

/// What the child wrote, in order for each stream.
pub enum Output<'a> {
    /// The user's own stdout output, to forward.
    Text(&'a str),
    /// A line of the child's stderr (engine logs, the user's `eprintln!`), to
    /// forward. It comes through here so that the progress block, drawn on the
    /// same stream, can make room for it.
    ErrorText(&'a str),
    /// A protocol record after the handshake (including the terminal one).
    Event(Event),
}

pub struct Outcome {
    pub status: ExitStatus,
    /// The terminal `done` or `error` record, if one arrived.
    pub terminal: Option<Event>,
}

impl Outcome {
    /// The terminal `done` record, or the error the child reported.
    pub fn into_done(self) -> CliResult<Event> {
        match self.terminal {
            Some(Event::Error { message }) => Err(CliError {
                code: match self.status.code() {
                    Some(2) => 2,
                    _ => 1,
                },
                message,
            }),
            Some(done) => Ok(done),
            None => Err(CliError::failure(format!(
                "the target binary exited ({}) without reporting a result",
                self.status
            ))),
        }
    }
}

/// How the child reacts to Ctrl-C.
#[derive(Clone, Copy)]
pub enum Interrupt {
    /// Default handling: Ctrl-C ends both processes.
    Default,
    /// First Ctrl-C asks the child to stop (it cancels the run and reports
    /// `done {cancelled}`); the second kills it.
    Cancel { quiet: bool },
}

/// The question for a `confirm` record; the user must type `yes`.
fn confirm_prompt(action: ConfirmAction, app: &str) -> String {
    match action {
        ConfirmAction::Drop => format!("Type 'yes' to drop app '{app}' and all its target states"),
        ConfirmAction::DropDbOnly => format!(
            "Type 'yes' to clear the state of app '{app}' (its target states are NOT reverted)"
        ),
        ConfirmAction::Reset => format!("Type 'yes' to reset app '{app}' (drop existing state)"),
    }
}

/// Prompt on this process's terminal from a plain thread, so a pending read
/// neither blocks the protocol loop (Ctrl-C still works) nor holds up exit.
fn ask(prompt: String) -> tokio::sync::oneshot::Receiver<bool> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut out = std::io::stdout().lock();
        let _ = write!(out, "{prompt}: ");
        let _ = out.flush();
        drop(out);
        let mut answer = String::new();
        let yes = std::io::stdin().read_line(&mut answer).is_ok()
            && answer.trim().eq_ignore_ascii_case("yes");
        let _ = tx.send(yes);
    });
    rx
}

async fn pending_answer(answer: &mut Option<tokio::sync::oneshot::Receiver<bool>>) -> bool {
    match answer {
        Some(rx) => {
            let yes = rx.await.unwrap_or(false);
            *answer = None;
            yes
        }
        None => std::future::pending().await,
    }
}

/// Forward what a finished read left in `buf`, and return whether the stream is
/// still open: the end of the stream, or a failed read, ends the forwarding, not
/// the run. `buf` decides what to forward, not the count: a read that `select!`
/// cancelled leaves its bytes there, so a last line with no newline can arrive
/// with a count of 0.
fn forward_stderr(
    read: std::io::Result<usize>,
    buf: &mut Vec<u8>,
    on_output: &mut impl FnMut(Output<'_>),
) -> bool {
    if !buf.is_empty() {
        let text = String::from_utf8_lossy(buf).into_owned();
        buf.clear();
        on_output(Output::ErrorText(text.trim_end_matches(['\n', '\r'])));
    }
    matches!(read, Ok(n) if n > 0)
}

/// Run `<exe> __coco <verb> --protocol N <flags>` and feed its output to
/// `on_output`. The first record must be a matching `hello`. A `confirm`
/// record (sent when `flags` has `--confirm`) is answered here: the user is
/// prompted and the answer goes to the child's stdin.
pub async fn run(
    exe: &Path,
    verb: &str,
    flags: &[String],
    interrupt: Interrupt,
    mut on_output: impl FnMut(Output<'_>),
) -> CliResult<Outcome> {
    let confirming = flags.iter().any(|f| f == "--confirm");
    let mut interrupts = match interrupt {
        Interrupt::Default => None,
        Interrupt::Cancel { .. } => Some(Interrupts::install()?),
    };

    let mut command = tokio::process::Command::new(exe);
    // Engine logs default to warnings, like the Python CLI
    // (COCOINDEX_LOG_LEVEL=WARNING); RUST_LOG still wins when set.
    if std::env::var_os("RUST_LOG").is_none() {
        command.env("RUST_LOG", "warn");
    }
    let mut child = command
        .arg(MARKER)
        .arg(verb)
        .arg("--protocol")
        .arg(PROTOCOL_VERSION.to_string())
        .args(flags)
        .stdin(if confirming {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(fail(&format!("could not run {}", exe.display())))?;
    let pid = child.id();
    let mut child_stdin = child.stdin.take();
    let mut answer: Option<tokio::sync::oneshot::Receiver<bool>> = None;
    let Some(stdout) = child.stdout.take() else {
        return Err(CliError::failure(
            "could not read the target binary's output",
        ));
    };
    let mut reader = BufReader::new(stdout);
    let Some(stderr) = child.stderr.take() else {
        return Err(CliError::failure(
            "could not read the target binary's error output",
        ));
    };
    let mut err_reader = BufReader::new(stderr);
    let mut err_buf = Vec::new();
    let mut err_open = true;

    let mut buf = Vec::new();
    let mut greeted = false;
    let mut mismatch: Option<CliError> = None;
    let mut terminal = None;
    let mut ctrl_c_count = 0u32;
    loop {
        tokio::select! {
            read = err_reader.read_until(b'\n', &mut err_buf), if err_open => {
                err_open = forward_stderr(read, &mut err_buf, &mut on_output);
            }
            read = reader.read_until(b'\n', &mut buf) => {
                let n = read.map_err(fail("reading the target binary's output"))?;
                // A cancelled read can leave a last line here with a count of 0.
                if n == 0 && buf.is_empty() {
                    break;
                }
                let text = String::from_utf8_lossy(&buf).into_owned();
                buf.clear();
                let line = text.trim_end_matches(['\n', '\r']);
                match decode_line(line) {
                    Line::Text(t) => on_output(Output::Text(t)),
                    Line::Malformed { text, raw } => {
                        if !text.is_empty() {
                            on_output(Output::Text(text));
                        }
                        on_output(Output::ErrorText(&format!(
                            "warning: ignoring a malformed protocol line: {raw}"
                        )));
                    }
                    Line::Record { text, event } => {
                        if !text.is_empty() {
                            on_output(Output::Text(text));
                        }
                        if !greeted {
                            match event {
                                Event::Hello { protocol, .. } if protocol == PROTOCOL_VERSION => {
                                    greeted = true;
                                }
                                Event::Hello { protocol, .. } => {
                                    mismatch = Some(CliError::usage(format!(
                                        "the target binary speaks cocoindex protocol {protocol}; \
                                         this cocoindex supports {PROTOCOL_VERSION}. Rebuild it \
                                         against a matching cocoindex SDK."
                                    )));
                                    let _ = child.start_kill();
                                }
                                _ => {
                                    mismatch = Some(CliError::usage(
                                        "the target binary did not start the cocoindex protocol \
                                         with a hello record",
                                    ));
                                    let _ = child.start_kill();
                                }
                            }
                            continue;
                        }
                        if let Event::Confirm { action, app } = &event {
                            if child_stdin.is_none() || answer.is_some() {
                                mismatch = Some(CliError::usage(
                                    "the target binary asked for a confirmation that was not expected",
                                ));
                                let _ = child.start_kill();
                                continue;
                            }
                            answer = Some(ask(confirm_prompt(*action, app)));
                            continue;
                        }
                        if matches!(event, Event::Done { .. } | Event::Error { .. }) {
                            terminal = Some(event.clone());
                        }
                        on_output(Output::Event(event));
                    }
                }
            }
            yes = pending_answer(&mut answer) => {
                if let Some(stdin) = child_stdin.as_mut() {
                    let reply: &[u8] = if yes { b"yes\n" } else { b"no\n" };
                    // A closed pipe means the child already ended.
                    let _ = stdin.write_all(reply).await;
                    let _ = stdin.flush().await;
                }
            }
            Some(()) = next_interrupt(&mut interrupts) => {
                ctrl_c_count += 1;
                if ctrl_c_count == 1 {
                    if let Interrupt::Cancel { quiet: false } = interrupt {
                        on_output(Output::ErrorText("\nStopping..."));
                    }
                    forward_interrupt(pid);
                } else {
                    let _ = child.start_kill();
                }
            }
        }
    }
    // The child can write to stderr while it shuts down, with stdout closed.
    let status = loop {
        tokio::select! {
            status = child.wait() => {
                break status.map_err(fail("waiting for the target binary"))?;
            }
            read = err_reader.read_until(b'\n', &mut err_buf), if err_open => {
                err_open = forward_stderr(read, &mut err_buf, &mut on_output);
            }
        }
    };
    let _ = tokio::time::timeout(STDERR_DRAIN, async {
        while err_open {
            let read = err_reader.read_until(b'\n', &mut err_buf).await;
            err_open = forward_stderr(read, &mut err_buf, &mut on_output);
        }
    })
    .await;
    // The drain can time out in the middle of a line.
    forward_stderr(Ok(0), &mut err_buf, &mut on_output);
    if let Some(err) = mismatch {
        return Err(err);
    }
    if !greeted {
        return Err(CliError::usage(format!(
            "{} did not answer the cocoindex protocol ({status}). Mark its main \
             with #[cocoindex::main], or call cocoindex::cli::try_dispatch() first in main.",
            exe.display()
        )));
    }
    Ok(Outcome { status, terminal })
}

/// Ctrl-C notifications, installed before the child starts so the first
/// Ctrl-C does not end this process.
struct Interrupts {
    #[cfg(unix)]
    signal: tokio::signal::unix::Signal,
    #[cfg(not(unix))]
    rx: tokio::sync::mpsc::Receiver<()>,
}

impl Interrupts {
    fn install() -> CliResult<Self> {
        #[cfg(unix)]
        {
            let signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(fail("installing the Ctrl-C handler"))?;
            Ok(Self { signal })
        }
        #[cfg(not(unix))]
        {
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                while tokio::signal::ctrl_c().await.is_ok() {
                    if tx.send(()).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Self { rx })
        }
    }

    async fn recv(&mut self) -> Option<()> {
        #[cfg(unix)]
        {
            self.signal.recv().await
        }
        #[cfg(not(unix))]
        {
            self.rx.recv().await
        }
    }
}

async fn next_interrupt(interrupts: &mut Option<Interrupts>) -> Option<()> {
    match interrupts {
        Some(interrupts) => interrupts.recv().await,
        None => std::future::pending().await,
    }
}

/// Pass the Ctrl-C on to the child (it may also get one from the terminal;
/// the child treats repeats as one request).
fn forward_interrupt(pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid.and_then(|p| i32::try_from(p).ok()) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGINT,
        );
    }
    #[cfg(not(unix))]
    let _ = pid; // The console delivers Ctrl-C to the whole process group.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_stderr_keeps_a_last_line_whose_count_was_lost() {
        let mut forwarded = Vec::new();
        let mut on_output = |output: Output<'_>| {
            if let Output::ErrorText(text) = output {
                forwarded.push(text.to_string());
            }
        };
        let mut buf = b"a line\r\n".to_vec();
        assert!(forward_stderr(Ok(8), &mut buf, &mut on_output));
        // A cancelled read left these bytes; the next read finds end of file.
        buf.extend_from_slice(b"fatal");
        assert!(!forward_stderr(Ok(0), &mut buf, &mut on_output));
        assert!(!forward_stderr(Ok(0), &mut buf, &mut on_output));
        assert_eq!(forwarded, ["a line", "fatal"]);
    }

    #[test]
    fn confirm_prompts_match_the_python_cli() {
        assert_eq!(
            confirm_prompt(ConfirmAction::Drop, "A"),
            "Type 'yes' to drop app 'A' and all its target states"
        );
        assert_eq!(
            confirm_prompt(ConfirmAction::Reset, "A"),
            "Type 'yes' to reset app 'A' (drop existing state)"
        );
        assert!(confirm_prompt(ConfirmAction::DropDbOnly, "A").contains("NOT reverted"));
    }
}
