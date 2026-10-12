//! Run a user binary as a `__coco` protocol child and read its records.

use std::io::Write as _;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use cocoindex::cli::protocol::{ConfirmAction, Event, Line, MARKER, PROTOCOL_VERSION, decode_line};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::error::{CliError, CliResult, fail};

/// What the child wrote on stdout, in order.
pub enum Output<'a> {
    /// The user's own output, to forward.
    Text(&'a str),
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
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(fail(&format!("could not run {}", exe.display())))?;
    let pid = child.id();
    let mut child_stdin = child.stdin.take();
    let mut answer: Option<tokio::sync::oneshot::Receiver<bool>> = None;
    let mut reader = BufReader::new(child.stdout.take().expect("stdout is piped"));

    let mut buf = Vec::new();
    let mut greeted = false;
    let mut mismatch: Option<CliError> = None;
    let mut terminal = None;
    let mut ctrl_c_count = 0u32;
    loop {
        tokio::select! {
            read = reader.read_until(b'\n', &mut buf) => {
                let n = read.map_err(fail("reading the target binary's output"))?;
                if n == 0 {
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
                        eprintln!("warning: ignoring a malformed protocol line: {raw}");
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
                        eprintln!("\nStopping...");
                    }
                    forward_interrupt(pid);
                } else {
                    let _ = child.start_kill();
                }
            }
        }
    }
    let status = child
        .wait()
        .await
        .map_err(fail("waiting for the target binary"))?;
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
