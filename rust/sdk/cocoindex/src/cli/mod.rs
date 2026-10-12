//! The user-binary side of the `cocoindex` CLI.
//!
//! The standalone `cocoindex` binary (crate `rust/cli`) builds a user's crate
//! and runs its binary as `<bin> __coco <verb> --protocol 1 [flags]`.
//! [`try_dispatch`] recognizes that invocation, runs the verb against the
//! environment the binary's `#[cocoindex::lifespan]`s describe, and answers
//! over the stdio protocol in [`protocol`]. `#[cocoindex::main]` calls it
//! before the user's own `main` body; a hand-written `main` can call it
//! directly as its first statement.

pub mod protocol;

use std::future::Future;
use std::io::Write;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::app::{Environment, Progress, UpdateOptions};
use crate::error::Error;
use crate::registry::{self, AppEntry};
use protocol::{
    AppInfo, ConfirmAction, Event, MARKER, PROTOCOL_VERSION, PreviewKind, RunStatsWire, encode,
};

/// Exit codes of a protocol child: success, failure, usage or protocol error.
const EXIT_OK: u8 = 0;
const EXIT_ERROR: u8 = 1;
const EXIT_USAGE: u8 = 2;

/// Set by the Ctrl-C task once cancellation has been requested, so a
/// cancelled run is reported as `done {cancelled: true}` and not as an error.
static CANCELLED: AtomicBool = AtomicBool::new(false);

/// If this process was started as a `__coco` protocol child, run the verb and
/// return the exit code the process should end with. Returns `None` when the
/// arguments are the user's own, so a custom `main` can continue:
///
/// ```ignore
/// #[tokio::main]
/// async fn main() -> cocoindex::Result<()> {
///     if let Some(code) = cocoindex::cli::try_dispatch().await {
///         std::process::exit(code_to_i32(code));
///     }
///     // ... the program's own behaviour ...
/// }
/// ```
///
/// `#[cocoindex::main]` does this for you.
pub async fn try_dispatch() -> Option<ExitCode> {
    let mut args = std::env::args();
    let _program = args.next();
    match args.next() {
        Some(marker) if marker == MARKER => Some(dispatch(args.collect()).await),
        _ => None,
    }
}

/// Run a protocol verb with `args` (everything after `__coco`), writing records
/// to stdout.
pub async fn dispatch(args: Vec<String>) -> ExitCode {
    let mut out = std::io::stdout();
    dispatch_to(args, &mut out).await
}

/// [`dispatch`] with an explicit record sink (tests capture the stream).
pub async fn dispatch_to(args: Vec<String>, out: &mut (dyn Write + Send)) -> ExitCode {
    let mut emitter = Emitter { out };
    emitter.emit(&Event::Hello {
        protocol: PROTOCOL_VERSION,
        sdk: env!("CARGO_PKG_VERSION").to_string(),
    });
    let parsed = match ChildArgs::parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => return emitter.fail(message, EXIT_USAGE),
    };
    if parsed.protocol != PROTOCOL_VERSION {
        return emitter.fail(
            format!(
                "this binary speaks cocoindex protocol {PROTOCOL_VERSION}, the CLI asked for {}",
                parsed.protocol
            ),
            EXIT_USAGE,
        );
    }
    let outcome = match parsed.verb {
        Verb::Ls => run_ls(&mut emitter).await,
        Verb::Update => run_update(&mut emitter, &parsed).await,
        Verb::Drop => run_drop(&mut emitter, &parsed).await,
    };
    match outcome {
        Ok(code) => ExitCode::from(code),
        Err(Failure::Usage(message)) => emitter.fail(message, EXIT_USAGE),
        Err(Failure::Error(err)) => emitter.fail(err.to_string(), EXIT_ERROR),
    }
}

// ---------------------------------------------------------------------------
// Child-side argument parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verb {
    Ls,
    Update,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildArgs {
    verb: Verb,
    protocol: u32,
    app: Option<String>,
    live: bool,
    full_reprocess: bool,
    preview: bool,
    reset: bool,
    db_only: bool,
    /// Ask the CLI (`confirm` record, answer on stdin) before destroying state.
    confirm: bool,
}

impl ChildArgs {
    fn parse(args: &[String]) -> std::result::Result<Self, String> {
        let mut iter = args.iter();
        let verb = match iter.next().map(String::as_str) {
            Some("ls") => Verb::Ls,
            Some("update") => Verb::Update,
            Some("drop") => Verb::Drop,
            Some(other) => return Err(format!("unknown __coco verb `{other}`")),
            None => return Err("missing __coco verb (ls, update or drop)".to_string()),
        };
        let mut parsed = ChildArgs {
            verb,
            protocol: PROTOCOL_VERSION,
            app: None,
            live: false,
            full_reprocess: false,
            preview: false,
            reset: false,
            db_only: false,
            confirm: false,
        };
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--protocol" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| "--protocol needs a value".to_string())?;
                    parsed.protocol = value
                        .parse()
                        .map_err(|_| format!("--protocol needs an integer, got {value:?}"))?;
                }
                "--app" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| "--app needs a value".to_string())?;
                    parsed.app = Some(value.clone());
                }
                "--live" => parsed.live = true,
                "--full-reprocess" => parsed.full_reprocess = true,
                "--preview" => parsed.preview = true,
                "--reset" => parsed.reset = true,
                "--db-only" => parsed.db_only = true,
                "--confirm" => parsed.confirm = true,
                other => return Err(format!("unknown __coco flag `{other}`")),
            }
        }
        if parsed.preview && (parsed.reset || parsed.live) {
            return Err("--preview cannot be combined with --reset or --live".to_string());
        }
        let flag_allowed = |flag: &str, ok: bool| {
            if ok {
                Ok(())
            } else {
                Err(format!("`{flag}` is not valid for `{}`", verb_name(verb)))
            }
        };
        let is_update = verb == Verb::Update;
        flag_allowed("--live", !parsed.live || is_update)?;
        flag_allowed("--full-reprocess", !parsed.full_reprocess || is_update)?;
        flag_allowed("--preview", !parsed.preview || is_update)?;
        flag_allowed("--reset", !parsed.reset || is_update)?;
        flag_allowed("--db-only", !parsed.db_only || verb == Verb::Drop)?;
        flag_allowed("--app", parsed.app.is_none() || verb != Verb::Ls)?;
        flag_allowed(
            "--confirm",
            !parsed.confirm || verb == Verb::Drop || (is_update && parsed.reset),
        )?;
        Ok(parsed)
    }
}

fn verb_name(verb: Verb) -> &'static str {
    match verb {
        Verb::Ls => "ls",
        Verb::Update => "update",
        Verb::Drop => "drop",
    }
}

// ---------------------------------------------------------------------------
// Verbs
// ---------------------------------------------------------------------------

enum Failure {
    Usage(String),
    Error(Error),
}

impl From<Error> for Failure {
    fn from(err: Error) -> Self {
        Failure::Error(err)
    }
}

struct Emitter<'a> {
    out: &'a mut (dyn Write + Send),
}

impl Emitter<'_> {
    fn emit(&mut self, event: &Event) {
        // A broken pipe means the CLI went away; there is nobody to tell.
        let _ = self.out.write_all(encode(event).as_bytes());
        let _ = self.out.flush();
    }

    fn fail(&mut self, message: String, code: u8) -> ExitCode {
        self.emit(&Event::Error { message });
        ExitCode::from(code)
    }
}

async fn open_environment() -> std::result::Result<Environment, Failure> {
    Ok(Environment::from_registered().await?)
}

async fn run_ls(emitter: &mut Emitter<'_>) -> std::result::Result<u8, Failure> {
    let env = open_environment().await?;
    let persisted = env.persisted_app_names().await?;
    let mut apps: Vec<AppInfo> = registry::apps()
        .iter()
        .map(|entry| AppInfo {
            name: entry.name.to_string(),
            persisted: persisted.iter().any(|p| p == entry.name),
            registered: true,
        })
        .collect();
    for name in persisted {
        if registry::find_app(&name).is_none() {
            apps.push(AppInfo {
                name,
                persisted: true,
                registered: false,
            });
        }
    }
    apps.sort_by(|a, b| a.name.cmp(&b.name));
    emitter.emit(&Event::Apps {
        apps,
        db_path: env.db_path().display().to_string(),
    });
    env.shutdown().await;
    Ok(EXIT_OK)
}

/// The app an `update`/`drop` acts on: `--app NAME`, else the binary's only
/// registered app.
fn resolve_app(requested: Option<&str>) -> std::result::Result<&'static AppEntry, Failure> {
    let apps = registry::apps();
    match requested {
        Some(name) => registry::find_app(name).ok_or_else(|| {
            Failure::Usage(format!(
                "no #[cocoindex::app] named `{name}` in this binary; available: {}",
                app_names(apps)
            ))
        }),
        None => match apps {
            [only] => Ok(only),
            [] => Err(Failure::Usage(
                "this binary has no #[cocoindex::app]".to_string(),
            )),
            many => Err(Failure::Usage(format!(
                "this binary has several apps; pass the name: {}",
                app_names(many)
            ))),
        },
    }
}

fn app_names(apps: &[AppEntry]) -> String {
    let mut names: Vec<&str> = apps.iter().map(|a| a.name).collect();
    names.sort_unstable();
    names.join(", ")
}

/// Turn the first Ctrl-C into engine cancellation. Every later Ctrl-C is a
/// repeat (the terminal and the CLI may both deliver one); the CLI decides
/// when to kill the child outright.
fn watch_ctrl_c() {
    tokio::spawn(async {
        while tokio::signal::ctrl_c().await.is_ok() {
            CANCELLED.store(true, Ordering::SeqCst);
            cocoindex_core::engine::runtime::cancel_all();
        }
    });
}

/// Start a cancellable verb (`update`, `drop`) before anything else runs, so
/// a Ctrl-C during the lifespan is a cancel too: clear any earlier
/// cancellation and turn Ctrl-C into engine cancellation.
fn arm_cancellation() {
    cocoindex_core::engine::runtime::reset_global_cancellation();
    CANCELLED.store(false, Ordering::SeqCst);
    watch_ctrl_c();
}

fn cancelled() -> bool {
    CANCELLED.load(Ordering::SeqCst)
}

/// Run `fut` unless Ctrl-C cancels first (`None`). Dropping the future stops
/// a lifespan or a prompt mid-way; the process ends right after.
async fn until_cancelled<T>(fut: impl Future<Output = T>) -> Option<T> {
    let token = cocoindex_core::engine::runtime::global_cancellation_token();
    tokio::select! {
        value = fut => Some(value),
        () = token.cancelled() => None,
    }
}

fn emit_done(emitter: &mut Emitter<'_>, stats: Option<RunStatsWire>, dropped: Option<bool>) {
    emitter.emit(&Event::Done {
        stats,
        cancelled: false,
        dropped,
        aborted: false,
    });
}

fn emit_cancelled(emitter: &mut Emitter<'_>) -> std::result::Result<u8, Failure> {
    emitter.emit(&Event::Done {
        stats: None,
        cancelled: true,
        dropped: None,
        aborted: false,
    });
    Ok(EXIT_OK)
}

fn emit_aborted(
    emitter: &mut Emitter<'_>,
    dropped: Option<bool>,
) -> std::result::Result<u8, Failure> {
    emitter.emit(&Event::Done {
        stats: None,
        cancelled: false,
        dropped,
        aborted: true,
    });
    Ok(EXIT_OK)
}

/// A failed step after Ctrl-C is the cancellation surfacing (the engine
/// reports a cancelled operation as an error), not a failure.
fn cancel_or(
    emitter: &mut Emitter<'_>,
    outcome: std::result::Result<u8, Failure>,
) -> std::result::Result<u8, Failure> {
    match outcome {
        Err(Failure::Error(_)) if cancelled() => emit_cancelled(emitter),
        other => other,
    }
}

enum Answer {
    Yes,
    No,
    Cancelled,
}

/// Ask the CLI to confirm `action`, and wait for its one-line answer on stdin
/// (EOF counts as `no`). The read runs on a plain thread so a pending read
/// never holds up the runtime's shutdown.
async fn confirm(emitter: &mut Emitter<'_>, action: ConfirmAction, app: &str) -> Answer {
    emitter.emit(&Event::Confirm {
        action,
        app: app.to_string(),
    });
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let yes = std::io::stdin().read_line(&mut line).is_ok() && line.trim() == "yes";
        let _ = tx.send(yes);
    });
    match until_cancelled(rx).await {
        None => Answer::Cancelled,
        Some(Ok(true)) => Answer::Yes,
        Some(_) => Answer::No,
    }
}

async fn run_update(
    emitter: &mut Emitter<'_>,
    args: &ChildArgs,
) -> std::result::Result<u8, Failure> {
    let entry = resolve_app(args.app.as_deref())?;
    arm_cancellation();
    let Some(env) = until_cancelled(open_environment()).await else {
        return emit_cancelled(emitter);
    };
    let env = env?;
    emitter.emit(&Event::Start {
        app: entry.name.to_string(),
        db_path: env.db_path().display().to_string(),
    });
    let outcome = update_app(emitter, args, entry, &env).await;
    env.shutdown().await;
    cancel_or(emitter, outcome)
}

async fn update_app(
    emitter: &mut Emitter<'_>,
    args: &ChildArgs,
    entry: &'static AppEntry,
    env: &Environment,
) -> std::result::Result<u8, Failure> {
    let app = env.app(entry.name).await?;
    if args.reset
        && env
            .persisted_app_names()
            .await?
            .iter()
            .any(|p| p == entry.name)
    {
        if args.confirm {
            match confirm(emitter, ConfirmAction::Reset, entry.name).await {
                Answer::Yes => {}
                Answer::No => return emit_aborted(emitter, None),
                Answer::Cancelled => return emit_cancelled(emitter),
            }
        }
        app.drop_state().await?;
    }
    let started = Instant::now();
    let options = UpdateOptions {
        full_reprocess: args.full_reprocess,
        live: args.live,
        preview: args.preview,
        ..UpdateOptions::default()
    };
    let mut handle = app.start_update_with_options(options, entry.run)?;
    // A failed wait means the run ended; `result()` below reports why.
    while let Ok(Progress::Changed(version)) = handle.changed().await {
        emitter.emit(&Event::progress(version, &handle.detailed_stats_snapshot()));
    }
    // Final counters; `u64::MAX` is the engine's "terminated" version.
    let final_stats = handle.detailed_stats_snapshot();
    emitter.emit(&Event::progress(u64::MAX, &final_stats));

    if args.preview {
        for action in &handle.preview_actions().await? {
            emitter.emit(&preview_event(action));
        }
        emit_done(emitter, None, None);
        return Ok(EXIT_OK);
    }
    handle.result().await?;
    let mut stats = final_stats.run_stats();
    stats.elapsed = started.elapsed();
    emit_done(emitter, Some(RunStatsWire::from(&stats)), None);
    Ok(EXIT_OK)
}

async fn run_drop(emitter: &mut Emitter<'_>, args: &ChildArgs) -> std::result::Result<u8, Failure> {
    arm_cancellation();
    let Some(env) = until_cancelled(open_environment()).await else {
        return emit_cancelled(emitter);
    };
    let env = env?;
    let outcome = drop_app(emitter, args, &env).await;
    env.shutdown().await;
    cancel_or(emitter, outcome)
}

async fn drop_app(
    emitter: &mut Emitter<'_>,
    args: &ChildArgs,
    env: &Environment,
) -> std::result::Result<u8, Failure> {
    let persisted = env.persisted_app_names().await?;
    // A persisted app whose code is gone can still be dropped by name.
    let name: String = match &args.app {
        Some(name) if persisted.iter().any(|p| p == name) => name.clone(),
        requested => resolve_app(requested.as_deref())?.name.to_string(),
    };
    emitter.emit(&Event::Start {
        app: name.clone(),
        db_path: env.db_path().display().to_string(),
    });
    if !persisted.contains(&name) {
        emit_done(emitter, None, Some(false));
        return Ok(EXIT_OK);
    }
    if args.confirm {
        let action = if args.db_only {
            ConfirmAction::DropDbOnly
        } else {
            ConfirmAction::Drop
        };
        match confirm(emitter, action, &name).await {
            Answer::Yes => {}
            Answer::No => return emit_aborted(emitter, Some(false)),
            Answer::Cancelled => return emit_cancelled(emitter),
        }
    }
    if args.db_only {
        env.drop_app_db_only(&name).await?;
    } else {
        env.app(&name).await?.drop_state().await?;
    }
    emit_done(emitter, None, Some(true));
    Ok(EXIT_OK)
}

/// Decode a preview action's payload for the `preview` record.
pub(crate) fn preview_event(action: &crate::app::PreviewAction) -> Event {
    use crate::app::PreviewAction;
    let (kind, value) = match action {
        PreviewAction::Create(v) => (PreviewKind::Create, v),
        PreviewAction::Update(v) => (PreviewKind::Update, v),
        PreviewAction::Delete(v) => (PreviewKind::Delete, v),
    };
    let value = value
        .decode::<serde_json::Value>()
        .unwrap_or_else(|e| serde_json::Value::String(format!("<undecodable: {e}>")));
    Event::Preview { kind, value }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_update_flags() {
        let parsed = ChildArgs::parse(&args(&[
            "update",
            "--protocol",
            "1",
            "--app",
            "A",
            "--live",
            "--full-reprocess",
        ]))
        .unwrap();
        assert_eq!(parsed.verb, Verb::Update);
        assert_eq!(parsed.protocol, 1);
        assert_eq!(parsed.app.as_deref(), Some("A"));
        assert!(parsed.live && parsed.full_reprocess);
        assert!(!parsed.preview && !parsed.reset && !parsed.db_only);
    }

    #[test]
    fn rejects_preview_with_reset_or_live() {
        let err = ChildArgs::parse(&args(&["update", "--preview", "--reset"])).unwrap_err();
        assert!(err.contains("--preview cannot be combined"));
        let err = ChildArgs::parse(&args(&["update", "--preview", "--live"])).unwrap_err();
        assert!(err.contains("--preview cannot be combined"));
    }

    #[test]
    fn rejects_flags_on_the_wrong_verb() {
        let err = ChildArgs::parse(&args(&["drop", "--live"])).unwrap_err();
        assert!(err.contains("not valid for `drop`"));
        let err = ChildArgs::parse(&args(&["update", "--db-only"])).unwrap_err();
        assert!(err.contains("not valid for `update`"));
        let err = ChildArgs::parse(&args(&["ls", "--app", "A"])).unwrap_err();
        assert!(err.contains("not valid for `ls`"));
    }

    #[test]
    fn rejects_unknown_input() {
        assert!(
            ChildArgs::parse(&args(&[]))
                .unwrap_err()
                .contains("missing")
        );
        assert!(
            ChildArgs::parse(&args(&["sync"]))
                .unwrap_err()
                .contains("unknown __coco verb")
        );
        assert!(
            ChildArgs::parse(&args(&["ls", "--bogus"]))
                .unwrap_err()
                .contains("unknown __coco flag")
        );
        assert!(
            ChildArgs::parse(&args(&["ls", "--protocol", "x"]))
                .unwrap_err()
                .contains("integer")
        );
    }

    #[tokio::test]
    async fn protocol_mismatch_is_a_usage_error() {
        let mut out: Vec<u8> = Vec::new();
        let code = dispatch_to(args(&["ls", "--protocol", "99"]), &mut out).await;
        assert_eq!(code, ExitCode::from(EXIT_USAGE));
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert!(matches!(
            protocol::decode_line(lines[0]),
            protocol::Line::Record {
                event: Event::Hello {
                    protocol: PROTOCOL_VERSION,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            protocol::decode_line(lines[1]),
            protocol::Line::Record {
                event: Event::Error { .. },
                ..
            }
        ));
    }
}
