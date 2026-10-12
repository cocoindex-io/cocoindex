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

use std::io::Write;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::app::{Environment, Progress, UpdateOptions};
use crate::error::{Error, Result};
use crate::registry::{self, AppEntry};
use protocol::{AppInfo, Event, MARKER, PROTOCOL_VERSION, PreviewKind, RunStatsWire, encode};

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

/// Converts a `#[cocoindex::main]` body's return value into the process exit
/// code. Implemented for `()`, `std::process::ExitCode` and `Result<(), E>`.
#[doc(hidden)]
pub trait IntoMainExit {
    fn into_exit_code(self) -> ExitCode;
}

impl IntoMainExit for () {
    fn into_exit_code(self) -> ExitCode {
        ExitCode::SUCCESS
    }
}

impl IntoMainExit for ExitCode {
    fn into_exit_code(self) -> ExitCode {
        self
    }
}

impl<E: std::fmt::Display> IntoMainExit for std::result::Result<(), E> {
    fn into_exit_code(self) -> ExitCode {
        match self {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        }
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
    let names: Vec<&str> = apps.iter().map(|a| a.name).collect();
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

async fn run_update(
    emitter: &mut Emitter<'_>,
    args: &ChildArgs,
) -> std::result::Result<u8, Failure> {
    let entry = resolve_app(args.app.as_deref())?;
    let env = open_environment().await?;
    emitter.emit(&Event::Start {
        app: entry.name.to_string(),
        db_path: env.db_path().display().to_string(),
    });
    cocoindex_core::engine::runtime::reset_global_cancellation();
    CANCELLED.store(false, Ordering::SeqCst);
    watch_ctrl_c();

    let outcome = update_app(emitter, args, entry, &env).await;
    env.shutdown().await;
    outcome
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
        let actions = match handle.preview_actions().await {
            Ok(actions) => actions,
            Err(err) => return finish(emitter, Err(err), None),
        };
        for action in &actions {
            emitter.emit(&preview_event(action));
        }
        return finish(emitter, Ok(()), None);
    }
    let result = handle.result().await;
    let mut stats = final_stats.run_stats();
    stats.elapsed = started.elapsed();
    finish(emitter, result, Some(RunStatsWire::from(&stats)))
}

/// Emit the terminal record for an update: `done` (with stats, or
/// `cancelled` after a Ctrl-C) or `error`.
fn finish(
    emitter: &mut Emitter<'_>,
    result: Result<()>,
    stats: Option<RunStatsWire>,
) -> std::result::Result<u8, Failure> {
    match result {
        Ok(()) => {
            emitter.emit(&Event::Done {
                stats,
                cancelled: false,
                dropped: None,
            });
            Ok(EXIT_OK)
        }
        Err(_) if CANCELLED.load(Ordering::SeqCst) => {
            emitter.emit(&Event::Done {
                stats: None,
                cancelled: true,
                dropped: None,
            });
            Ok(EXIT_OK)
        }
        Err(err) => Err(Failure::Error(err)),
    }
}

async fn run_drop(emitter: &mut Emitter<'_>, args: &ChildArgs) -> std::result::Result<u8, Failure> {
    let env = open_environment().await?;
    let persisted = env.persisted_app_names().await?;
    // A persisted app whose code is gone can still be dropped by name.
    let name: String = match (&args.app, registry::apps()) {
        (Some(name), _) if persisted.iter().any(|p| p == name) => name.clone(),
        (requested, _) => resolve_app(requested.as_deref())?.name.to_string(),
    };
    emitter.emit(&Event::Start {
        app: name.clone(),
        db_path: env.db_path().display().to_string(),
    });
    let outcome = drop_app(args, &env, &name, &persisted).await;
    env.shutdown().await;
    let dropped = outcome?;
    emitter.emit(&Event::Done {
        stats: None,
        cancelled: false,
        dropped: Some(dropped),
    });
    Ok(EXIT_OK)
}

async fn drop_app(
    args: &ChildArgs,
    env: &Environment,
    name: &str,
    persisted: &[String],
) -> std::result::Result<bool, Failure> {
    if !persisted.iter().any(|p| p == name) {
        return Ok(false);
    }
    if args.db_only {
        env.drop_app_db_only(name).await?;
    } else {
        env.app(name).await?.drop_state().await?;
    }
    Ok(true)
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
