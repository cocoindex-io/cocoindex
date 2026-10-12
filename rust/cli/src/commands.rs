//! `ls`, `show`, `update` and `drop`.

use std::path::{Path, PathBuf};

use cocoindex::cli::protocol::{AppInfo, Event, Outcome};

use crate::build::{self, BuildOpts};
use crate::child::{self, Interrupt, Output};
use crate::error::{CliError, CliResult};
use crate::inspect::{self, ShowMode};
use crate::progress::Renderer;
use crate::target::{Target, TargetPath, parse_target};

/// The binary for a target: built from its manifest, or used as given.
async fn resolve_exe(target: &Target, build: &BuildOpts) -> CliResult<PathBuf> {
    match &target.path {
        TargetPath::Executable(exe) => Ok(exe.clone()),
        TargetPath::Manifest(manifest) => build::build(manifest, build).await,
    }
}

/// Ask the binary for its apps and state db path.
async fn query_apps(exe: &Path) -> CliResult<(Vec<AppInfo>, String)> {
    let mut found = None;
    let outcome = child::run(exe, "ls", &[], Interrupt::Default, |output| match output {
        Output::Text(text) => println!("{text}"),
        Output::Event(Event::Apps { apps, db_path }) => found = Some((apps, db_path)),
        Output::Event(_) => {}
    })
    .await?;
    if let Some(Event::Error { .. }) = &outcome.terminal {
        outcome.into_done()?;
    }
    found.ok_or_else(|| CliError::failure("the target binary did not list its apps"))
}

/// The app `show` inspects: the TARGET's `:app_name` (a registered app, or a
/// persisted one whose code is gone), else the binary's only registered app.
fn pick_app(requested: Option<&str>, apps: &[AppInfo]) -> CliResult<AppInfo> {
    let usable = |a: &&AppInfo| a.registered || a.persisted;
    match requested {
        Some(name) => apps
            .iter()
            .filter(usable)
            .find(|a| a.name == name)
            .cloned()
            .ok_or_else(|| {
                CliError::usage(format!(
                    "No app named '{name}'. Available apps: {}",
                    app_names(apps.iter().filter(usable))
                ))
            }),
        None => {
            let registered: Vec<&AppInfo> = apps.iter().filter(|a| a.registered).collect();
            match registered.as_slice() {
                [one] => Ok((*one).clone()),
                [] => Err(CliError::usage(
                    "No apps found in the target binary. Make sure it has a #[cocoindex::app] function.",
                )),
                many => Err(CliError::usage(format!(
                    "Multiple apps found: {}. Please specify which app to use with ':app_name' syntax.",
                    app_names(many.iter().copied())
                ))),
            }
        }
    }
}

fn app_names<'a>(apps: impl Iterator<Item = &'a AppInfo>) -> String {
    apps.map(|a| a.name.as_str()).collect::<Vec<_>>().join(", ")
}

/// `./relative` when under the current directory, else as given.
pub fn format_db_path(path: &str) -> String {
    let Ok(cwd) = std::env::current_dir() else {
        return path.to_string();
    };
    let absolute = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        cwd.join(path)
    };
    // Compare real paths (macOS: /var is a link to /private/var).
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    let absolute = absolute.canonicalize().unwrap_or(absolute);
    match absolute.strip_prefix(&cwd) {
        Ok(rel) if !rel.as_os_str().is_empty() => format!("./{}", rel.display()),
        _ => path.to_string(),
    }
}

// ---------------------------------------------------------------------------
// ls
// ---------------------------------------------------------------------------

pub async fn ls(target: Option<String>, db: Option<PathBuf>, build: BuildOpts) -> CliResult<()> {
    if target.is_some() && db.is_some() {
        eprintln!("Warning: --db is ignored when APP_TARGET is specified.");
    }
    let use_db = match (&target, &db) {
        (Some(_), _) => None,
        (None, Some(db)) => Some(db.clone()),
        (None, None) if Path::new("Cargo.toml").is_file() => None,
        (None, None) => match std::env::var_os("COCOINDEX_DB") {
            Some(db) => Some(PathBuf::from(db)),
            None => {
                return Err(CliError::usage(
                    "Please specify either APP_TARGET or --db option (or set COCOINDEX_DB environment variable).\n  \
                     cocoindex ls ./my_crate      # List apps of a crate\n  \
                     cocoindex ls --db ./my.db    # List apps from database",
                ));
            }
        },
    };
    if let Some(db) = use_db {
        let storage = inspect::open_db(&db).await?;
        let names = inspect::list_apps(&storage).await;
        storage.close().await;
        let names = names?;
        if names.is_empty() {
            println!("No persisted apps found in the database.");
        } else {
            println!("{}:", db.display());
            for name in names {
                println!("  {name}");
            }
        }
        return Ok(());
    }

    let target = parse_target(target.as_deref())?;
    let exe = resolve_exe(&target, &build).await?;
    let (apps, db_path) = query_apps(&exe).await?;
    if apps.is_empty() {
        println!("No apps are defined in the target binary.");
        return Ok(());
    }
    println!("{}:", format_db_path(&db_path));
    let mut not_run = false;
    let mut not_registered = false;
    for app in &apps {
        match (app.registered, app.persisted) {
            (true, true) => println!("  {}", app.name),
            (true, false) => {
                println!("  {} [+]", app.name);
                not_run = true;
            }
            (false, _) => {
                println!("  {} [-]", app.name);
                not_registered = true;
            }
        }
    }
    if not_run || not_registered {
        println!();
        println!("Notes:");
        if not_run {
            println!("  [+]: Apps present in the binary, but not yet run (no persisted state).");
        }
        if not_registered {
            println!("  [-]: Apps with persisted state but no #[cocoindex::app] in the binary.");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

pub struct ShowArgs {
    pub first: Option<String>,
    pub second: Option<String>,
    pub db: Option<PathBuf>,
    pub app_name: Option<String>,
    pub mode: ShowMode,
    pub build: BuildOpts,
}

pub async fn show(args: ShowArgs) -> CliResult<()> {
    let ShowArgs {
        first,
        second,
        db,
        app_name,
        mode,
        build,
    } = args;
    let db_mode = db.is_some() || app_name.is_some();
    // With --db, the only positional is STABLE_PATH; otherwise TARGET [STABLE_PATH].
    let stable_path = if db_mode {
        first.clone()
    } else {
        second.clone()
    };
    if (mode.recursive || mode.parents) && stable_path.is_none() {
        return Err(CliError::usage(
            "-r/--recursive and -p/--parents require a stable_path argument.",
        ));
    }
    if mode.target_states && (stable_path.is_some() || mode.long || mode.recursive || mode.parents)
    {
        return Err(CliError::usage(
            "--target-states cannot be combined with stable_path, -l, -r or -p.",
        ));
    }

    let (db_path, app) = if db_mode {
        let (Some(db), Some(app)) = (db, app_name) else {
            return Err(CliError::usage(
                "Both --db and --app-name are required when APP_TARGET is not specified.",
            ));
        };
        if second.is_some() {
            return Err(CliError::usage(
                "With --db and --app-name, the only positional argument is STABLE_PATH.",
            ));
        }
        (db, app)
    } else {
        let target = parse_target(first.as_deref())?;
        let exe = resolve_exe(&target, &build).await?;
        let (apps, db_path) = query_apps(&exe).await?;
        let app = pick_app(target.app.as_deref(), &apps)?;
        (PathBuf::from(db_path), app.name)
    };

    let storage = inspect::open_db(&db_path).await?;
    let shown = inspect::show(
        &storage,
        &app,
        stable_path.as_deref(),
        mode,
        &mut std::io::stdout().lock(),
    )
    .await;
    storage.close().await;
    shown?;
    Ok(())
}

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

pub struct UpdateArgs {
    pub target: Option<String>,
    pub force: bool,
    pub quiet: bool,
    pub reset: bool,
    pub full_reprocess: bool,
    pub live: bool,
    pub preview: bool,
    pub build: BuildOpts,
}

pub async fn update(args: UpdateArgs) -> CliResult<()> {
    if args.preview && args.reset {
        return Err(CliError::usage(
            "--preview and --reset cannot be used together.",
        ));
    }
    if args.preview && args.live {
        return Err(CliError::usage(
            "--preview and --live cannot be used together.",
        ));
    }
    let target = parse_target(args.target.as_deref())?;
    let exe = resolve_exe(&target, &args.build).await?;

    let mut flags: Vec<String> = Vec::new();
    if let Some(name) = &target.app {
        flags.extend(["--app".to_string(), name.clone()]);
    }
    if args.reset && !args.force {
        // The child asks before dropping state, so the lifespan runs once.
        flags.push("--confirm".to_string());
    }
    for (on, flag) in [
        (args.live, "--live"),
        (args.full_reprocess, "--full-reprocess"),
        (args.preview, "--preview"),
        (args.reset, "--reset"),
    ] {
        if on {
            flags.push(flag.to_string());
        }
    }

    let mut renderer = Renderer::new(args.quiet || args.preview, args.live);
    let mut previews = Vec::new();
    let quiet = args.quiet;
    let outcome = child::run(
        &exe,
        "update",
        &flags,
        Interrupt::Cancel { quiet },
        |output| match output {
            Output::Text(text) => renderer.text(text),
            Output::Event(Event::Start { app, db_path }) => {
                if !quiet {
                    renderer.text(&format!(
                        "Running app '{app}' (db path: {})",
                        format_db_path(&db_path)
                    ));
                }
            }
            Output::Event(Event::Progress {
                ready,
                by_component,
                ..
            }) => renderer.progress(ready, by_component),
            Output::Event(Event::Preview { kind, value }) => previews.push((kind, value)),
            Output::Event(_) => {}
        },
    )
    .await;
    renderer.clear();
    let done = outcome?.into_done()?;
    let Event::Done { outcome, stats } = done else {
        return Ok(());
    };
    match outcome {
        Outcome::Completed => {}
        Outcome::Cancelled => return Ok(()),
        Outcome::Aborted => {
            if !quiet {
                println!("Update operation aborted.");
            }
            return Ok(());
        }
        Outcome::NothingToDrop => {
            return Err(CliError::failure(
                "the target binary answered `update` with a drop result",
            ));
        }
    }
    if args.preview {
        println!("Preview: planned target actions");
        if previews.is_empty() {
            println!("  No target actions planned.");
        }
        for (kind, value) in previews {
            println!("  {kind} {value}");
        }
        return Ok(());
    }
    if !quiet {
        for line in renderer.final_lines() {
            println!("{line}");
        }
        if let Some(stats) = stats {
            println!("[Stats] {stats}");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// drop
// ---------------------------------------------------------------------------

pub struct DropArgs {
    pub target: Option<String>,
    pub force: bool,
    pub quiet: bool,
    pub db_only: bool,
    pub build: BuildOpts,
}

pub async fn drop_app(args: DropArgs) -> CliResult<()> {
    let target = parse_target(args.target.as_deref())?;
    let exe = resolve_exe(&target, &args.build).await?;
    let mut flags: Vec<String> = Vec::new();
    if let Some(name) = &target.app {
        flags.extend(["--app".to_string(), name.clone()]);
    }
    if args.db_only {
        flags.push("--db-only".to_string());
    }
    if !args.force {
        // The child asks before dropping, so the lifespan runs once.
        flags.push("--confirm".to_string());
    }
    let quiet = args.quiet;
    let mut app_name = String::new();
    let outcome = child::run(
        &exe,
        "drop",
        &flags,
        Interrupt::Cancel { quiet },
        |output| match output {
            Output::Text(text) => println!("{text}"),
            Output::Event(Event::Start { app, db_path }) => {
                if !quiet {
                    println!(
                        "Preparing to drop app '{app}' (db path: {})",
                        format_db_path(&db_path)
                    );
                }
                app_name = app;
            }
            Output::Event(_) => {}
        },
    )
    .await?;
    let Event::Done { outcome, .. } = outcome.into_done()? else {
        return Ok(());
    };
    if quiet {
        return Ok(());
    }
    match outcome {
        Outcome::Cancelled => {}
        Outcome::Aborted => println!("Drop operation aborted."),
        Outcome::NothingToDrop => {
            println!("App '{app_name}' has no persisted state. Nothing to drop.");
        }
        Outcome::Completed if args.db_only => {
            println!("Cleared the state of app '{app_name}'.");
        }
        Outcome::Completed => println!("Dropped app '{app_name}'."),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str, registered: bool, persisted: bool) -> AppInfo {
        AppInfo {
            name: name.into(),
            registered,
            persisted,
        }
    }

    #[test]
    fn picks_the_only_registered_app() {
        let apps = vec![app("A", true, false), app("Old", false, true)];
        assert_eq!(pick_app(None, &apps).unwrap().name, "A");
        assert_eq!(pick_app(Some("Old"), &apps).unwrap().name, "Old");
        let err = pick_app(Some("Gone"), &apps).unwrap_err();
        assert!(
            err.message.contains("No app named 'Gone'"),
            "{}",
            err.message
        );
    }

    #[test]
    fn several_apps_need_a_name() {
        let apps = vec![app("A", true, false), app("B", true, true)];
        let err = pick_app(None, &apps).unwrap_err();
        assert_eq!(err.code, 2);
        assert!(err.message.contains("Multiple apps found: A, B"));
        let err = pick_app(None, &[]).unwrap_err();
        assert!(err.message.contains("No apps found"));
    }

    #[test]
    fn formats_db_paths_relative_to_cwd() {
        let cwd = std::env::current_dir().unwrap();
        let inside = cwd.join("state").join("db");
        assert_eq!(format_db_path(inside.to_str().unwrap()), "./state/db");
        assert_eq!(
            format_db_path("/definitely/elsewhere"),
            "/definitely/elsewhere"
        );
    }
}
