//! `cocoindex`: the CocoIndex command-line tool for Rust projects.
//!
//! A Rust pipeline is compiled into the user's binary, so this tool works like
//! `cargo run`: it builds the target crate, runs its binary as a `__coco`
//! protocol child (see `cocoindex::cli`), and renders the results. `ls --db`
//! and `show --db` read the state db directly. Spec: `dev/plans/rust-cli.md`.

mod build;
mod child;
mod commands;
mod error;
mod init;
mod inspect;
mod progress;
mod target;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

use build::BuildOpts;
use commands::{DropArgs, ShowArgs, UpdateArgs};
use error::CliResult;
use inspect::ShowMode;

#[derive(Parser)]
#[command(
    name = "cocoindex",
    version,
    about = "CocoIndex command-line tool for Rust projects.",
    long_about = "CocoIndex command-line tool for Rust projects.\n\n\
                  APP_TARGET is `[path][:app_name]`: a crate directory (default `.`), a \
                  Cargo.toml, or an already-built binary, optionally followed by the name of \
                  one of its #[cocoindex::app]s."
)]
struct Cli {
    /// Path to a .env file to load environment variables from. If not provided, attempts to
    /// load '.env' from the current directory or its parents.
    #[arg(short = 'e', long, global = true, value_name = "PATH")]
    env_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone, Default)]
struct BuildArgs {
    /// Build the target crate with the release profile.
    #[arg(long)]
    release: bool,
    /// The binary to run when the target crate builds several.
    #[arg(long, value_name = "NAME")]
    bin: Option<String>,
}

impl BuildArgs {
    fn opts(&self, quiet: bool) -> BuildOpts {
        BuildOpts {
            release: self.release,
            bin: self.bin.clone(),
            quiet,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// List all apps. With APP_TARGET, lists the apps of that crate and their persisted
    /// status. Without it, lists the apps persisted in the database given by --db (or
    /// COCOINDEX_DB).
    Ls {
        #[arg(value_name = "APP_TARGET")]
        target: Option<String>,
        /// Path to database to list apps from (only used when APP_TARGET is not specified).
        #[arg(long, value_name = "PATH")]
        db: Option<PathBuf>,
        #[command(flatten)]
        build: BuildArgs,
    },
    /// Show the persisted state of an app: its stable paths, or its target states.
    /// Use `show [APP_TARGET] [STABLE_PATH]`, or `show --db PATH --app-name NAME [STABLE_PATH]`.
    Show {
        #[arg(value_name = "APP_TARGET | STABLE_PATH")]
        first: Option<String>,
        #[arg(value_name = "STABLE_PATH")]
        second: Option<String>,
        /// Path to database (used with --app-name when APP_TARGET is not specified).
        #[arg(long, value_name = "PATH")]
        db: Option<PathBuf>,
        /// App name to inspect (used with --db when APP_TARGET is not specified).
        #[arg(long, value_name = "NAME")]
        app_name: Option<String>,
        /// Display stable paths (or --target-states entries) as a tree.
        #[arg(long)]
        tree: bool,
        /// Display detailed information in multi-line format.
        #[arg(short = 'l', long = "long")]
        long: bool,
        /// Show all children recursively (requires stable_path).
        #[arg(short = 'r', long)]
        recursive: bool,
        /// Show all parent paths (requires stable_path).
        #[arg(short = 'p', long)]
        parents: bool,
        /// Show target-state paths as raw fingerprints (as stored) instead of readable keys.
        #[arg(long)]
        fingerprints: bool,
        /// List all tracked target states with their owner components.
        #[arg(long)]
        target_states: bool,
        #[command(flatten)]
        build: BuildArgs,
    },
    /// Run an app in catch-up mode. With --live, run in live mode.
    Update {
        #[arg(value_name = "APP_TARGET")]
        target: Option<String>,
        /// Skip confirmation prompt.
        #[arg(short = 'f', long)]
        force: bool,
        /// Avoid printing anything to the standard output, e.g. statistics.
        #[arg(short = 'q', long)]
        quiet: bool,
        /// Drop existing setup before updating (equivalent to running 'cocoindex drop' first).
        #[arg(long)]
        reset: bool,
        /// Reprocess everything, bypassing all memoization caches.
        #[arg(long)]
        full_reprocess: bool,
        /// Run in live mode (live components continue processing after initial update).
        #[arg(short = 'L', long)]
        live: bool,
        /// Compute target actions without applying them. Prints planned actions.
        #[arg(long)]
        preview: bool,
        #[command(flatten)]
        build: BuildArgs,
    },
    /// Drop an app and all its target states.
    Drop {
        #[arg(value_name = "APP_TARGET")]
        target: Option<String>,
        /// Skip confirmation prompt.
        #[arg(short = 'f', long)]
        force: bool,
        /// Avoid printing anything to the standard output.
        #[arg(short = 'q', long)]
        quiet: bool,
        /// Only clear the app's state database; do not revert its target states.
        #[arg(long)]
        db_only: bool,
        #[command(flatten)]
        build: BuildArgs,
    },
    /// Initialize a new Rust CocoIndex project.
    Init {
        /// Name of the project (defaults to the directory name).
        project_name: Option<String>,
        /// Directory to create the project in.
        #[arg(long, value_name = "DIR")]
        dir: Option<PathBuf>,
        /// Depend on a local checkout of the cocoindex Rust SDK (rust/sdk/cocoindex)
        /// instead of the git repository.
        #[arg(long, value_name = "PATH")]
        sdk_path: Option<PathBuf>,
    },
}

fn load_env(env_file: Option<&PathBuf>) -> CliResult<()> {
    let loaded = match env_file {
        Some(path) => dotenvy::from_path(path)
            .map(|()| Some(path.clone()))
            .map_err(error::fail(&format!("--env-file {}", path.display())))?,
        None => dotenvy::dotenv().ok(),
    };
    if let Some(path) = loaded {
        let shown = std::path::absolute(&path).unwrap_or(path);
        eprintln!("Loaded environment variables from: {}\n", shown.display());
    }
    Ok(())
}

async fn run(cli: Cli) -> CliResult<()> {
    load_env(cli.env_file.as_ref())?;
    match cli.command {
        Command::Ls { target, db, build } => commands::ls(target, db, build.opts(false)).await,
        Command::Show {
            first,
            second,
            db,
            app_name,
            tree,
            long,
            recursive,
            parents,
            fingerprints,
            target_states,
            build,
        } => {
            commands::show(ShowArgs {
                first,
                second,
                db,
                app_name,
                mode: ShowMode {
                    tree,
                    long,
                    recursive,
                    parents,
                    fingerprints,
                    target_states,
                },
                build: build.opts(false),
            })
            .await
        }
        Command::Update {
            target,
            force,
            quiet,
            reset,
            full_reprocess,
            live,
            preview,
            build,
        } => {
            commands::update(UpdateArgs {
                target,
                force,
                quiet,
                reset,
                full_reprocess,
                live,
                preview,
                build: build.opts(quiet),
            })
            .await
        }
        Command::Drop {
            target,
            force,
            quiet,
            db_only,
            build,
        } => {
            commands::drop_app(DropArgs {
                target,
                force,
                quiet,
                db_only,
                build: build.opts(quiet),
            })
            .await
        }
        Command::Init {
            project_name,
            dir,
            sdk_path,
        } => init::init(project_name, dir, sdk_path),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            if !err.message.is_empty() {
                eprintln!("Error: {}", err.message);
            }
            ExitCode::from(err.code)
        }
    }
}
