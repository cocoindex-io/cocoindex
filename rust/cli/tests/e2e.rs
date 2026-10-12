//! End-to-end: the real `cocoindex` binary builds and drives
//! `tests/fixture_app` (a workspace member, so the nested `cargo build` shares
//! this workspace's target directory).
//!
//! Each test has its own state db, input and output directories, and runs the
//! CLI from a temporary working directory (no `.env` above it). A few calls go
//! through the build (`FIXTURE`); the rest run the built binary (`exe_target`)
//! to skip cargo's up-to-date check.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixture_app");

/// The fixture binary, built once per test run through the CLI itself.
fn fixture_exe() -> &'static Path {
    static EXE: OnceLock<PathBuf> = OnceLock::new();
    EXE.get_or_init(|| {
        let sb = Sandbox::new(&[]);
        let output = sb.cli(&["ls", FIXTURE]);
        assert!(
            output.status.success(),
            "building the fixture through the CLI"
        );
        let exe = Path::new(env!("CARGO_BIN_EXE_cocoindex")).with_file_name(format!(
            "cocoindex_cli_fixture_app{}",
            std::env::consts::EXE_SUFFIX
        ));
        assert!(
            exe.is_file(),
            "the fixture was built next to the CLI: {}",
            exe.display()
        );
        exe
    })
}

/// `<built fixture binary>:<app>`.
fn exe_target(app: &str) -> String {
    format!("{}:{app}", fixture_exe().display())
}

struct Sandbox {
    dir: tempfile::TempDir,
}

impl Sandbox {
    fn new(files: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        for (name, content) in files {
            std::fs::write(dir.path().join("src").join(name), content).unwrap();
        }
        Self { dir }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    fn db(&self) -> String {
        self.path("db").display().to_string()
    }

    fn cli(&self, args: &[&str]) -> Output {
        self.cli_with_input(args, "")
    }

    /// Run the CLI with `input` on its stdin (answers to its prompts).
    fn cli_with_input(&self, args: &[&str], input: &str) -> Output {
        use std::io::Write as _;
        let started = std::time::Instant::now();
        let mut child = Command::new(env!("CARGO_BIN_EXE_cocoindex"))
            .args(args)
            .current_dir(self.dir.path())
            .env("COCOINDEX_DB", self.path("db"))
            .env("FIXTURE_SRC", self.path("src"))
            .env("FIXTURE_OUT", self.path("out"))
            .env_remove("RUST_LOG")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("run the cocoindex binary");
        child
            .stdin
            .take()
            .expect("stdin is piped")
            .write_all(input.as_bytes())
            .expect("write the CLI's stdin");
        let output = child
            .wait_with_output()
            .expect("wait for the cocoindex binary");
        eprintln!(
            "[{:.1}s] $ cocoindex {}\n--- stdout\n{}--- stderr\n{}",
            started.elapsed().as_secs_f64(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn out_files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.path("out"))
            .map(|d| {
                d.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn target(app: &str) -> String {
    format!("{FIXTURE}:{app}")
}

#[track_caller]
fn assert_ok(output: &Output) {
    assert!(output.status.success(), "exit {:?}", output.status.code());
}

#[test]
fn update_preview_ls_show_drop() {
    let sb = Sandbox::new(&[("a.txt", "alpha"), ("b.txt", "beta")]);

    let ls = sb.cli(&["ls", FIXTURE]);
    assert_ok(&ls);
    let text = stdout(&ls);
    assert!(text.contains("  FixtureApp [+]\n"), "{text}");
    assert!(text.contains("  never_run [+]\n"));
    assert!(text.contains("[+]: Apps present in the binary, but not yet run"));

    // First update writes both files; the app's own stdout passes through.
    let update = sb.cli(&["update", &target("FixtureApp")]); // through the build
    assert_ok(&update);
    let text = stdout(&update);
    assert!(text.contains("Running app 'FixtureApp'"), "{text}");
    assert!(text.contains("fixture: processed 2"), "{text}");
    assert!(text.contains("✅ mount:upper: 2 total | 2 added"), "{text}");
    assert!(
        text.contains("[Stats] processed 3, wrote 3, skipped 0, deleted 0"),
        "{text}"
    );
    assert!(
        stderr(&update).contains("fixture: shutdown"),
        "on_shutdown ran"
    );
    assert!(
        !stderr(&update).contains("INFO"),
        "engine logs default to warnings"
    );
    assert_eq!(sb.out_files(), ["a.txt", "b.txt"]);
    assert_eq!(
        std::fs::read_to_string(sb.path("out/a.txt")).unwrap(),
        "ALPHA"
    );

    // Unchanged inputs are skipped.
    let again = sb.cli(&["update", &exe_target("FixtureApp")]);
    assert_ok(&again);
    assert!(stdout(&again).contains("2 unchanged"), "{}", stdout(&again));

    // Quiet mode prints only the app's own output.
    let quiet = sb.cli(&["update", "-q", &exe_target("FixtureApp")]);
    assert_ok(&quiet);
    assert_eq!(stdout(&quiet), "fixture: processed 2\n");

    // Preview plans the change without applying it.
    std::fs::remove_file(sb.path("src/b.txt")).unwrap();
    let preview = sb.cli(&["update", "--preview", &exe_target("FixtureApp")]);
    assert_ok(&preview);
    let text = stdout(&preview);
    assert!(text.contains("Preview: planned target actions\n"), "{text}");
    assert!(text.contains("b.txt"), "{text}");
    assert_eq!(
        sb.out_files(),
        ["a.txt", "b.txt"],
        "preview applies nothing"
    );

    let ls = sb.cli(&["ls", &exe_target("FixtureApp")]);
    assert!(stdout(&ls).contains("  FixtureApp\n"), "{}", stdout(&ls));

    let ls_db = sb.cli(&["ls", "--db", &sb.db()]);
    assert_ok(&ls_db);
    assert_eq!(stdout(&ls_db), format!("{}:\n  FixtureApp\n", sb.db()));

    // show, straight from the db.
    let show = sb.cli(&["show", "--db", &sb.db(), "--app-name", "FixtureApp"]);
    assert_ok(&show);
    assert_eq!(
        stdout(&show),
        "Stable paths:\n  /\n  /\"upper/a.txt\"\n  /\"upper/b.txt\"\n"
    );
    let tree = sb.cli(&[
        "show",
        "--db",
        &sb.db(),
        "--app-name",
        "FixtureApp",
        "--tree",
    ]);
    assert!(
        stdout(&tree).contains("- upper/a.txt [component]"),
        "{}",
        stdout(&tree)
    );
    let long = sb.cli(&["show", "--db", &sb.db(), "--app-name", "FixtureApp", "-l"]);
    assert!(
        stdout(&long).contains("type:component version:"),
        "{}",
        stdout(&long)
    );
    assert!(stdout(&long).contains("processor:app:FixtureApp"));
    let one = sb.cli(&[
        "show",
        "--db",
        &sb.db(),
        "--app-name",
        "FixtureApp",
        "-p",
        "/upper%2Fa.txt",
    ]);
    assert_ok(&one);
    let states = sb.cli(&[
        "show",
        "--db",
        &sb.db(),
        "--app-name",
        "FixtureApp",
        "--target-states",
    ]);
    let text = stdout(&states);
    assert!(text.starts_with("Target states:\n"), "{text}");
    assert!(
        text.contains("\"a.txt\"") && text.contains("owner:/"),
        "{text}"
    );

    // show through the binary (resolves the db path and app over the protocol).
    let show_target = sb.cli(&["show", &target("FixtureApp")]);
    assert_ok(&show_target);
    assert_eq!(stdout(&show_target), stdout(&show));

    // drop
    let never_run = sb.cli(&["drop", "-f", &exe_target("never_run")]);
    assert_ok(&never_run);
    assert!(
        stdout(&never_run).contains("App 'never_run' has no persisted state. Nothing to drop.")
    );
    let drop = sb.cli(&["drop", "-f", &exe_target("FixtureApp")]);
    assert_ok(&drop);
    assert!(
        stdout(&drop).contains("Dropped app 'FixtureApp'."),
        "{}",
        stdout(&drop)
    );
    let ls_db = sb.cli(&["ls", "--db", &sb.db()]);
    assert_eq!(stdout(&ls_db), "No persisted apps found in the database.\n");
}

#[test]
fn reset_db_only_and_prompts() {
    let sb = Sandbox::new(&[("a.txt", "alpha")]);
    assert_ok(&sb.cli(&["update", "-q", &exe_target("FixtureApp")]));

    // A declined prompt (stdin is empty) aborts with success.
    let declined = sb.cli(&["update", "--reset", &exe_target("FixtureApp")]);
    assert_ok(&declined);
    assert!(
        stdout(&declined).contains("Type 'yes' to reset app 'FixtureApp' (drop existing state)")
    );
    assert!(stdout(&declined).contains("Update operation aborted."));

    let reset = sb.cli(&["update", "-f", "--reset", &exe_target("FixtureApp")]);
    assert_ok(&reset);
    assert!(
        stdout(&reset).contains("1 added"),
        "state was dropped first: {}",
        stdout(&reset)
    );

    let declined = sb.cli(&["drop", "--db-only", &exe_target("FixtureApp")]);
    assert_ok(&declined);
    assert!(stdout(&declined).contains("its target states are NOT reverted"));
    assert!(stdout(&declined).contains("Drop operation aborted."));

    // Answering the prompt: the lifespan runs once (one shutdown line).
    let reset = sb.cli_with_input(&["update", "--reset", &exe_target("FixtureApp")], "yes\n");
    assert_ok(&reset);
    assert!(stdout(&reset).contains("1 added"), "{}", stdout(&reset));
    assert_eq!(stderr(&reset).matches("fixture: shutdown").count(), 1);

    let cleared = sb.cli_with_input(&["drop", "--db-only", &exe_target("FixtureApp")], "YES\n");
    assert_ok(&cleared);
    assert!(stdout(&cleared).contains("Cleared the state of app 'FixtureApp'."));
    assert_eq!(stderr(&cleared).matches("fixture: shutdown").count(), 1);
    assert_ok(&sb.cli(&["update", "-q", &exe_target("FixtureApp")]));

    let cleared = sb.cli(&["drop", "-f", "--db-only", &exe_target("FixtureApp")]);
    assert_ok(&cleared);
    assert!(stdout(&cleared).contains("Cleared the state of app 'FixtureApp'."));
    assert_eq!(sb.out_files(), ["a.txt"], "--db-only leaves targets alone");
    let ls = sb.cli(&["ls", &exe_target("FixtureApp")]);
    assert!(
        stdout(&ls).contains("  FixtureApp [+]\n"),
        "{}",
        stdout(&ls)
    );
}

#[test]
fn executable_target_skips_the_build() {
    let sb = Sandbox::new(&[("a.txt", "alpha")]);
    let update = sb.cli(&["update", &exe_target("FixtureApp")]);
    assert_ok(&update);
    assert!(!stderr(&update).contains("Compiling"), "no build step");
    assert!(!stderr(&update).contains("Finished"), "cargo did not run");
    assert_eq!(sb.out_files(), ["a.txt"]);
}

#[test]
fn usage_errors() {
    let sb = Sandbox::new(&[]);
    let both = sb.cli(&["update", "--preview", "--reset", FIXTURE]);
    assert_eq!(both.status.code(), Some(2));
    assert!(stderr(&both).contains("Error: --preview and --reset cannot be used together."));

    let missing = sb.cli(&["update", "/definitely/not/here"]);
    assert_eq!(missing.status.code(), Some(2));
    assert!(stderr(&missing).contains("does not exist"));

    let no_db = sb.cli(&["ls", "--db", "/definitely/not/here"]);
    assert_eq!(no_db.status.code(), Some(1));
    assert!(stderr(&no_db).contains("Database path does not exist"));

    let recursive = sb.cli(&["show", "-r", "--db", "x", "--app-name", "y"]);
    assert_eq!(recursive.status.code(), Some(2));
    assert!(stderr(&recursive).contains("require a stable_path"));

    let half = sb.cli(&["show", "--db", "x"]);
    assert_eq!(half.status.code(), Some(2));
    assert!(stderr(&half).contains("Both --db and --app-name are required"));

    // Two apps and no name.
    let ambiguous = sb.cli(&["update", FIXTURE]);
    assert_eq!(ambiguous.status.code(), Some(2));
    assert!(
        stderr(&ambiguous).contains("FixtureApp, never_run"),
        "{}",
        stderr(&ambiguous)
    );

    let unknown = sb.cli(&["update", &exe_target("Nope")]);
    assert_eq!(unknown.status.code(), Some(2));
    assert!(stderr(&unknown).contains("Nope"));
}

#[cfg(unix)]
#[test]
fn a_binary_without_the_protocol_is_refused() {
    let sb = Sandbox::new(&[]);
    let output = sb.cli(&["ls", "/bin/echo"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("did not answer the cocoindex protocol"));
}

#[test]
fn env_file_is_loaded() {
    let sb = Sandbox::new(&[("a.txt", "alpha")]);
    let env_file = sb.path("custom.env");
    std::fs::write(
        &env_file,
        format!("FIXTURE_OUT={}\n", sb.path("elsewhere").display()),
    )
    .unwrap();
    // FIXTURE_OUT is already set by the sandbox: .env values never override.
    let output = sb.cli(&[
        "-e",
        env_file.to_str().unwrap(),
        "update",
        "-q",
        &exe_target("FixtureApp"),
    ]);
    assert_ok(&output);
    assert!(stderr(&output).contains("Loaded environment variables from:"));
    assert_eq!(sb.out_files(), ["a.txt"]);
    assert!(!sb.path("elsewhere").exists());
}

/// Ctrl-C while the lifespan is still running cancels cleanly: the CLI prints
/// `Stopping...`, the child reports a cancel, and both exit 0 right away.
#[cfg(unix)]
#[test]
fn ctrl_c_during_the_lifespan_is_a_cancel() {
    for verb in [&["update"][..], &["drop", "-f"][..]] {
        interrupt_slow_lifespan(verb);
    }
}

#[cfg(unix)]
fn interrupt_slow_lifespan(verb: &[&str]) {
    use std::io::{BufRead, BufReader};
    use std::time::{Duration, Instant};

    let sb = Sandbox::new(&[("a.txt", "alpha")]);
    let target = exe_target("FixtureApp");
    let mut args = verb.to_vec();
    args.push(&target);
    let mut child = Command::new(env!("CARGO_BIN_EXE_cocoindex"))
        .args(&args)
        .current_dir(sb.dir.path())
        .env("COCOINDEX_DB", sb.path("db"))
        .env("FIXTURE_SRC", sb.path("src"))
        .env("FIXTURE_OUT", sb.path("out"))
        .env("FIXTURE_SLOW_LIFESPAN", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("run the cocoindex binary");
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut seen = String::new();
    let mut line = String::new();
    while stderr.read_line(&mut line).unwrap() > 0 {
        seen.push_str(&line);
        if line.contains("slow lifespan started") {
            break;
        }
        line.clear();
    }
    assert!(seen.contains("slow lifespan started"), "{verb:?}: {seen}");

    let started = Instant::now();
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let mut rest = String::new();
    std::io::Read::read_to_string(&mut stderr, &mut rest).unwrap();
    let status = child.wait().unwrap();
    eprintln!("{verb:?}\n{seen}{rest}");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{verb:?}: did not wait for the lifespan"
    );
    assert_eq!(status.code(), Some(0), "{verb:?}: a cancel is a clean exit");
    assert!(rest.contains("Stopping..."), "{verb:?}: {rest}");
    assert!(
        !rest.contains("without reporting a result"),
        "{verb:?}: {rest}"
    );
    assert_eq!(
        sb.out_files(),
        Vec::<String>::new(),
        "{verb:?}: nothing ran"
    );
}

/// The binary's own `main` still runs, and its error (a `Debug`-only type, as
/// a plain `main` allows) becomes exit status 1.
#[test]
fn the_binarys_own_main_runs() {
    let ok = Command::new(fixture_exe()).output().unwrap();
    assert!(ok.status.success());
    assert_eq!(String::from_utf8_lossy(&ok.stdout), "fixture: own main\n");
    let failed = Command::new(fixture_exe())
        .env("FIXTURE_MAIN_FAILS", "1")
        .output()
        .unwrap();
    assert_eq!(failed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("Error: FixtureError"));
}

/// `init` writes a project that builds and runs: `update` turns its sample
/// markdown into `out/hello.txt`.
#[test]
fn init_scaffold_builds_and_runs() {
    // Inside the target dir: the repository's cargo config applies, and the
    // workspace lockfile and target dir are reused, so only the scaffold
    // itself compiles.
    let target_dir = Path::new(env!("CARGO_BIN_EXE_cocoindex"))
        .parent()
        .and_then(Path::parent)
        .expect("the CLI is built under <target>/<profile>/");
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let project = tmp.path().join("scaffold_app");
    let sdk = concat!(env!("CARGO_MANIFEST_DIR"), "/../sdk/cocoindex");
    let init = Command::new(env!("CARGO_BIN_EXE_cocoindex"))
        .args([
            "init",
            "--dir",
            project.to_str().unwrap(),
            "--sdk-path",
            sdk,
        ])
        .current_dir(tmp.path())
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"),
        project.join("Cargo.lock"),
    )
    .unwrap();

    let update = Command::new(env!("CARGO_BIN_EXE_cocoindex"))
        .args(["update", "-q", project.to_str().unwrap()])
        .current_dir(tmp.path())
        .env("CARGO_TARGET_DIR", target_dir)
        .env_remove("COCOINDEX_DB")
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&update.stderr));
    assert!(update.status.success());
    let written = std::fs::read_to_string(project.join("out/hello.txt")).unwrap();
    assert!(written.starts_with("# HELLO"), "{written}");
    assert!(
        project.join(".cocoindex_db").is_dir(),
        "the lifespan's db path"
    );
}
