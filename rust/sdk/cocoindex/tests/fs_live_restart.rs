//! A live app killed mid-burst and started again (`fs_live` feature).
//!
//! The first run is a child process — this test binary re-run on its own
//! `restart_child_process` entry — so it can be killed with SIGKILL while
//! events are in flight, as a crash or `kill -9` would. The parent then
//! changes the tree offline and starts the same app again on the same state
//! db. Checks:
//! * no lost work: the output tree ends equal to the source tree (files
//!   created in the burst the child never processed, edited, deleted and
//!   added while nothing ran are all reconciled);
//! * no duplicated work: a file untouched since the first run reached it does
//!   not run its memoized body again;
//! * committed live state reads back: a live component's boot counter, kept
//!   with `write_committed_state`, is read with `read_committed_state` on the
//!   next run.
//!
//!   cargo test -p cocoindex --features fs_live --test fs_live_restart
#![cfg(all(feature = "fs_live", unix))]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cocoindex::resources::fs::{self, FileEntry};
use cocoindex::{
    App, Ctx, LiveComponent, LiveComponentOperator, Result, UpdateOptions, async_trait,
};

const CHILD_ENV: &str = "COCO_FS_LIVE_RESTART_CHILD";
const APP: &str = "FsLiveRestart";

fn append(journal: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal)
        .unwrap();
    // One write per line, so concurrent components never interleave.
    file.write_all(format!("{line}\n").as_bytes()).unwrap();
}

fn journal_lines(journal: &Path) -> Vec<String> {
    std::fs::read_to_string(journal)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn tree(dir: &Path) -> BTreeMap<String, String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return BTreeMap::new();
    };
    entries
        .filter_map(|e| {
            let e = e.ok()?;
            Some((
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read_to_string(e.path()).ok()?,
            ))
        })
        .collect()
}

/// Counts the runs of the app: reads its counter from committed live state,
/// journals the new value and commits it.
struct BootCounter {
    journal: PathBuf,
}

#[async_trait]
impl LiveComponent for BootCounter {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        let boots: u64 = operator.read_committed_state("boots").await?.unwrap_or(0);
        operator
            .write_committed_state("boots", &(boots + 1))
            .await?;
        append(&self.journal, &format!("boot={}", boots + 1));
        operator.update_full().await?;
        operator.mark_ready().await;
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// The app: a boot counter, then one component per source file that runs a
/// memoized body (journaled) and writes the file's content to `out`.
async fn start_live(root: &Path) -> (App, cocoindex::UpdateHandle<()>) {
    let app = App::builder(APP)
        .db_path(root.join("db"))
        .build()
        .await
        .unwrap();
    let (src, out, journal) = (root.join("src"), root.join("out"), root.join("journal"));
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            move |ctx| async move {
                ctx.mount_live(
                    &"boots",
                    BootCounter {
                        journal: journal.clone(),
                    },
                )
                .await?;
                let target = fs::mount_dir_target(&ctx, out)?;
                let feed = fs::walk_dir(src)
                    .live()
                    .rescan_interval(Some(Duration::from_secs(3600)));
                ctx.mount_each_live(&"files", feed, move |ctx, file: FileEntry| {
                    let target = target.clone();
                    let journal = journal.clone();
                    async move {
                        // Keyed by the file's path, size and mtime.
                        let content: String = ctx
                            .memo(&file, |_ctx| {
                                let file = file.clone();
                                let journal = journal.clone();
                                async move {
                                    append(&journal, &format!("run={}", file.key()));
                                    file.content_str()
                                }
                            })
                            .await?;
                        target.declare_file(&ctx, &file.key(), content.as_bytes())
                    }
                })
                .await
            },
        )
        .unwrap();
    (app, handle)
}

/// The first run, in a child process: runs live until killed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "entry point of the child process of `killed_live_app_restarts_without_lost_or_duplicated_work`"]
async fn restart_child_process() {
    let Ok(root) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (_app, _handle) = start_live(Path::new(&root)).await;
    std::future::pending::<()>().await;
}

async fn wait_for(what: &str, timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_live_app_restarts_without_lost_or_duplicated_work() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let (src, out, journal) = (root.join("src"), root.join("out"), root.join("journal"));
    std::fs::create_dir_all(&src).unwrap();
    for i in 0..20 {
        std::fs::write(src.join(format!("base{i:02}.txt")), format!("base{i}")).unwrap();
    }

    // --- First run, killed while a burst is in flight. ---
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "restart_child_process",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ENV, &root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for("the first run's catch-up", Duration::from_secs(60), || {
        tree(&out).len() == 20
    })
    .await;
    // Let the armed-watch rescan settle before the burst.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for i in 0..300 {
        std::fs::write(src.join(format!("burst{i:03}.txt")), format!("burst{i}")).unwrap();
    }
    for i in 0..5 {
        std::fs::write(
            src.join(format!("base{i:02}.txt")),
            format!("edited-live{i}"),
        )
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    child.kill().unwrap();
    child.wait().unwrap();
    let processed_by_child = tree(&out).len();
    eprintln!("the killed run had written {processed_by_child} outputs");

    // --- Offline changes. ---
    for i in 5..8 {
        std::fs::remove_file(src.join(format!("base{i:02}.txt"))).unwrap();
    }
    for i in 8..10 {
        std::fs::write(
            src.join(format!("base{i:02}.txt")),
            format!("edited-offline{i}"),
        )
        .unwrap();
    }
    for i in 0..3 {
        std::fs::write(src.join(format!("offline{i}.txt")), format!("offline{i}")).unwrap();
    }
    let runs_before_restart = journal_lines(&journal).len();

    // --- Second run, in process. ---
    let (app, handle) = start_live(&root).await;
    let expected = tree(&src);
    wait_for("the restart to converge", Duration::from_secs(60), || {
        tree(&out) == expected
    })
    .await;
    // And it stays live after the restart.
    std::fs::write(src.join("after_restart.txt"), b"live").unwrap();
    wait_for(
        "a live change after the restart",
        Duration::from_secs(20),
        || tree(&out).get("after_restart.txt").map(String::as_str) == Some("live"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let lines = journal_lines(&journal);
    let boots: Vec<&String> = lines.iter().filter(|l| l.starts_with("boot=")).collect();
    assert_eq!(
        boots,
        ["boot=1", "boot=2"],
        "committed state did not read back"
    );
    let reruns: Vec<&String> = lines[runs_before_restart..]
        .iter()
        .filter(|l| l.starts_with("run=base"))
        .collect();
    // base00..base04 were edited during the first run, base08/09 offline;
    // base10..base19 were untouched since the first run processed them.
    for line in &reruns {
        let key = line.trim_start_matches("run=");
        let index: usize = key
            .trim_start_matches("base")
            .trim_end_matches(".txt")
            .parse()
            .unwrap_or_else(|_| panic!("unexpected journal line {line:?}"));
        assert!(
            index < 5 || (8..10).contains(&index),
            "untouched {key} ran its memoized body again after the restart: {reruns:?}"
        );
    }

    let _ = tokio::time::timeout(Duration::from_secs(20), app.drop_state()).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}
