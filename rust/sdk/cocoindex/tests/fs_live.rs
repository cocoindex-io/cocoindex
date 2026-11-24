//! Live LocalFS directory watching e2e (`fs_live` feature).
//!
//! Runs everywhere — no external service, just a temp directory and the OS file
//! watcher. Mirrors Python's `walk_dir(..., live=True)` localfs live tests:
//! the catch-up scan picks up existing files, then files created, edited and
//! deleted while the source is live are processed, re-processed and removed
//! one component at a time, and a short `rescan_interval` still catches
//! changes.
#![cfg(feature = "fs_live")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::resources::fs::{self, FileEntry, LiveDirWalker};
use cocoindex::{App, UpdateHandle, UpdateOptions};

/// What the per-file components have seen: for each key, the number of times
/// its component ran and the content it last read.
#[derive(Default, Debug, Clone)]
struct Processed {
    runs: BTreeMap<String, usize>,
    content: BTreeMap<String, String>,
}

impl Processed {
    fn total_runs(&self) -> usize {
        self.runs.values().sum()
    }
}

type Seen = Arc<Mutex<Processed>>;

async fn wait_until<F: Fn(&Processed) -> bool>(seen: &Seen, pred: F, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if pred(&seen.lock().unwrap()) {
            return;
        }
        if Instant::now() > deadline {
            let got = seen.lock().unwrap().clone();
            panic!("timed out waiting for {what}; seen={got:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Start a live update over `src` with `walker`; each file's component records
/// its run and reads the file (as a real pipeline would: the reads must not
/// feed back into the watcher).
fn start(
    app: &App,
    seen: &Seen,
    src: &Path,
    walker: impl Fn(fs::DirWalker) -> LiveDirWalker + Send + Sync + Clone + 'static,
) -> UpdateHandle<()> {
    let seen = seen.clone();
    let src = src.to_path_buf();
    app.start_update_with_options(
        UpdateOptions {
            full_reprocess: false,
            live: true,
            ..UpdateOptions::default()
        },
        move |ctx| {
            let seen = seen.clone();
            let src = src.clone();
            let walker = walker.clone();
            async move {
                let feed = walker(fs::walk_dir(src).recursive(true));
                ctx.mount_each_live(&"files", feed, move |_ctx, file: FileEntry| {
                    let seen = seen.clone();
                    async move {
                        let text = file.content_str()?;
                        let mut seen = seen.lock().unwrap();
                        *seen.runs.entry(file.key()).or_default() += 1;
                        seen.content.insert(file.key(), text);
                        Ok(())
                    }
                })
                .await
            }
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn localfs_live_applies_each_change_to_one_component() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(src.join("sub")).unwrap();
    // Many files exist before the source goes live → the initial scan.
    for i in 0..20 {
        std::fs::write(src.join(format!("f{i:02}.txt")), format!("v{i}")).unwrap();
    }
    std::fs::write(src.join("sub/nested.txt"), b"nested").unwrap();
    let initial = 21;

    let seen: Seen = Arc::default();
    let app = App::builder("FsLiveTest")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let handle = start(&app, &seen, &src, |w| {
        w.live()
            .poll_interval(Duration::from_millis(200))
            .rescan_interval(None)
    });

    // Catch-up scan processed every pre-existing file, once each.
    wait_until(&seen, |s| s.runs.len() == initial, "catch-up scan").await;
    // The feed rescans once when its watcher is armed (the framework's catch-up
    // scan ran before the watch existed); nothing else runs after that — in
    // particular the components' own reads of the files, which the Linux
    // inotify watch reports as open events, must not trigger another scan.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let settled = seen.lock().unwrap().clone();
    assert!(
        settled.runs.values().all(|&n| n <= 2),
        "a component ran more than the scan plus the armed-watch rescan: {settled:?}"
    );
    let baseline = settled.total_runs();

    // --- Add one file: its component runs; no other component does. The
    // file lands by a rename, as an editor saves, so no platform sees a
    // create before its content. ---
    std::fs::write(dir.path().join("new.txt.tmp"), b"new").unwrap();
    std::fs::rename(dir.path().join("new.txt.tmp"), src.join("new.txt")).unwrap();
    wait_until(
        &seen,
        |s| s.content.get("new.txt").is_some_and(|c| c == "new"),
        "live add",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after_add = seen.lock().unwrap().clone();
    assert_eq!(after_add.runs["new.txt"], 1);
    assert_eq!(
        after_add.total_runs(),
        baseline + 1,
        "adding one file must not re-run the other components: {after_add:?}"
    );

    // --- Edit one file: that component runs once more, nothing else. ---
    std::fs::write(src.join("f03.txt"), b"v3-edited").unwrap();
    wait_until(
        &seen,
        |s| s.content.get("f03.txt").is_some_and(|c| c == "v3-edited"),
        "live edit",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after_edit = seen.lock().unwrap().clone();
    assert_eq!(after_edit.total_runs(), baseline + 2, "{after_edit:?}");

    // --- Reading every file (as a rescan does) is not a change. ---
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            std::fs::read(&path).unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        seen.lock().unwrap().total_runs(),
        baseline + 2,
        "reads are not changes"
    );

    // --- Delete one file: its component is dropped; a fresh scan of the
    // directory no longer lists it, and nothing re-runs. ---
    std::fs::remove_file(src.join("f05.txt")).unwrap();
    // A later edit to another file proves the loop is still reacting after
    // the delete was applied.
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::fs::write(src.join("f07.txt"), b"v7-edited").unwrap();
    wait_until(
        &seen,
        |s| s.content.get("f07.txt").is_some_and(|c| c == "v7-edited"),
        "edit after delete",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(seen.lock().unwrap().total_runs(), baseline + 3);

    // --- A directory moved in: its files are mounted, each once. ---
    let staging = dir.path().join("staging");
    std::fs::create_dir_all(staging.join("deep")).unwrap();
    std::fs::write(staging.join("a.txt"), b"A").unwrap();
    std::fs::write(staging.join("deep/b.txt"), b"B").unwrap();
    std::fs::rename(&staging, src.join("moved")).unwrap();
    wait_until(
        &seen,
        |s| s.content.contains_key("moved/a.txt") && s.content.contains_key("moved/deep/b.txt"),
        "moved-in directory",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let after_move = seen.lock().unwrap().clone();
    // Each moved file's component ran (Windows reports the moved directory's
    // files after the walk too, so a file may run twice); no other ran.
    let moved_runs = after_move.runs["moved/a.txt"] + after_move.runs["moved/deep/b.txt"];
    assert_eq!(
        after_move.total_runs() - moved_runs,
        baseline + 3,
        "a moved-in directory must not re-run other components: {after_move:?}"
    );

    // --- The directory moved out again: nothing re-runs, and a later change
    // is still applied. ---
    std::fs::rename(src.join("moved"), dir.path().join("gone")).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::fs::write(src.join("f09.txt"), b"v9-edited").unwrap();
    wait_until(
        &seen,
        |s| s.content.get("f09.txt").is_some_and(|c| c == "v9-edited"),
        "edit after a directory moved out",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        seen.lock().unwrap().total_runs(),
        after_move.total_runs() + 1
    );

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn localfs_live_periodic_rescan_keeps_following_changes() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"alpha").unwrap();

    let seen: Seen = Arc::default();
    let app = App::builder("FsLiveRescanTest")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let handle = start(&app, &seen, &src, |w| {
        w.live().rescan_interval(Some(Duration::from_secs(1)))
    });
    wait_until(
        &seen,
        |s| s.content.contains_key("a.txt"),
        "catch-up of a.txt",
    )
    .await;

    // A file added while live → picked up by an event or by the rescan.
    std::fs::write(src.join("b.txt"), b"beta").unwrap();
    wait_until(
        &seen,
        |s| s.content.contains_key("b.txt"),
        "live add of b.txt",
    )
    .await;

    // After at least one rescan cycle (a fresh watcher), an edit is still seen.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    std::fs::write(src.join("a.txt"), b"alpha-v2").unwrap();
    wait_until(
        &seen,
        |s| s.content.get("a.txt").is_some_and(|c| c == "alpha-v2"),
        "edit after a rescan cycle",
    )
    .await;

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}
