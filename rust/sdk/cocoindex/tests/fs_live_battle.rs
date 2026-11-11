//! Adversarial live LocalFS tests (`fs_live` feature): event storms, rapid
//! create-delete-create of one path, editor-style rename-over saves,
//! directories moved and removed, symlinks, permission changes, a file
//! rewritten while its component reads it, an event the watcher cannot see
//! (caught by the periodic rescan), and the watched root itself removed.
//!
//! Each file's component writes `<key>` (with `/` as `__`) into an output
//! directory through a `DirTarget`, so a test checks the end state on disk:
//! what the pipeline wrote, updated and removed.
//!
//!   cargo test -p cocoindex --features fs_live --test fs_live_battle
#![cfg(feature = "fs_live")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::resources::file::PatternFilePathMatcher;
use cocoindex::resources::fs::{self, FileEntry, LiveDirWalker};
use cocoindex::{App, UpdateHandle, UpdateOptions};

/// How many times each key's component ran.
type Runs = Arc<Mutex<BTreeMap<String, usize>>>;

fn out_name(key: &str) -> String {
    key.replace('/', "__")
}

/// The output directory's files, by name, with their content.
fn outputs(out: &Path) -> BTreeMap<String, String> {
    let Ok(entries) = std::fs::read_dir(out) else {
        return BTreeMap::new();
    };
    entries
        .filter_map(|e| {
            let e = e.ok()?;
            let content = std::fs::read_to_string(e.path()).ok()?;
            Some((e.file_name().to_string_lossy().into_owned(), content))
        })
        .collect()
}

async fn wait_for(what: &str, timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_output(out: &Path, key: &str, expected: Option<&str>) {
    let name = out_name(key);
    wait_for(
        &format!("output {name} to be {expected:?}"),
        Duration::from_secs(20),
        || outputs(out).get(&name).map(String::as_str) == expected,
    )
    .await;
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    src: PathBuf,
    out: PathBuf,
    app: App,
    runs: Runs,
    handle: Option<UpdateHandle<()>>,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let src = root.join("src");
        let out = root.join("out");
        std::fs::create_dir_all(&src).unwrap();
        let app = App::builder(name)
            .db_path(root.join(".cocoindex_db"))
            .build()
            .await
            .unwrap();
        Self {
            _dir: dir,
            root,
            src,
            out,
            app,
            runs: Arc::default(),
            handle: None,
        }
    }

    /// Start the live update. `configure` turns the recursive walker into the
    /// live feed; `delay` is how long each component waits between reading its
    /// file and declaring the output.
    fn start(
        &mut self,
        configure: impl Fn(fs::DirWalker) -> LiveDirWalker + Send + Sync + 'static,
        delay: Duration,
    ) {
        let (src, out, runs) = (self.src.clone(), self.out.clone(), self.runs.clone());
        let handle = self
            .app
            .start_update_with_options(
                UpdateOptions {
                    live: true,
                    ..UpdateOptions::default()
                },
                move |ctx| async move {
                    let target = fs::mount_dir_target(&ctx, out)?;
                    let feed = configure(fs::walk_dir(src).recursive(true));
                    ctx.mount_each_live(&"files", feed, move |ctx, file: FileEntry| {
                        let target = target.clone();
                        let runs = runs.clone();
                        async move {
                            *runs.lock().unwrap().entry(file.key()).or_default() += 1;
                            let content = file.content()?;
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            target.declare_file(&ctx, &out_name(&file.key()), &content)
                        }
                    })
                    .await
                },
            )
            .unwrap();
        self.handle = Some(handle);
    }

    fn start_default(&mut self) {
        self.start(|w| w.live().rescan_interval(None), Duration::ZERO);
    }

    fn runs_of(&self, key: &str) -> usize {
        self.runs.lock().unwrap().get(key).copied().unwrap_or(0)
    }

    fn total_runs(&self) -> usize {
        self.runs.lock().unwrap().values().sum()
    }

    async fn stop(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(20), self.app.drop_state()).await;
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
        }
    }
}

/// Thousands of writes in one burst: 2000 new files, a hot file rewritten 200
/// times, and 200 files the matcher excludes. Every included file ends with
/// its content on the output side, the hot file with its last write, and the
/// excluded ones nowhere. Coalescing keeps the hot file's runs far below its
/// writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_thousands_of_writes_converges() {
    let mut f = Fixture::new("FsBattleStorm").await;
    std::fs::write(f.src.join("hot.txt"), b"hot-init").unwrap();
    f.start(
        |w| {
            w.path_matcher(PatternFilePathMatcher::new(["**/*.txt"], ["**/*.skip.txt"]).unwrap())
                .live()
                .rescan_interval(None)
        },
        Duration::ZERO,
    );
    wait_output(&f.out, "hot.txt", Some("hot-init")).await;

    let started = Instant::now();
    for i in 0..2000 {
        std::fs::write(f.src.join(format!("n{i:04}.txt")), format!("c{i}")).unwrap();
        if i % 10 == 0 {
            std::fs::write(f.src.join("hot.txt"), format!("hot-{i}")).unwrap();
        }
        if i % 10 == 5 {
            std::fs::write(f.src.join(format!("x{i:04}.skip.txt")), b"no").unwrap();
        }
    }
    std::fs::write(f.src.join("hot.txt"), b"hot-final").unwrap();
    let written = started.elapsed();

    wait_for(
        "every burst file processed",
        Duration::from_secs(120),
        || {
            let out = outputs(&f.out);
            out.len() == 2001 && out.get("hot.txt").map(String::as_str) == Some("hot-final")
        },
    )
    .await;
    let converged = started.elapsed();
    eprintln!(
        "burst: 2000 files + 201 hot writes written in {written:?}, converged in {converged:?}"
    );

    let out = outputs(&f.out);
    for i in 0..2000 {
        assert_eq!(out[&format!("n{i:04}.txt")], format!("c{i}"));
    }
    assert!(
        !out.keys().any(|k| k.contains(".skip")),
        "excluded file processed"
    );
    assert!(
        f.runs_of("hot.txt") < 100,
        "201 writes of one file should coalesce; it ran {} times",
        f.runs_of("hot.txt")
    );
    for i in 0..2000 {
        let key = format!("n{i:04}.txt");
        assert!(f.runs_of(&key) <= 3, "{key} ran {} times", f.runs_of(&key));
    }
    f.stop().await;
}

/// One path created and deleted a hundred times in a row: the output follows
/// the last operation, whichever it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_create_delete_create_of_one_path() {
    let mut f = Fixture::new("FsBattleFlip").await;
    f.start_default();
    wait_for("watch armed", Duration::from_secs(10), || {
        f.handle.is_some()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let path = f.src.join("flip.txt");
    for i in 0..100 {
        std::fs::write(&path, format!("v{i}")).unwrap();
        std::fs::remove_file(&path).unwrap();
    }
    std::fs::write(&path, b"final").unwrap();
    wait_output(&f.out, "flip.txt", Some("final")).await;

    // Spread the flips over several debounce windows, ending in a delete.
    for i in 0..20 {
        std::fs::remove_file(&path).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(&path, format!("w{i}")).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    std::fs::remove_file(&path).unwrap();
    wait_output(&f.out, "flip.txt", None).await;
    // And settles there.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(outputs(&f.out).get("flip.txt"), None);
    f.stop().await;
}

/// Editor-style saves: write a temp file next to the target, then rename it
/// over the target. The target's output follows every save, and no temp file
/// survives on the output side, whether or not its own component ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_over_saves_leave_only_the_saved_file() {
    let mut f = Fixture::new("FsBattleRenameOver").await;
    std::fs::write(f.src.join("doc.txt"), b"v0").unwrap();
    f.start_default();
    wait_output(&f.out, "doc.txt", Some("v0")).await;

    for i in 1..=20 {
        let tmp = f.src.join(format!(".doc.txt.{i}.tmp"));
        std::fs::write(&tmp, format!("v{i}")).unwrap();
        // Some saves land in the same debounce window as the temp file, some
        // after the temp file was already processed on its own.
        if i % 2 == 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        std::fs::rename(&tmp, f.src.join("doc.txt")).unwrap();
    }
    wait_output(&f.out, "doc.txt", Some("v20")).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let out = outputs(&f.out);
    assert_eq!(
        out.keys().collect::<Vec<_>>(),
        vec!["doc.txt"],
        "a temp file's output survived its rename: {out:?}"
    );
    f.stop().await;
}

/// A directory moved in is walked; moved out, every output under it goes;
/// a directory removed with its files likewise; a file moved between
/// directories inside the root changes key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directories_moved_in_out_and_removed() {
    let mut f = Fixture::new("FsBattleDirs").await;
    std::fs::create_dir_all(f.src.join("keep")).unwrap();
    std::fs::write(f.src.join("keep/k.txt"), b"k").unwrap();
    f.start_default();
    wait_output(&f.out, "keep/k.txt", Some("k")).await;

    let staging = f.root.join("staging");
    std::fs::create_dir_all(staging.join("deep/deeper")).unwrap();
    std::fs::write(staging.join("a.txt"), b"A").unwrap();
    std::fs::write(staging.join("deep/b.txt"), b"B").unwrap();
    std::fs::write(staging.join("deep/deeper/c.txt"), b"C").unwrap();
    std::fs::rename(&staging, f.src.join("moved")).unwrap();
    wait_output(&f.out, "moved/deep/deeper/c.txt", Some("C")).await;
    wait_output(&f.out, "moved/a.txt", Some("A")).await;
    wait_output(&f.out, "moved/deep/b.txt", Some("B")).await;

    // A file renamed across directories inside the root: old key gone, new
    // key present.
    std::fs::rename(f.src.join("moved/a.txt"), f.src.join("keep/a2.txt")).unwrap();
    wait_output(&f.out, "keep/a2.txt", Some("A")).await;
    wait_output(&f.out, "moved/a.txt", None).await;

    // Moved out: every output under it is removed.
    std::fs::rename(f.src.join("moved"), f.root.join("gone")).unwrap();
    wait_output(&f.out, "moved/deep/b.txt", None).await;
    wait_output(&f.out, "moved/deep/deeper/c.txt", None).await;

    // Moved back in and then removed with its files.
    std::fs::rename(f.root.join("gone"), f.src.join("back")).unwrap();
    wait_output(&f.out, "back/deep/b.txt", Some("B")).await;
    std::fs::remove_dir_all(f.src.join("back")).unwrap();
    wait_output(&f.out, "back/deep/b.txt", None).await;
    wait_output(&f.out, "back/deep/deeper/c.txt", None).await;

    // An empty directory created and removed is nothing.
    std::fs::create_dir(f.src.join("empty")).unwrap();
    std::fs::remove_dir(f.src.join("empty")).unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let out = outputs(&f.out);
    assert_eq!(
        out.keys().cloned().collect::<Vec<_>>(),
        vec!["keep__a2.txt".to_string(), "keep__k.txt".to_string()],
        "{out:?}"
    );
    f.stop().await;
}

/// Symlinks: a link to a file is a file with the target's content, a link to
/// a directory is walked, a dangling link is skipped without stopping the
/// watch, and removing a link removes its output.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn symlinks_are_followed_and_removed() {
    use std::os::unix::fs::symlink;
    let mut f = Fixture::new("FsBattleSymlinks").await;
    let outside = f.root.join("outside");
    std::fs::create_dir_all(outside.join("d")).unwrap();
    std::fs::write(outside.join("t.txt"), b"target").unwrap();
    std::fs::write(outside.join("d/in_dir.txt"), b"in-dir").unwrap();
    f.start_default();
    tokio::time::sleep(Duration::from_millis(500)).await;

    symlink(outside.join("t.txt"), f.src.join("link.txt")).unwrap();
    wait_output(&f.out, "link.txt", Some("target")).await;
    symlink(outside.join("d"), f.src.join("linkdir")).unwrap();
    wait_output(&f.out, "linkdir/in_dir.txt", Some("in-dir")).await;
    symlink(f.root.join("nowhere"), f.src.join("dangling.txt")).unwrap();
    // The watch is still alive after the dangling link.
    std::fs::write(f.src.join("after.txt"), b"after").unwrap();
    wait_output(&f.out, "after.txt", Some("after")).await;
    assert_eq!(outputs(&f.out).get("dangling.txt"), None);

    std::fs::remove_file(f.src.join("link.txt")).unwrap();
    wait_output(&f.out, "link.txt", None).await;
    std::fs::remove_file(f.src.join("linkdir")).unwrap();
    wait_output(&f.out, "linkdir/in_dir.txt", None).await;
    f.stop().await;
}

/// A file that becomes unreadable fails its component, which keeps its last
/// output; once readable again, the permission change itself is an event and
/// the file is processed again.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreadable_file_keeps_output_and_recovers() {
    use std::os::unix::fs::PermissionsExt;
    if nix_is_root() {
        eprintln!("skipped: permission bits do not stop root");
        return;
    }
    let mut f = Fixture::new("FsBattlePerms").await;
    let path = f.src.join("p.txt");
    std::fs::write(&path, b"readable").unwrap();
    f.start_default();
    wait_output(&f.out, "p.txt", Some("readable")).await;

    std::fs::write(&path, b"secret").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    let runs_before = f.runs_of("p.txt");
    wait_for(
        "the unreadable file's component to run",
        Duration::from_secs(10),
        || f.runs_of("p.txt") > runs_before,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        outputs(&f.out).get("p.txt").map(String::as_str),
        Some("readable"),
        "a failed read must keep the last output"
    );

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    wait_output(&f.out, "p.txt", Some("secret")).await;
    // Another file keeps flowing throughout.
    std::fs::write(f.src.join("q.txt"), b"q").unwrap();
    wait_output(&f.out, "q.txt", Some("q")).await;
    f.stop().await;
}

#[cfg(unix)]
fn nix_is_root() -> bool {
    use std::os::unix::fs::MetadataExt;
    // A file we create is owned by our uid.
    let probe = tempfile::NamedTempFile::new().unwrap();
    probe.as_file().metadata().unwrap().uid() == 0
}

/// A file rewritten while its component is between reading it and declaring
/// its output: the component's stale result is replaced by a later run that
/// reads the new content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_rewritten_while_being_processed_ends_with_new_content() {
    let mut f = Fixture::new("FsBattleRewriteDuringRead").await;
    f.start(
        |w| w.live().rescan_interval(None),
        Duration::from_millis(400),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let path = f.src.join("busy.txt");
    std::fs::write(&path, b"first").unwrap();
    wait_for("first run to start", Duration::from_secs(10), || {
        f.runs_of("busy.txt") >= 1
    })
    .await;
    // The first run has read "first" and is sleeping before it declares.
    std::fs::write(&path, b"second").unwrap();
    wait_output(&f.out, "busy.txt", Some("second")).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        outputs(&f.out).get("busy.txt").map(String::as_str),
        Some("second")
    );
    f.stop().await;
}

/// A write the watcher cannot see — through a hard link outside the watched
/// tree, which inotify reports only for the directory of the path written —
/// is caught by the periodic rescan, and only by it.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescan_catches_a_change_the_watcher_missed() {
    for rescan in [None, Some(Duration::from_millis(800))] {
        let mut f = Fixture::new(&format!("FsBattleRescan{}", rescan.is_some())).await;
        std::fs::write(f.src.join("linked.txt"), b"v1").unwrap();
        let alias = f.root.join("alias.txt");
        std::fs::hard_link(f.src.join("linked.txt"), &alias).unwrap();
        f.start(move |w| w.live().rescan_interval(rescan), Duration::ZERO);
        wait_output(&f.out, "linked.txt", Some("v1")).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Rewrite in place through the alias (same inode, no new file).
        {
            use std::io::{Seek, Write};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&alias)
                .unwrap();
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
            file.write_all(b"v2").unwrap();
        }
        match rescan {
            None => {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                assert_eq!(
                    outputs(&f.out).get("linked.txt").map(String::as_str),
                    Some("v1"),
                    "the watcher saw the aliased write, so this test proves nothing"
                );
            }
            Some(_) => wait_output(&f.out, "linked.txt", Some("v2")).await,
        }
        f.stop().await;
    }
}

/// The watched root removed and created again: the outputs of its files go
/// when it goes, and its new files are processed once it is back (picked up
/// by the rescan, since the old watch died with the directory).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watched_root_removed_and_recreated() {
    let mut f = Fixture::new("FsBattleRootGone").await;
    std::fs::write(f.src.join("a.txt"), b"a").unwrap();
    f.start(
        |w| w.live().rescan_interval(Some(Duration::from_millis(800))),
        Duration::ZERO,
    );
    wait_output(&f.out, "a.txt", Some("a")).await;

    std::fs::remove_dir_all(&f.src).unwrap();
    wait_output(&f.out, "a.txt", None).await;
    // Stay gone across a couple of rescans, which find no root to watch.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    std::fs::create_dir_all(&f.src).unwrap();
    std::fs::write(f.src.join("b.txt"), b"b").unwrap();
    wait_output(&f.out, "b.txt", Some("b")).await;
    // And the watch is live again for ordinary events.
    std::fs::write(f.src.join("c.txt"), b"c").unwrap();
    wait_output(&f.out, "c.txt", Some("c")).await;
    f.stop().await;
}

/// The feed's own reads never feed back: after the catch-up and the
/// armed-watch rescan, a quiet tree stays quiet even while every file is
/// read repeatedly by someone else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_never_trigger_work() {
    let mut f = Fixture::new("FsBattleQuiet").await;
    for i in 0..50 {
        std::fs::write(f.src.join(format!("q{i}.txt")), format!("{i}")).unwrap();
    }
    f.start_default();
    wait_for("catch-up", Duration::from_secs(20), || {
        outputs(&f.out).len() == 50
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let settled = f.total_runs();
    for _ in 0..5 {
        for i in 0..50 {
            let _ = std::fs::read(f.src.join(format!("q{i}.txt"))).unwrap();
        }
        let _ = std::fs::read_dir(&f.src).unwrap().count();
    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(f.total_runs(), settled, "reads triggered work");
    f.stop().await;
}

/// The files of one burst are processed concurrently: 30 new files whose
/// components take 300ms each land well before the nine seconds one at a
/// time would take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_burst_is_processed_concurrently() {
    let mut f = Fixture::new("FsBattleConcurrent").await;
    f.start(
        |w| w.live().rescan_interval(None),
        Duration::from_millis(300),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = Instant::now();
    for i in 0..30 {
        std::fs::write(f.src.join(format!("c{i:02}.txt")), format!("{i}")).unwrap();
    }
    wait_for("the burst", Duration::from_secs(30), || {
        outputs(&f.out).len() == 30
    })
    .await;
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(4),
        "30 files at 300ms each took {took:?}: processed one at a time"
    );
    f.stop().await;
}
