//! End-to-end tests for the dependencies a memoized function declares beyond
//! its arguments, mirroring the Python SDK's `test_logic_dep.py`,
//! `test_data_dep.py` and `test_logic_propagated_to_callers.py`:
//!
//! - `LogicDep` / `Ctx::use_logic_dep`: a later release that changes a
//!   dependency's value is simulated by constructing a new `LogicDep` under
//!   the same name and dropping the old one, which leaves the logic set as an
//!   edited function's fingerprint does on recompile. The functions never
//!   change; only the recorded dependency does.
//! - `DataDep` / `Ctx::use_data_dep`: a body records the files it chose to
//!   read, and the next probe validates them from the stored key and state.
//! - `Ctx::logic_propagated_to_callers`: a callback's dependency reaches a
//!   `logic_tracking = "self"` library function's entry only through the
//!   wrapper.
//! - `#[function(deps = ...)]`: the value is part of the logic and evaluated
//!   once per process.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime};

use cocoindex::memo::MemoStateDecision;
use cocoindex::{App, Ctx, DataDep, Environment, LogicDep, PropagatedCallback, Result};
use cocoindex_utils::fingerprint::Fingerprint;

// ---------------------------------------------------------------------------
// Shared per-test state. Tests run in parallel threads, so every global is
// keyed by a test-specific name.
// ---------------------------------------------------------------------------

static METRICS: LazyLock<Mutex<HashMap<String, usize>>> = LazyLock::new(Default::default);

fn increment(key: &str) {
    *METRICS.lock().unwrap().entry(key.to_string()).or_default() += 1;
}

/// Collect and clear the metrics under `prefix/`, keyed by the remainder.
fn collect(prefix: &str) -> HashMap<String, usize> {
    let mut metrics = METRICS.lock().unwrap();
    let keys: Vec<String> = metrics
        .keys()
        .filter(|k| k.starts_with(&format!("{prefix}/")))
        .cloned()
        .collect();
    keys.into_iter()
        .map(|k| {
            let count = metrics.remove(&k).unwrap();
            (k[prefix.len() + 1..].to_string(), count)
        })
        .collect()
}

fn metrics(entries: &[(&str, usize)]) -> HashMap<String, usize> {
    entries.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

/// The current "release" of each named logic dependency.
static DEPS: LazyLock<Mutex<HashMap<String, LogicDep>>> = LazyLock::new(Default::default);

fn dep(name: &str) -> LogicDep {
    DEPS.lock().unwrap()[name].clone()
}

/// Ship `name` at `version`: the previous object, if any, is dropped and
/// leaves the logic set.
fn release(name: &str, version: u64) {
    let new = LogicDep::new(&("dep", name, version)).unwrap();
    DEPS.lock().unwrap().insert(name.to_string(), new);
}

fn app_at(name: &str, db_path: &Path) -> App {
    App::builder(name)
        .db_path(db_path)
        .build_blocking()
        .unwrap()
}

// ===========================================================================
// LogicDep
// ===========================================================================

/// A memoized function recording a dependency in its own frame.
#[cocoindex::function(memo)]
async fn leaf(ctx: &Ctx, test: &str, value: &str) -> Result<String> {
    ctx.use_logic_dep(&dep(&format!("{test}/rust")))?;
    increment(&format!("{test}/leaf"));
    Ok(format!("leaf: {value}"))
}

#[test]
fn a_dependency_recorded_in_a_memoized_function_moves_its_entry() {
    const T: &str = "ld_memoized";
    let dir = tempfile::tempdir().unwrap();
    let app = app_at(T, &dir.path().join("lmdb"));
    release(&format!("{T}/rust"), 1);

    let run = |app: &App| {
        app.update_blocking(|ctx| async move {
            assert_eq!(leaf(&ctx, T, "value1").await?, "leaf: value1");
            Ok(())
        })
        .unwrap();
    };

    run(&app);
    assert_eq!(collect(T), metrics(&[("leaf", 1)]));

    run(&app);
    assert_eq!(
        collect(T),
        metrics(&[]),
        "an unchanged dependency must memo-hit"
    );

    release(&format!("{T}/rust"), 2);
    run(&app);
    assert_eq!(
        collect(T),
        metrics(&[("leaf", 1)]),
        "a new release of the dependency re-runs"
    );

    run(&app);
    assert_eq!(collect(T), metrics(&[]));
}

/// A plain "full" callee that records `name`'s dependency.
#[cocoindex::function]
async fn records(ctx: &Ctx, test: &str, name: &str, value: &str) -> Result<String> {
    ctx.use_logic_dep(&dep(&format!("{test}/{name}")))?;
    increment(&format!("{test}/records"));
    Ok(format!("{name}: {value}"))
}

#[cocoindex::function(memo)]
async fn caller_full(ctx: &Ctx, test: &str, value: &str) -> Result<String> {
    increment(&format!("{test}/caller_full"));
    records(ctx, test, "rust", value).await
}

#[cocoindex::function(memo, logic_tracking = "self", version = 1)]
async fn caller_self(ctx: &Ctx, test: &str, value: &str) -> Result<String> {
    increment(&format!("{test}/caller_self"));
    records(ctx, test, "rust", value).await
}

#[test]
fn a_dependency_travels_like_the_callees_own_fingerprint() {
    // Offered through a "full" caller; hidden by a "self" caller, exactly as
    // an edit to the callee's body would be.
    const T: &str = "ld_callee_modes";
    let dir = tempfile::tempdir().unwrap();
    let app = app_at(T, &dir.path().join("lmdb"));
    release(&format!("{T}/rust"), 1);

    let run = |app: &App| {
        app.update_blocking(|ctx| async move {
            assert_eq!(caller_full(&ctx, T, "value1").await?, "rust: value1");
            assert_eq!(caller_self(&ctx, T, "value1").await?, "rust: value1");
            Ok(())
        })
        .unwrap();
    };

    run(&app);
    assert_eq!(
        collect(T),
        metrics(&[("caller_full", 1), ("caller_self", 1), ("records", 2)])
    );

    run(&app);
    assert_eq!(collect(T), metrics(&[]));

    release(&format!("{T}/rust"), 2);
    run(&app);
    assert_eq!(collect(T), metrics(&[("caller_full", 1), ("records", 1)]));

    run(&app);
    assert_eq!(collect(T), metrics(&[]));
}

// ===========================================================================
// The callback tunnel: `logic_propagated_to_callers`
// ===========================================================================

/// A "self" callback whose dependency is decided per call by the value's
/// "language" (its suffix).
#[cocoindex::function(logic_tracking = "self", version = 1)]
async fn visit(ctx: &Ctx, test: &str, value: &str) -> Result<String> {
    let language = value.rsplit('.').next().unwrap();
    ctx.use_logic_dep(&dep(&format!("{test}/{language}")))?;
    increment(&format!("{test}/visit:{value}"));
    Ok(format!("visited {value}"))
}

type Visit = Box<dyn Fn(Ctx, String) -> VisitFuture + Send + Sync>;
type VisitFuture = std::pin::Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// A memoized "self" library function calling a callback it received, carried
/// by `logic_propagated_to_callers`.
#[cocoindex::function(memo, logic_tracking = "self", version = 1, memo_key(callback = skip))]
async fn traverse(
    ctx: &Ctx,
    test: &str,
    value: &str,
    callback: &Arc<PropagatedCallback<Visit>>,
) -> Result<String> {
    increment(&format!("{test}/traverse:{value}"));
    callback.call(ctx, value.to_string()).await
}

/// The same library function calling the bare callback: a "self" layer hides
/// what the callback records.
#[cocoindex::function(memo, logic_tracking = "self", version = 1, memo_key(callback = skip))]
async fn traverse_unwrapped(
    ctx: &Ctx,
    test: &str,
    value: &str,
    callback: &Arc<Visit>,
) -> Result<String> {
    increment(&format!("{test}/traverse_unwrapped:{value}"));
    callback(ctx.clone(), value.to_string()).await
}

#[test]
fn a_dependency_rides_the_callback_tunnel_through_a_self_layer() {
    // The tree-walk shape: the dependency the callback records per call lands
    // in the library function's entry, so only the values of the released
    // language re-run. Without the wrapper the "self" layer hides it.
    const T: &str = "ld_tunnel";
    let dir = tempfile::tempdir().unwrap();
    let app = app_at(T, &dir.path().join("lmdb"));
    release(&format!("{T}/rs"), 1);
    release(&format!("{T}/py"), 1);

    let run = |app: &App| {
        app.update_blocking(|ctx| async move {
            let make_callback = || -> Visit {
                Box::new(|ctx: Ctx, value: String| {
                    Box::pin(async move { visit(&ctx, T, &value).await })
                })
            };
            let callback = Arc::new(make_callback());
            // The app root owns the callback.
            let wrapped = Arc::new(ctx.logic_propagated_to_callers(make_callback())?);
            assert_eq!(traverse(&ctx, T, "a.rs", &wrapped).await?, "visited a.rs");
            assert_eq!(traverse(&ctx, T, "b.py", &wrapped).await?, "visited b.py");
            assert_eq!(
                traverse_unwrapped(&ctx, T, "c.rs", &callback).await?,
                "visited c.rs"
            );
            Ok(())
        })
        .unwrap();
    };

    run(&app);
    assert_eq!(
        collect(T),
        metrics(&[
            ("traverse:a.rs", 1),
            ("traverse:b.py", 1),
            ("traverse_unwrapped:c.rs", 1),
            ("visit:a.rs", 1),
            ("visit:b.py", 1),
            ("visit:c.rs", 1),
        ])
    );

    run(&app);
    assert_eq!(collect(T), metrics(&[]));

    release(&format!("{T}/rs"), 2);
    run(&app);
    assert_eq!(
        collect(T),
        metrics(&[("traverse:a.rs", 1), ("visit:a.rs", 1)]),
        "only the entry that recorded the released dependency through the tunnel re-runs"
    );

    run(&app);
    assert_eq!(collect(T), metrics(&[]));
}

#[test]
fn a_propagated_callback_called_after_its_owner_returned_is_an_error() {
    const T: &str = "ld_tunnel_extent";
    let dir = tempfile::tempdir().unwrap();
    let app = app_at(T, &dir.path().join("lmdb"));
    release(&format!("{T}/rs"), 1);

    #[cocoindex::function]
    async fn owner(ctx: &Ctx) -> Result<Arc<PropagatedCallback<Visit>>> {
        let callback: Visit = Box::new(|ctx: Ctx, value: String| {
            Box::pin(async move { visit(&ctx, T, &value).await })
        });
        Ok(Arc::new(ctx.logic_propagated_to_callers(callback)?))
    }

    app.update_blocking(|ctx| async move {
        let escaped = owner(&ctx).await?;
        let err = escaped.call(&ctx, "a.rs".to_string()).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("after the function that wrapped it returned"),
            "{err}"
        );
        Ok(())
    })
    .unwrap();
}

// ===========================================================================
// `#[function(deps = ...)]`
// ===========================================================================

static DEPS_VALUE: AtomicU64 = AtomicU64::new(1);
const PROMPT: &str = "summarize";

#[cocoindex::function(memo, deps = (PROMPT, DEPS_VALUE.load(Ordering::SeqCst)))]
async fn with_deps(_ctx: &Ctx, test: &str, x: &i64) -> Result<i64> {
    increment(&format!("{test}/with_deps"));
    Ok(x * 2)
}

#[test]
fn deps_value_is_part_of_the_registered_logic_and_evaluated_once() {
    const T: &str = "deps_once";
    let entry = cocoindex::COCO_FN_LOGIC
        .iter()
        .find(|e| e.name == "with_deps")
        .expect("with_deps is registered");
    let deps = entry.deps.expect("the entry carries its deps value");
    assert_eq!(
        deps.get().unwrap(),
        cocoindex::memo::deps_fingerprint(&(PROMPT, 1u64)).unwrap()
    );

    let dir = tempfile::tempdir().unwrap();
    let app = app_at(T, &dir.path().join("lmdb"));
    let run = |app: &App| {
        app.update_blocking(|ctx| async move {
            assert_eq!(with_deps(&ctx, T, &21).await?, 42);
            Ok(())
        })
        .unwrap();
    };
    run(&app);
    assert_eq!(collect(T), metrics(&[("with_deps", 1)]));

    // Evaluated once, like Python's `deps=` at decoration time: a later change
    // to the value is not observed, so the entry still hits.
    DEPS_VALUE.store(2, Ordering::SeqCst);
    run(&app);
    assert_eq!(collect(T), metrics(&[]));
    assert_eq!(
        deps.get().unwrap(),
        cocoindex::memo::deps_fingerprint(&(PROMPT, 1u64)).unwrap()
    );
}

#[test]
fn deps_changes_the_logic_fingerprint() {
    let without = cocoindex::memo::fn_logic_fingerprint("m", "f", 7, None).unwrap();
    let with_a = cocoindex::memo::fn_logic_fingerprint(
        "m",
        "f",
        7,
        Some(cocoindex::memo::deps_fingerprint(&"a").unwrap()),
    )
    .unwrap();
    let with_b = cocoindex::memo::fn_logic_fingerprint(
        "m",
        "f",
        7,
        Some(cocoindex::memo::deps_fingerprint(&"b").unwrap()),
    )
    .unwrap();
    assert_ne!(without, with_a);
    assert_ne!(with_a, with_b);
}

// ===========================================================================
// DataDep
// ===========================================================================

/// A file read: keyed by path, with a two-level (mtime, digest) state. One kind
/// per test, so validator counts do not cross between parallel tests.
type FileState = (u64, Fingerprint);

fn modified_nanos(path: &Path) -> u64 {
    std::fs::metadata(path)
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn file_kind(test: &str) -> DataDep<String, FileState> {
    let test = test.to_string();
    DataDep::new(
        format!("test-file/{test}"),
        move |key: String, prev: FileState| {
            let test = test.clone();
            async move {
                increment(&format!("{test}/validate"));
                let path = PathBuf::from(&key);
                if !path.exists() {
                    return MemoStateDecision::new(&prev, false);
                }
                let mtime = modified_nanos(&path);
                if mtime == prev.0 {
                    return MemoStateDecision::new(&prev, true);
                }
                increment(&format!("{test}/validate.digest"));
                let digest = Fingerprint::from_bytes(&std::fs::read(&path)?);
                MemoStateDecision::new(&(mtime, digest), digest == prev.1)
            }
        },
    )
    .unwrap()
}

/// Read a file and record the read: stat *before* reading, so the recorded
/// mtime is never newer than the bytes it is paired with.
fn read(ctx: &Ctx, kind: &DataDep<String, FileState>, path: &Path) -> Result<Vec<u8>> {
    let mtime = modified_nanos(path);
    let data = std::fs::read(path)?;
    ctx.use_data_dep(
        kind,
        &path.to_string_lossy().into_owned(),
        &(mtime, Fingerprint::from_bytes(&data)),
    )?;
    Ok(data)
}

/// Write and bump the mtime past any earlier stat.
fn write(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
    touch(path);
}

/// The last mtime `touch` set, by path. A rewrite in the same timestamp tick as an
/// earlier write keeps that write's mtime, so a bump must also pass the last mtime it set.
static LAST_MTIME: LazyLock<Mutex<HashMap<PathBuf, SystemTime>>> = LazyLock::new(Default::default);

fn touch(path: &Path) {
    let current = std::fs::metadata(path).unwrap().modified().unwrap();
    let mut last_mtime = LAST_MTIME.lock().unwrap();
    let later = last_mtime
        .get(path)
        .map_or(current, |&last| last.max(current))
        + Duration::from_millis(1);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(later)
        .unwrap();
    last_mtime.insert(path.to_path_buf(), later);
}

/// The files a session reads are decided at run time, not by its arguments.
static READS: LazyLock<Mutex<HashMap<String, Vec<PathBuf>>>> = LazyLock::new(Default::default);
static KINDS: LazyLock<Mutex<HashMap<String, DataDep<String, FileState>>>> =
    LazyLock::new(Default::default);

fn kind(test: &str) -> DataDep<String, FileState> {
    KINDS
        .lock()
        .unwrap()
        .entry(test.to_string())
        .or_insert_with(|| file_kind(test))
        .clone()
}

fn session_body(ctx: &Ctx, test: &str, name: &str) -> Result<String> {
    increment(&format!("{test}/run.{name}"));
    let kind = kind(test);
    let paths = READS.lock().unwrap().get(name).cloned().unwrap_or_default();
    let mut content = Vec::new();
    for path in paths {
        content.extend(read(ctx, &kind, &path)?);
    }
    Ok(String::from_utf8(content).unwrap())
}

#[cocoindex::function(memo)]
async fn session(ctx: &Ctx, test: &str, name: &str) -> Result<String> {
    session_body(ctx, test, name)
}

#[test]
fn a_memoized_function_is_reused_while_its_reads_are_unchanged() {
    const T: &str = "dd_function";
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    write(&a, "A1");
    READS.lock().unwrap().insert("f".into(), vec![a.clone()]);
    let _kind = kind(T); // declared before the app runs
    let app = app_at(T, &dir.path().join("lmdb"));

    let run = |app: &App, expected: &'static str| {
        app.update_blocking(move |ctx| async move {
            assert_eq!(session(&ctx, T, "f").await?, expected);
            Ok(())
        })
        .unwrap();
    };

    run(&app, "A1");
    assert_eq!(collect(T), metrics(&[("run.f", 1)]));

    // Nothing changed: the body does not run.
    run(&app, "A1");
    assert_eq!(collect(T), metrics(&[("validate", 1)]));

    // The read changed: the body runs and the new read is stored.
    write(&a, "A2");
    run(&app, "A2");
    assert_eq!(
        collect(T),
        metrics(&[("validate", 1), ("validate.digest", 1), ("run.f", 1)])
    );

    // Changed but valid (mtime only): refreshed in place without a run, and
    // the next probe compares against the refreshed state.
    touch(&a);
    run(&app, "A2");
    assert_eq!(
        collect(T),
        metrics(&[("validate", 1), ("validate.digest", 1)])
    );
    run(&app, "A2");
    assert_eq!(collect(T), metrics(&[("validate", 1)]));
}

#[cocoindex::function(memo, logic_tracking = "self", version = 1)]
async fn outer_self(ctx: &Ctx, test: &str, name: &str) -> Result<String> {
    increment(&format!("{test}/run.outer"));
    Ok(format!("outer:{}", session(ctx, test, name).await?))
}

#[test]
fn a_record_reaches_every_enclosing_memoized_function() {
    // The outer function hits on its own entry, which recorded the inner's
    // read; a change misses the outer entry too, whatever its logic_tracking.
    const T: &str = "dd_nested";
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    write(&a, "A1");
    READS.lock().unwrap().insert("n".into(), vec![a.clone()]);
    let _kind = kind(T);
    let app = app_at(T, &dir.path().join("lmdb"));

    let run = |app: &App, expected: &'static str| {
        app.update_blocking(move |ctx| async move {
            assert_eq!(outer_self(&ctx, T, "n").await?, expected);
            Ok(())
        })
        .unwrap();
    };

    run(&app, "outer:A1");
    assert_eq!(collect(T), metrics(&[("run.outer", 1), ("run.n", 1)]));

    run(&app, "outer:A1");
    assert_eq!(collect(T), metrics(&[("validate", 1)]));

    write(&a, "A2");
    run(&app, "outer:A2");
    let after = collect(T);
    assert_eq!(after["run.outer"], 1);
    assert_eq!(after["run.n"], 1);
}

#[cocoindex::function]
async fn session_component(ctx: &Ctx, test: &'static str, name: String) -> Result<String> {
    session_body(ctx, test, &name)
}

#[cocoindex::function]
async fn parent_component(ctx: &Ctx, test: &'static str, name: String) -> Result<String> {
    increment(&format!("{test}/run.parent"));
    cocoindex::use_mount!(session_component(ctx, test, name)).await
}

#[test]
fn an_open_component_is_reused_while_its_reads_are_unchanged() {
    // The component's own entry holds the records (`handle_memo_states`), and
    // its parent's memo is reused while the open child's reads validate from
    // the child's row, without mounting it (`validate_data_deps`).
    const T: &str = "dd_component";
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    write(&a, "A1");
    READS.lock().unwrap().insert("c".into(), vec![a.clone()]);
    let _kind = kind(T);
    let app = app_at(T, &dir.path().join("lmdb"));

    let run = |app: &App, expected: &'static str| {
        app.update_blocking(move |ctx| async move {
            let out = cocoindex::use_mount!(parent_component(ctx, T, "c".to_string())).await?;
            assert_eq!(out, expected);
            Ok(())
        })
        .unwrap();
    };

    run(&app, "A1");
    assert_eq!(collect(T), metrics(&[("run.parent", 1), ("run.c", 1)]));

    // The parent is skipped whole: its own entry validates and the child's
    // record validates from the child's row.
    run(&app, "A1");
    assert_eq!(collect(T), metrics(&[("validate", 1)]));

    // The read changed: the parent's walk finds the child's record invalid
    // and the parent re-runs; the child's own probe validates once more when
    // it is mounted, misses, and re-runs.
    write(&a, "A2");
    run(&app, "A2");
    assert_eq!(
        collect(T),
        metrics(&[
            ("validate", 2),
            ("validate.digest", 2),
            ("run.parent", 1),
            ("run.c", 1)
        ])
    );

    // The re-stored rows reuse again.
    run(&app, "A2");
    assert_eq!(collect(T), metrics(&[("validate", 1)]));
}

#[cocoindex::function(memo)]
async fn conflicting(ctx: &Ctx, test: &str) -> Result<()> {
    increment(&format!("{test}/run"));
    let kind = kind(test);
    ctx.use_data_dep(&kind, &"k".to_string(), &(1, Fingerprint::from_bytes(b"x")))?;
    ctx.use_data_dep(&kind, &"k".to_string(), &(2, Fingerprint::from_bytes(b"x")))?;
    Ok(())
}

#[test]
fn conflicting_observations_store_no_entry() {
    // One key observed in two states has no single state to validate
    // against: the call stores nothing and runs again next time.
    const T: &str = "dd_conflict";
    let dir = tempfile::tempdir().unwrap();
    let _kind = kind(T);
    let app = app_at(T, &dir.path().join("lmdb"));
    for _ in 0..2 {
        app.update_blocking(|ctx| async move { conflicting(&ctx, T).await })
            .unwrap();
    }
    assert_eq!(collect(T), metrics(&[("run", 2)]));
}

#[test]
fn a_dropped_kind_invalidates_its_entries() {
    // The kind is no longer registered when the entry is probed: a logged
    // miss, and the body re-runs and re-records under the new kind.
    const T: &str = "dd_dropped_kind";
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    write(&a, "A1");
    READS.lock().unwrap().insert("d".into(), vec![a.clone()]);
    let _kind = kind(T);
    let app = app_at(T, &dir.path().join("lmdb"));
    let run = |app: &App| {
        app.update_blocking(|ctx| async move {
            session(&ctx, T, "d").await?;
            Ok(())
        })
        .unwrap();
    };
    run(&app);
    assert_eq!(collect(T), metrics(&[("run.d", 1)]));

    drop(_kind);
    KINDS.lock().unwrap().remove(T);
    run(&app);
    assert_eq!(collect(T), metrics(&[("run.d", 1)]));
}

#[tokio::test]
async fn use_data_dep_and_use_logic_dep_record_into_the_component_at_the_root() {
    // Recorded outside any function frame, a record and a dependency go to
    // the component itself: the root component's memo is not involved (the
    // root always runs), so this only checks the calls succeed there.
    const T: &str = "dd_root";
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap()
        .app(T)
        .await
        .unwrap();
    release(&format!("{T}/rust"), 1);
    let kind = kind(T);
    app.update(move |ctx| async move {
        ctx.use_logic_dep(&dep(&format!("{T}/rust")))?;
        ctx.use_data_dep(&kind, &"k".to_string(), &(1, Fingerprint::from_bytes(b"x")))?;
        Ok(())
    })
    .await
    .unwrap();
}
