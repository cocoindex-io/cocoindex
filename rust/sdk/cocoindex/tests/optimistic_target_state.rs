//! End-to-end tests for optimistic target-state writes.
//!
//! Two distinct capabilities are exercised here and kept apart on purpose:
//!
//! `declare_target_state_optimistic` combines immediate visibility,
//! mark-before-write recovery, ordinary submit confirmation, and an AppStore
//! absence CAS that elects one writer per target-state path.
//!
//!   cargo test -p cocoindex --test optimistic_target_state

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use cocoindex::{
    App, Error, Result, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, declare_target_state, declare_target_state_optimistic,
    register_root_target_states_provider,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use tokio::time::{Duration, timeout};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Stand-in for the external system. Sink applies land here immediately, so
/// a test can read it from inside a still-running processor.
type Db = Arc<Mutex<BTreeMap<String, String>>>;
type Log = Arc<Mutex<Vec<String>>>;
/// Log entries (`"create a=v1"`, `"delete a"`) whose next sink call must
/// fail. Each entry fails once, then is consumed.
type FailOnce = Arc<Mutex<HashSet<String>>>;

fn new_db() -> Db {
    Arc::new(Mutex::new(BTreeMap::new()))
}

fn new_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn new_fail_once() -> FailOnce {
    Arc::new(Mutex::new(HashSet::new()))
}

fn drain(log: &Log) -> Vec<String> {
    std::mem::take(&mut *log.lock().unwrap())
}

fn db_snapshot(db: &Db) -> BTreeMap<String, String> {
    db.lock().unwrap().clone()
}

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        StableKey::Int(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

/// Applies actions to `db` and appends `"<verb> <key>[=<value>]"` to `log`.
/// An entry present in `fail_once` fails instead — and is consumed, so a
/// retry through a different path succeeds.
fn recording_sink(db: Db, log: Log, fail_once: FailOnce) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let db = db.clone();
        let log = log.clone();
        let fail_once = fail_once.clone();
        async move {
            for action in actions {
                let (verb, row) = match action {
                    TargetAction::Create(r) => ("create", r),
                    TargetAction::Update(r) => ("update", r),
                    TargetAction::Delete(r) => ("delete", r),
                };
                let entry = match &row.value {
                    Some(v) => format!("{verb} {}={}", row.key, v),
                    None => format!("{verb} {}", row.key),
                };
                if fail_once.lock().unwrap().remove(&entry) {
                    return Err(Error::engine(format!("injected sink failure: {entry}")));
                }
                match row.value {
                    Some(v) => {
                        db.lock().unwrap().insert(row.key.clone(), v);
                    }
                    None => {
                        db.lock().unwrap().remove(&row.key);
                    }
                }
                log.lock().unwrap().push(entry);
            }
            Ok(())
        }
    })
}

/// Flat row handler with no-change detection; the tracking record is the
/// row value itself.
struct RowHandler {
    sink: TargetActionSink<RowAction>,
}

impl TargetHandler<String> for RowHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        let k = key_str(&key);
        match desired {
            Some(value) => {
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value);
                if unchanged {
                    return Ok(None);
                }
                let action = if prev.is_empty() {
                    TargetAction::Create(RowAction {
                        key: k,
                        value: Some(value.clone()),
                    })
                } else {
                    TargetAction::Update(RowAction {
                        key: k,
                        value: Some(value.clone()),
                    })
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink: self.sink.clone(),
                    tracking_record: Some(value),
                    child_invalidation: None,
                }))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(RowAction {
                        key: k,
                        value: None,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

fn handler(db: &Db, log: &Log, fail_once: &FailOnce) -> RowHandler {
    RowHandler {
        sink: recording_sink(db.clone(), log.clone(), fail_once.clone()),
    }
}

const CRASH_MODE_ENV: &str = "COCOINDEX_OPTIMISTIC_CRASH_MODE";
const CRASH_DB_ENV: &str = "COCOINDEX_OPTIMISTIC_CRASH_DB";
const CRASH_ROW_ENV: &str = "COCOINDEX_OPTIMISTIC_CRASH_ROW";
const CRASH_LOG_ENV: &str = "COCOINDEX_OPTIMISTIC_CRASH_LOG";
const CRASH_APP_NAME: &str = "optimistic_crash_recovery";

fn append_disk_log(path: &Path, entry: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{entry}").unwrap();
    file.sync_all().unwrap();
}

fn disk_handler(row_path: PathBuf, log_path: PathBuf, crash_on_delete: bool) -> RowHandler {
    let sink = TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let row_path = row_path.clone();
        let log_path = log_path.clone();
        async move {
            for action in actions {
                let row = match action {
                    TargetAction::Create(row) | TargetAction::Update(row) => row,
                    TargetAction::Delete(row) => row,
                };
                match row.value {
                    Some(value) => {
                        fs::write(&row_path, &value).unwrap();
                        append_disk_log(&log_path, &format!("create {}={value}", row.key));
                    }
                    None if crash_on_delete => {
                        append_disk_log(&log_path, &format!("delete-start {}", row.key));
                        std::process::exit(0);
                    }
                    None => {
                        match fs::remove_file(&row_path) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(Error::engine(format!("delete row: {e}"))),
                        }
                        append_disk_log(&log_path, &format!("delete {}", row.key));
                    }
                }
            }
            Ok(())
        }
    });
    RowHandler { sink }
}

/// Subprocess-only crash injector. The parent tests below invoke this exact
/// test name with environment variables pointing at a durable LMDB and a
/// file-backed stand-in for the external target.
#[test]
fn optimistic_crash_child() {
    let Ok(mode) = std::env::var(CRASH_MODE_ENV) else {
        return;
    };
    let db_path = PathBuf::from(std::env::var_os(CRASH_DB_ENV).unwrap());
    let row_path = PathBuf::from(std::env::var_os(CRASH_ROW_ENV).unwrap());
    let log_path = PathBuf::from(std::env::var_os(CRASH_LOG_ENV).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async move {
        let app = App::builder(CRASH_APP_NAME)
            .db_path(db_path)
            .build()
            .await
            .unwrap();
        let crash_on_delete = mode == "during-cleanup";
        app.update(move |ctx| {
            let row_path = row_path.clone();
            let log_path = log_path.clone();
            let mode = mode.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/crash",
                    disk_handler(row_path, log_path, crash_on_delete),
                )?;
                ctx.scope(&"writer", move |child| async move {
                    let won = declare_target_state_optimistic(
                        &child,
                        provider.target_state("a", "stale".to_string()),
                    )
                    .await?;
                    assert!(won);
                    if mode == "after-write" {
                        std::process::exit(0);
                    }
                    Err::<(), Error>(Error::engine("enter cleanup before crashing"))
                })
                .await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        panic!("crash injector returned without exiting");
    });
}

fn spawn_crash_child(mode: &str, db_path: &Path, row_path: &Path, log_path: &Path) {
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("optimistic_crash_child")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, mode)
        .env(CRASH_DB_ENV, db_path)
        .env(CRASH_ROW_ENV, row_path)
        .env(CRASH_LOG_ENV, log_path)
        .status()
        .unwrap();
    assert!(status.success(), "crash child failed with {status}");
}

async fn assert_recovery_after_crash(mode: &str, expected_prefix: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("state");
    let row_path = dir.path().join("row");
    let log_path = dir.path().join("sink.log");
    spawn_crash_child(mode, &db_path, &row_path, &log_path);

    assert_eq!(fs::read_to_string(&row_path).unwrap(), "stale");
    let app = App::builder(CRASH_APP_NAME)
        .db_path(&db_path)
        .build()
        .await
        .unwrap();
    app.update({
        let row_path = row_path.clone();
        let log_path = log_path.clone();
        move |ctx| {
            let row_path = row_path.clone();
            let log_path = log_path.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/crash",
                    disk_handler(row_path.clone(), log_path.clone(), false),
                )?;
                ctx.scope(&"writer", move |child| async move {
                    assert!(
                        !row_path.exists(),
                        "lazy recovery must delete the stale row before user processing"
                    );
                    append_disk_log(&log_path, "processor-start");
                    let won = declare_target_state_optimistic(
                        &child,
                        provider.target_state("a", "fresh".to_string()),
                    )
                    .await?;
                    assert!(won, "recovery must release the stale CAS claim");
                    Ok(())
                })
                .await?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(fs::read_to_string(&row_path).unwrap(), "fresh");
    let entries: Vec<_> = fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let expected: Vec<_> = expected_prefix
        .iter()
        .copied()
        .chain([
            "delete a",
            "processor-start",
            "create a=fresh",
            "create a=fresh",
        ])
        .map(str::to_owned)
        .collect();
    assert_eq!(entries, expected);
    assert_no_leftover_records(&app).await;
}

/// Scenario 8: a process dies after the eager write. Reopening the engine
/// lazily deletes the owning component's stale row before its processor runs,
/// releases the claim, and lets that processor create and confirm a new row.
#[tokio::test]
async fn process_crash_after_eager_write_recovers_on_reopen() {
    assert_recovery_after_crash("after-write", &["create a=stale"]).await;
}

/// Scenario 10: the process dies after its marker reaches `Cleaning` but
/// before the external delete. Reopen resumes the idempotent delete before
/// processing and then completes a fresh optimistic lifecycle.
#[tokio::test]
async fn process_crash_during_cleanup_resumes_on_reopen() {
    assert_recovery_after_crash("during-cleanup", &["create a=stale", "delete-start a"]).await;
}

/// `drop_state` runs component-local recovery/delete before clearing state;
/// success proves the component can finish its lifecycle without a stranded
/// optimistic operation.
async fn assert_no_leftover_records(app: &App) {
    app.drop_state()
        .await
        .expect("app.drop_state must succeed — leftover optimistic records block it");
}

// ---------------------------------------------------------------------------
// CAS-backed optimistic writes
// ---------------------------------------------------------------------------

/// The row is in the external system *before* the processor returns, and
/// normal submit re-applies it afterwards.
#[tokio::test]
async fn eager_write_is_visible_during_processing() {
    let (app, _dir) = temp_app("optimistic_visible").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    let observed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    app.update({
        let (db, log, fail, observed) = (db.clone(), log.clone(), fail.clone(), observed.clone());
        move |ctx| {
            let (db, log, fail, observed) =
                (db.clone(), log.clone(), fail.clone(), observed.clone());
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/opt",
                    handler(&db, &log, &fail),
                )?;
                declare_target_state_optimistic(&ctx, provider.target_state("a", "v1".to_string()))
                    .await?;
                // Still inside the processor: the write is already visible.
                *observed.lock().unwrap() = db.lock().unwrap().get("a").cloned();
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(observed.lock().unwrap().as_deref(), Some("v1"));
    // Eager write, then the authoritative re-apply from normal submit.
    assert_eq!(drain(&log), vec!["create a=v1", "create a=v1"]);
    assert_eq!(db_snapshot(&db).get("a").map(String::as_str), Some("v1"));
    assert_no_leftover_records(&app).await;
}

/// Scenario 3: once the row is confirmed, a reprocessing component reads it
/// and redeclares it normally; it does not issue another optimistic write.
#[tokio::test]
async fn confirmed_row_is_reused_without_another_optimistic_write() {
    let (app, _dir) = temp_app("optimistic_rerun").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());

    async fn run(app: &App, db: Db, log: Log, fail: FailOnce) {
        app.update(move |ctx| {
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/opt",
                    handler(&db, &log, &fail),
                )?;
                if let Some(existing) = db.lock().unwrap().get("a").cloned() {
                    declare_target_state(&ctx, provider.target_state("a", existing))?;
                } else {
                    let won = declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("a", "v1".to_string()),
                    )
                    .await?;
                    assert!(won);
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    run(&app, db.clone(), log.clone(), fail.clone()).await;
    assert_eq!(drain(&log), vec!["create a=v1", "create a=v1"]);

    run(&app, db.clone(), log.clone(), fail.clone()).await;
    assert!(drain(&log).is_empty());
    assert_no_leftover_records(&app).await;
}

/// Scenario 11: the eager sink call fails, the caller catches the error,
/// and normal submit still lands the row.
#[tokio::test]
async fn caught_eager_failure_heals_at_submit() {
    let (app, _dir) = temp_app("optimistic_heal").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    fail.lock().unwrap().insert("create a=v1".to_string());
    let eager_failed = Arc::new(Mutex::new(false));

    app.update({
        let (db, log, fail, eager_failed) =
            (db.clone(), log.clone(), fail.clone(), eager_failed.clone());
        move |ctx| {
            let (db, log, fail, eager_failed) =
                (db.clone(), log.clone(), fail.clone(), eager_failed.clone());
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/opt",
                    handler(&db, &log, &fail),
                )?;
                let result = declare_target_state_optimistic(
                    &ctx,
                    provider.target_state("a", "v1".to_string()),
                )
                .await;
                *eager_failed.lock().unwrap() = result.is_err();
                // Deliberately swallowed: the declaration stays registered.
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert!(
        *eager_failed.lock().unwrap(),
        "eager write should have failed"
    );
    assert_eq!(
        drain(&log),
        vec!["create a=v1"],
        "only submit's apply lands"
    );
    assert_eq!(db_snapshot(&db).get("a").map(String::as_str), Some("v1"));
    assert_no_leftover_records(&app).await;
}

/// Scenarios 4/5: the processor fails after the eager write, so engine
/// cleanup deletes the row it may have written before unmarking it.
#[tokio::test]
async fn processor_failure_deletes_the_eager_row() {
    let (app, _dir) = temp_app("optimistic_cleanup").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());

    let result = app
        .update({
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            move |ctx| {
                let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
                async move {
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "test/opt",
                        handler(&db, &log, &fail),
                    )?;
                    declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("a", "v1".to_string()),
                    )
                    .await?;
                    Err::<(), Error>(Error::engine("processor blew up"))
                }
            }
        })
        .await;

    assert!(result.is_err());
    assert_eq!(drain(&log), vec!["create a=v1", "delete a"]);
    assert!(db_snapshot(&db).is_empty(), "eager row must be removed");
    assert_no_leftover_records(&app).await;
}

/// Delete-before-unmark: when the cleanup delete keeps failing, the row is
/// gone only once the sink actually accepted the delete. The `fail_once`
/// injection makes the first delete attempt fail, and the bounded retry
/// finishes the job.
#[tokio::test]
async fn cleanup_retries_a_failing_delete() {
    let (app, _dir) = temp_app("optimistic_cleanup_retry").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    fail.lock().unwrap().insert("delete a".to_string());

    let result = app
        .update({
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            move |ctx| {
                let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
                async move {
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "test/opt",
                        handler(&db, &log, &fail),
                    )?;
                    declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("a", "v1".to_string()),
                    )
                    .await?;
                    Err::<(), Error>(Error::engine("processor blew up"))
                }
            }
        })
        .await;

    assert!(result.is_err());
    assert!(
        db_snapshot(&db).is_empty(),
        "retry must complete the delete"
    );
    assert_eq!(drain(&log), vec!["create a=v1", "delete a"]);
    assert_no_leftover_records(&app).await;
}

/// Preview must never touch the outside world, so an optimistic write is
/// refused there — before any marker is written or any sink is called.
#[tokio::test]
async fn preview_rejects_optimistic_writes() {
    let (app, _dir) = temp_app("optimistic_preview").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());

    let result = app
        .preview({
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            move |ctx| {
                let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
                async move {
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "test/opt",
                        handler(&db, &log, &fail),
                    )?;
                    declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("a", "v1".to_string()),
                    )
                    .await?;
                    Ok(())
                }
            }
        })
        .await;

    assert!(result.is_err(), "preview must reject optimistic writes");
    assert!(db_snapshot(&db).is_empty());
    assert!(drain(&log).is_empty());
    assert_no_leftover_records(&app).await;
}

/// Independent target-state keys do not contend.
#[tokio::test]
async fn independent_optimistic_keys_both_win() {
    let (app, _dir) = temp_app("optimistic_independent_pair").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());

    app.update({
        let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
        move |ctx| {
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            async move {
                let provider = Arc::new(register_root_target_states_provider(
                    &ctx,
                    "test/opt",
                    handler(&db, &log, &fail),
                )?);
                // Two independent keys from two components: both write.
                ctx.mount_each(
                    vec!["c1", "c2"],
                    |item| (*item).to_string(),
                    move |child, item| {
                        let provider = provider.clone();
                        async move {
                            declare_target_state_optimistic(
                                &child,
                                provider.target_state(item, format!("from-{item}")),
                            )
                            .await?;
                            Ok::<(), Error>(())
                        }
                    },
                )
                .await?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    let snapshot = db_snapshot(&db);
    assert_eq!(snapshot.get("c1").map(String::as_str), Some("from-c1"));
    assert_eq!(snapshot.get("c2").map(String::as_str), Some("from-c2"));
    assert_no_leftover_records(&app).await;
}

/// Two components propose different values for one logical key at the same
/// moment. Exactly one wins; the loser writes nothing at all.
#[tokio::test]
async fn cas_elects_one_winner_across_components() {
    let (app, _dir) = temp_app("optimistic_cas_race").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    let outcomes: Arc<Mutex<Vec<(String, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));

    app.update({
        let (db, log, fail, outcomes, barrier) = (
            db.clone(),
            log.clone(),
            fail.clone(),
            outcomes.clone(),
            barrier.clone(),
        );
        move |ctx| {
            let (db, log, fail, outcomes, barrier) = (
                db.clone(),
                log.clone(),
                fail.clone(),
                outcomes.clone(),
                barrier.clone(),
            );
            async move {
                let provider = Arc::new(register_root_target_states_provider(
                    &ctx,
                    "test/cas",
                    handler(&db, &log, &fail),
                )?);
                ctx.mount_each(
                    vec!["c1", "c2"],
                    |item| (*item).to_string(),
                    move |child, item| {
                        let provider = provider.clone();
                        let outcomes = outcomes.clone();
                        let barrier = barrier.clone();
                        async move {
                            // Both components have "looked up and found
                            // nothing" before either tries to create.
                            timeout(Duration::from_secs(10), barrier.wait())
                                .await
                                .map_err(|_| {
                                    Error::engine("components did not run concurrently")
                                })?;
                            let won = declare_target_state_optimistic(
                                &child,
                                provider.target_state("shared", format!("from-{item}")),
                            )
                            .await?;
                            outcomes.lock().unwrap().push((item.to_string(), won));
                            Ok::<(), Error>(())
                        }
                    },
                )
                .await?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    let outcomes = outcomes.lock().unwrap().clone();
    assert_eq!(outcomes.len(), 2);
    let winners: Vec<&String> = outcomes
        .iter()
        .filter(|(_, w)| *w)
        .map(|(n, _)| n)
        .collect();
    assert_eq!(
        winners.len(),
        1,
        "exactly one claimer must win: {outcomes:?}"
    );

    // Exactly one row exists, and it carries the winner's value.
    let snapshot = db_snapshot(&db);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot.get("shared"),
        Some(&format!("from-{}", winners[0]))
    );
    assert_no_leftover_records(&app).await;
}

/// An in-flight optimistic claim on the same path makes a second call lose.
#[tokio::test]
async fn second_call_loses_to_an_active_claim() {
    let (app, _dir) = temp_app("optimistic_cas_vs_main").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    let claimed: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let after_winner = Arc::new(Barrier::new(2));
    let after_claim = Arc::new(Barrier::new(2));

    app.update({
        let (db, log, fail, claimed, after_winner, after_claim) = (
            db.clone(),
            log.clone(),
            fail.clone(),
            claimed.clone(),
            after_winner.clone(),
            after_claim.clone(),
        );
        move |ctx| {
            let (db, log, fail, claimed, after_winner, after_claim) = (
                db.clone(),
                log.clone(),
                fail.clone(),
                claimed.clone(),
                after_winner.clone(),
                after_claim.clone(),
            );
            async move {
                let provider = Arc::new(register_root_target_states_provider(
                    &ctx,
                    "test/cas",
                    handler(&db, &log, &fail),
                )?);
                ctx.mount_each(
                    vec!["writer", "claimer"],
                    |item| (*item).to_string(),
                    move |child, item| {
                        let provider = provider.clone();
                        let claimed = claimed.clone();
                        let after_winner = after_winner.clone();
                        let after_claim = after_claim.clone();
                        async move {
                            if item == "writer" {
                                declare_target_state_optimistic(
                                    &child,
                                    provider.target_state("shared", "winner".to_string()),
                                )
                                .await?;
                                timeout(Duration::from_secs(10), after_winner.wait())
                                    .await
                                    .map_err(|_| Error::engine("not concurrent"))?;
                                // Hold the winning operation open until the
                                // second claim attempt has been made.
                                timeout(Duration::from_secs(10), after_claim.wait())
                                    .await
                                    .map_err(|_| Error::engine("not concurrent"))?;
                            } else {
                                timeout(Duration::from_secs(10), after_winner.wait())
                                    .await
                                    .map_err(|_| Error::engine("not concurrent"))?;
                                let won = declare_target_state_optimistic(
                                    &child,
                                    provider.target_state("shared", "conditional".to_string()),
                                )
                                .await?;
                                *claimed.lock().unwrap() = Some(won);
                                timeout(Duration::from_secs(10), after_claim.wait())
                                    .await
                                    .map_err(|_| Error::engine("not concurrent"))?;
                            }
                            Ok::<(), Error>(())
                        }
                    },
                )
                .await?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(*claimed.lock().unwrap(), Some(false));
    assert_eq!(
        db_snapshot(&db).get("shared").map(String::as_str),
        Some("winner"),
    );
    assert_no_leftover_records(&app).await;
}

/// A confirmed owner from a previous run makes if-absent return `false`;
/// the caller then reuses the existing row with an ordinary declaration
/// instead of claiming ownership again.
#[tokio::test]
async fn cas_loses_to_a_confirmed_owner_on_rerun() {
    let (app, _dir) = temp_app("optimistic_cas_confirmed").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    let results: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));

    async fn run(app: &App, db: Db, log: Log, fail: FailOnce, results: Arc<Mutex<Vec<bool>>>) {
        app.update(move |ctx| {
            let (db, log, fail, results) = (db.clone(), log.clone(), fail.clone(), results.clone());
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/cas",
                    handler(&db, &log, &fail),
                )?;
                let won = declare_target_state_optimistic(
                    &ctx,
                    provider.target_state("e1", "uuid-1".to_string()),
                )
                .await?;
                results.lock().unwrap().push(won);
                if !won {
                    // Reuse the confirmed row rather than re-creating it.
                    declare_target_state(&ctx, provider.target_state("e1", "uuid-1".to_string()))?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    run(&app, db.clone(), log.clone(), fail.clone(), results.clone()).await;
    run(&app, db.clone(), log.clone(), fail.clone(), results.clone()).await;

    assert_eq!(*results.lock().unwrap(), vec![true, false]);
    // Run 1: eager create + submit re-apply. Run 2: nothing at all — the
    // loser performs no sink action and the value is unchanged.
    assert_eq!(drain(&log), vec!["create e1=uuid-1", "create e1=uuid-1"]);
    assert_eq!(db_snapshot(&db).len(), 1);
    assert_no_leftover_records(&app).await;
}

/// Independent target-state paths don't contend: both conditional writers
/// win.
#[tokio::test]
async fn cas_independent_keys_both_win() {
    let (app, _dir) = temp_app("optimistic_cas_independent").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());
    let outcomes: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));

    app.update({
        let (db, log, fail, outcomes) = (db.clone(), log.clone(), fail.clone(), outcomes.clone());
        move |ctx| {
            let (db, log, fail, outcomes) =
                (db.clone(), log.clone(), fail.clone(), outcomes.clone());
            async move {
                let provider = Arc::new(register_root_target_states_provider(
                    &ctx,
                    "test/cas",
                    handler(&db, &log, &fail),
                )?);
                ctx.mount_each(
                    vec!["k1", "k2"],
                    |item| (*item).to_string(),
                    move |child, item| {
                        let provider = provider.clone();
                        let outcomes = outcomes.clone();
                        async move {
                            let won = declare_target_state_optimistic(
                                &child,
                                provider.target_state(item, format!("v-{item}")),
                            )
                            .await?;
                            outcomes.lock().unwrap().push(won);
                            Ok::<(), Error>(())
                        }
                    },
                )
                .await?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(*outcomes.lock().unwrap(), vec![true, true]);
    assert_eq!(db_snapshot(&db).len(), 2);
    assert_no_leftover_records(&app).await;
}

/// A winner whose component then fails releases both its marker and its CAS
/// claim, so the path is claimable again on the next run.
#[tokio::test]
async fn cas_winner_failure_frees_the_path_for_a_retry() {
    let (app, _dir) = temp_app("optimistic_cas_retry").await;
    let (db, log, fail) = (new_db(), new_log(), new_fail_once());

    let failed = app
        .update({
            let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
            move |ctx| {
                let (db, log, fail) = (db.clone(), log.clone(), fail.clone());
                async move {
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "test/cas",
                        handler(&db, &log, &fail),
                    )?;
                    let won = declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("e1", "first".to_string()),
                    )
                    .await?;
                    assert!(won);
                    Err::<(), Error>(Error::engine("processor blew up"))
                }
            }
        })
        .await;
    assert!(failed.is_err());
    assert!(db_snapshot(&db).is_empty());
    assert_eq!(drain(&log), vec!["create e1=first", "delete e1"]);

    // The path is free again: a later claimer wins it.
    let second: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    app.update({
        let (db, log, fail, second) = (db.clone(), log.clone(), fail.clone(), second.clone());
        move |ctx| {
            let (db, log, fail, second) = (db.clone(), log.clone(), fail.clone(), second.clone());
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/cas",
                    handler(&db, &log, &fail),
                )?;
                *second.lock().unwrap() = Some(
                    declare_target_state_optimistic(
                        &ctx,
                        provider.target_state("e1", "second".to_string()),
                    )
                    .await?,
                );
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(*second.lock().unwrap(), Some(true));
    assert_eq!(
        db_snapshot(&db).get("e1").map(String::as_str),
        Some("second")
    );
    assert_no_leftover_records(&app).await;
}
