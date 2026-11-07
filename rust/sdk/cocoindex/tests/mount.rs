//! Background mounts (`Ctx::mount` / `mount!`), the environment-level and
//! scoped exception handlers that route their failures, and `with_subpath`.
//!
//! Mirrors the Python `test_exception_handlers.py` family and the
//! `component_subpath` block semantics against the Rust SDK surface.
//!
//!   cargo test -p cocoindex --test mount

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cocoindex::prelude::*;
use cocoindex::{Environment, ExceptionSource};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Shared helpers (mirrors tests/live_component.rs)
// ---------------------------------------------------------------------------

type Log = Arc<Mutex<Vec<String>>>;

/// What a handler saw: the error text, the mount kind, the source and the
/// background flag.
type SeenFailure = (String, MountKind, ExceptionSource, bool);

/// The failing path and its parent path, as a handler saw them.
type SeenPaths = (String, Option<String>);

fn new_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn drain_sorted(log: &Log) -> Vec<String> {
    let mut v = std::mem::take(&mut *log.lock().unwrap());
    v.sort();
    v
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

/// An app whose environment installs `handler` as the global exception
/// handler, the way a Python lifespan's `builder.set_exception_handler` does.
async fn temp_app_with_handler<H>(name: &str, handler: H) -> (App, tempfile::TempDir)
where
    H: Fn(&Error, &ExceptionContext) -> Result<()> + Send + Sync + 'static,
{
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .exception_handler(handler)
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    (app, dir)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

fn recording_sink(log: Log) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let log = log.clone();
        async move {
            let mut log = log.lock().unwrap();
            for action in actions {
                let (verb, row) = match action {
                    TargetAction::Create(r) => ("create", r),
                    TargetAction::Update(r) => ("update", r),
                    TargetAction::Delete(r) => ("delete", r),
                };
                log.push(match row.value {
                    Some(v) => format!("{verb} {}={}", row.key, v),
                    None => format!("{verb} {}", row.key),
                });
            }
            Ok(())
        }
    })
}

#[derive(Clone)]
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
                let row = RowAction {
                    key: k,
                    value: Some(value.clone()),
                };
                let action = if prev.is_empty() {
                    TargetAction::Create(row)
                } else {
                    TargetAction::Update(row)
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

/// Declare `rows` into a root provider registered by this component.
fn declare_rows(ctx: &Ctx, sink: TargetActionSink<RowAction>, rows: &[(&str, &str)]) -> Result<()> {
    let provider = root_provider(ctx, sink)?;
    for (k, v) in rows {
        declare_target_state(ctx, provider.target_state(*k, v.to_string()))?;
    }
    Ok(())
}

/// Register the recording root provider in this component. A provider is
/// reachable for cleanup only while a run registers it, so a parent that
/// mounts children it may stop mounting registers it itself and hands it down.
fn root_provider(
    ctx: &Ctx,
    sink: TargetActionSink<RowAction>,
) -> Result<TargetStateProvider<String>> {
    register_root_target_states_provider(ctx, "mount/rows", RowHandler { sink })
}

#[derive(Debug)]
struct DeadLetter(String);

impl std::fmt::Display for DeadLetter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dead-letter: {}", self.0)
    }
}

impl std::error::Error for DeadLetter {}

// ---------------------------------------------------------------------------
// Ctx::mount: the parent does not wait; `ready()` does
// ---------------------------------------------------------------------------

/// `mount` returns as soon as the child is scheduled: the parent runs on while
/// the child is still blocked, and `ready()` waits for the child's target
/// states to sync.
#[tokio::test]
async fn mount_returns_before_child_finishes_and_ready_waits_for_sync() {
    let (app, _dir) = temp_app("mount_background").await;
    let log = new_log();
    let sink = recording_sink(log.clone());
    let release = Arc::new(Notify::new());
    let synced_at_ready = Arc::new(Mutex::new(Vec::new()));

    app.update({
        let release = release.clone();
        let log = log.clone();
        let synced_at_ready = synced_at_ready.clone();
        move |ctx| async move {
            let handle = ctx
                .mount(&"child", {
                    let release = release.clone();
                    move |child| async move {
                        release.notified().await;
                        declare_rows(&child, sink, &[("a", "v1")])
                    }
                })
                .await?;
            // The child has not run: nothing is synced yet.
            assert!(
                log.lock().unwrap().is_empty(),
                "mount must not wait for the child"
            );
            release.notify_one();
            handle.ready().await?;
            *synced_at_ready.lock().unwrap() = log.lock().unwrap().clone();
            Ok(())
        }
    })
    .await
    .unwrap();

    assert_eq!(
        *synced_at_ready.lock().unwrap(),
        vec!["create a=v1"],
        "ready() resolves once the child's target states are synced"
    );
    assert_eq!(drain_sorted(&log), vec!["create a=v1"]);
}

/// The parent's completion still waits for the children it mounted, even when
/// nobody awaits their handles; a child no longer mounted is cleaned up.
#[tokio::test]
async fn mount_children_sync_before_update_completes_and_are_garbage_collected() {
    let (app, _dir) = temp_app("mount_gc").await;
    let log = new_log();

    async fn run(app: &App, log: &Log, keys: Vec<&'static str>) {
        let sink = recording_sink(log.clone());
        app.update(move |ctx| async move {
            let provider = root_provider(&ctx, sink)?;
            for key in keys {
                let provider = provider.clone();
                // The handle is dropped without `ready()`, as in a Python
                // mount loop that relies on the parent waiting for children.
                ctx.mount(&key, move |child| async move {
                    declare_target_state(&child, provider.target_state(key, "v1".to_string()))
                })
                .await?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }

    run(&app, &log, vec!["a", "b"]).await;
    assert_eq!(drain_sorted(&log), vec!["create a=v1", "create b=v1"]);

    run(&app, &log, vec!["a"]).await;
    assert_eq!(drain_sorted(&log), vec!["delete b"]);
}

/// With no handler registered, a background failure is logged and dropped:
/// the parent, its siblings and `ready()` all succeed (the Python default).
#[tokio::test]
async fn mount_failure_without_handler_is_logged_not_propagated() {
    let (app, _dir) = temp_app("mount_no_handler").await;
    let log = new_log();
    let sink = recording_sink(log.clone());

    app.update(move |ctx| async move {
        let failing = ctx
            .mount(
                &"failing",
                |_child| async move { Err(Error::engine("boom")) },
            )
            .await?;
        ctx.mount(&"sibling", move |child| async move {
            declare_rows(&child, sink, &[("s", "v1")])
        })
        .await?;
        failing
            .ready()
            .await
            .expect("an unhandled background failure is logged, not propagated");
        Ok(())
    })
    .await
    .expect("the parent is unaffected by a background failure");

    assert_eq!(drain_sorted(&log), vec!["create s=v1"]);
}

// ---------------------------------------------------------------------------
// Exception handlers: global (environment) and scoped
// ---------------------------------------------------------------------------

/// Mirrors `test_global_exception_handler_invoked_for_background_mount`.
#[tokio::test]
async fn global_exception_handler_invoked_for_background_mount() {
    let seen: Arc<Mutex<Vec<SeenFailure>>> = Arc::new(Mutex::new(Vec::new()));
    let (app, _dir) = temp_app_with_handler("mount_global_handler", {
        let seen = seen.clone();
        move |err, exc| {
            seen.lock().unwrap().push((
                err.to_string(),
                exc.mount_kind,
                exc.source,
                exc.is_background,
            ));
            Ok(())
        }
    })
    .await;

    app.update(|ctx| async move {
        ctx.mount(&"child", |_child| async move { Err(Error::engine("boom")) })
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        vec![(
            "boom".to_string(),
            MountKind::Mount,
            ExceptionSource::Component,
            true
        )]
    );
}

/// Mirrors `test_scoped_handler_overrides_global_and_fallback_on_handler_error`:
/// the scoped handler sees the component's error first; when it returns `Err`,
/// the global handler receives that error with `source == Handler`.
#[tokio::test]
async fn scoped_handler_overrides_global_and_falls_back_on_handler_error() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (app, _dir) = temp_app_with_handler("mount_scoped_handler", {
        let calls = calls.clone();
        move |err, exc| {
            calls
                .lock()
                .unwrap()
                .push(format!("global:{:?}:{err}", exc.source));
            Ok(())
        }
    })
    .await;

    app.update({
        let calls = calls.clone();
        move |ctx| async move {
            let scoped = ctx.with_exception_handler({
                let calls = calls.clone();
                move |err, exc| {
                    calls
                        .lock()
                        .unwrap()
                        .push(format!("inner:{:?}:{err}", exc.source));
                    Err(Error::engine("handler failed"))
                }
            });
            scoped
                .mount(&"child", |_child| async move { Err(Error::engine("boom")) })
                .await?;
            Ok(())
        }
    })
    .await
    .unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            "inner:Component:boom".to_string(),
            "global:Handler:handler failed".to_string(),
        ]
    );
}

/// A scoped handler covers only mounts made from the scoped `Ctx`: a mount
/// from the unscoped parent goes straight to the global handler.
#[tokio::test]
async fn scoped_handler_applies_only_to_mounts_from_the_scoped_ctx() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (app, _dir) = temp_app_with_handler("mount_scoped_only", {
        let calls = calls.clone();
        move |_err, exc| {
            calls
                .lock()
                .unwrap()
                .push(format!("global:{}", exc.stable_path));
            Ok(())
        }
    })
    .await;

    app.update({
        let calls = calls.clone();
        move |ctx| async move {
            let scoped = ctx.with_exception_handler({
                let calls = calls.clone();
                move |_err, exc| {
                    calls
                        .lock()
                        .unwrap()
                        .push(format!("scoped:{}", exc.stable_path));
                    Ok(())
                }
            });
            scoped
                .mount(&"in", |_child| async move { Err(Error::engine("boom")) })
                .await?
                .ready()
                .await?;
            ctx.mount(&"out", |_child| async move { Err(Error::engine("boom")) })
                .await?
                .ready()
                .await?;
            Ok(())
        }
    })
    .await
    .unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        vec!["scoped:/\"in\"".to_string(), "global:/\"out\"".to_string()]
    );
}

/// Mirrors `test_handler_raise_preserves_type_through_ready`: the error a
/// handler returns reaches `ready()` as the same value, so the caller can
/// match on its type.
#[tokio::test]
async fn handler_error_propagates_through_ready_with_its_type() {
    let (app, _dir) = temp_app_with_handler("mount_handler_propagates", |err, _exc| {
        Err(Error::user(DeadLetter(err.to_string())))
    })
    .await;

    let caught = app
        .update(|ctx| async move {
            let handle = ctx
                .mount(&"child", |_child| async move { Err(Error::engine("boom")) })
                .await?;
            Ok(handle.ready().await.err().map(|e| {
                let is_dead_letter = match &e {
                    Error::User(inner) => inner.downcast_ref::<DeadLetter>().is_some(),
                    _ => false,
                };
                (is_dead_letter, e.to_string())
            }))
        })
        .await
        .unwrap();

    assert_eq!(caught, Some((true, "dead-letter: boom".to_string())));
}

// ---------------------------------------------------------------------------
// with_subpath: nested component paths without repeating a prefix
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn fail_under_subpath(_ctx: &Ctx, _name: String) -> Result<()> {
    Err(Error::engine("boom"))
}

/// `with_subpath("process")` + key `a.md` mounts at `/process/a.md`, the
/// Python `with coco.component_subpath("process"):` block; the mount's parent
/// path is the subpath. Chained calls add parts. Both `Ctx::mount` and the
/// `mount!` macro honour it.
#[tokio::test]
async fn with_subpath_nests_child_paths() {
    let seen: Arc<Mutex<Vec<SeenPaths>>> = Arc::new(Mutex::new(Vec::new()));
    let (app, _dir) = temp_app_with_handler("mount_subpath", {
        let seen = seen.clone();
        move |_err, exc| {
            seen.lock()
                .unwrap()
                .push((exc.stable_path.clone(), exc.parent_stable_path.clone()));
            Ok(())
        }
    })
    .await;

    app.update(|ctx| async move {
        let process = ctx.with_subpath("process");
        process
            .mount(&"a.md", |_child| async move { Err(Error::engine("boom")) })
            .await?
            .ready()
            .await?;
        let ctx = &process.with_subpath("nested");
        cocoindex::mount!("b.md", fail_under_subpath(ctx, "b".to_string()))
            .await?
            .ready()
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (
                "/\"process\"/\"a.md\"".to_string(),
                Some("/\"process\"".to_string())
            ),
            (
                "/\"process\"/\"nested\"/\"b.md\"".to_string(),
                Some("/\"process\"/\"nested\"".to_string())
            ),
        ]
    );
}

/// A component mounted through a subpath is the component at that path: a
/// later run that mounts it again, through the subpath or as the child of a
/// scope at the prefix, finds its state unchanged instead of recreating it.
#[tokio::test]
async fn with_subpath_is_the_same_component_as_the_direct_path() {
    let (app, _dir) = temp_app("mount_subpath_identity").await;
    let log = new_log();

    let sink = recording_sink(log.clone());
    app.update(move |ctx| async move {
        let provider = root_provider(&ctx, sink)?;
        ctx.with_subpath("process")
            .mount(&"x", move |child| async move {
                declare_target_state(&child, provider.target_state("x", "v1".to_string()))
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["create x=v1"]);

    // Mounting it again through the subpath finds the component unchanged.
    let sink = recording_sink(log.clone());
    app.update(move |ctx| async move {
        let provider = root_provider(&ctx, sink)?;
        ctx.with_subpath("process")
            .mount(&"x", move |child| async move {
                declare_target_state(&child, provider.target_state("x", "v1".to_string()))
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), Vec::<String>::new());

    // The component's identity is its path: mounted as the child `x` of a
    // `process` scope component, it is still `/process/x`, unchanged.
    let sink = recording_sink(log.clone());
    app.update(move |ctx| async move {
        let provider = root_provider(&ctx, sink)?;
        ctx.scope(&"process", move |process| async move {
            process
                .mount(&"x", move |child| async move {
                    declare_target_state(&child, provider.target_state("x", "v1".to_string()))
                })
                .await?;
            Ok(())
        })
        .await
    })
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), Vec::<String>::new());
}

/// Mirrors the Python rule: `use_state` inside a subpath block is an error,
/// because the state belongs to the component, not to the subpath.
#[tokio::test]
async fn use_state_is_rejected_on_a_subpath_ctx() {
    let (app, _dir) = temp_app("mount_subpath_use_state").await;

    let message = app
        .update(|ctx| async move {
            let err = ctx
                .with_subpath("process")
                .use_state("counter", 0u32)
                .err()
                .expect("use_state on a subpath ctx must fail");
            // The unscoped ctx still works.
            ctx.use_state("counter", 0u32)?;
            Ok(err.to_string())
        })
        .await
        .unwrap();
    assert!(message.contains("with_subpath"), "{message}");
}

// ---------------------------------------------------------------------------
// mount! macro: component-memo fast path, like use_mount!
// ---------------------------------------------------------------------------

static BG_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cocoindex::function]
async fn bg_count(_ctx: &Ctx, _n: u32) -> Result<()> {
    BG_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// `mount!` fingerprints the function's logic and arguments: an unchanged
/// component is skipped on the next run, and a changed argument re-runs it.
#[tokio::test]
async fn mount_macro_skips_unchanged_component_across_runs() {
    let (app, _dir) = temp_app("mount_macro_skip").await;

    async fn run(app: &App, n: u32) {
        app.update(move |ctx| async move {
            let ctx = &ctx;
            cocoindex::mount!(bg_count(ctx, n)).await?.ready().await?;
            Ok(())
        })
        .await
        .unwrap();
    }

    run(&app, 7).await;
    assert_eq!(BG_CALLS.load(Ordering::SeqCst), 1);
    run(&app, 7).await;
    assert_eq!(BG_CALLS.load(Ordering::SeqCst), 1, "unchanged: skipped");
    run(&app, 8).await;
    assert_eq!(
        BG_CALLS.load(Ordering::SeqCst),
        2,
        "changed argument: re-run"
    );
}
