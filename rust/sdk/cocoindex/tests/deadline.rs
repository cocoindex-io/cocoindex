use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use cocoindex::{
    App, Error, StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    UpdateOptions, declare_target_state, register_root_target_states_provider,
};
use cocoindex_core::engine::deadline::{
    testing_advance_deadline_clock, testing_disable_deadline_clock, testing_reset_deadline_clock,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

static MEMO_CHILD_CALLS: AtomicUsize = AtomicUsize::new(0);

struct TestClockGuard;

impl TestClockGuard {
    fn new() -> Self {
        testing_reset_deadline_clock();
        Self
    }

    fn reset(&self) {
        testing_reset_deadline_clock();
    }
}

impl Drop for TestClockGuard {
    fn drop(&mut self) {
        testing_disable_deadline_clock();
    }
}

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

fn assert_deadline(err: Error) {
    assert!(
        err.is_deadline_exceeded(),
        "expected typed DeadlineExceeded, got {err:?}"
    );
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct WriteAction {
    key: String,
    value: String,
}

#[derive(Clone)]
struct MemoryHandler {
    sink: TargetActionSink<WriteAction>,
}

impl TargetHandler<String> for MemoryHandler {
    type TrackingRecord = String;
    type Action = WriteAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<String>,
        prev_possible_records: Vec<Self::TrackingRecord>,
        _prev_may_be_missing: bool,
    ) -> cocoindex::Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>> {
        let Some(value) = desired_target_state else {
            return Ok(None);
        };
        if prev_possible_records.iter().any(|prev| prev == &value) {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(WriteAction {
                key: key.to_string(),
                value: value.clone(),
            }),
            sink: self.sink.clone(),
            tracking_record: Some(value),
            child_invalidation: None,
        }))
    }
}

fn declare_memory_row(
    ctx: &cocoindex::Ctx,
    provider_name: &'static str,
    value: &'static str,
    applied: Arc<Mutex<Vec<WriteAction>>>,
    advance_in_sink: bool,
) -> cocoindex::Result<()> {
    let sink = TargetActionSink::from_async_fn(move |actions| {
        let applied = applied.clone();
        async move {
            if advance_in_sink {
                testing_advance_deadline_clock(Duration::from_secs(2));
            }
            let mut applied = applied.lock().unwrap();
            for action in actions {
                if let TargetAction::Update(action) = action {
                    applied.push(action);
                }
            }
            Ok(())
        }
    });
    let provider =
        register_root_target_states_provider(ctx, provider_name, MemoryHandler { sink })?;
    declare_target_state(ctx, provider.target_state("row-1", value.to_string()))
}

#[cocoindex::function(memo)]
async fn memo_deadline_child(_ctx: &cocoindex::Ctx) -> cocoindex::Result<u32> {
    MEMO_CHILD_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(7)
}

#[tokio::test(flavor = "current_thread")]
async fn rust_sdk_deadline_conformance_subset() {
    let clock = TestClockGuard::new();

    // Nested deadlines are min-composed on the immutable Ctx carrier, and the
    // SDK exposes the same typed error that the Rust core produces.
    let (app, _dir) = temp_app("sdk_deadline_nested").await;
    app.update(|ctx| async move {
        assert!(!ctx.has_deadline());
        let outer = ctx.with_timeout(Duration::from_secs(10));
        assert_eq!(outer.remaining_deadline(), Some(Duration::from_secs(10)));

        testing_advance_deadline_clock(Duration::from_secs(5));
        let wider = outer.with_timeout(Duration::from_secs(20));
        assert_eq!(wider.remaining_deadline(), Some(Duration::from_secs(5)));
        let narrower = outer.with_timeout(Duration::from_secs(1));
        assert_eq!(narrower.remaining_deadline(), Some(Duration::from_secs(1)));

        testing_advance_deadline_clock(Duration::from_secs(2));
        assert_deadline(narrower.check_deadline().unwrap_err());
        wider.check_deadline()?;
        Ok(())
    })
    .await
    .unwrap();

    // A foreground child mounted through scope/use_mount receives the caller's
    // narrowed deadline. The core pre-memo/entry checkpoint catches an already
    // expired child before its body can run.
    clock.reset();
    let (app, _dir) = temp_app("sdk_deadline_use_mount_inherit").await;
    let child_body_started = Arc::new(AtomicBool::new(false));
    let err = app
        .update({
            let child_body_started = child_body_started.clone();
            move |ctx| async move {
                let scoped = ctx.with_timeout(Duration::from_secs(1));
                testing_advance_deadline_clock(Duration::from_secs(2));
                scoped
                    .scope(&"child", move |_child| {
                        let child_body_started = child_body_started.clone();
                        async move {
                            child_body_started.store(true, Ordering::SeqCst);
                            Ok(())
                        }
                    })
                    .await
            }
        })
        .await
        .unwrap_err();
    assert_deadline(err);
    assert!(
        !child_body_started.load(Ordering::SeqCst),
        "expired child body must not start before the core pre-memo checkpoint"
    );

    // Background/live-style components are isolated. The child processor Ctx
    // sees no deadline even though the parent has one.
    clock.reset();
    let (app, _dir) = temp_app("sdk_deadline_mount_isolate").await;
    let child_saw_no_deadline = Arc::new(AtomicBool::new(false));
    app.update_with_options(
        UpdateOptions {
            timeout: Some(Duration::from_secs(10)),
            ..UpdateOptions::default()
        },
        {
            let child_saw_no_deadline = child_saw_no_deadline.clone();
            move |ctx| async move {
                ctx.auto_refresh(&"isolated", Duration::from_secs(60), move |child| {
                    let child_saw_no_deadline = child_saw_no_deadline.clone();
                    async move {
                        child_saw_no_deadline.store(!child.has_deadline(), Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            }
        },
    )
    .await
    .unwrap();
    assert!(child_saw_no_deadline.load(Ordering::SeqCst));

    // ctx.map is cooperative, not cancel-by-drop: after one item observes the
    // deadline, already-started siblings still run to completion before map()
    // reports the first error in input order. Replacing join_all with
    // try_join_all makes this assertion fail because the slow future is dropped.
    clock.reset();
    let (app, _dir) = temp_app("sdk_deadline_map_drains").await;
    let slow_started = Arc::new(AtomicBool::new(false));
    let slow_released = Arc::new(Notify::new());
    let slow_completed = Arc::new(AtomicBool::new(false));
    let err = app
        .update_with_options(
            UpdateOptions {
                timeout: Some(Duration::from_secs(1)),
                ..UpdateOptions::default()
            },
            {
                let slow_started = slow_started.clone();
                let slow_released = slow_released.clone();
                let slow_completed = slow_completed.clone();
                move |ctx| async move {
                    let scoped = ctx.with_timeout(Duration::from_secs(1));
                    let check_ctx = scoped.clone();
                    let _: Vec<&'static str> = scoped
                        .map(["slow", "deadline"], move |item| {
                            let check_ctx = check_ctx.clone();
                            let slow_started = slow_started.clone();
                            let slow_released = slow_released.clone();
                            let slow_completed = slow_completed.clone();
                            async move {
                                match item {
                                    "slow" => {
                                        slow_started.store(true, Ordering::SeqCst);
                                        slow_released.notified().await;
                                        slow_completed.store(true, Ordering::SeqCst);
                                        Ok("slow")
                                    }
                                    "deadline" => {
                                        assert!(
                                            slow_started.load(Ordering::SeqCst),
                                            "slow item must be pending before deadline item fails"
                                        );
                                        slow_released.notify_one();
                                        testing_advance_deadline_clock(Duration::from_secs(2));
                                        check_ctx.check_deadline()?;
                                        Ok("deadline")
                                    }
                                    _ => unreachable!("test input is fixed"),
                                }
                            }
                        })
                        .await?;
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();
    assert_deadline(err);
    assert!(
        slow_completed.load(Ordering::SeqCst),
        "ctx.map must drain already-started siblings before returning a deadline error"
    );

    // If a processor expires after declaring target states, the core
    // post-body/pre-submit checkpoint stops submit; the sink is never called.
    clock.reset();
    let (app, _dir) = temp_app("sdk_deadline_before_submit").await;
    let applied = Arc::new(Mutex::new(Vec::<WriteAction>::new()));
    let err = app
        .update_with_options(
            UpdateOptions {
                timeout: Some(Duration::from_secs(1)),
                ..UpdateOptions::default()
            },
            {
                let applied = applied.clone();
                move |ctx| async move {
                    declare_memory_row(
                        &ctx,
                        "test/sdk_deadline_before_submit",
                        "v1",
                        applied,
                        false,
                    )?;
                    testing_advance_deadline_clock(Duration::from_secs(2));
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();
    assert_deadline(err);
    assert!(applied.lock().unwrap().is_empty());

    // Submit/sink work is isolated from the processor deadline. If the sink
    // advances the clock past the caller's deadline, the write still lands and
    // the update result reports the caller's expired deadline afterward.
    clock.reset();
    let (app, _dir) = temp_app("sdk_deadline_sink_isolate").await;
    let applied = Arc::new(Mutex::new(Vec::<WriteAction>::new()));
    let err = app
        .update_with_options(
            UpdateOptions {
                timeout: Some(Duration::from_secs(1)),
                ..UpdateOptions::default()
            },
            {
                let applied = applied.clone();
                move |ctx| async move {
                    declare_memory_row(
                        &ctx,
                        "test/sdk_deadline_sink_isolate",
                        "v1",
                        applied,
                        true,
                    )?;
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();
    assert_deadline(err);
    assert_eq!(
        applied.lock().unwrap().as_slice(),
        &[WriteAction {
            key: "\"row-1\"".to_string(),
            value: "v1".to_string(),
        }]
    );

    // Component memo hits still observe the core pre-memo deadline checkpoint:
    // after the first run populates the cache, an expired second run fails
    // before the memoized child body can execute again.
    clock.reset();
    MEMO_CHILD_CALLS.store(0, Ordering::SeqCst);
    let (app, _dir) = temp_app("sdk_deadline_memo_hit").await;
    app.update(|ctx| async move {
        let out = cocoindex::use_mount!(memo_deadline_child(ctx)).await?;
        assert_eq!(out, 7);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(MEMO_CHILD_CALLS.load(Ordering::SeqCst), 1);

    clock.reset();
    let err = app
        .update_with_options(
            UpdateOptions {
                timeout: Some(Duration::from_secs(1)),
                ..UpdateOptions::default()
            },
            |ctx| async move {
                testing_advance_deadline_clock(Duration::from_secs(2));
                let _ = cocoindex::use_mount!(memo_deadline_child(ctx)).await?;
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert_deadline(err);
    assert_eq!(MEMO_CHILD_CALLS.load(Ordering::SeqCst), 1);
}
