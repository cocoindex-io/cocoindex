//! Tests for the detailed per-component stats surface (`ComponentStats`,
//! `UpdateStats`, `UpdateStatus`) — Python parity with `cocoindex.UpdateStats`.

use cocoindex::{App, ComponentStats, UpdateStats, UpdateStatus};

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

// ─── Pure-data tests (no engine) ─────────────────────────────────────────────

#[test]
fn component_stats_derived_counts() {
    let s = ComponentStats {
        num_execution_starts: 10,
        num_unchanged: 2,
        num_adds: 3,
        num_deletes: 1,
        num_reprocesses: 1,
        num_errors: 2,
    };
    // processed = unchanged + adds + deletes + reprocesses = 2+3+1+1 = 7
    assert_eq!(s.num_processed(), 7);
    // finished = processed + errors = 7 + 2 = 9
    assert_eq!(s.num_finished(), 9);
    // in_progress = starts - finished = 10 - 9 = 1
    assert_eq!(s.num_in_progress(), 1);
    assert!(s.has_errors());
}

#[test]
fn component_stats_in_progress_saturates() {
    // finished > starts (shouldn't happen in practice) must not underflow.
    let s = ComponentStats {
        num_execution_starts: 1,
        num_adds: 5,
        ..Default::default()
    };
    assert_eq!(s.num_in_progress(), 0);
    assert!(!s.has_errors());
}

#[test]
fn update_stats_total_aggregates_components() {
    let mut by_component = std::collections::BTreeMap::new();
    by_component.insert(
        "a".to_string(),
        ComponentStats {
            num_adds: 3,
            num_errors: 1,
            num_execution_starts: 4,
            ..Default::default()
        },
    );
    by_component.insert(
        "b".to_string(),
        ComponentStats {
            num_adds: 2,
            num_unchanged: 5,
            num_execution_starts: 7,
            ..Default::default()
        },
    );
    let stats = UpdateStats {
        by_component,
        status: UpdateStatus::Ready,
    };
    let total = stats.total();
    assert_eq!(total.num_adds, 5);
    assert_eq!(total.num_unchanged, 5);
    assert_eq!(total.num_errors, 1);
    assert_eq!(total.num_execution_starts, 11);
}

#[test]
fn update_stats_run_stats_derives_coarse_view() {
    let mut by_component = std::collections::BTreeMap::new();
    by_component.insert(
        "a".to_string(),
        ComponentStats {
            num_unchanged: 4,
            num_adds: 3,
            num_reprocesses: 2,
            num_deletes: 1,
            num_errors: 5, // errors are excluded from processed/written
            num_execution_starts: 15,
        },
    );
    let stats = UpdateStats {
        by_component,
        status: UpdateStatus::Ready,
    };
    let run = stats.run_stats();
    // Same mapping as the engine→RunStats conversion: skipped=unchanged,
    // written=adds+reprocesses, deleted=deletes, processed=their sum.
    assert_eq!(run.skipped, 4);
    assert_eq!(run.written, 5); // adds(3) + reprocesses(2)
    assert_eq!(run.deleted, 1);
    assert_eq!(run.processed, 10); // 4 + 3 + 2 + 1
    assert_eq!(run.processed, run.written + run.skipped + run.deleted);
    assert!(run.elapsed.is_zero());
}

#[test]
fn update_status_default_is_running() {
    assert_eq!(UpdateStatus::default(), UpdateStatus::Running);
    assert_eq!(UpdateStats::default().status, UpdateStatus::Running);
    assert!(UpdateStats::default().by_component.is_empty());
}

// ─── Engine-integration tests ────────────────────────────────────────────────

#[tokio::test]
async fn detailed_stats_after_completion_are_ready_and_consistent() {
    let (app, _dir) = temp_app("detailed_stats_consistency").await;

    let mut handle = app
        .start_update(|ctx| async move {
            // A memoized unit of work registers a processing component, so the
            // stats map is non-empty.
            let v: i32 = ctx.memo(&"k", |_ctx| async move { Ok(42) }).await?;
            Ok::<_, cocoindex::Error>(v)
        })
        .unwrap();

    loop {
        if handle.changed().await.unwrap().is_done() {
            break;
        }
    }

    let detailed = handle.detailed_stats_snapshot();
    let coarse = handle.stats_snapshot();

    // Completed run → Ready, no errors.
    assert_eq!(detailed.status, UpdateStatus::Ready);
    assert!(!detailed.by_component.is_empty(), "expected at least one component");
    assert_eq!(detailed.total().num_errors, 0);

    // The detailed view must agree with the coarse RunStats:
    // RunStats.processed == sum of every component's num_processed().
    assert_eq!(
        detailed.total().num_processed(),
        coarse.processed,
        "detailed total must match the coarse RunStats.processed"
    );
    // RunStats.skipped == total unchanged; RunStats.written == adds + reprocesses.
    assert_eq!(detailed.total().num_unchanged, coarse.skipped);
    assert_eq!(
        detailed.total().num_adds + detailed.total().num_reprocesses,
        coarse.written
    );
    assert_eq!(detailed.total().num_deletes, coarse.deleted);

    assert_eq!(handle.result().await.unwrap(), 42);
}

#[tokio::test]
async fn detailed_stats_snapshot_before_completion_is_running() {
    let (app, _dir) = temp_app("detailed_stats_running").await;

    // Before driving the handle to Done, status should be Running.
    let handle = app
        .start_update(|ctx| async move {
            let v: i32 = ctx.memo(&"k", |_ctx| async move { Ok(1) }).await?;
            Ok::<_, cocoindex::Error>(v)
        })
        .unwrap();

    let detailed = handle.detailed_stats_snapshot();
    assert_eq!(detailed.status, UpdateStatus::Running);

    // Drain so the handle completes cleanly.
    let mut handle = handle;
    loop {
        if handle.changed().await.unwrap().is_done() {
            break;
        }
    }
    let _ = handle.result().await.unwrap();
}
