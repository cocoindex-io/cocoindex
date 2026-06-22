//! Tests for the `#[function(logic_tracking = ...)]` knob, mirroring Python's
//! `@coco.fn(logic_tracking="full"|"self"|"none")`.
//!
//! The `full` vs `self` distinction (whether a *transitively-called* function's
//! logic change invalidates this one) only manifests across a code change between
//! runs — and the code hash is fixed at compile time — so it can't be exercised
//! in a single compiled test. What we verify here is the macro lowering that is
//! observable at runtime:
//!   1. every mode compiles and the function executes with the right result;
//!   2. `"none"` does NOT register a logic fingerprint, while `"full"`/`"self"` do.

use cocoindex::{App, Ctx, Result};

#[cocoindex::function]
async fn full_default(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(*x + 1)
}

#[cocoindex::function(logic_tracking = "full")]
async fn full_explicit(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(*x + 2)
}

#[cocoindex::function(logic_tracking = "self")]
async fn self_only(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(*x + 3)
}

#[cocoindex::function(logic_tracking = "none")]
async fn untracked(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(*x + 4)
}

// `none` combined with `memo` must still memoize (memo keys are independent of
// logic-set registration) but must not register a logic fingerprint.
#[cocoindex::function(memo, logic_tracking = "none")]
async fn untracked_memo(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(x * 10)
}

// `self` combined with `memo`.
#[cocoindex::function(memo, logic_tracking = "self")]
async fn self_memo(_ctx: &Ctx, x: &i64) -> Result<i64> {
    Ok(x * 100)
}

#[test]
fn all_logic_tracking_modes_execute() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("logic_tracking_exec")
        .db_path(dir.path().join("lmdb"))
        .build_blocking()
        .unwrap();

    app.update_blocking(|ctx| async move {
        assert_eq!(full_default(&ctx, &10).await?, 11);
        assert_eq!(full_explicit(&ctx, &10).await?, 12);
        assert_eq!(self_only(&ctx, &10).await?, 13);
        assert_eq!(untracked(&ctx, &10).await?, 14);
        assert_eq!(untracked_memo(&ctx, &5).await?, 50);
        assert_eq!(self_memo(&ctx, &5).await?, 500);
        Ok(())
    })
    .unwrap();
}

/// The names of every function registered in the link-time logic set.
fn registered_logic_names() -> Vec<&'static str> {
    cocoindex::COCO_FN_LOGIC.iter().map(|e| e.name).collect()
}

#[test]
fn tracked_modes_register_logic_none_does_not() {
    let names = registered_logic_names();

    // full (default + explicit) and self register their logic fingerprint.
    assert!(names.contains(&"full_default"), "full_default should register");
    assert!(names.contains(&"full_explicit"), "full_explicit should register");
    assert!(names.contains(&"self_only"), "self_only should register");
    assert!(names.contains(&"self_memo"), "self_memo should register");

    // none must NOT register — its logic is not tracked at all.
    assert!(
        !names.contains(&"untracked"),
        "logic_tracking=\"none\" must not register a logic fingerprint"
    );
    assert!(
        !names.contains(&"untracked_memo"),
        "logic_tracking=\"none\" + memo must not register a logic fingerprint"
    );
}

#[test]
fn memo_with_logic_tracking_caches() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A memoized function whose body counts executions, to confirm memo still
    // works under a non-default logic_tracking mode.
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo, logic_tracking = "self")]
    async fn counting(_ctx: &Ctx, x: &i64) -> Result<i64> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(x + 1)
    }

    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("logic_tracking_memo")
        .db_path(dir.path().join("lmdb"))
        .build_blocking()
        .unwrap();

    // Run 1: cache miss → executes.
    app.update_blocking(|ctx| async move {
        assert_eq!(counting(&ctx, &7).await?, 8);
        Ok(())
    })
    .unwrap();
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "first call should execute");

    // Run 2 (same input, same process/db): cache hit → does not re-execute.
    app.update_blocking(|ctx| async move {
        assert_eq!(counting(&ctx, &7).await?, 8);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        1,
        "second call with same input should be a memo hit (self-mode memo still caches)"
    );
}
