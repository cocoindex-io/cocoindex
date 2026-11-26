//! Edge cases of `#[cocoindex::function(batching)]`: batch sizes around
//! `max_batch_size`, bodies that break their contract (wrong output count,
//! panics, errors), callers that go away while their item is queued, and
//! memoization with the same item twice in one batch.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cocoindex::testing::TestEnv;
use cocoindex::{App, Ctx, Error, Result};
use futures::future::join_all;
use tokio::sync::Notify;

async fn temp_app(name: &str) -> (App, TestEnv) {
    let env = TestEnv::new().await.unwrap();
    let app = env.app(name).await.unwrap();
    (app, env)
}

/// Call `f` on every item concurrently in one update; each result as
/// `Ok(value)` or `Err(message)`.
async fn call_all<F, Fut>(app: &App, items: Vec<i64>, f: F) -> Vec<std::result::Result<i64, String>>
where
    F: Fn(Ctx, i64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<i64>> + Send,
{
    app.update(move |ctx| async move {
        Ok(join_all(items.into_iter().map(|item| f(ctx.clone(), item)))
            .await
            .into_iter()
            .map(|result| result.map_err(|err| err.to_string()))
            .collect::<Vec<_>>())
    })
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------
// Batch sizes at and around max_batch_size
// ---------------------------------------------------------------------------

static SIZED_BATCHES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

#[cocoindex::function(batching, max_batch_size = 4)]
async fn sized(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    SIZED_BATCHES.lock().unwrap().push(items.len());
    // Give the next callers time to queue behind this batch.
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(items.into_iter().map(|item| item * 10).collect())
}

#[tokio::test]
async fn physical_batches_never_exceed_max_batch_size() {
    let (app, _env) = temp_app("batch_sizes").await;
    for count in [1, 3, 4, 5, 8, 9, 13, 64] {
        SIZED_BATCHES.lock().unwrap().clear();
        let items: Vec<i64> = (0..count).collect();
        let results = call_all(&app, items.clone(), |ctx, item| async move {
            sized(&ctx, item).await
        })
        .await;
        let expected: Vec<std::result::Result<i64, String>> =
            items.iter().map(|item| Ok(item * 10)).collect();
        assert_eq!(results, expected, "count {count}");
        let sizes = SIZED_BATCHES.lock().unwrap().clone();
        assert!(
            sizes.iter().all(|size| (1..=4).contains(size)),
            "count {count}: {sizes:?}"
        );
        assert_eq!(
            sizes.iter().sum::<usize>(),
            count as usize,
            "count {count}: {sizes:?}"
        );
        if count >= 9 {
            assert!(
                sizes.contains(&4),
                "count {count}: full batches expected, got {sizes:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A body that breaks its contract
// ---------------------------------------------------------------------------

#[cocoindex::function(batching)]
async fn one_output_too_many(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    Ok(vec![0; items.len() + 1])
}

#[cocoindex::function(batching)]
async fn one_output_too_few(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    Ok(vec![0; items.len() - 1])
}

#[tokio::test]
async fn a_wrong_output_count_fails_every_caller() {
    let (app, _env) = temp_app("wrong_output_count").await;
    let results = call_all(&app, (0..6).collect(), |ctx, item| async move {
        one_output_too_many(&ctx, item).await
    })
    .await;
    for result in &results {
        let err = result.as_ref().unwrap_err();
        assert!(err.contains("outputs for"), "{err}");
    }
    let results = call_all(&app, (0..6).collect(), |ctx, item| async move {
        one_output_too_few(&ctx, item).await
    })
    .await;
    for result in &results {
        let err = result.as_ref().unwrap_err();
        assert!(err.contains("outputs for"), "{err}");
    }
}

static PANICKY_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cocoindex::function(batching, max_batch_size = 3)]
async fn panicky(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    PANICKY_CALLS.fetch_add(1, Ordering::SeqCst);
    if items.contains(&-1) {
        panic!("panicky body saw -1");
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(items.into_iter().map(|item| item + 1).collect())
}

/// A panicking body fails the callers of its batch with an error instead of
/// tearing down the update, and the batcher keeps serving later calls.
#[tokio::test]
async fn a_panicking_body_fails_its_batch_and_the_batcher_recovers() {
    let (app, _env) = temp_app("panicking_body").await;
    // Alone: the inline path.
    let results = call_all(&app, vec![-1], |ctx, item| async move {
        panicky(&ctx, item).await
    })
    .await;
    assert!(results[0].is_err(), "{results:?}");

    // Among others: only the callers sharing its batch fail.
    let items: Vec<i64> = vec![1, 2, 3, -1, 5, 6, 7, 8, 9];
    let results = call_all(&app, items.clone(), |ctx, item| async move {
        panicky(&ctx, item).await
    })
    .await;
    assert!(results[3].is_err(), "{results:?}");
    let failed = results.iter().filter(|result| result.is_err()).count();
    assert!(failed <= 3, "only the panicking batch fails: {results:?}");
    for (item, result) in items.iter().zip(&results) {
        if let Ok(value) = result {
            assert_eq!(*value, item + 1);
        }
    }

    // The batcher still works.
    let results = call_all(&app, (10..20).collect(), |ctx, item| async move {
        panicky(&ctx, item).await
    })
    .await;
    assert!(results.iter().all(|result| result.is_ok()), "{results:?}");
}

#[derive(Debug, thiserror::Error)]
#[error("bad input {0}")]
struct BadInput(i64);

#[cocoindex::function(batching, max_batch_size = 2)]
async fn fails_on_13(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    tokio::time::sleep(Duration::from_millis(5)).await;
    if items.contains(&13) {
        return Err(Error::user(BadInput(13)));
    }
    Ok(items)
}

/// Without `retry_with_smaller_batch`, an error fails the whole physical
/// batch, and no other.
#[tokio::test]
async fn an_error_fails_only_the_callers_of_its_batch() {
    let (app, _env) = temp_app("error_in_batch").await;
    let items: Vec<i64> = (10..20).collect();
    let results = call_all(&app, items.clone(), |ctx, item| async move {
        fails_on_13(&ctx, item).await
    })
    .await;
    let err = results[3].as_ref().unwrap_err();
    assert!(err.contains("bad input 13"), "{err}");
    let failed: Vec<&std::result::Result<i64, String>> =
        results.iter().filter(|result| result.is_err()).collect();
    assert!(
        failed.len() <= 2,
        "a batch holds at most two items: {results:?}"
    );
    for (item, result) in items.iter().zip(&results) {
        match result {
            Ok(value) => assert_eq!(value, item),
            Err(err) => assert!(err.contains("bad input 13"), "{err}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Callers that go away while their item is queued
// ---------------------------------------------------------------------------

static GATED_STARTED: Notify = Notify::const_new();
static GATED_RELEASE: Notify = Notify::const_new();
static GATED_SEEN: Mutex<Vec<Vec<i64>>> = Mutex::new(Vec::new());

/// The batch holding item 0 waits for `GATED_RELEASE`, so later calls queue.
#[cocoindex::function(batching)]
async fn gated(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    GATED_SEEN.lock().unwrap().push(items.clone());
    if items.contains(&0) {
        GATED_STARTED.notify_one();
        GATED_RELEASE.notified().await;
    }
    Ok(items.into_iter().map(|item| item * 2).collect())
}

#[tokio::test]
async fn a_caller_dropped_while_queued_leaves_the_others_their_results() {
    let (app, _env) = temp_app("dropped_caller").await;
    let results = app
        .update(|ctx| async move {
            let first = {
                let ctx = ctx.clone();
                async move { gated(&ctx, 0).await }
            };
            let rest = {
                let ctx = ctx.clone();
                async move {
                    GATED_STARTED.notified().await;
                    // Item 1 gives up while queued behind the running batch;
                    // items 2 and 3 wait.
                    let gives_up = tokio::time::timeout(Duration::from_millis(50), gated(&ctx, 1));
                    let waits = join_all([gated(&ctx, 2), gated(&ctx, 3)]);
                    let release = async {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        GATED_RELEASE.notify_one();
                    };
                    let (gave_up, waited, ()) = tokio::join!(gives_up, waits, release);
                    assert!(gave_up.is_err(), "item 1 timed out while queued");
                    waited.into_iter().collect::<Result<Vec<_>>>()
                }
            };
            let (first, rest) = tokio::join!(first, rest);
            let mut out = vec![first?];
            out.extend(rest?);
            Ok(out)
        })
        .await
        .unwrap();
    assert_eq!(results, vec![0, 4, 6]);
    let seen = GATED_SEEN.lock().unwrap().clone();
    let processed: BTreeSet<i64> = seen.iter().flatten().copied().collect();
    assert!(processed.contains(&2) && processed.contains(&3), "{seen:?}");
}

static ABANDONED_STARTED: Notify = Notify::const_new();
static ABANDONED_RELEASE: Notify = Notify::const_new();
static ABANDONED_SEEN: Mutex<Vec<Vec<i64>>> = Mutex::new(Vec::new());

#[cocoindex::function(batching)]
async fn abandonable(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    ABANDONED_SEEN.lock().unwrap().push(items.clone());
    if items.contains(&0) {
        ABANDONED_STARTED.notify_one();
        ABANDONED_RELEASE.notified().await;
    }
    Ok(items)
}

/// A queued batch whose callers have all gone away never runs its body.
#[tokio::test]
async fn a_batch_abandoned_by_all_its_callers_does_not_run() {
    let (app, _env) = temp_app("abandoned_batch").await;
    app.update(|ctx| async move {
        let first = {
            let ctx = ctx.clone();
            async move { abandonable(&ctx, 0).await }
        };
        let rest = {
            let ctx = ctx.clone();
            async move {
                ABANDONED_STARTED.notified().await;
                let queued = join_all([abandonable(&ctx, 1), abandonable(&ctx, 2)]);
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), queued)
                        .await
                        .is_err()
                );
                ABANDONED_RELEASE.notify_one();
            }
        };
        let (first, ()) = tokio::join!(first, rest);
        first?;
        // Let a wrongly started batch reach its body.
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .await
    .unwrap();
    let seen = ABANDONED_SEEN.lock().unwrap().clone();
    assert_eq!(seen, vec![vec![0]], "the abandoned batch must not run");
}

// ---------------------------------------------------------------------------
// Memoization with duplicates in one batch
// ---------------------------------------------------------------------------

static MEMO_DUP_ITEMS: Mutex<Vec<i64>> = Mutex::new(Vec::new());

#[cocoindex::function(memo, batching)]
async fn memo_dup(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    MEMO_DUP_ITEMS.lock().unwrap().extend(items.iter().copied());
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(items.into_iter().map(|item| item * 3).collect())
}

/// The same item several times in one update gets the same result each time,
/// and a later update serves every one of them from the memo.
#[tokio::test]
async fn memo_batching_with_duplicate_items() {
    let (app, _env) = temp_app("memo_duplicates").await;
    let items: Vec<i64> = vec![1, 2, 1, 1, 3, 2, 1];
    for round in 0..2 {
        let results = call_all(&app, items.clone(), |ctx, item| async move {
            memo_dup(&ctx, item).await
        })
        .await;
        let expected: Vec<std::result::Result<i64, String>> =
            items.iter().map(|item| Ok(item * 3)).collect();
        assert_eq!(results, expected, "round {round}");
        if round == 0 {
            let seen: BTreeSet<i64> = MEMO_DUP_ITEMS.lock().unwrap().iter().copied().collect();
            assert_eq!(seen, BTreeSet::from([1, 2, 3]));
            MEMO_DUP_ITEMS.lock().unwrap().clear();
        }
    }
    assert!(
        MEMO_DUP_ITEMS.lock().unwrap().is_empty(),
        "the second update should be served from the memo"
    );
}

// ---------------------------------------------------------------------------
// Runners that break their contract
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("runner refused")]
struct Refused;

struct RefusingRunner;

#[async_trait::async_trait]
impl cocoindex::Runner for RefusingRunner {
    async fn run(&self, _body: cocoindex::RunnerBody<'_>) -> Result<cocoindex::RunnerOutput> {
        Err(Error::user(Refused))
    }
}

static REFUSED_BODY_RAN: AtomicUsize = AtomicUsize::new(0);

#[cocoindex::function(batching, runner = RefusingRunner)]
async fn refused(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    REFUSED_BODY_RAN.fetch_add(1, Ordering::SeqCst);
    Ok(items)
}

/// A runner that fails without running the body fails the callers with its
/// error, and the body never runs.
#[tokio::test]
async fn a_runner_error_reaches_every_caller() {
    let (app, _env) = temp_app("refusing_runner").await;
    let results = call_all(&app, (0..5).collect(), |ctx, item| async move {
        refused(&ctx, item).await
    })
    .await;
    for result in &results {
        let err = result.as_ref().unwrap_err();
        assert!(err.contains("runner refused"), "{err}");
    }
    assert_eq!(REFUSED_BODY_RAN.load(Ordering::SeqCst), 0);
}
