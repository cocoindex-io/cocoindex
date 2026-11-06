//! End-to-end tests for batch splitting (`Error::retry_with_smaller_batch`)
//! and the GPU runner (`runner = ...`) in `#[cocoindex::function]`.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use cocoindex::testing::TestEnv;
use cocoindex::{App, Batched, Ctx, Error, GPU, GPURunner, Result, configure_gpu_pool};
use tokio::sync::Notify;
use tokio::time::sleep;

async fn temp_app(name: &str) -> (App, TestEnv) {
    let env = TestEnv::new().await.unwrap();
    let app = env.app(name).await.unwrap();
    (app, env)
}

/// The GPU pool is process-wide, so tests that configure it take turns.
static GPU_POOL_TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, thiserror::Error)]
#[error("provider rejected the batch: {0}")]
struct BatchLimitError(String);

#[derive(Debug, thiserror::Error)]
#[error("bad input {0}")]
struct BadInput(i64);

fn user_error_message(err: &Error) -> String {
    match err {
        Error::User(inner) => inner.to_string(),
        other => panic!("expected the body's own error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// RetryWithSmallerBatch: the batch is halved until it succeeds
// ---------------------------------------------------------------------------

static LIMITED_BATCH_SIZES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
static LIMITED_STARTED: Notify = Notify::const_new();
static LIMITED_RELEASE: Notify = Notify::const_new();

/// Accepts at most two items per request.
#[cocoindex::function(batching)]
async fn limited(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    let is_first = {
        let mut sizes = LIMITED_BATCH_SIZES.lock().unwrap();
        sizes.push(items.len());
        sizes.len() == 1
    };
    if is_first {
        LIMITED_STARTED.notify_one();
        LIMITED_RELEASE.notified().await;
    }
    if items.len() > 2 {
        return Err(Error::retry_with_smaller_batch(Error::user(
            BatchLimitError("too big".into()),
        )));
    }
    Ok(items.into_iter().map(|item| item * 2).collect())
}

#[tokio::test]
async fn retry_with_smaller_batch_splits_until_success() {
    let (app, _env) = temp_app("split_until_success").await;
    app.update(|ctx| async move {
        // The first call runs inline and blocks, so the next four queue into
        // one batch of four behind it.
        let blocker_ctx = ctx.clone();
        let blocker = tokio::spawn(async move { limited(&blocker_ctx, 0).await });
        LIMITED_STARTED.notified().await;
        let queued: Vec<_> = [1, 2, 3, 4]
            .into_iter()
            .map(|item| {
                let ctx = ctx.clone();
                tokio::spawn(async move { limited(&ctx, item).await })
            })
            .collect();
        sleep(Duration::from_millis(50)).await;
        LIMITED_RELEASE.notify_one();

        assert_eq!(blocker.await.unwrap()?, 0);
        let mut outputs = Vec::new();
        for task in queued {
            outputs.push(task.await.unwrap()?);
        }
        assert_eq!(
            outputs,
            vec![2, 4, 6, 8],
            "results map back to their callers"
        );
        Ok(())
    })
    .await
    .unwrap();

    let sizes = LIMITED_BATCH_SIZES.lock().unwrap();
    // Inline [0], then the rejected batch of four, then its two halves.
    assert_eq!(sizes[..2], [1, 4]);
    let mut halves = sizes[2..].to_vec();
    halves.sort_unstable();
    assert_eq!(halves, [2, 2]);
}

static POISON_CALLS: AtomicUsize = AtomicUsize::new(0);
static POISON_STARTED: Notify = Notify::const_new();
static POISON_RELEASE: Notify = Notify::const_new();

/// Rejects any batch holding a negative item; names the item only alone.
#[cocoindex::function(batching)]
async fn poisonable(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    if POISON_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
        POISON_STARTED.notify_one();
        POISON_RELEASE.notified().await;
    }
    if let Some(bad) = items.iter().copied().find(|item| *item < 0) {
        if items.len() == 1 {
            return Err(Error::user(BadInput(bad)));
        }
        return Err(Error::retry_with_smaller_batch(Error::user(
            BatchLimitError("rejected".into()),
        )));
    }
    Ok(items.into_iter().map(|item| item * 2).collect())
}

#[tokio::test]
async fn retry_with_smaller_batch_isolates_poison_item() {
    let (app, _env) = temp_app("split_poison_item").await;
    app.update(|ctx| async move {
        let blocker_ctx = ctx.clone();
        let blocker = tokio::spawn(async move { poisonable(&blocker_ctx, 1).await });
        POISON_STARTED.notified().await;
        let queued: Vec<_> = [2, 3, -7, 4]
            .into_iter()
            .map(|item| {
                let ctx = ctx.clone();
                tokio::spawn(async move { poisonable(&ctx, item).await })
            })
            .collect();
        sleep(Duration::from_millis(50)).await;
        POISON_RELEASE.notify_one();

        assert_eq!(blocker.await.unwrap()?, 2);
        let mut outputs = Vec::new();
        for task in queued {
            outputs.push(task.await.unwrap());
        }
        assert_eq!(outputs[0].as_ref().unwrap(), &4);
        assert_eq!(outputs[1].as_ref().unwrap(), &6);
        assert_eq!(
            user_error_message(outputs[2].as_ref().unwrap_err()),
            "bad input -7",
            "only the bad input's caller fails, with the body's own error"
        );
        assert_eq!(outputs[3].as_ref().unwrap(), &8);
        Ok(())
    })
    .await
    .unwrap();
}

#[cocoindex::function(batching)]
async fn always_split(_ctx: &Ctx, _items: Vec<i64>) -> Result<Vec<i64>> {
    Err(Error::retry_with_smaller_batch(Error::user(BadInput(0))))
}

#[tokio::test]
async fn retry_with_smaller_batch_at_size_one_fails_with_cause() {
    let (app, _env) = temp_app("split_size_one").await;
    let err = app
        .update(|ctx| async move { always_split(&ctx, 1).await })
        .await
        .unwrap_err();
    assert_eq!(user_error_message(&err), "bad input 0");
}

static MEMO_SPLIT_CALLS: AtomicUsize = AtomicUsize::new(0);
static MEMO_SPLIT_STARTED: Notify = Notify::const_new();
static MEMO_SPLIT_RELEASE: Notify = Notify::const_new();

#[cocoindex::function(memo, batching)]
async fn memo_limited(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    if MEMO_SPLIT_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
        MEMO_SPLIT_STARTED.notify_one();
        MEMO_SPLIT_RELEASE.notified().await;
    }
    if items.len() > 1 {
        return Err(Error::retry_with_smaller_batch(Error::user(
            BatchLimitError("one at a time".into()),
        )));
    }
    Ok(items.into_iter().map(|item| item + 100).collect())
}

#[tokio::test]
async fn memo_batching_memoizes_results_of_retried_halves() {
    let (app, _env) = temp_app("split_memo").await;
    async fn run(app: &App) -> Result<Vec<i64>> {
        app.update(|ctx| async move {
            let blocker_ctx = ctx.clone();
            let blocker = tokio::spawn(async move { memo_limited(&blocker_ctx, 0).await });
            if MEMO_SPLIT_CALLS.load(Ordering::SeqCst) == 0 {
                MEMO_SPLIT_STARTED.notified().await;
            }
            let queued: Vec<_> = [1, 2, 3]
                .into_iter()
                .map(|item| {
                    let ctx = ctx.clone();
                    tokio::spawn(async move { memo_limited(&ctx, item).await })
                })
                .collect();
            sleep(Duration::from_millis(50)).await;
            MEMO_SPLIT_RELEASE.notify_one();
            let mut outputs = vec![blocker.await.unwrap()?];
            for task in queued {
                outputs.push(task.await.unwrap()?);
            }
            Ok(outputs)
        })
        .await
    }

    assert_eq!(run(&app).await.unwrap(), vec![100, 101, 102, 103]);
    let calls_after_first_run = MEMO_SPLIT_CALLS.load(Ordering::SeqCst);
    // Inline [0], the batch [1, 2, 3], halves [1] and [2, 3], then [2], [3].
    assert_eq!(calls_after_first_run, 6);

    assert_eq!(run(&app).await.unwrap(), vec![100, 101, 102, 103]);
    assert_eq!(
        MEMO_SPLIT_CALLS.load(Ordering::SeqCst),
        calls_after_first_run,
        "every item of a retried half is memoized"
    );
}

#[tokio::test]
async fn batched_adapter_splits_on_retry_with_smaller_batch() {
    static SIZES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    static STARTED: Notify = Notify::const_new();
    static RELEASE: Notify = Notify::const_new();
    static BATCHED: LazyLock<Batched<i64, i64>> = LazyLock::new(|| {
        Batched::new(
            |items: Vec<i64>| async move {
                let is_first = {
                    let mut sizes = SIZES.lock().unwrap();
                    sizes.push(items.len());
                    sizes.len() == 1
                };
                if is_first {
                    STARTED.notify_one();
                    RELEASE.notified().await;
                }
                if items.len() > 1 {
                    return Err(Error::retry_with_smaller_batch(Error::user(
                        BatchLimitError("one at a time".into()),
                    )));
                }
                Ok(items.into_iter().map(|item| -item).collect())
            },
            0xB47C,
        )
    });

    let (app, _env) = temp_app("split_batched_adapter").await;
    app.update(|ctx| async move {
        let blocker_ctx = ctx.clone();
        let blocker = tokio::spawn(async move { BATCHED.call(&blocker_ctx, 1).await });
        STARTED.notified().await;
        let queued: Vec<_> = [2, 3, 4]
            .into_iter()
            .map(|item| {
                let ctx = ctx.clone();
                tokio::spawn(async move { BATCHED.call(&ctx, item).await })
            })
            .collect();
        sleep(Duration::from_millis(50)).await;
        RELEASE.notify_one();
        assert_eq!(blocker.await.unwrap()?, -1);
        let mut outputs = Vec::new();
        for task in queued {
            outputs.push(task.await.unwrap()?);
        }
        assert_eq!(outputs, vec![-2, -3, -4]);
        Ok(())
    })
    .await
    .unwrap();

    let mut sizes = SIZES.lock().unwrap().clone();
    sizes.sort_unstable();
    // Inline [1]; [2, 3, 4] -> [2] and [3, 4] -> [3] and [4].
    assert_eq!(sizes, [1, 1, 1, 1, 2, 3]);
}

// ---------------------------------------------------------------------------
// runner = GPU: calls hold a share of a GPU from the process-wide pool
// ---------------------------------------------------------------------------

/// Tracks how many bodies run at once.
struct InFlight {
    now: AtomicUsize,
    peak: AtomicUsize,
}

impl InFlight {
    const fn new() -> Self {
        Self {
            now: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn enter(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }

    fn reset(&self) {
        self.now.store(0, Ordering::SeqCst);
        self.peak.store(0, Ordering::SeqCst);
    }
}

static WHOLE_GPU_IN_FLIGHT: InFlight = InFlight::new();
static WHOLE_GPU_SEEN: Mutex<Vec<Option<usize>>> = Mutex::new(Vec::new());

#[cocoindex::function(runner = GPU)]
async fn whole_gpu(_ctx: &Ctx, item: i64) -> Result<i64> {
    WHOLE_GPU_IN_FLIGHT.enter();
    WHOLE_GPU_SEEN
        .lock()
        .unwrap()
        .push(cocoindex::current_gpu());
    assert_eq!(cocoindex::current_gpu_fraction(), Some(1.0));
    sleep(Duration::from_millis(30)).await;
    WHOLE_GPU_IN_FLIGHT.leave();
    Ok(item + 1)
}

#[tokio::test]
async fn gpu_runner_serializes_calls_on_one_gpu() {
    let _turn = GPU_POOL_TURN.lock().await;
    configure_gpu_pool(NonZeroUsize::new(1).unwrap());
    WHOLE_GPU_IN_FLIGHT.reset();
    WHOLE_GPU_SEEN.lock().unwrap().clear();

    let (app, _env) = temp_app("runner_one_gpu").await;
    let outputs = app
        .update(|ctx| async move {
            assert_eq!(
                cocoindex::current_gpu(),
                None,
                "no GPU outside a runner body"
            );
            let calls = (0..3).map(|item| whole_gpu(&ctx, item));
            futures::future::join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap();

    assert_eq!(outputs, vec![1, 2, 3]);
    assert_eq!(WHOLE_GPU_IN_FLIGHT.peak.load(Ordering::SeqCst), 1);
    assert_eq!(*WHOLE_GPU_SEEN.lock().unwrap(), vec![Some(0); 3]);
}

#[tokio::test]
async fn gpu_runner_spreads_calls_over_two_gpus() {
    let _turn = GPU_POOL_TURN.lock().await;
    configure_gpu_pool(NonZeroUsize::new(2).unwrap());
    WHOLE_GPU_IN_FLIGHT.reset();
    WHOLE_GPU_SEEN.lock().unwrap().clear();

    let (app, _env) = temp_app("runner_two_gpus").await;
    app.update(|ctx| async move {
        let calls = (0..4).map(|item| whole_gpu(&ctx, item));
        futures::future::join_all(calls)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
    })
    .await
    .unwrap();

    assert_eq!(WHOLE_GPU_IN_FLIGHT.peak.load(Ordering::SeqCst), 2);
    let mut seen = WHOLE_GPU_SEEN.lock().unwrap().clone();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, vec![Some(0), Some(1)], "both GPUs are used");
}

static HALF_GPU_IN_FLIGHT: InFlight = InFlight::new();

#[cocoindex::function(memo, runner = GPURunner::fraction(0.5))]
async fn half_gpu(_ctx: &Ctx, item: i64) -> Result<i64> {
    HALF_GPU_IN_FLIGHT.enter();
    assert_eq!(cocoindex::current_gpus(), vec![0]);
    assert_eq!(cocoindex::current_gpu_fraction(), Some(0.5));
    sleep(Duration::from_millis(30)).await;
    HALF_GPU_IN_FLIGHT.leave();
    Ok(item * 10)
}

#[tokio::test]
async fn fractional_gpu_runner_shares_one_gpu_between_two_calls() {
    let _turn = GPU_POOL_TURN.lock().await;
    configure_gpu_pool(NonZeroUsize::new(1).unwrap());
    HALF_GPU_IN_FLIGHT.reset();

    let (app, _env) = temp_app("runner_half_gpu").await;
    let outputs = app
        .update(|ctx| async move {
            let calls = (0..3).map(|item| half_gpu(&ctx, item));
            futures::future::join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap();

    assert_eq!(outputs, vec![0, 10, 20]);
    assert_eq!(HALF_GPU_IN_FLIGHT.peak.load(Ordering::SeqCst), 2);
}

static GPU_BATCHES: Mutex<Vec<(Option<usize>, Vec<i64>)>> = Mutex::new(Vec::new());
static GPU_BATCH_IN_FLIGHT: InFlight = InFlight::new();

#[cocoindex::function(batching, max_batch_size = 2, runner = GPU)]
async fn gpu_batch(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    GPU_BATCH_IN_FLIGHT.enter();
    GPU_BATCHES
        .lock()
        .unwrap()
        .push((cocoindex::current_gpu(), items.clone()));
    sleep(Duration::from_millis(30)).await;
    GPU_BATCH_IN_FLIGHT.leave();
    if items.len() > 1 && items.contains(&-1) {
        return Err(Error::retry_with_smaller_batch(Error::user(
            BatchLimitError("rejected".into()),
        )));
    }
    Ok(items.into_iter().map(|item| item * 3).collect())
}

#[tokio::test]
async fn gpu_runner_holds_the_gpu_per_physical_batch_and_per_retried_half() {
    let _turn = GPU_POOL_TURN.lock().await;
    configure_gpu_pool(NonZeroUsize::new(1).unwrap());
    GPU_BATCH_IN_FLIGHT.reset();
    GPU_BATCHES.lock().unwrap().clear();

    let (app, _env) = temp_app("runner_batching").await;
    let outputs = app
        .update(|ctx| async move {
            let calls = [5, -1, 6, 7].map(|item| gpu_batch(&ctx, item));
            futures::future::join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap();

    assert_eq!(outputs, vec![15, -3, 18, 21]);
    assert_eq!(GPU_BATCH_IN_FLIGHT.peak.load(Ordering::SeqCst), 1);
    let batches = GPU_BATCHES.lock().unwrap();
    assert!(batches.iter().all(|(gpu, _)| *gpu == Some(0)));
    assert!(
        batches.iter().any(|(_, items)| items.len() == 2),
        "calls still coalesce under a runner: {batches:?}"
    );
    assert!(
        batches.iter().any(|(_, items)| items == &[-1]),
        "the rejected pair is retried as halves: {batches:?}"
    );
}

// ---------------------------------------------------------------------------
// A user-defined runner: any `Runner` bounds a function's bodies its own way
// ---------------------------------------------------------------------------

/// Runs at most `permits` bodies at once, and tags each with a task-local the
/// body reads, as `GPURunner` sets `current_gpu()`.
struct Permits(tokio::sync::Semaphore);

tokio::task_local! {
    static UNDER_PERMITS: bool;
}

#[async_trait::async_trait]
impl cocoindex::Runner for Permits {
    async fn run(&self, body: cocoindex::RunnerBody<'_>) -> Result<cocoindex::RunnerOutput> {
        let _permit = self
            .0
            .acquire()
            .await
            .map_err(|e| Error::engine(e.to_string()))?;
        UNDER_PERMITS.scope(true, body).await
    }
}

static PERMITS_IN_FLIGHT: InFlight = InFlight::new();

#[cocoindex::function(memo, runner = Permits(tokio::sync::Semaphore::new(2)))]
async fn two_at_once(_ctx: &Ctx, item: i64) -> Result<i64> {
    PERMITS_IN_FLIGHT.enter();
    assert!(UNDER_PERMITS.get(), "the body runs inside the runner");
    sleep(Duration::from_millis(30)).await;
    PERMITS_IN_FLIGHT.leave();
    Ok(item + 1)
}

#[tokio::test]
async fn user_runner_bounds_calls_of_a_plain_function() {
    PERMITS_IN_FLIGHT.reset();
    let (app, _dir) = temp_app("user_runner_plain").await;
    let outputs = app
        .update(|ctx| async move {
            let calls = (0..5).map(|item| two_at_once(&ctx, item));
            futures::future::join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap();
    assert_eq!(outputs, vec![1, 2, 3, 4, 5]);
    assert_eq!(PERMITS_IN_FLIGHT.peak.load(Ordering::SeqCst), 2);
}

static PERMITS_BATCH_IN_FLIGHT: InFlight = InFlight::new();

#[cocoindex::function(batching, max_batch_size = 2, runner = Permits(tokio::sync::Semaphore::new(1)))]
async fn one_batch_at_once(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    PERMITS_BATCH_IN_FLIGHT.enter();
    assert!(UNDER_PERMITS.get());
    sleep(Duration::from_millis(30)).await;
    PERMITS_BATCH_IN_FLIGHT.leave();
    Ok(items.into_iter().map(|item| item * 2).collect())
}

#[tokio::test]
async fn user_runner_bounds_physical_batches() {
    PERMITS_BATCH_IN_FLIGHT.reset();
    let (app, _dir) = temp_app("user_runner_batching").await;
    let outputs = app
        .update(|ctx| async move {
            let calls = (0..6).map(|item| one_batch_at_once(&ctx, item));
            futures::future::join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap();
    assert_eq!(outputs, vec![0, 2, 4, 6, 8, 10]);
    assert_eq!(PERMITS_BATCH_IN_FLIGHT.peak.load(Ordering::SeqCst), 1);
}
