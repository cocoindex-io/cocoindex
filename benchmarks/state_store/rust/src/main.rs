//! State-store benchmark pipeline, Rust SDK side. Mirrors `../python/main.py`.
//!
//! Mounts N child components. Each is a logic-tracked function that declares
//! M target states against a no-op target: the engine still writes the
//! per-target tracking records into the state store and runs the full
//! pre-commit / commit lifecycle, but the user-facing sink does nothing, so
//! only cocoindex's own state-store traffic is measured.
//!
//! `M = 0` (the default) declares no target states at all: component-path
//! bookkeeping, component memo read/write and tombstone GC on drop.
//!
//! ```sh
//! state_store update   # one update against $COCOINDEX_DB
//! state_store drop     # drop the app's state
//! ```
//!
//! Environment knobs, the same as the Python side:
//!   COCOINDEX_DB — the LMDB state directory.
//!   BENCH_N      — number of mounted child components (default 100).
//!   BENCH_M      — number of target states declared per component (default 0).

use std::env;
use std::process::ExitCode;
use std::sync::Mutex;

use cocoindex::prelude::*;

const APP_NAME: &str = "StateStoreBench";

fn env_count(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|e| Error::engine(format!("{name}={value:?} is not a count: {e}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(Error::engine(format!("{name}: {e}"))),
    }
}

/// Target handler that drives the per-state tracking record through
/// pre-commit and commit but does nothing user-visible: every declared state
/// reconciles to an action the no-op sink discards.
#[derive(Clone)]
struct NoopHandler {
    sink: TargetActionSink<()>,
}

impl NoopHandler {
    fn new() -> Self {
        Self {
            sink: TargetActionSink::from_async_fn(|_actions: Vec<TargetAction<()>>| async {
                Ok(())
            }),
        }
    }
}

impl TargetHandler<()> for NoopHandler {
    type TrackingRecord = ();
    type Action = ();

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<()>,
        _prev: Vec<()>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<(), ()>>> {
        let (action, tracking_record) = match desired {
            Some(()) => (TargetAction::Create(()), Some(())),
            None => (TargetAction::Delete(()), None),
        };
        Ok(Some(TargetReconcileOutput {
            action,
            sink: self.sink.clone(),
            tracking_record,
            child_invalidation: None,
        }))
    }
}

/// The run's no-op provider and the per-component state count, set by the
/// root component before it mounts the children. The Python side keeps the
/// provider at module level; the Rust SDK registers a root provider per run.
static RUN: Mutex<Option<(TargetStateProvider<()>, usize)>> = Mutex::new(None);

/// One child component: declares M no-op target states. Logic-tracked, so a
/// warm run skips it on the component-memo fast path, while its declared
/// target states still flow through pre-commit and commit.
#[cocoindex::function]
async fn noop_component(ctx: &Ctx, idx: i64) -> Result<()> {
    let (provider, m) = RUN
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .ok_or_else(|| Error::engine("the run's provider is not set"))?;
    for j in 0..m {
        declare_target_state(ctx, provider.target_state(format!("{idx}-{j}"), ()))?;
    }
    Ok(())
}

async fn app_main(ctx: Ctx, n: usize, m: usize) -> Result<()> {
    let provider: TargetStateProvider<()> =
        register_root_target_states_provider(&ctx, "cocoindex/bench/noop", NoopHandler::new())?;
    *RUN.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((provider, m));
    let items = (0..n).map(|i| (i.to_string(), i as i64));
    mount_each!("noop_component", items, |idx| noop_component(ctx, idx)).await?;
    Ok(())
}

async fn run(verb: &str) -> Result<()> {
    let db_path = env::var_os("COCOINDEX_DB")
        .ok_or_else(|| Error::engine("COCOINDEX_DB is not set"))?;
    let n = env_count("BENCH_N", 100)?;
    let m = env_count("BENCH_M", 0)?;
    let app = App::builder(APP_NAME).db_path(db_path).build().await?;
    match verb {
        "update" => {
            app.update(move |ctx| app_main(ctx, n, m)).await?;
        }
        "drop" => app.drop_state().await?,
        other => return Err(Error::engine(format!("unknown verb `{other}`"))),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let verb = env::args().nth(1).unwrap_or_default();
    match run(&verb).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("state_store {verb}: {e}");
            ExitCode::FAILURE
        }
    }
}
