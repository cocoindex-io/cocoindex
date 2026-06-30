//! App lifecycle: builder, update loop, open/run convenience API.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cocoindex_core::engine::app::{App as CoreApp, AppOpHandle, AppUpdateOptions};
use cocoindex_core::engine::environment::{Environment as CoreEnvironment, EnvironmentSettings};
use cocoindex_core::engine::progress_display::{ProgressDisplayOptions, show_progress};
use cocoindex_core::engine::stats::{ProcessingStats, TERMINATED_VERSION};
use cocoindex_core::engine::target_state::TargetStateProviderRegistry;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedProcessor, RustProfile, Value};
use crate::stats::{ComponentStats, RunStats, UpdateStats, UpdateStatus};
use crate::typemap::TypeMap;

// ---------------------------------------------------------------------------
// Environment — the home for provided resources + LMDB settings
// ---------------------------------------------------------------------------

/// A CocoIndex environment: the LMDB store plus the resources shared with every
/// app (and target sink) built from it. Build one with [`Environment::builder`],
/// then create apps with [`Environment::app`]. Multiple environments (separate
/// `db_path`s) can coexist for multi-tenancy or test isolation.
///
/// # Examples
///
/// ```no_run
/// # use cocoindex::Environment;
/// # async fn run() -> cocoindex::error::Result<()> {
/// let env = Environment::builder()
///     .db_path("./.cocoindex")
///     .provide(String::from("shared resource"))
///     .build()
///     .await?;
/// let app = env.app("MyApp").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Environment {
    inner: Arc<EnvironmentInner>,
}

pub(crate) struct EnvironmentInner {
    core_env: CoreEnvironment<RustProfile>,
    state: Arc<TypeMap>,
    context: Arc<ContextStore>,
    max_inflight_components: Option<usize>,
}

impl Environment {
    /// Start building an environment.
    pub fn builder() -> EnvironmentBuilder {
        EnvironmentBuilder::new()
    }

    /// Create an app in this environment. Multiple apps may share one
    /// environment — each `name` is its own LMDB namespace and they share the
    /// environment's provided resources.
    ///
    /// # Errors
    /// Returns an error if the app's state store cannot be created.
    pub async fn app(&self, name: &str) -> Result<App> {
        let core_app = CoreApp::new(
            name,
            self.inner.core_env.clone(),
            self.inner.max_inflight_components,
        )
        .await
        .map_err(|e| Error::engine(format!("failed to create app: {e}")))?;

        Ok(App {
            inner: Arc::new(AppInner {
                name: name.to_owned(),
                core_app,
                state: self.inner.state.clone(),
                context: self.inner.context.clone(),
            }),
        })
    }

    /// Create an app in this environment (blocking). Convenience for sync
    /// callers — creates a tokio runtime internally and awaits [`Self::app`].
    pub fn app_blocking(&self, name: &str) -> Result<App> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.app(name))
    }
}

/// Builder for [`Environment`]. Provided resources, the LMDB `db_path`, and
/// other engine settings live here — apps are created from a built environment
/// via [`Environment::app`].
pub struct EnvironmentBuilder {
    db_path: Option<PathBuf>,
    lmdb_max_dbs: u32,
    lmdb_map_size: usize,
    max_inflight_components: Option<usize>,
    state: TypeMap,
    context: ContextStore,
}

impl EnvironmentBuilder {
    fn new() -> Self {
        Self {
            db_path: None,
            lmdb_max_dbs: 1024,
            lmdb_map_size: 0x1_0000_0000,
            max_inflight_components: None,
            state: TypeMap::new(),
            context: ContextStore::default(),
        }
    }

    /// Set the LMDB database directory. Default: `./coco_state`.
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Set the LMDB maximum number of named databases.
    pub fn lmdb_max_dbs(mut self, value: u32) -> Self {
        self.lmdb_max_dbs = value;
        self
    }

    /// Set the LMDB map size in bytes.
    pub fn lmdb_map_size(mut self, value: usize) -> Self {
        self.lmdb_map_size = value;
        self
    }

    /// Limit the number of concurrently processing components (per app).
    pub fn max_inflight_components(mut self, value: usize) -> Self {
        self.max_inflight_components = Some(value);
        self
    }

    /// Inject a shared resource. Retrieved later via `ctx.get::<T>()`.
    /// The type IS the key — each type can only be provided once.
    ///
    /// # Panics
    /// Panics if a value of type `T` has already been provided.
    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        if self.state.contains::<T>() {
            panic!(
                "Environment::provide: type `{}` has already been provided",
                std::any::type_name::<T>()
            );
        }
        self.state.insert(value);
        self
    }

    /// Inject a shared resource by named [`ContextKey`].
    ///
    /// Named keys are useful when multiple resources share the same Rust type
    /// and carry change-tracking.
    ///
    /// # Panics
    /// Panics if a change-tracked key cannot fingerprint the provided value.
    pub fn provide_key<T: Send + Sync + 'static>(mut self, key: &ContextKey<T>, value: T) -> Self {
        self.context
            .provide(key, value)
            .unwrap_or_else(|e| panic!("Environment::provide_key({}): {e}", key.name()));
        self
    }

    /// Build the environment, opening (or creating) the LMDB database.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize
    /// (e.g., due to permissions, disk space, or a corrupted state directory).
    pub async fn build(self) -> Result<Environment> {
        // Register every `#[coco::function]`'s logic fingerprint into the engine's
        // logic set, so memo entries that depend on them validate correctly
        // (see `crate::logic`). Idempotent across builds.
        crate::logic::register_all_fn_logic();

        let db_path = self
            .db_path
            .unwrap_or_else(|| PathBuf::from("./coco_state"));

        let settings = EnvironmentSettings {
            db_path,
            lmdb_max_dbs: self.lmdb_max_dbs,
            lmdb_map_size: self.lmdb_map_size,
        };
        let providers = Arc::new(std::sync::Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let core_env = CoreEnvironment::<RustProfile>::new(settings, providers, ())
            .await
            .map_err(|e| Error::engine(format!("failed to open LMDB: {e}")))?;
        self.context.register_logic(&core_env);

        Ok(Environment {
            inner: Arc::new(EnvironmentInner {
                core_env,
                state: Arc::new(self.state),
                context: Arc::new(self.context),
                max_inflight_components: self.max_inflight_components,
            }),
        })
    }

    /// Build the environment (blocking). Convenience for sync callers.
    pub fn build_blocking(self) -> Result<Environment> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.build())
    }
}

// ---------------------------------------------------------------------------
// AppBuilder — single-app convenience (no provided resources). To provide
// shared resources, use `Environment::builder().provide(…).build()` then
// `env.app(name)`.
// ---------------------------------------------------------------------------

pub struct AppBuilder {
    name: String,
    env: EnvironmentBuilder,
}

impl AppBuilder {
    /// Set the LMDB database directory. Default: `./coco_state`.
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.env = self.env.db_path(path);
        self
    }

    /// Set the LMDB maximum number of named databases.
    pub fn lmdb_max_dbs(mut self, value: u32) -> Self {
        self.env = self.env.lmdb_max_dbs(value);
        self
    }

    /// Set the LMDB map size in bytes.
    pub fn lmdb_map_size(mut self, value: usize) -> Self {
        self.env = self.env.lmdb_map_size(value);
        self
    }

    /// Limit the number of concurrently processing components.
    pub fn max_inflight_components(mut self, value: usize) -> Self {
        self.env = self.env.max_inflight_components(value);
        self
    }

    /// Build the app. Creates a single-app environment with no provided
    /// resources, then the app. To provide resources, use
    /// [`Environment::builder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize.
    pub async fn build(self) -> Result<App> {
        self.env.build().await?.app(&self.name).await
    }

    /// Build the app (blocking). Convenience for sync callers.
    pub fn build_blocking(self) -> Result<App> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provide_panics_on_duplicate_type() {
        let result = std::panic::catch_unwind(|| {
            let _ = Environment::builder().provide(1u32).provide(2u32);
        });
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub(crate) struct AppInner {
    pub(crate) name: String,
    pub(crate) core_app: CoreApp<RustProfile>,
    pub(crate) state: Arc<TypeMap>,
    pub(crate) context: Arc<ContextStore>,
}

#[derive(Clone)]
pub struct App {
    pub(crate) inner: Arc<AppInner>,
}

impl App {
    /// Convenience: open an app with a specific DB path. Equivalent to
    /// `App::builder(name).db_path(db_path).build().await`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # async fn run() -> cocoindex::error::Result<()> {
    /// let app = App::open("my_app", "./data").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize.
    pub async fn open(name: &str, db_path: impl Into<PathBuf>) -> Result<App> {
        App::builder(name).db_path(db_path).build().await
    }

    /// Convenience: synchronously open an app with a specific DB path. Same
    /// as [`Self::open`] but blocks on a tokio runtime internally.
    pub fn open_blocking(name: &str, db_path: impl Into<PathBuf>) -> Result<App> {
        App::builder(name).db_path(db_path).build_blocking()
    }

    /// Start building a single-app environment. Name determines the LMDB
    /// database namespace. To provide shared resources, use
    /// [`Environment::builder`] then [`Environment::app`] instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # async fn run() -> cocoindex::error::Result<()> {
    /// let app = App::builder("my_app").db_path("./data").build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(name: &str) -> AppBuilder {
        AppBuilder {
            name: name.to_owned(),
            env: EnvironmentBuilder::new(),
        }
    }

    /// Run the pipeline (async), returning statistics. The closure receives
    /// a `Ctx` for scoping, memoization, and file output.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # async fn run() -> cocoindex::error::Result<()> {
    /// let app = App::open("my_app", "./data").await?;
    /// let stats = app.run(|ctx| async move {
    ///     // ... pipeline logic ...
    ///     Ok(())
    /// }).await?;
    /// println!("Stats: {stats}");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if internal engine
    /// orchestration fails.
    pub async fn run<F, Fut, T>(&self, f: F) -> Result<RunStats>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let start = Instant::now();
        let (_result, processing_stats) =
            self.update_with_stats(UpdateOptions::default(), f).await?;
        let mut run_stats = Self::run_stats_from_processing(&processing_stats);
        run_stats.elapsed = start.elapsed();
        Ok(run_stats)
    }

    /// Run the pipeline (async). The closure receives a `Ctx` for mounting
    /// components and calling `memo::cached()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # async fn doc() -> cocoindex::error::Result<()> {
    /// let app = App::open("my_app", "./data").await?;
    /// app.update(|ctx| async move {
    ///     // ... update logic ...
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the provided closure returns an error, or if internal engine
    /// orchestration fails.
    pub async fn update<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let (result, _stats) = self.update_with_stats(UpdateOptions::default(), f).await?;
        Ok(result)
    }

    /// Run the pipeline once with explicit update options.
    pub async fn update_with_options<F, Fut, T>(&self, options: UpdateOptions, f: F) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let (result, _stats) = self.update_with_stats(options, f).await?;
        Ok(result)
    }

    async fn update_with_stats<F, Fut, T>(
        &self,
        options: UpdateOptions,
        f: F,
    ) -> Result<(T, ProcessingStats)>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let handle = self.start_update_with_options(options, f)?;
        let processing_stats = handle.core.stats().clone();
        let result = if options.report_to_stdout {
            let value = show_progress(
                handle.into_core(),
                ProgressDisplayOptions::from_refresh_secs(
                    options.progress_refresh_interval.map(|d| d.as_secs_f64()),
                ),
            )
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
            value.deserialize()?
        } else {
            handle.result().await?
        };
        Ok((result, processing_stats))
    }

    /// Run the pipeline in preview mode: compute the target actions that would
    /// be applied, without applying them. Returns the collected actions (cf.
    /// Python's `App.update(preview=True)`).
    pub async fn preview<F, Fut, T>(&self, f: F) -> Result<Vec<PreviewAction>>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let options = UpdateOptions {
            preview: true,
            ..UpdateOptions::default()
        };
        let handle = self.start_update_with_options::<F, Fut, T>(options, f)?;
        handle.preview_actions().await
    }

    /// Run the pipeline in preview mode (blocking). Convenience for sync callers.
    pub fn preview_blocking<F, Fut, T>(&self, f: F) -> Result<Vec<PreviewAction>>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.preview(f))
    }

    /// Start an update and return a handle for progress/stat polling.
    pub fn start_update<F, Fut, T>(&self, f: F) -> Result<UpdateHandle<T>>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        self.start_update_with_options(UpdateOptions::default(), f)
    }

    /// Start an update with explicit options.
    pub fn start_update_with_options<F, Fut, T>(
        &self,
        options: UpdateOptions,
        f: F,
    ) -> Result<UpdateHandle<T>>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let state = self.inner.clone();
        let processor = BoxedProcessor::new(
            move |comp_ctx| {
                let ctx = Ctx::new(Some(comp_ctx), state.clone());
                Box::pin(async move {
                    let ret = f(ctx).await?;
                    Value::from_serializable(&ret)
                        .map_err(|e| Error::engine(format!("failed to serialize app result: {e}")))
                })
            },
            None,
            format!("app:{}", self.inner.name),
        );

        let core_options = AppUpdateOptions {
            full_reprocess: options.full_reprocess,
            live: options.live,
        };

        // In preview mode the engine collects target actions into this shared
        // buffer instead of applying them. The element type (`Action`) is
        // inferred from the `update` call below — the core's collector alias is
        // crate-private, but the underlying `Arc<Mutex<Vec<_>>>` is not.
        let preview_collector: Option<Arc<std::sync::Mutex<Vec<Action>>>> = options
            .preview
            .then(|| Arc::new(std::sync::Mutex::new(Vec::new())));

        let (core, preview_collector) = self
            .inner
            .core_app
            .update(
                processor,
                core_options,
                self.inner.context.clone(),
                preview_collector,
            )
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(UpdateHandle {
            core,
            preview_collector,
            _marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn run_stats_from_processing(processing_stats: &ProcessingStats) -> RunStats {
        let mut run_stats = RunStats::default();
        for group in processing_stats.snapshot().stats.values() {
            run_stats.processed += group.num_processed();
            run_stats.skipped += group.num_unchanged;
            run_stats.written += group.num_adds;
            run_stats.written += group.num_reprocesses;
            run_stats.deleted += group.num_deletes;
        }
        run_stats
    }

    /// Build the detailed per-component [`UpdateStats`] from the engine's
    /// processing stats (preserves the per-operation breakdown that
    /// [`RunStats`] flattens away).
    pub(crate) fn update_stats_from_processing(processing_stats: &ProcessingStats) -> UpdateStats {
        let snapshot = processing_stats.snapshot();
        let by_component = snapshot
            .stats
            .iter()
            .map(|(name, group)| {
                (
                    name.clone(),
                    ComponentStats {
                        num_execution_starts: group.num_execution_starts,
                        num_unchanged: group.num_unchanged,
                        num_adds: group.num_adds,
                        num_deletes: group.num_deletes,
                        num_reprocesses: group.num_reprocesses,
                        num_errors: group.num_errors,
                    },
                )
            })
            .collect();
        let status = if snapshot.ready {
            UpdateStatus::Ready
        } else {
            UpdateStatus::Running
        };
        UpdateStats {
            by_component,
            status,
        }
    }

    /// Run the pipeline (blocking). Convenience for sync callers — creates a
    /// tokio runtime internally.
    ///
    /// # Panics
    /// Panics if called from within an active tokio runtime (use `update`
    /// instead in async contexts).
    pub fn update_blocking<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.update(f))
    }

    /// Run the pipeline once with explicit update options (blocking).
    pub fn update_blocking_with_options<F, Fut, T>(&self, options: UpdateOptions, f: F) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.update_with_options(options, f))
    }

    /// Start dropping all persisted app state and return a handle.
    pub fn start_drop_state(&self) -> Result<DropHandle> {
        let core = self
            .inner
            .core_app
            .drop_app(self.inner.context.clone())
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(DropHandle { core })
    }

    /// Drop all persisted state (LMDB data). Irreversible.
    pub async fn drop_state(&self) -> Result<()> {
        self.start_drop_state()?.result().await
    }

    /// Drop all persisted state (blocking). Convenience for sync callers.
    pub fn drop_state_blocking(&self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.drop_state())
    }

    /// Get the app name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }
}

/// Options for a Rust SDK app update.
#[derive(Debug, Clone, Copy, Default)]
pub struct UpdateOptions {
    pub full_reprocess: bool,
    pub live: bool,
    /// Compute target actions without applying them to external systems. The
    /// planned actions are collected and retrievable via
    /// [`UpdateHandle::preview_actions`] / [`App::preview`] (cf. Python's
    /// `App.update(preview=True)`).
    pub preview: bool,
    /// Periodically print processing progress to stdout while the update runs
    /// (cf. Python's `update_blocking(report_to_stdout=True)`).
    pub report_to_stdout: bool,
    /// When `report_to_stdout` is set, refresh the stdout display at this
    /// interval. `None` uses the engine's default cadence.
    pub progress_refresh_interval: Option<Duration>,
}

/// Options for [`Ctx::stats_group_with_options`](crate::Ctx::stats_group_with_options).
#[derive(Debug, Clone, Copy, Default)]
pub struct StatsGroupOptions {
    /// Print the group's scoped progress to stdout.
    pub report_to_stdout: bool,
    /// When `report_to_stdout` is set, refresh the stdout display at this
    /// interval. `None` uses the engine's default cadence. Ignored when
    /// `report_to_stdout` is `false`.
    pub refresh_interval: Option<Duration>,
}

/// Outcome of waiting on a running operation via [`UpdateHandle::changed`] /
/// [`DropHandle::changed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// The operation made progress; carries the new monotonic version counter.
    Changed(u64),
    /// The operation has terminated; no further changes will occur.
    Done,
}

impl Progress {
    fn from_version(version: u64) -> Self {
        if version == TERMINATED_VERSION {
            Progress::Done
        } else {
            Progress::Changed(version)
        }
    }

    /// Whether the operation has finished.
    pub fn is_done(self) -> bool {
        matches!(self, Progress::Done)
    }
}

/// A target-state change computed during a [preview](App::preview) run but not
/// applied to any external system.
pub enum PreviewAction {
    /// A target state that would be created.
    Create(PreviewValue),
    /// A target state that would be updated.
    Update(PreviewValue),
    /// A target state that would be deleted.
    Delete(PreviewValue),
}

/// The serialized payload of a [`PreviewAction`]. Decode it to the concrete
/// target-state type with [`PreviewValue::decode`].
pub struct PreviewValue(Value);

impl PreviewValue {
    /// Decode the payload into the target-state type that produced it.
    pub fn decode<V: DeserializeOwned>(&self) -> Result<V> {
        self.0.deserialize()
    }
}

impl PreviewAction {
    fn from_action(action: Action) -> Self {
        match action {
            Action::Create(v) => PreviewAction::Create(PreviewValue(v)),
            Action::Update(v) => PreviewAction::Update(PreviewValue(v)),
            Action::Delete(v) => PreviewAction::Delete(PreviewValue(v)),
        }
    }
}

impl std::fmt::Debug for PreviewAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            PreviewAction::Create(_) => "Create",
            PreviewAction::Update(_) => "Update",
            PreviewAction::Delete(_) => "Delete",
        };
        write!(f, "PreviewAction::{kind}")
    }
}

/// Handle returned by [`App::start_update`].
pub struct UpdateHandle<T> {
    core: AppOpHandle<Value>,
    preview_collector: Option<Arc<std::sync::Mutex<Vec<Action>>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> UpdateHandle<T>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    pub(crate) fn into_core(self) -> AppOpHandle<Value> {
        self.core
    }

    /// Drive the (preview) update to completion and return the planned target
    /// actions. Only meaningful when the update was started with
    /// [`UpdateOptions::preview`] set; otherwise returns an empty list.
    pub async fn preview_actions(self) -> Result<Vec<PreviewAction>> {
        let collector = self.preview_collector.clone();
        // Run the pipeline to completion (the app result is discarded — preview
        // callers want the actions, not the return value).
        self.core
            .result()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        let actions = collector
            .map(|c| std::mem::take(&mut *c.lock().unwrap()))
            .unwrap_or_default();
        Ok(actions
            .into_iter()
            .map(PreviewAction::from_action)
            .collect())
    }
    /// A point-in-time snapshot of processing statistics for live progress.
    ///
    /// Note: [`RunStats::elapsed`] is not populated for in-flight snapshots
    /// (it is only set by [`App::run`] once the run completes).
    pub fn stats_snapshot(&self) -> RunStats {
        App::run_stats_from_processing(self.core.stats())
    }

    /// A detailed, per-component snapshot of update statistics (mirrors Python's
    /// `UpdateHandle.stats()`). Use this instead of [`stats_snapshot`](Self::stats_snapshot)
    /// when you need the per-operation breakdown, error counts, or in-flight counts.
    pub fn detailed_stats_snapshot(&self) -> UpdateStats {
        App::update_stats_from_processing(self.core.stats())
    }

    /// Wait for the operation to advance, returning whether it progressed or
    /// terminated. Loop until [`Progress::Done`] to drive it to completion.
    pub async fn changed(&mut self) -> Result<Progress> {
        let version = self
            .core
            .changed()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(Progress::from_version(version))
    }

    pub async fn result(self) -> Result<T> {
        let value = self
            .core
            .result()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        value.deserialize()
    }
}

/// Handle returned by [`App::start_drop_state`].
pub struct DropHandle {
    core: AppOpHandle<()>,
}

impl DropHandle {
    /// A point-in-time snapshot of processing statistics for live progress.
    pub fn stats_snapshot(&self) -> RunStats {
        App::run_stats_from_processing(self.core.stats())
    }

    /// A detailed, per-component snapshot of statistics (see
    /// [`UpdateHandle::detailed_stats_snapshot`]).
    pub fn detailed_stats_snapshot(&self) -> UpdateStats {
        App::update_stats_from_processing(self.core.stats())
    }

    /// Wait for the operation to advance, returning whether it progressed or
    /// terminated. Loop until [`Progress::Done`] to drive it to completion.
    pub async fn changed(&mut self) -> Result<Progress> {
        let version = self
            .core
            .changed()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(Progress::from_version(version))
    }

    pub async fn result(self) -> Result<()> {
        self.core
            .result()
            .await
            .map_err(|e| Error::engine(format!("{e}")))
    }
}

/// Handle returned by [`Ctx::stats_group`](crate::Ctx::stats_group).
#[derive(Clone)]
pub struct StatsGroupHandle {
    stats: ProcessingStats,
    version_rx: watch::Receiver<u64>,
}

impl StatsGroupHandle {
    pub(crate) fn new(stats: ProcessingStats) -> Self {
        let version_rx = stats.subscribe();
        Self { stats, version_rx }
    }

    /// A point-in-time snapshot of this group's processing statistics.
    pub fn stats_snapshot(&self) -> RunStats {
        App::run_stats_from_processing(&self.stats)
    }

    /// A detailed, per-component snapshot of this group's statistics (see
    /// [`UpdateHandle::detailed_stats_snapshot`]).
    pub fn detailed_stats_snapshot(&self) -> UpdateStats {
        App::update_stats_from_processing(&self.stats)
    }

    /// Wait for the group to advance, returning whether it progressed or
    /// terminated. Loop until [`Progress::Done`] to watch the group through
    /// completion.
    pub async fn changed(&mut self) -> Result<Progress> {
        self.version_rx
            .changed()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(Progress::from_version(*self.version_rx.borrow()))
    }
}
