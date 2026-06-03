//! App lifecycle: builder, update loop, open/run convenience API.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cocoindex_core::engine::app::{App as CoreApp, AppOpHandle, AppUpdateOptions};
use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_core::engine::stats::{ProcessingStats, TERMINATED_VERSION};
use cocoindex_core::engine::target_state::TargetStateProviderRegistry;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::profile::{BoxedProcessor, RustProfile, Value};
use crate::stats::RunStats;
use crate::typemap::TypeMap;

// ---------------------------------------------------------------------------
// AppBuilder
// ---------------------------------------------------------------------------

pub struct AppBuilder {
    name: String,
    db_path: Option<PathBuf>,
    lmdb_max_dbs: u32,
    lmdb_map_size: usize,
    max_inflight_components: Option<usize>,
    state: TypeMap,
    context: ContextStore,
}

impl AppBuilder {
    /// Set the LMDB database directory. Default: `./coco_state/{name}/`.
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

    /// Limit the number of concurrently processing components.
    pub fn max_inflight_components(mut self, value: usize) -> Self {
        self.max_inflight_components = Some(value);
        self
    }

    /// Inject a shared resource. Retrieved later via `ctx.get::<T>()`.
    /// The type IS the key — each type can only be provided once.
    ///
    /// # Panics
    /// Panics if a value of type `T` has already been provided for this app.
    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        if self.state.contains::<T>() {
            panic!(
                "AppBuilder::provide: type `{}` has already been provided",
                std::any::type_name::<T>()
            );
        }
        self.state.insert(value);
        self
    }

    /// Inject a shared resource by named [`ContextKey`].
    ///
    /// Named keys are useful when multiple resources share the same Rust type.
    ///
    /// # Panics
    /// Panics if a change-tracked key cannot fingerprint the provided value.
    pub fn provide_key<T: Send + Sync + 'static>(mut self, key: &ContextKey<T>, value: T) -> Self {
        self.context
            .provide(key, value)
            .unwrap_or_else(|e| panic!("AppBuilder::provide_key({}): {e}", key.name()));
        self
    }

    /// Build the app. Opens (or creates) the LMDB database.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize
    /// (e.g., due to permissions, disk space, or a corrupted state directory).
    pub async fn build(self) -> Result<App> {
        let db_path = self
            .db_path
            .unwrap_or_else(|| PathBuf::from(format!("./coco_state/{}/", self.name)));

        let settings = EnvironmentSettings {
            db_path,
            lmdb_max_dbs: self.lmdb_max_dbs,
            lmdb_map_size: self.lmdb_map_size,
        };
        let providers = Arc::new(std::sync::Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let env = Environment::<RustProfile>::new(settings, providers, ())
            .await
            .map_err(|e| Error::engine(format!("failed to open LMDB: {e}")))?;
        self.context.register_logic(&env);
        let core_app = CoreApp::new(&self.name, env, self.max_inflight_components)
            .await
            .map_err(|e| Error::engine(format!("failed to create app: {e}")))?;

        Ok(App {
            inner: Arc::new(AppInner {
                name: self.name,
                core_app,
                state: self.state,
                context: self.context,
            }),
        })
    }

    /// Build the app (blocking). Convenience for sync callers — creates a
    /// tokio runtime internally and awaits [`Self::build`].
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
            let _ = App::builder("test").provide(1u32).provide(2u32);
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
    pub(crate) state: TypeMap,
    pub(crate) context: ContextStore,
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

    /// Start building an app. Name determines the LMDB database namespace.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # async fn run() -> cocoindex::error::Result<()> {
    /// let app = App::builder("my_app")
    ///     .db_path("./data")
    ///     .provide(String::from("shared resource"))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(name: &str) -> AppBuilder {
        AppBuilder {
            name: name.to_owned(),
            db_path: None,
            lmdb_max_dbs: 1024,
            lmdb_map_size: 0x1_0000_0000,
            max_inflight_components: None,
            state: TypeMap::new(),
            context: ContextStore::default(),
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
        let result = handle.result().await?;
        Ok((result, processing_stats))
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

        let (core, _preview_collector) = self
            .inner
            .core_app
            .update(processor, core_options, Arc::new(()), None)
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(UpdateHandle {
            core,
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
            .drop_app(Arc::new(()))
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

/// Handle returned by [`App::start_update`].
pub struct UpdateHandle<T> {
    core: AppOpHandle<Value>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> UpdateHandle<T>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    /// A point-in-time snapshot of processing statistics for live progress.
    ///
    /// Note: [`RunStats::elapsed`] is not populated for in-flight snapshots
    /// (it is only set by [`App::run`] once the run completes).
    pub fn stats_snapshot(&self) -> RunStats {
        App::run_stats_from_processing(self.core.stats())
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
