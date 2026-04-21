//! App lifecycle: builder, update loop, open/run convenience API.

use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cocoindex_core::engine::app::{App as CoreApp, AppUpdateOptions};
use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_core::engine::stats::ProcessingStats;
use cocoindex_core::engine::target_state::TargetStateProviderRegistry;

use crate::ctx::Ctx;
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
    state: TypeMap,
}

impl AppBuilder {
    /// Set the LMDB database directory. Default: `./coco_state/{name}/`.
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
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

    /// Build the app. Opens (or creates) the LMDB database.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize
    /// (e.g., due to permissions, disk space, or a corrupted state directory).
    pub fn build(self) -> Result<App> {
        let db_path = self
            .db_path
            .unwrap_or_else(|| PathBuf::from(format!("./coco_state/{}/", self.name)));

        let settings = EnvironmentSettings {
            db_path,
            lmdb_max_dbs: 1024,
            lmdb_map_size: 0x1_0000_0000, // 4 GiB
        };
        let providers = Arc::new(std::sync::Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let env = Environment::<RustProfile>::new(settings, providers, ())
            .map_err(|e| Error::engine(format!("failed to open LMDB: {e}")))?;
        let core_app = CoreApp::new(&self.name, env, None)
            .map_err(|e| Error::engine(format!("failed to create app: {e}")))?;

        Ok(App {
            inner: Arc::new(AppInner {
                name: self.name,
                core_app,
                state: self.state,
            }),
        })
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
}

#[derive(Clone)]
pub struct App {
    pub(crate) inner: Arc<AppInner>,
}

impl App {
    /// Convenience: open an app with a specific DB path. Equivalent to
    /// `App::builder(name).db_path(db_path).build()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # fn main() -> cocoindex::error::Result<()> {
    /// let app = App::open("my_app", "./data")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB database environment fails to initialize.
    pub fn open(name: &str, db_path: impl Into<PathBuf>) -> Result<App> {
        App::builder(name).db_path(db_path).build()
    }

    /// Start building an app. Name determines the LMDB database namespace.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::app::App;
    /// # fn main() -> cocoindex::error::Result<()> {
    /// let app = App::builder("my_app")
    ///     .db_path("./data")
    ///     .provide(String::from("shared resource"))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(name: &str) -> AppBuilder {
        AppBuilder {
            name: name.to_owned(),
            db_path: None,
            state: TypeMap::new(),
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
    /// let app = App::open("my_app", "./data")?;
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
    pub async fn run<F, Fut>(&self, f: F) -> Result<RunStats>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let start = Instant::now();
        let processing_stats = self.update_with_stats(f).await?;
        let mut run_stats = Self::run_stats_from_processing(processing_stats);
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
    /// let app = App::open("my_app", "./data")?;
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
    pub async fn update<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.update_with_stats(f).await?;
        Ok(())
    }

    async fn update_with_stats<F, Fut>(&self, f: F) -> Result<ProcessingStats>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let state = self.inner.clone();
        let processing_stats = Arc::new(Mutex::new(None::<ProcessingStats>));
        let processing_stats_ref = processing_stats.clone();
        let processor = BoxedProcessor::new(
            move |comp_ctx| {
                let processor_stats = comp_ctx.processing_stats().clone();
                let ctx = Ctx {
                    comp_ctx: Some(comp_ctx),
                    state: state.clone(),
                };
                let processing_stats = processing_stats_ref.clone();
                Box::pin(async move {
                    let ret = f(ctx).await;
                    {
                        let mut snapshot = processing_stats.lock().map_err(|e| {
                            Error::engine(format!("failed to access stats mutex: {e}"))
                        })?;
                        *snapshot = Some(processor_stats);
                    }
                    ret?;
                    Ok(Value::unit())
                })
            },
            None,
            format!("app:{}", self.inner.name),
        );

        let options = AppUpdateOptions {
            full_reprocess: false,
            live: false,
        };

        let handle = self
            .inner
            .core_app
            .update(processor, options, Arc::new(()))
            .map_err(|e| Error::engine(format!("{e}")))?;
        handle
            .result()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;

        processing_stats
            .lock()
            .map_err(|e| Error::engine(format!("failed to access stats mutex: {e}")))?
            .take()
            .ok_or_else(|| {
                Error::engine("processing stats were not collected from the pipeline".to_string())
            })
    }

    fn run_stats_from_processing(processing_stats: ProcessingStats) -> RunStats {
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
    pub fn update_blocking<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::engine(format!("failed to create tokio runtime: {e}")))?;
        rt.block_on(self.update(f))
    }

    /// Drop all persisted state (LMDB data). Irreversible.
    pub async fn drop_state(&self) -> Result<()> {
        let handle = self
            .inner
            .core_app
            .drop_app(Arc::new(()))
            .map_err(|e| Error::engine(format!("{e}")))?;
        handle
            .result()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(())
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
