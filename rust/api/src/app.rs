//! App lifecycle: builder, update loop, and pipeline context (Ctx).

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use cocoindex_core::engine::app::{App as CoreApp, AppDropOptions, AppUpdateOptions};
use cocoindex_core::engine::context::ComponentProcessorContext;
use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_core::engine::target_state::TargetStateProviderRegistry;

use crate::error::{Error, Result};
use crate::internal::profile::{BoxedProcessor, RustProfile, Value};
use crate::internal::typemap::TypeMap;

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
    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.state.insert(value);
        self
    }

    /// Build the app. Opens (or creates) the LMDB database.
    pub fn build(self) -> Result<App> {
        let db_path = self
            .db_path
            .unwrap_or_else(|| PathBuf::from(format!("./coco_state/{}/", self.name)));

        let settings = EnvironmentSettings { db_path };
        let providers = Arc::new(std::sync::Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let env = Environment::<RustProfile>::new(settings, providers, ())
            .map_err(|e| Error::engine(format!("failed to open LMDB: {e}")))?;
        let core_app = CoreApp::new(&self.name, env)
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

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub(crate) struct AppInner {
    name: String,
    core_app: CoreApp<RustProfile>,
    state: TypeMap,
}

#[derive(Clone)]
pub struct App {
    inner: Arc<AppInner>,
}

impl App {
    /// Start building an app. Name determines the LMDB database namespace.
    pub fn builder(name: &str) -> AppBuilder {
        AppBuilder {
            name: name.to_owned(),
            db_path: None,
            state: TypeMap::new(),
        }
    }

    /// Run the pipeline (async). The closure receives a `Ctx` for mounting
    /// components and calling `memo::cached()`.
    pub async fn update<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let state = self.inner.clone();
        let processor = BoxedProcessor::new(
            move |comp_ctx| {
                let ctx = Ctx {
                    comp_ctx: Some(comp_ctx),
                    state: state.clone(),
                };
                Box::pin(async move {
                    f(ctx).await?;
                    Ok(Value::unit())
                })
            },
            None,
            format!("app:{}", self.inner.name),
        );

        let options = AppUpdateOptions {
            report_to_stdout: false,
            full_reprocess: false,
        };

        self.inner
            .core_app
            .update(processor, options)
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(())
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
        let options = AppDropOptions {
            report_to_stdout: false,
        };
        self.inner
            .core_app
            .drop_app(options)
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

// ---------------------------------------------------------------------------
// Ctx — pipeline context
// ---------------------------------------------------------------------------

/// Pipeline context passed to closures inside `App::update()`.
pub struct Ctx {
    /// The core component processor context. Some when running inside a
    /// pipeline (enables LMDB memoization), None for standalone usage.
    pub(crate) comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
    pub(crate) state: Arc<AppInner>,
}

impl Ctx {
    /// Get a shared resource by type. Panics if not provided.
    ///
    /// # Panics
    /// Panics if `T` was not provided via `App::builder().provide()`.
    pub fn get<T: Send + Sync + 'static>(&self) -> &T {
        self.state.state.get::<T>().unwrap_or_else(|| {
            panic!(
                "type `{}` not provided — call App::builder().provide() first",
                std::any::type_name::<T>()
            )
        })
    }

    /// Try to get a shared resource. Returns None if not provided.
    pub fn try_get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.state.state.get::<T>()
    }

    /// Returns true if this context has LMDB memoization available
    /// (i.e., running inside an `App::update()` pipeline).
    pub fn has_pipeline_context(&self) -> bool {
        self.comp_ctx.is_some()
    }
}

// ---------------------------------------------------------------------------
// MountHandle
// ---------------------------------------------------------------------------

/// Handle to a mounted component. Drop-safe: component continues running.
#[must_use = "mount handle dropped without waiting — use .wait() or drop explicitly"]
pub struct MountHandle {
    handle: cocoindex_core::engine::component::ComponentExecutionHandle,
}

impl MountHandle {
    /// Wait for the child component to complete.
    pub async fn wait(self) -> Result<()> {
        self.handle
            .ready()
            .await
            .map_err(|e| Error::engine(format!("{e}")))
    }
}
