//! Pipeline context: scope, memo, write_file.

use std::fmt::Display;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};
use cocoindex_core::state::stable_path::StableKey;
use serde::{Deserialize, Serialize};

use crate::app::AppInner;
use crate::error::{Error, Result};
use crate::profile::{BoxedProcessor, RustProfile, Value};

/// Pipeline context passed to closures inside `App::update()` / `App::run()`.
pub struct Ctx {
    /// The core component processor context. Some when running inside a
    /// pipeline (enables LMDB memoization), None for standalone usage.
    pub(crate) comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
    pub(crate) state: Arc<AppInner>,
}

pub(crate) struct FnCallGuard<'a> {
    comp_ctx: &'a ComponentProcessorContext<RustProfile>,
    fn_ctx: &'a FnCallContext,
}

impl<'a> Drop for FnCallGuard<'a> {
    fn drop(&mut self) {
        self.comp_ctx.join_fn_call(self.fn_ctx);
    }
}

pub(crate) fn fn_call_guard<'a>(
    comp_ctx: &'a ComponentProcessorContext<RustProfile>,
    fn_ctx: &'a FnCallContext,
) -> FnCallGuard<'a> {
    FnCallGuard { comp_ctx, fn_ctx }
}

impl Ctx {
    /// Try to get a shared resource and return a typed error if missing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingContext`] if the requested type `T` was not
    /// provided to the app builder.
    pub fn get_or_err<T: Send + Sync + 'static>(&self) -> Result<&T> {
        self.state
            .state
            .get::<T>()
            .ok_or(Error::MissingContext(std::any::type_name::<T>()))
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

    /// Named sub-component. Creates a child scope in the pipeline tree.
    ///
    /// The key determines the child's stable path for memoization and
    /// target state tracking.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let val = ctx.scope(&"child", |child_ctx| async move {
    ///     Ok(42)
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if stable
    /// path/component tracking fails internally.
    pub async fn scope<K, T, F, Fut>(&self, key: &K, f: F) -> Result<T>
    where
        K: Display,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — just run the closure directly.
            let child_ctx = Ctx {
                comp_ctx: None,
                state: self.state.clone(),
            };
            return f(child_ctx).await;
        };

        let key_str = key.to_string();
        let child_stable_key = StableKey::Str(Arc::from(key_str.as_str()));
        let child_path = comp_ctx.stable_path().concat_part(child_stable_key);

        let fn_ctx = FnCallContext::default();
        let child_component = comp_ctx
            .component()
            .mount_child(&fn_ctx, child_path)
            .map_err(|e| Error::engine(format!("{e}")))?;

        // Guard to ensure `join_fn_call` is executed even if `f` panics or the future
        // is dropped/cancelled early.
        let _guard = fn_call_guard(comp_ctx, &fn_ctx);

        let state = self.state.clone();
        let processor = BoxedProcessor::new(
            move |child_comp_ctx| {
                let ctx = Ctx {
                    comp_ctx: Some(child_comp_ctx),
                    state: state.clone(),
                };
                Box::pin(async move {
                    let result = f(ctx).await?;
                    Value::from_serializable(&result)
                })
            },
            None,
            format!("scope:{key_str}"),
        );

        let handle = match child_component.use_mount(comp_ctx, processor).await {
            Ok(handle) => handle,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        let value = handle.result(Some(comp_ctx)).await;
        let value = match value {
            Ok(value) => value,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        let result: T = match value.deserialize() {
            Ok(result) => result,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        Ok(result)
    }

    /// Cached computation. If `key` hasn't changed since the last run,
    /// returns the cached result from LMDB without executing `f`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, fingerprint: &str) -> cocoindex::error::Result<()> {
    /// let processed = ctx.memo(&fingerprint, || async move {
    ///     // ... expensive computation ...
    ///     Ok("result".to_string())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if LMDB cache
    /// serialization/deserialization fails.
    pub async fn memo<K, T, F, Fut>(&self, key: &K, f: F) -> Result<T>
    where
        K: Serialize,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        crate::memo::cached(self, key, f).await
    }

    /// Batch-process items with per-item memoization.
    ///
    /// Probes the memo cache for each item. Cache hits return stored values.
    /// Cache misses are collected and passed to `f` as a single batch.
    /// Results are stored back and merged in original order.
    ///
    /// `f` must return exactly one result per miss item, in the same order.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, items: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.batch(
    ///     items,
    ///     |item| item.len(), // using length as cache key for demonstration
    ///     |misses| async move {
    ///         Ok(misses.into_iter().map(|s| s.to_uppercase()).collect())
    ///     }
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if there's an
    /// LMDB or serialization error during memoization.
    pub async fn batch<I, K, T, F, Fut>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: Serialize,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Vec<I::Item>) -> Fut + Send,
        Fut: Future<Output = Result<Vec<T>>> + Send,
    {
        crate::memo::batch(self, items, move |item| Ok(key_fn(item)), f).await
    }

    /// Run a closure concurrently for each item, creating a child scope per item.
    ///
    /// Each item gets its own `Ctx` child scope keyed by `key_fn(item)`.
    /// All closures run concurrently via `try_join_all`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.mount_each(
    ///     tasks,
    ///     |task| task.clone(), // use the task string as scope key
    ///     |child_ctx, task| async move {
    ///         Ok(format!("processed {task}"))
    ///     }
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if any of the closures fail. The first encountered error
    /// is returned.
    pub async fn mount_each<I, K, F, Fut, T>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: Display,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: Fn(Ctx, I::Item) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let mut keys = rustc_hash::FxHashSet::default();
        let mut keyed = Vec::new();

        for item in items {
            let key = key_fn(&item).to_string();
            if !keys.insert(key.clone()) {
                return Err(Error::engine(format!(
                    "duplicate key `{}` in mount_each batch",
                    key
                )));
            }
            keyed.push((key, item));
        }

        let futs: Vec<_> = keyed
            .into_iter()
            .map(|(key, item)| {
                let f = f.clone();
                async move { self.scope(&key, move |child| f(child, item)).await }
            })
            .collect();

        futures::future::try_join_all(futs).await
    }

    /// Run a closure concurrently for each item within the current scope (no child scopes).
    ///
    /// Like `futures::future::try_join_all` but with a mapping closure.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.map(tasks, |task| async move {
    ///     Ok(format!("processed {task}"))
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if any of the closures return an error.
    pub async fn map<I, F, Fut, T>(&self, items: I, f: F) -> Result<Vec<T>>
    where
        I: IntoIterator,
        T: Send + 'static,
        F: Fn(I::Item) -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let futs: Vec<_> = items.into_iter().map(f).collect();
        futures::future::try_join_all(futs).await
    }

    /// Declare a file output. Writes content to the given path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// ctx.write_file("output.txt", b"hello world")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Io`] if the parent directory cannot be created or
    /// if the file cannot be written to disk.
    pub fn write_file(&self, path: impl AsRef<Path>, content: &[u8]) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        std::fs::write(path, content).map_err(Error::Io)
    }
}
