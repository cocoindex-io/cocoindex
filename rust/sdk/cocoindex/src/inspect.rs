//! Read what an app has stored in the state db: its stable paths, the details
//! of each one, and the target states it tracks.
//!
//! Every function comes in two forms. The plain form takes an [`App`]; the
//! `_by_name` form takes an [`Environment`] and an app name, for an app whose
//! pipeline this binary does not hold. A `_by_name` read of an app that has no
//! stored state yields nothing.
//!
//! The functions read; they never change stored state. The `cocoindex show`
//! command prints the same data.
//!
//! # Examples
//!
//! ```no_run
//! # use cocoindex::{App, inspect};
//! # use futures::TryStreamExt;
//! # async fn run(app: App) -> cocoindex::Result<()> {
//! for path in inspect::list_stable_paths(&app).await? {
//!     println!("{path}");
//! }
//! let mut target_states = std::pin::pin!(inspect::iter_target_states(&app).await);
//! while let Some(entry) = target_states.try_next().await? {
//!     println!("{} owner:{}", entry.readable_path, entry.owner_component_path);
//! }
//! # Ok(())
//! # }
//! ```

use cocoindex_core::inspect::db_inspect;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::app::{App, Environment};
use crate::error::{Error, Result};

pub use cocoindex_core::inspect::db_inspect::{
    ProviderGeneration, StablePathDetail, StablePathInfo, StablePathNodeType, TargetStateEntry,
    TargetStateInfoItemSummary, TargetStateVersion,
};
pub use cocoindex_core::state::stable_path::StablePath;

/// Which paths [`query_stable_path_details`] returns besides the queried one.
/// The default is the queried path alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StablePathQuery {
    /// Also return the paths directly under the queried path.
    pub include_children: bool,
    /// With `include_children`, return every path under the queried path,
    /// not only the direct children.
    pub recursive: bool,
    /// Also return the queried path's ancestors, the root first.
    pub include_parents: bool,
}

/// Convert an engine stream into one that yields the SDK's [`Error`].
fn sdk_stream<T>(
    stream: impl Stream<Item = cocoindex_utils::error::Result<T>> + Send + 'static,
) -> impl Stream<Item = Result<T>> + Send + 'static {
    stream.map(|item| item.map_err(Error::from))
}

/// The app's stable paths, collected.
pub async fn list_stable_paths(app: &App) -> Result<Vec<StablePath>> {
    iter_stable_paths(app)
        .await
        .map_ok(|info| info.path)
        .try_collect()
        .await
}

/// The app's stable paths with their node type, as a stream (nothing is
/// buffered). It yields component nodes and the directory nodes between them.
pub async fn iter_stable_paths(
    app: &App,
) -> impl Stream<Item = Result<StablePathInfo>> + Send + 'static {
    sdk_stream(db_inspect::iter_stable_paths(&app.inner.core_app).await)
}

/// Like [`iter_stable_paths`], for an app named in an environment.
pub async fn iter_stable_paths_by_name(
    env: &Environment,
    app_name: &str,
) -> Result<impl Stream<Item = Result<StablePathInfo>> + Send + 'static> {
    Ok(sdk_stream(
        db_inspect::iter_stable_paths_by_name(env.core(), app_name).await?,
    ))
}

/// The details of every stable path, as a stream.
///
/// The whole iteration runs in one read transaction, so target-state path
/// prefixes shared between paths are resolved once. Prefer it to calling
/// [`get_stable_path_detail`] for each path.
pub async fn iter_stable_path_details(
    app: &App,
) -> impl Stream<Item = Result<StablePathDetail>> + Send + 'static {
    sdk_stream(db_inspect::iter_stable_path_details(&app.inner.core_app).await)
}

/// Like [`iter_stable_path_details`], for an app named in an environment.
pub async fn iter_stable_path_details_by_name(
    env: &Environment,
    app_name: &str,
) -> Result<impl Stream<Item = Result<StablePathDetail>> + Send + 'static> {
    Ok(sdk_stream(
        db_inspect::iter_stable_path_details_by_name(env.core(), app_name).await?,
    ))
}

/// The details of one stable path.
///
/// A path the app never stored reads as a directory node with version 0 and
/// no target states. `None` means the app has no stored state at all, which
/// only the `_by_name` form can meet.
pub async fn get_stable_path_detail(
    app: &App,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    Ok(db_inspect::get_stable_path_detail(&app.inner.core_app, path).await?)
}

/// Like [`get_stable_path_detail`], for an app named in an environment.
pub async fn get_stable_path_detail_by_name(
    env: &Environment,
    app_name: &str,
    path: &StablePath,
) -> Result<Option<StablePathDetail>> {
    Ok(db_inspect::get_stable_path_detail_by_name(env.core(), app_name, path).await?)
}

/// The details of one stable path and, as `query` asks, of its children and
/// parents. Parents come first, then the path, then the children.
pub async fn query_stable_path_details(
    app: &App,
    path: &StablePath,
    query: StablePathQuery,
) -> Result<Vec<StablePathDetail>> {
    Ok(db_inspect::query_stable_path_details(
        &app.inner.core_app,
        path,
        query.include_children,
        query.recursive,
        query.include_parents,
    )
    .await?)
}

/// Like [`query_stable_path_details`], for an app named in an environment.
pub async fn query_stable_path_details_by_name(
    env: &Environment,
    app_name: &str,
    path: &StablePath,
    query: StablePathQuery,
) -> Result<Vec<StablePathDetail>> {
    Ok(db_inspect::query_stable_path_details_by_name(
        env.core(),
        app_name,
        path,
        query.include_children,
        query.recursive,
        query.include_parents,
    )
    .await?)
}

/// The target states the app tracks, each with the component that owns it,
/// as a stream.
///
/// Entries come in stored order: the children of one parent are adjacent and
/// follow it, but the order has no other meaning.
pub async fn iter_target_states(
    app: &App,
) -> impl Stream<Item = Result<TargetStateEntry>> + Send + 'static {
    sdk_stream(db_inspect::iter_target_states(&app.inner.core_app).await)
}

/// Like [`iter_target_states`], for an app named in an environment.
pub async fn iter_target_states_by_name(
    env: &Environment,
    app_name: &str,
) -> Result<impl Stream<Item = Result<TargetStateEntry>> + Send + 'static> {
    Ok(sdk_stream(
        db_inspect::iter_target_states_by_name(env.core(), app_name).await?,
    ))
}
