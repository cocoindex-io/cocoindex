//! Link-time registry of `#[cocoindex::lifespan]` and `#[cocoindex::app]` items.
//!
//! The macros push one entry per item into a [`linkme`] distributed slice, the
//! same mechanism `#[cocoindex::function]` uses for logic fingerprints (see
//! `crate::logic`). This is the Rust analogue of Python's import-time
//! registration: [`crate::Environment::from_registered`] runs every lifespan,
//! and the `__coco` CLI protocol (see `crate::cli`) finds apps by name — so the
//! CLI and a user's own `main` initialize the environment through the identical
//! code.
//!
//! Order within a slice is link order and is not guaranteed; a binary normally
//! has one lifespan. App names must be unique per binary
//! ([`check_unique_app_names`]).

use crate::app::EnvironmentBuilder;
use crate::ctx::Ctx;
use crate::error::{Error, Result};
use futures::future::BoxFuture;

/// Type-erased `#[cocoindex::lifespan]` function: takes the builder, returns it
/// configured.
pub type LifespanFn = fn(EnvironmentBuilder) -> BoxFuture<'static, Result<EnvironmentBuilder>>;

/// Type-erased `#[cocoindex::app]` entry function. The app's own return value
/// is discarded (`()` is what the CLI stores as the root result).
pub type AppFn = fn(Ctx) -> BoxFuture<'static, Result<()>>;

/// One registered lifespan.
pub struct LifespanEntry {
    pub module: &'static str,
    pub name: &'static str,
    pub run: LifespanFn,
}

/// One registered app.
pub struct AppEntry {
    pub module: &'static str,
    /// The app name: `name = "..."` on the attribute, else the function name.
    pub name: &'static str,
    pub run: AppFn,
}

#[linkme::distributed_slice]
pub static COCO_LIFESPANS: [LifespanEntry] = [..];

#[linkme::distributed_slice]
pub static COCO_APPS: [AppEntry] = [..];

/// Every `#[cocoindex::lifespan]` linked into this binary.
pub fn lifespans() -> &'static [LifespanEntry] {
    &COCO_LIFESPANS
}

/// Every `#[cocoindex::app]` linked into this binary.
pub fn apps() -> &'static [AppEntry] {
    &COCO_APPS
}

/// Find a registered app by name.
pub fn find_app(name: &str) -> Option<&'static AppEntry> {
    COCO_APPS.iter().find(|e| e.name == name)
}

/// Error if two `#[cocoindex::app]` items share a name. Names are LMDB
/// namespaces, so a collision would silently merge two apps' state.
pub fn check_unique_app_names() -> Result<()> {
    let mut seen = std::collections::HashMap::<&str, &AppEntry>::new();
    for entry in COCO_APPS.iter() {
        if let Some(prev) = seen.insert(entry.name, entry) {
            return Err(Error::engine(format!(
                "two #[cocoindex::app] functions register the app name `{}`, in {} and {}",
                entry.name, prev.module, entry.module
            )));
        }
    }
    Ok(())
}
