mod error;
mod path;
mod internal;
mod app;
pub mod memo;
mod connector;
mod connectors;
pub mod ops;
mod store;

// Flat re-exports — the public API surface
pub use error::{Error, Result};
pub use path::ComponentPath;
pub use app::{App, AppBuilder, Ctx, MountHandle};
pub use connector::{Source, Target};
pub use connectors::localfs::{FileRef, DirTarget, walk_dir, WalkOpts};
pub use store::{
    ContentStore, SyncReport, CheckpointInfo, RestoreReport, ImportReport, Entry, DiffEntry,
};
