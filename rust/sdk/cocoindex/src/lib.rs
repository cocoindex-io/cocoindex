pub mod app;
pub mod ctx;
pub mod error;
pub mod fs;
pub mod memo;
pub mod prelude;
pub(crate) mod profile;
mod stats;
mod typemap;

// Flat re-exports — the public API surface
pub use app::{App, AppBuilder};
pub use ctx::Ctx;
pub use error::{Error, Result};
pub use fs::{DirTarget, FileEntry, walk};
pub use stats::RunStats;

// Re-export proc macros
pub use cocoindex_macros::function;
