pub mod app;
pub mod ctx;
pub mod error;
pub mod fs;
pub mod id;
pub mod memo;
pub mod prelude;
pub(crate) mod profile;
mod stats;
mod typemap;

// Flat re-exports — the public API surface
pub use app::{
    App, AppBuilder, DropHandle, Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle,
    UpdateOptions,
};
pub use ctx::{ContextKey, Ctx};
pub use error::{Error, Result};
pub use fs::{DirTarget, FileEntry, walk};
pub use id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use stats::RunStats;

// Re-export proc macros
pub use cocoindex_macros::function;
