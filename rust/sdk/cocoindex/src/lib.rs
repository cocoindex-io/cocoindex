pub mod app;
pub mod ctx;
pub mod entity_resolution;
pub mod error;
pub mod fs;
pub mod id;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod memo;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod prelude;
pub(crate) mod profile;
mod stats;
#[cfg(feature = "surrealdb")]
pub mod surrealdb;
mod typemap;

// Flat re-exports — the public API surface
pub use app::{
    App, AppBuilder, DropHandle, Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle,
    UpdateOptions,
};
pub use ctx::{ContextKey, Ctx};
pub use entity_resolution::{
    CanonicalSide, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, ResolvedEntities, resolve_entities,
};
pub use error::{Error, Result};
pub use fs::{DirTarget, FileEntry, mount_dir_target, walk};
pub use id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use stats::RunStats;

// Re-export proc macros
pub use cocoindex_macros::function;
