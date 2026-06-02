pub mod app;
pub mod ctx;
pub mod entity_resolution;
pub mod error;
pub mod fs;
#[cfg(feature = "google_drive")]
pub mod gdrive;
pub mod id;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "lancedb")]
pub mod lancedb;
pub mod memo;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod prelude;
pub(crate) mod profile;
mod stats;
#[cfg(feature = "surrealdb")]
pub mod surrealdb;
pub mod target_state;
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
pub use fs::{
    DirTarget, DirTargetState, DirWalker, FileEntry, FileLike, FileMetadata, FilePath,
    FilePathMatcher, MatchAllFilePathMatcher, PatternFilePathMatcher, declare_dir_target,
    dir_target, mount_dir_target, walk, walk_dir,
};
pub use id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use stats::RunStats;
pub use target_state::{
    ChildTargetDef, IntoStableKey, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
    register_root_target_states_provider,
};

// Re-export proc macros
pub use cocoindex_macros::function;
