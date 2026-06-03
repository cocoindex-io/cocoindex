pub mod app;
pub mod ctx;
#[cfg(any(feature = "neo4j", feature = "falkordb"))]
mod cypher_graph;
pub mod entity_resolution;
pub mod error;
#[cfg(feature = "falkordb")]
pub mod falkordb;
pub mod fs;
#[cfg(feature = "google_drive")]
pub mod gdrive;
pub mod id;
#[cfg(feature = "iggy")]
pub mod iggy;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "lancedb")]
pub mod lancedb;
pub mod live_component;
pub mod memo;
#[cfg(feature = "neo4j")]
pub mod neo4j;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod prelude;
pub(crate) mod profile;
#[cfg(feature = "qdrant")]
pub mod qdrant;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod statediff;
mod stats;
#[cfg(feature = "surrealdb")]
pub mod surrealdb;
pub mod target_state;
#[cfg(feature = "turbopuffer")]
pub mod turbopuffer;
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
pub use live_component::{
    ExceptionContext, ExceptionHandler, LiveComponent, LiveComponentOperator, LiveMapFeed,
    LiveMapSubscriber, LiveMapView, MountKind,
};
pub use statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord,
    TrackingRecordTransition, diff, diff_composite, resolve_system_transition,
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

// Re-exported so users can implement the async `LiveComponent` / `LiveMapFeed`
// / `LiveMapView` traits as `#[cocoindex::async_trait]` without taking their own
// dependency on `async-trait`.
pub use async_trait::async_trait;
