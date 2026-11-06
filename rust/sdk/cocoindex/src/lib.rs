//! Declarative, incremental data pipelines.
//!
//! CocoIndex keeps a target in sync with its source. You declare what the
//! output should look like (*target states*: files, rows, vectors, graph
//! nodes) as a function of the input, and the engine works out what changed
//! since the last run and applies the minimal set of creates, updates and
//! deletes. The mental model is the one of React or a spreadsheet: declare the
//! result, and the engine recomputes only what its inputs changed.
//!
//! This crate is the Rust SDK. It calls the same core engine as the Python
//! package `cocoindex`, with the same state database, so the two SDKs share
//! concepts one for one: an [`App`] with a main function, *processing
//! components* mounted at stable component paths ([`mount!`], [`use_mount!`],
//! [`mount_each!`]), memoized functions (`#[cocoindex::function(memo)]`),
//! typed context resources ([`ContextKey`], [`context_key!`]) and target
//! connectors ([`connectors`]).
//!
//! # Example
//!
//! A pipeline that upper-cases every `.txt` file under `./input` into
//! `./output`. On the second run only the files that changed are processed,
//! and a file removed from `./input` is removed from `./output`.
//!
//! ```no_run
//! use cocoindex::prelude::*;
//!
//! #[cocoindex::function(memo)]
//! async fn upper(_ctx: &Ctx, file: FileEntry) -> Result<(String, String)> {
//!     Ok((file.key(), file.content_str()?.to_uppercase()))
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let app = App::open("upper_case", "./.cocoindex_db").await?;
//!     let stats = app
//!         .run(|ctx| async move {
//!             let target = mount_dir_target(&ctx, "./output")?;
//!             let files = walk_items("./input", &["**/*.txt"])?;
//!             let outputs = mount_each!(files, |file| upper(ctx, file)).await?;
//!             for (name, text) in &outputs {
//!                 target.declare_file(&ctx, name, text.as_bytes())?;
//!             }
//!             Ok(())
//!         })
//!         .await?;
//!     println!("{stats}");
//!     Ok(())
//! }
//! ```
//!
//! `#[cocoindex::app]`, `#[cocoindex::lifespan]` and `#[cocoindex::main]`
//! register apps for the `cocoindex` command-line tool for Rust projects
//! (`rust/cli` in the repository), which runs, previews and inspects them.
//! The [showcase](https://github.com/cocoindex-io/cocoindex/blob/main/rust/sdk/SHOWCASE.md)
//! walks through every part of the SDK; the
//! [documentation site](https://cocoindex.io/docs) has the concepts and the
//! quickstart.
//!
//! # Modules
//!
//! - [`app`]: [`Environment`], [`App`], updates, previews and dropping state.
//! - [`ctx`]: [`Ctx`], the handle a component uses to mount children, memoize
//!   work, read context and declare target states.
//! - [`memo`]: [`MemoInput`], the identity of a memoized argument.
//! - [`deps`]: dependencies a function declares beyond its arguments.
//! - [`batched`]: batched function calls and the [`Batched`] input type.
//! - [`runner`]: GPU runners for batched functions.
//! - [`target_state`]: the target-state model and the [`TargetHandler`]
//!   trait a connector implements.
//! - [`user_state`]: per-component state kept between runs.
//! - [`live_component`]: long-running components that react to source
//!   changes.
//! - [`resources`]: files and directories, chunks, embedders, id generation,
//!   rate limiting and schemas.
//! - [`ops`]: text splitting, code matching, embedding and transcription.
//! - [`connectors`]: sources and targets for external systems, one per
//!   feature.
//! - [`inspect`]: read the state database of an app.
//! - [`cli`]: the protocol between a user binary and the `cocoindex` CLI.
//! - [`prelude`]: the names most pipelines use.
//!
//! # Features
//!
//! The crate has no default features. Every connector and op is behind a
//! feature so that a pipeline compiles only what it uses.
//!
//! | Feature | Enables | Needs at build time |
//! |---|---|---|
//! | `text` | [`ops::text`]: language detection, recursive and separator chunking | a C compiler (tree-sitter grammars) |
//! | `code_match` | [`ops::code`]: structural code matching over tree-sitter ASTs | a C compiler |
//! | `fastembed` | `ops::sentence_transformers`, `ops::image`: local embeddings | the ONNX Runtime, downloaded by `ort-sys` (network); models download at run time |
//! | `embed_api` | [`ops::api`]: embeddings and transcription over an OpenAI-compatible HTTP API | |
//! | `fs_live` | [`DirWalker::live`]: follow a directory with native file events | |
//! | `chrono`, `rust_decimal` | [`MemoInput`] for those crates' types | |
//! | `postgres` | [`connectors::postgres`] source and target (pgvector) | |
//! | `sqlite` | [`connectors::sqlite`] target | |
//! | `doris` | [`connectors::doris`] target (MySQL protocol + Stream Load) | |
//! | `surrealdb` | [`connectors::surrealdb`] target | |
//! | `lancedb` | [`connectors::lancedb`] target (embedded) | |
//! | `qdrant` | [`connectors::qdrant`] target | `protoc` |
//! | `turbopuffer` | [`connectors::turbopuffer`] target | |
//! | `zvec` | [`connectors::zvec`] target (embedded) | the prebuilt `libzvec_c_api`, downloaded by `zvec-rust-sys` (network) |
//! | `valkey` | [`connectors::valkey`] target (Valkey/Redis with RediSearch) | OpenSSL headers on Linux (native TLS) |
//! | `neo4j` | [`connectors::neo4j`] graph target | |
//! | `falkordb` | [`connectors::falkordb`] graph target | |
//! | `bigquery` | [`connectors::bigquery`] target | |
//! | `snowflake` | [`connectors::snowflake`] target | |
//! | `kafka` | [`connectors::kafka`] source and target | |
//! | `iggy` | [`connectors::iggy`] source and target | |
//! | `amazon_s3` | [`connectors::amazon_s3`] source (S3 and S3-compatible stores) | |
//! | `azure_blob` | [`connectors::azure_blob`] source | |
//! | `google_drive` | [`connectors::gdrive`] source | |
//! | `oci_object_storage` | [`connectors::oci_object_storage`] source | |
//! | `testing` | [`testing`]: an [`Environment`] on a temporary database for tests | |
//!
//! Features that speak HTTP through `reqwest` (`embed_api`, `doris`,
//! `google_drive`, `oci_object_storage`, `azure_blob`, `turbopuffer`,
//! `bigquery`, `snowflake`) use the platform's native TLS, which on Linux
//! needs the OpenSSL development headers.
//!
//! The documentation on docs.rs is built with every feature whose build needs
//! no network access (all but `fastembed` and `zvec`).

extern crate self as cocoindex;

pub mod app;
pub mod batched;
pub mod cli;
pub mod connectors;
pub mod ctx;
#[cfg(any(feature = "neo4j", feature = "falkordb"))]
mod cypher_graph;
pub mod deps;
pub mod entity_resolution;
pub mod error;
// Rejects non-finite floats before the JSON round-trip in target connectors that
// serialize rows through `serde_json` (which maps NaN/±Inf to null).
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    feature = "doris",
    feature = "surrealdb",
    feature = "neo4j",
    feature = "falkordb",
    feature = "lancedb",
    feature = "zvec",
    feature = "bigquery",
    feature = "snowflake"
))]
mod finite;
pub mod inspect;
pub mod live_component;
#[doc(hidden)]
pub mod logic;
pub mod memo;
pub mod mount;
pub mod ops;
pub mod prelude;
pub(crate) mod profile;
#[doc(hidden)]
pub mod registry;
pub mod resources;
pub mod row_schema;
pub mod runner;
#[cfg(any(
    feature = "doris",
    feature = "sqlite",
    feature = "surrealdb",
    feature = "bigquery",
    feature = "snowflake"
))]
pub(crate) mod sql_ident;
pub mod statediff;
mod stats;
pub mod target_state;
#[cfg(feature = "testing")]
pub mod testing;
mod typemap;
pub mod user_state;

// Flat re-exports — the public API surface
pub use app::{
    App, AppBuilder, DropHandle, Environment, EnvironmentBuilder, PreviewAction, PreviewValue,
    Progress, StatsGroupHandle, StatsGroupOptions, UpdateHandle, UpdateOptions,
};
pub use batched::Batched;
pub use ctx::{ComponentMountHandle, ContextKey, Ctx};
pub use deps::{DataDep, LogicDep, PropagatedCallback};
pub use entity_resolution::{
    CanonicalSide, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, ResolvedEntities, resolve_entities,
    resolve_entities_with_events,
};
pub use error::{Error, Result};
pub use memo::MemoInput;
// The fingerprint type in public signatures ([`FileMetadata::content_fingerprint`],
// [`FnDeps::get`]); users cannot name it through `cocoindex_utils`.
pub use cocoindex_utils::fingerprint::Fingerprint;
pub use resources::file::{
    DecodeErrors, FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher,
    FileSourceItem, MatchAllFilePathMatcher, PatternFilePathMatcher,
};
pub use resources::fs::{
    DirTarget, DirTargetState, DirWalker, FileEntry, declare_dir_target, dir_target,
    mount_dir_target, walk, walk_dir, walk_items,
};
pub use resources::id::{
    IdGenerator, UuidGenerator, generate_id, generate_id_default, generate_uuid,
    generate_uuid_default,
};
pub use runner::{
    GPU, GPURunner, Runner, RunnerBody, RunnerOutput, configure_gpu_pool, current_gpu,
    current_gpu_fraction, current_gpus,
};
// Re-exported so `#[cocoindex::function]` output can register each function's
// logic fingerprint without the user crate needing a direct `linkme` dependency.
#[doc(hidden)]
pub use linkme;
pub use live_component::{
    ExceptionContext, ExceptionHandler, ExceptionSource, LiveComponent, LiveComponentOperator,
    LiveMapFeed, LiveMapSubscriber, LiveMapView, LiveStream, LiveStreamSubscriber, MountKind,
    ReadyHandle, SingleWatcherGuard, SingleWatcherToken,
};
#[doc(hidden)]
pub use logic::{COCO_FN_LOGIC, FnDeps, FnLogicEntry};
// Re-exported for `#[cocoindex::app]` / `#[cocoindex::lifespan]` output.
#[doc(hidden)]
pub use registry::{AppEntry, COCO_APPS, COCO_LIFESPANS, LifespanEntry};
// Re-exported so `#[cocoindex::main]` can build its runtime without the user
// crate depending on tokio.
#[doc(hidden)]
pub use tokio;

/// Items the proc macros expand to; not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use futures::future::BoxFuture;
}
pub use resources::chunk::{Chunk, TextPosition};
pub use resources::embedder::Embedder;
pub use resources::live_map::{LiveMap, LiveMapKey};
pub use resources::rate_limit::RateLimiter;
pub use resources::view::{SegmentKind, SourceView, ViewSegment};
// The content/memo fingerprint type: `memo::key_fingerprint_result`,
// `FileMetadata::content_fingerprint` and `FileLike::content_fingerprint`
// return it, and `MemoInput` accepts a precomputed one.
pub use cocoindex_utils::fingerprint::Fingerprint;
pub use resources::schema::{
    MultiVectorSchema, MultiVectorSchemaProvider, VectorElementType, VectorSchema,
    VectorSchemaProvider,
};
pub use statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord,
    TrackingRecordTransition, diff, diff_composite, resolve_system_transition,
};
pub use stats::{ComponentStats, RunStats, UpdateStats, UpdateStatus};
pub use target_state::{
    ChildSlot, ChildTargetDef, IntoStableKey, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
    register_root_target_states_provider,
};
pub use user_state::{IntoStateKey, StateHandle};

// Re-export proc macros
pub use cocoindex_macros::{
    MemoInput, SchemaFields, app, function, lifespan, main, mount, mount_each, use_mount,
    use_mount_each,
};
pub use row_schema::{LogicalType, SchemaField, SchemaFields};

// Re-exported so users can implement the async `LiveComponent` / `LiveMapFeed`
// / `LiveMapView` traits as `#[cocoindex::async_trait]` without taking their own
// dependency on `async-trait`.
pub use async_trait::async_trait;
