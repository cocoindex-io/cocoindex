//! The SDK's integration tests, as one test binary with a module per area.
//!
//! Cargo builds every file directly under `tests/` as a test binary of its
//! own, and each of them links the whole SDK with every enabled connector:
//! several GB of memory per link, and Cargo runs as many links at a time as
//! the machine has cores. As modules of one crate, the tests link once.
//!
//! Run one area by its module name:
//!
//!   cargo test -p cocoindex --features lancedb --test it lancedb_target::
//!
//! A new test file goes here, with its `mod` line below. It is a binary of
//! its own, directly under `tests/`, only when it needs a process of its own;
//! [`OWN_PROCESS_TESTS`] lists those, each with what it needs one for.

mod admission;
mod admission_common;
mod amazon_s3_source;
mod azure_blob_source;
mod bigquery_target;
mod code;
mod code_fuzz;
mod component_selector;
mod component_selector_memo_key;
mod doris_target;
mod drop_state;
mod entity_resolution;
mod fs_live;
mod fs_live_battle;
mod fs_live_restart;
mod fs_target;
mod gdrive_source;
mod gdrive_user_account;
mod graph_target;
mod graph_vector_index;
mod iggy_target;
mod inspect;
mod kafka_target;
mod lancedb_battle;
mod lancedb_target;
mod live_battle;
mod live_component;
mod live_map;
mod live_soak;
mod live_stream;
mod logic_tracking;
mod memo_clone;
mod memo_deps;
mod model_based;
mod mount_each;
mod oci_live;
mod ops_api;
mod ops_text;
mod ops_text_fuzz;
mod pipeline;
mod postgres_source;
mod preview;
mod qdrant_target;
mod schema_from_row;
mod snowflake_target;
mod surrealdb_scenarios;
mod surrealdb_target;
mod target_state;
mod turbopuffer_target;
mod user_state;
mod valkey_target;
mod zvec_battle;
mod zvec_target;

// These build their environment with `cocoindex::testing`, so they need the
// `testing` feature: without it they are left out instead of failing to
// compile (`cargo check --all-targets` with no features). A self
// dev-dependency would enable the feature too, but it links the crate twice
// into its own unit tests and breaks the `linkme` registry (duplicate
// `COCO_FN_LOGIC`).
#[cfg(feature = "testing")]
mod batching_edges;
#[cfg(feature = "testing")]
mod batching_runner;
#[cfg(feature = "testing")]
mod detailed_stats;
#[cfg(feature = "testing")]
mod mount;

/// The test files that are binaries of their own, directly under `tests/`.
/// Each one changes or reads something the whole process shares, so it cannot
/// run beside the other tests.
const OWN_PROCESS_TESTS: &[&str] = &[
    // Sets `COCOINDEX_MAX_INFLIGHT_COMPONENTS`.
    "admission_env",
    // Sets the `AZURE_*` variables of the credential it resolves.
    "azure_blob_token",
    // Drives the engine's deadline test clock.
    "deadline",
    // Stops its live run through the engine's cancellation of every run.
    "iggy_source",
    // Stops its live run through the engine's cancellation of every run.
    "kafka_source",
    // Sets `HF_ENDPOINT` and `HF_HOME`.
    "ops_sentence_transformers",
    // Record the SQL statements `sqlx` runs through the process's `tracing`
    // subscriber, which is the engine's once any other test has started the
    // engine.
    "postgres_target",
    "postgres_target_edge",
    // Reads the registries of every `#[cocoindex::app]` and
    // `#[cocoindex::lifespan]` linked into the binary, and sets `COCOINDEX_DB`.
    "registered",
    // Registers two apps with one name, which no other binary may hold.
    "registered_dup",
    // Needs a binary that registers no lifespan; sets `COCOINDEX_DB` and the
    // working directory.
    "registered_nodb",
    // Record the SQL statements `sqlx` runs, as `postgres_target` does.
    "sqlite_target",
    "sqlite_target_edge",
];

/// A file under `tests/it/` that no `mod` line names is never compiled, and a
/// new file directly under `tests/` is one more binary to link; neither shows
/// up as a failure by itself.
#[test]
fn every_test_file_is_a_module_here_or_a_listed_binary() {
    let tests = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let rust_files = |dir: &std::path::Path| -> Vec<String> {
        let mut stems: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
            .map(|path| path.file_stem().unwrap().to_str().unwrap().to_owned())
            .collect();
        stems.sort();
        stems
    };

    let main = include_str!("main.rs");
    let unnamed: Vec<String> = rust_files(&tests.join("it"))
        .into_iter()
        .filter(|stem| stem != "main")
        .filter(|stem| !main.lines().any(|line| line == format!("mod {stem};")))
        .collect();
    assert!(
        unnamed.is_empty(),
        "tests/it/main.rs has no `mod` line for {unnamed:?}, so their tests never run"
    );

    assert_eq!(
        rust_files(&tests),
        OWN_PROCESS_TESTS,
        "a test file directly under tests/ is a binary of its own: move it to tests/it/ as a \
         module, or list it in OWN_PROCESS_TESTS with what it needs its own process for"
    );
}
