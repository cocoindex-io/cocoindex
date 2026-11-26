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
// Shared with `tests/admission_env.rs`, which is a binary of its own.
#[path = "../admission_common/mod.rs"]
mod admission_common;
mod amazon_s3_source;
mod azure_blob_source;
mod batching_edges;
mod batching_runner;
mod bigquery_target;
mod code;
mod code_fuzz;
mod component_selector;
mod component_selector_memo_key;
mod detailed_stats;
mod doris_target;
mod drop_state;
mod entity_resolution;
mod fs_live;
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
mod live_component;
mod live_soak;
mod live_stream;
mod logic_tracking;
mod memo_clone;
mod memo_deps;
mod model_based;
mod mount;
mod mount_each;
mod oci_live;
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

/// The test files that are binaries of their own, directly under `tests/`.
/// Each one changes or reads something the whole process shares, or needs the
/// process to itself, so it cannot run beside the other tests.
const OWN_PROCESS_TESTS: &[&str] = &[
    // Sets `COCOINDEX_MAX_INFLIGHT_COMPONENTS`.
    "admission_env",
    // Sets the `AZURE_*` variables of the credential it resolves.
    "azure_blob_token",
    // Drives the engine's deadline test clock.
    "deadline",
    // Assert that work ends within a few seconds, to show that a timeout
    // fired or that items ran side by side. Beside the other tests, which
    // share the machine and the engine's runtime, they miss the bound.
    "fs_live_battle",
    // Stop their live runs through the engine's cancellation of every run.
    "iggy_source",
    "kafka_source",
    // Assert that work ends within a few seconds, as `fs_live_battle` does.
    "live_battle",
    "live_map",
    "ops_api",
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

/// A module under `tests/it/` that no `mod` line names is never compiled, and a
/// new test directly under `tests/` is one more binary to link; neither shows
/// up as a failure by itself.
#[test]
fn every_test_file_is_a_module_here_or_a_listed_binary() {
    use std::path::{Path, PathBuf};

    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let entries = |dir: &Path| -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        paths
    };
    let is_rust_file = |path: &Path| path.extension().is_some_and(|ext| ext == "rs");
    let name = |path: &Path| path.file_stem().unwrap().to_str().unwrap().to_owned();

    // A module of this crate is `x.rs` or `x/mod.rs`.
    let main = include_str!("main.rs");
    let unnamed: Vec<String> = entries(&tests.join("it"))
        .iter()
        .filter(|path| is_rust_file(path) || path.join("mod.rs").exists())
        .map(|path| name(path))
        .filter(|module| module != "main")
        .filter(|module| !main.lines().any(|line| line == format!("mod {module};")))
        .collect();
    assert!(
        unnamed.is_empty(),
        "tests/it/main.rs has no `mod` line for {unnamed:?}, so their tests never run"
    );

    // A test binary is `x.rs` or `x/main.rs` directly under `tests/`.
    let binaries: Vec<String> = entries(&tests)
        .iter()
        .filter(|path| is_rust_file(path) || path.join("main.rs").exists())
        .map(|path| name(path))
        .filter(|binary| binary != "it")
        .collect();
    assert_eq!(
        binaries, OWN_PROCESS_TESTS,
        "a test directly under tests/ is a binary of its own: move it to tests/it/ as a module, \
         or list it in OWN_PROCESS_TESTS with what it needs its own process for"
    );
}
