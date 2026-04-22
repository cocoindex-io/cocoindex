//! Integration test for the post-shutdown safety guard in `track()`.
//!
//! Runs as its own binary so `shutdown_runtime` (which consumes the global
//! static Tokio runtime) can be called without affecting other tests.

use cocoindex_core::engine::runtime::shutdown_runtime;
use cocoindex_core::telemetry;

#[test]
fn track_noops_after_runtime_shutdown() {
    shutdown_runtime();
    telemetry::track("app_drop");
    telemetry::track("app_update");
}
