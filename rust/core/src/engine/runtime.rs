use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

fn init_runtime() -> Runtime {
    // Initialize tracing subscriber with env filter for log level control // Default to "info" level if RUST_LOG is not set
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .try_init();

    Runtime::new().unwrap()
}

static TOKIO_RUNTIME: LazyLock<ManuallyDrop<Runtime>> =
    LazyLock::new(|| ManuallyDrop::new(init_runtime()));

static RUNTIME_SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Global cancellation token for graceful shutdown (e.g. Ctrl+C).
///
/// Wrapped in a `Mutex` so it can be replaced with a fresh token when a new
/// operation starts after a previous cancellation.
static GLOBAL_CANCEL: LazyLock<Mutex<CancellationToken>> =
    LazyLock::new(|| Mutex::new(CancellationToken::new()));

pub fn get_runtime() -> &'static Runtime {
    &**TOKIO_RUNTIME
}

/// Return a clone of the current global cancellation token.
///
/// The returned token stays valid even if the global slot is later replaced
/// by `reset_global_cancellation()`.
pub fn global_cancellation_token() -> CancellationToken {
    GLOBAL_CANCEL.lock().unwrap().clone()
}

/// Cancel the current global token, causing all operations that selected on
/// it to exit promptly.
pub fn cancel_all() {
    GLOBAL_CANCEL.lock().unwrap().cancel();
}

/// Returns `true` if the current global token has been cancelled.
pub fn is_cancelled() -> bool {
    GLOBAL_CANCEL.lock().unwrap().is_cancelled()
}

/// Replace the global token with a fresh (non-cancelled) one so that new
/// operations can proceed after a previous cancellation.
pub fn reset_global_cancellation() {
    let mut token = GLOBAL_CANCEL.lock().unwrap();
    if token.is_cancelled() {
        *token = CancellationToken::new();
    }
}

/// Gracefully shut down the Tokio runtime, waiting for all threads to exit.
///
/// Must be called before `Py_Finalize()` to prevent Tokio blocking-pool threads
/// from calling `PyGILState_Release` after `_PyGILState_Fini` has deleted the
/// `autoTSSkey` TLS key (which causes a fatal `PyGILState_Release` error on Python < 3.13).
pub fn shutdown_runtime() {
    if RUNTIME_SHUTDOWN.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = &**TOKIO_RUNTIME;
    let runtime = unsafe {
        let md_ref: &ManuallyDrop<Runtime> = &*TOKIO_RUNTIME;
        std::ptr::read(md_ref as *const ManuallyDrop<Runtime> as *const Runtime)
    };
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
}
