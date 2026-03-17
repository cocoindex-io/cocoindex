use std::mem::ManuallyDrop;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::runtime::Runtime;
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

pub fn get_runtime() -> &'static Runtime {
    &**TOKIO_RUNTIME
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
