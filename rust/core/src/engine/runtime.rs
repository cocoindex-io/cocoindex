use std::sync::LazyLock;

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

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(init_runtime);

pub fn get_runtime() -> &'static Runtime {
    &TOKIO_RUNTIME
}
