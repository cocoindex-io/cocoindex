use std::sync::LazyLock;

use tokio::runtime::Runtime;

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

pub fn get_runtime() -> &'static Runtime {
    &TOKIO_RUNTIME
}
