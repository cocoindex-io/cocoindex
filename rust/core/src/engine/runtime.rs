use std::sync::LazyLock;

use tokio::runtime::Runtime;

fn init_runtime() -> Runtime {
    let _ = env_logger::try_init();
    Runtime::new().unwrap()
}

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(init_runtime);

pub fn get_runtime() -> &'static Runtime {
    &TOKIO_RUNTIME
}
