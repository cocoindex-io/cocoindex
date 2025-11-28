mod app;
mod component;
mod context;
mod effect;
mod environment;
mod prelude;
mod runtime;
mod state_path;
mod value;

#[pyo3::pymodule]
#[pyo3(name = "core")]
fn core_module(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::prelude::*;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(runtime::init_runtime, m)?)?;

    m.add_class::<app::PyApp>()?;

    m.add_class::<component::PyComponentBuilder>()?;

    m.add_class::<context::PyComponentBuilderContext>()?;

    m.add_function(wrap_pyfunction!(effect::init_module, m)?)?;
    m.add_class::<effect::PyEffectSink>()?;
    m.add_class::<effect::PyEffectReconciler>()?;
    m.add_class::<effect::PyEffectProvider>()?;
    m.add_function(wrap_pyfunction!(effect::declare_effect, m)?)?;

    m.add_class::<environment::PyEnvironment>()?;

    m.add_class::<runtime::PyAsyncContext>()?;

    m.add_class::<state_path::PyStatePath>()?;
    Ok(())
}
