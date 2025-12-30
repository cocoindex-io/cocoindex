mod app;
mod component;
mod context;
mod effect;
mod environment;
mod extras;
mod inspect;
mod prelude;
mod profile;
mod runtime;
mod stable_path;
mod value;

#[pyo3::pymodule]
#[pyo3(name = "core")]
fn core_module(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::prelude::*;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(runtime::init_runtime, m)?)?;

    m.add_class::<app::PyApp>()?;

    m.add_class::<component::PyComponentProcessor>()?;
    m.add_class::<component::PyComponentMountHandle>()?;
    m.add_class::<component::PyComponentMountRunHandle>()?;
    m.add_function(wrap_pyfunction!(component::mount, m)?)?;
    m.add_function(wrap_pyfunction!(component::mount_run, m)?)?;

    m.add_class::<context::PyComponentProcessorContext>()?;

    m.add_function(wrap_pyfunction!(effect::init_effect_module, m)?)?;
    m.add_class::<effect::PyEffectSink>()?;
    m.add_class::<effect::PyEffectHandler>()?;
    m.add_class::<effect::PyEffectProvider>()?;
    m.add_function(wrap_pyfunction!(effect::declare_effect, m)?)?;
    m.add_function(wrap_pyfunction!(effect::declare_effect_with_child, m)?)?;
    m.add_function(wrap_pyfunction!(effect::register_root_effect_provider, m)?)?;

    m.add_class::<environment::PyEnvironment>()?;

    m.add_function(wrap_pyfunction!(inspect::list_stable_paths, m)?)?;

    m.add_class::<runtime::PyAsyncContext>()?;

    m.add_class::<stable_path::PyStablePath>()?;

    // Extras text processing
    m.add_class::<extras::PyChunk>()?;
    m.add_class::<extras::PySeparatorSplitter>()?;
    m.add_class::<extras::PyCustomLanguageConfig>()?;
    m.add_class::<extras::PyRecursiveSplitter>()?;
    m.add_function(wrap_pyfunction!(extras::detect_code_language, m)?)?;
    Ok(())
}
