mod app;
mod component;
mod context;
mod environment;
mod fingerprint;
mod function;
mod inspect;
mod memo_key;
mod ops;
mod prelude;
mod profile;
mod runtime;
mod stable_path;
mod target_state;
mod value;

#[pyo3::pymodule]
#[pyo3(name = "core")]
fn core_module(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::prelude::*;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(runtime::init_runtime, m)?)?;

    m.add_class::<app::PyApp>()?;

    m.add_class::<component::PyComponentProcessorInfo>()?;
    m.add_class::<component::PyComponentProcessor>()?;
    m.add_class::<component::PyComponentMountHandle>()?;
    m.add_class::<component::PyComponentMountRunHandle>()?;
    m.add_function(wrap_pyfunction!(component::mount, m)?)?;
    m.add_function(wrap_pyfunction!(component::mount_run, m)?)?;

    m.add_class::<context::PyComponentProcessorContext>()?;
    m.add_class::<context::PyFnCallContext>()?;

    m.add_class::<target_state::PyTargetActionSink>()?;
    m.add_class::<target_state::PyTargetHandler>()?;
    m.add_class::<target_state::PyTargetStateProvider>()?;
    m.add_function(wrap_pyfunction!(target_state::declare_target_state, m)?)?;
    m.add_function(wrap_pyfunction!(
        target_state::declare_target_state_with_child,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        target_state::register_root_target_states_provider,
        m
    )?)?;

    m.add_class::<environment::PyEnvironment>()?;

    m.add_function(wrap_pyfunction!(inspect::list_stable_paths, m)?)?;
    m.add_function(wrap_pyfunction!(inspect::list_app_names, m)?)?;

    m.add_class::<runtime::PyAsyncContext>()?;

    m.add_class::<stable_path::PyStablePath>()?;

    // Fingerprints (stable 16-byte digest wrapper)
    m.add_class::<fingerprint::PyFingerprint>()?;

    // Function memoization
    m.add_class::<function::PyPendingFnCallMemo>()?;
    m.add_function(wrap_pyfunction!(function::reserve_memoization, m)?)?;
    m.add_function(wrap_pyfunction!(function::reserve_memoization_async, m)?)?;

    // Memoization fingerprinting (deterministic)
    m.add_function(wrap_pyfunction!(memo_key::fingerprint_simple_object, m)?)?;
    m.add_function(wrap_pyfunction!(memo_key::fingerprint_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(memo_key::fingerprint_str, m)?)?;

    // Text processing operations
    m.add_class::<ops::PyChunk>()?;
    m.add_class::<ops::PySeparatorSplitter>()?;
    m.add_class::<ops::PyCustomLanguageConfig>()?;
    m.add_class::<ops::PyRecursiveSplitter>()?;
    m.add_function(wrap_pyfunction!(ops::detect_code_language, m)?)?;
    Ok(())
}
