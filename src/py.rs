use crate::base::spec::VectorSimilarityMetric;
use crate::execution::query;
use crate::get_lib_context;
use crate::lib_context::create_lib_context;
use crate::ops::interface::QueryResults;
use crate::ops::py_factory::PyOpArgSchema;
use crate::ops::{interface::ExecutorFactory, py_factory::PyFunctionFactory, register_factory};
use crate::server::{self, ServerSettings};
use crate::settings::Settings;
use crate::LIB_CONTEXT;
use crate::{api_error, setup};
use crate::{builder, execution};
use anyhow::anyhow;
use pyo3::{exceptions::PyException, prelude::*};
use pythonize::{depythonize, pythonize};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::btree_map;
use std::ops::Deref;
use std::sync::Arc;

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T, E: std::fmt::Debug> IntoPyResult<T> for Result<T, E> {
    fn into_py_result(self) -> PyResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(PyException::new_err(format!("{:?}", err))),
        }
    }
}

pub struct Pythonized<T>(pub T);

impl<'py, T: DeserializeOwned> FromPyObject<'py> for Pythonized<T> {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Pythonized(depythonize(obj).into_py_result()?))
    }
}

impl<'py, T: Serialize> IntoPyObject<'py> for &Pythonized<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        pythonize(py, &self.0).into_py_result()
    }
}

impl<'py, T: Serialize> IntoPyObject<'py> for Pythonized<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        (&self).into_pyobject(py)
    }
}

impl<T> Pythonized<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Pythonized<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pyfunction]
fn init(py: Python<'_>, settings: Pythonized<Settings>) -> PyResult<()> {
    py.allow_threads(|| -> anyhow::Result<()> {
        let mut lib_context_locked = LIB_CONTEXT.write().unwrap();
        if lib_context_locked.is_some() {
            return Ok(());
        }
        *lib_context_locked = Some(Arc::new(create_lib_context(settings.into_inner())?));
        Ok(())
    })
    .into_py_result()
}

#[pyfunction]
fn start_server(py: Python<'_>, settings: Pythonized<ServerSettings>) -> PyResult<()> {
    py.allow_threads(|| -> anyhow::Result<()> {
        let lib_context =
            get_lib_context().ok_or_else(|| api_error!("Cocoindex is not initialized"))?;
        let server = lib_context.runtime.block_on(server::init_server(
            lib_context.clone(),
            settings.into_inner(),
        ))?;
        lib_context.runtime.spawn(server);
        Ok(())
    })
    .into_py_result()
}

#[pyfunction]
fn stop(py: Python<'_>) -> PyResult<()> {
    py.allow_threads(|| {
        let mut runtime_context_locked = LIB_CONTEXT.write().unwrap();
        *runtime_context_locked = None;
    });
    Ok(())
}

#[pyfunction]
fn register_function_factory(name: String, py_function_factory: Py<PyAny>) -> PyResult<()> {
    let factory = PyFunctionFactory {
        py_function_factory,
    };
    register_factory(name, ExecutorFactory::SimpleFunction(Arc::new(factory))).into_py_result()
}

#[pyclass]
pub struct IndexUpdateInfo(pub execution::indexer::IndexUpdateInfo);

#[pymethods]
impl IndexUpdateInfo {
    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

#[pyclass]
pub struct Flow(pub Arc<builder::AnalyzedFlow>);

#[pymethods]
impl Flow {
    pub fn __str__(&self) -> String {
        serde_json::to_string_pretty(&self.0.flow_instance).unwrap()
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn update(&self, py: Python<'_>) -> PyResult<IndexUpdateInfo> {
        py.allow_threads(|| {
            let lib_context = get_lib_context()
                .ok_or_else(|| PyException::new_err("cocoindex library not initialized"))?;
            let update_info = lib_context
                .runtime
                .block_on(async {
                    let exec_plan = self.0.get_execution_plan().await?;
                    execution::indexer::update(
                        &self.0.flow_instance,
                        &exec_plan,
                        &self.0.data_schema,
                        &lib_context.pool,
                    )
                    .await
                })
                .into_py_result()?;
            Ok(IndexUpdateInfo(update_info))
        })
    }
}

#[pyclass]
pub struct TransientFlow(pub Arc<builder::AnalyzedTransientFlow>);

#[pymethods]
impl TransientFlow {
    pub fn __str__(&self) -> String {
        serde_json::to_string_pretty(&self.0.transient_flow_instance).unwrap()
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

#[pyclass]
pub struct SimpleSemanticsQueryHandler(pub Arc<query::SimpleSemanticsQueryHandler>);

#[pymethods]
impl SimpleSemanticsQueryHandler {
    #[new]
    pub fn new(
        py: Python<'_>,
        flow: &Flow,
        target_name: &str,
        query_transform_flow: &TransientFlow,
        default_similarity_metric: Pythonized<VectorSimilarityMetric>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let lib_context = get_lib_context()
                .ok_or_else(|| api_error!("Cocoindex is not initialized"))
                .into_py_result()?;
            let handler = lib_context
                .runtime
                .block_on(query::SimpleSemanticsQueryHandler::new(
                    flow.0.clone(),
                    target_name,
                    query_transform_flow.0.clone(),
                    default_similarity_metric.0,
                ))
                .into_py_result()?;
            Ok(Self(Arc::new(handler)))
        })
    }

    pub fn register_query_handler(&self, name: String) -> PyResult<()> {
        let lib_context = get_lib_context()
            .ok_or_else(|| PyException::new_err("cocoindex library not initialized"))?;
        let mut flows = lib_context.flows.write().unwrap();
        let flow_ctx = flows
            .get_mut(&self.0.flow_name)
            .ok_or_else(|| PyException::new_err(format!("flow not found: {}", self.0.flow_name)))?;
        match flow_ctx.query_handlers.entry(name) {
            btree_map::Entry::Occupied(entry) => {
                return Err(PyException::new_err(format!(
                    "query handler name already exists: {}",
                    entry.key()
                )));
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(self.0.clone());
            }
        }
        Ok(())
    }

    #[pyo3(signature = (query, limit, vector_field_name = None, similarity_matric = None))]
    pub fn search(
        &self,
        py: Python<'_>,
        query: String,
        limit: u32,
        vector_field_name: Option<String>,
        similarity_matric: Option<Pythonized<VectorSimilarityMetric>>,
    ) -> PyResult<(
        Pythonized<QueryResults>,
        Pythonized<query::SimpleSemanticsQueryInfo>,
    )> {
        py.allow_threads(|| {
            let lib_context = get_lib_context()
                .ok_or_else(|| anyhow!("cocoindex library not initialized"))
                .into_py_result()?;
            let (results, info) = lib_context
                .runtime
                .block_on(async move {
                    self.0
                        .search(
                            query,
                            limit,
                            vector_field_name,
                            similarity_matric.map(|m| m.0),
                        )
                        .await
                })
                .into_py_result()?;
            Ok((Pythonized(results), Pythonized(info)))
        })
    }
}

#[pyclass]
pub struct SetupStatusCheck(setup::AllSetupStatusCheck);

#[pymethods]
impl SetupStatusCheck {
    pub fn __str__(&self) -> String {
        format!("{}", &self.0)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn is_up_to_date(&self) -> bool {
        self.0.is_up_to_date()
    }
}

#[pyfunction]
fn check_setup_status(
    options: Pythonized<setup::CheckSetupStatusOptions>,
) -> PyResult<SetupStatusCheck> {
    let lib_context = get_lib_context()
        .ok_or_else(|| PyException::new_err("cocoindex library not initialized"))?;
    let all_css = lib_context.combined_setup_states.read().unwrap();
    let flows = lib_context.flows.read().unwrap();
    let setup_status =
        setup::check_setup_status(&flows, &all_css, options.into_inner()).into_py_result()?;
    Ok(SetupStatusCheck(setup_status))
}

#[pyfunction]
fn apply_setup_changes(py: Python<'_>, setup_status: &SetupStatusCheck) -> PyResult<()> {
    py.allow_threads(|| {
        let lib_context = get_lib_context()
            .ok_or_else(|| PyException::new_err("cocoindex library not initialized"))?;
        lib_context
            .runtime
            .block_on(async {
                setup::apply_changes(&mut std::io::stdout(), &setup_status.0, &lib_context.pool)
                    .await
            })
            .into_py_result()?;
        Ok(())
    })
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_engine")]
fn cocoindex_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init, m)?)?;
    m.add_function(wrap_pyfunction!(start_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop, m)?)?;
    m.add_function(wrap_pyfunction!(register_function_factory, m)?)?;
    m.add_function(wrap_pyfunction!(check_setup_status, m)?)?;
    m.add_function(wrap_pyfunction!(apply_setup_changes, m)?)?;

    m.add_class::<builder::flow_builder::FlowBuilder>()?;
    m.add_class::<builder::flow_builder::DataCollector>()?;
    m.add_class::<builder::flow_builder::DataSlice>()?;
    m.add_class::<builder::flow_builder::DataScopeRef>()?;
    m.add_class::<Flow>()?;
    m.add_class::<TransientFlow>()?;
    m.add_class::<IndexUpdateInfo>()?;
    m.add_class::<SimpleSemanticsQueryHandler>()?;
    m.add_class::<SetupStatusCheck>()?;
    m.add_class::<PyOpArgSchema>()?;

    Ok(())
}
