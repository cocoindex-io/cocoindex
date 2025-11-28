use pyo3::exceptions::PyRuntimeError;

use crate::prelude::*;

#[derive(Clone)]
pub struct PyKey {
    value: Arc<Py<PyAny>>,
    hash: isize,
}

impl PartialEq for PyKey {
    fn eq(&self, other: &Self) -> bool {
        if self.value.is(other.value.as_ref()) {
            return true;
        }
        Python::attach(|py| self.value.bind(py).eq(other.value.bind(py)))
            .expect("failed to compare keys")
    }
}

impl Eq for PyKey {}

impl std::hash::Hash for PyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PyKey {
    pub fn new(py: Python<'_>, value: Arc<Py<PyAny>>) -> PyResult<Self> {
        let hash = value.bind(py).hash().map_err(|e| {
            let py_err = PyErr::new::<PyRuntimeError, _>(format!("key must be hashable"));
            py_err.set_cause(py, Some(e));
            py_err
        })?;
        Ok(Self { value, hash })
    }

    pub fn value(&self) -> &Arc<Py<PyAny>> {
        &self.value
    }
    pub fn hash(&self) -> isize {
        self.hash
    }
}
