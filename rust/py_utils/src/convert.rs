use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};
use serde::{Serialize, de::DeserializeOwned};
use std::ops::Deref;

use crate::error::IntoPyResult;

#[derive(Debug)]
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
