use pyo3::{BoundObject, prelude::*};
use pythonize::{depythonize, pythonize};
use serde::{Serialize, de::DeserializeOwned};
use std::ops::Deref;

#[derive(Debug)]
pub struct Pythonized<T>(pub T);

impl<'py, T: DeserializeOwned> FromPyObject<'_, '_> for Pythonized<T> {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let bound = obj.into_bound();
        Ok(Pythonized(depythonize(&bound)?))
    }
}

impl<'py, T: Serialize> IntoPyObject<'py> for &Pythonized<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        Ok(pythonize(py, &self.0)?)
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
