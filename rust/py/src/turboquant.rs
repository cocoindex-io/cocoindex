use std::fmt::Display;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use numpy::ndarray::{Array2, Dimension};
use numpy::{Element, IntoPyArray, PyArray2, PyReadonlyArray, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::prelude::*;
use pyo3::types::PyType;

fn py_value_err(e: impl Display) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

fn py_io_err(e: impl Display) -> PyErr {
    pyo3::exceptions::PyIOError::new_err(e.to_string())
}

fn py_poisoned_err() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(
        "TurboQuantIdMapIndex is unusable after a panic during an earlier call",
    )
}

// Copies the array so the GIL can be released without Python mutating the
// buffer while it is read.
fn contiguous_vec<T: Element + Copy, D: Dimension>(
    array: &PyReadonlyArray<'_, T, D>,
    name: &str,
) -> PyResult<Vec<T>> {
    array
        .as_slice()
        .map(<[T]>::to_vec)
        .map_err(|_| py_value_err(format!("{name} must be a C-contiguous array")))
}

fn to_pyarray_2d<T: Element>(
    py: Python<'_>,
    rows: usize,
    cols: usize,
    data: Vec<T>,
) -> PyResult<Bound<'_, PyArray2<T>>> {
    Ok(Array2::from_shape_vec((rows, cols), data)
        .map_err(py_value_err)?
        .into_pyarray(py))
}

/// Search, add, prepare, write, and load run with the GIL released. The lock
/// lets a loaded index be searched by several threads while another thread
/// updates it: readers share the index and writers wait instead of failing.
#[pyclass(name = "TurboQuantIdMapIndex", frozen)]
pub struct PyTurboQuantIdMapIndex {
    inner: RwLock<turbovec::IdMapIndex>,
}

impl PyTurboQuantIdMapIndex {
    fn from_inner(inner: turbovec::IdMapIndex) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    fn read(&self) -> PyResult<RwLockReadGuard<'_, turbovec::IdMapIndex>> {
        self.inner.read().map_err(|_| py_poisoned_err())
    }

    fn write_lock(&self) -> PyResult<RwLockWriteGuard<'_, turbovec::IdMapIndex>> {
        self.inner.write().map_err(|_| py_poisoned_err())
    }
}

#[pymethods]
impl PyTurboQuantIdMapIndex {
    #[new]
    #[pyo3(signature = (dim=None, bit_width=4))]
    fn new(dim: Option<usize>, bit_width: usize) -> PyResult<Self> {
        let inner = match dim {
            Some(dim) => turbovec::IdMapIndex::new(dim, bit_width),
            None => turbovec::IdMapIndex::new_lazy(bit_width),
        }
        .map_err(py_value_err)?;
        Ok(Self::from_inner(inner))
    }

    fn add_with_ids(
        &self,
        py: Python<'_>,
        vectors: PyReadonlyArray2<f32>,
        ids: PyReadonlyArray1<u64>,
    ) -> PyResult<()> {
        let dim = vectors.as_array().ncols();
        let vectors = contiguous_vec(&vectors, "vectors")?;
        let ids = contiguous_vec(&ids, "ids")?;
        py.detach(|| {
            self.write_lock()?
                .add_with_ids_2d(&vectors, dim, &ids)
                .map_err(py_value_err)
        })
    }

    fn remove(&self, id: u64) -> PyResult<bool> {
        Ok(self.write_lock()?.remove(id))
    }

    #[pyo3(signature = (queries, k, *, allowlist=None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        queries: PyReadonlyArray2<f32>,
        k: usize,
        allowlist: Option<PyReadonlyArray1<u64>>,
    ) -> PyResult<(Bound<'py, PyArray2<f32>>, Bound<'py, PyArray2<u64>>)> {
        let num_queries = queries.as_array().nrows();
        let queries = contiguous_vec(&queries, "queries")?;
        let allowlist = allowlist
            .as_ref()
            .map(|allowlist| contiguous_vec(allowlist, "allowlist"))
            .transpose()?;

        let results = py.detach(|| -> PyResult<Option<turbovec::IdSearchResults>> {
            let inner = self.read()?;
            // turbovec rejects an empty allowlist and ids absent from the index,
            // while callers pass allowlists precomputed from metadata that may
            // name ids without stored vectors.
            let allowlist: Option<Vec<u64>> =
                allowlist.map(|ids| ids.into_iter().filter(|id| inner.contains(*id)).collect());
            if allowlist.as_ref().is_some_and(Vec::is_empty) {
                return Ok(None);
            }
            inner
                .try_search_with_allowlist(&queries, k, allowlist.as_deref())
                .map(Some)
                .map_err(py_value_err)
        })?;

        let (cols, scores, ids) = match results {
            Some(results) => (results.k, results.scores, results.ids),
            None => (0, Vec::new(), Vec::new()),
        };
        Ok((
            to_pyarray_2d(py, num_queries, cols, scores)?,
            to_pyarray_2d(py, num_queries, cols, ids)?,
        ))
    }

    fn contains(&self, id: u64) -> PyResult<bool> {
        Ok(self.read()?.contains(id))
    }

    fn prepare(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.read()?.prepare();
            Ok(())
        })
    }

    fn write(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        py.detach(|| self.read()?.write(path).map_err(py_io_err))
    }

    #[classmethod]
    fn load(_cls: &Bound<PyType>, py: Python<'_>, path: &str) -> PyResult<Self> {
        let inner = py
            .detach(|| turbovec::IdMapIndex::load(path))
            .map_err(py_io_err)?;
        Ok(Self::from_inner(inner))
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.read()?.len())
    }

    fn __contains__(&self, id: u64) -> PyResult<bool> {
        self.contains(id)
    }

    #[getter]
    fn dim(&self) -> PyResult<Option<usize>> {
        Ok(self.read()?.dim_opt())
    }

    #[getter]
    fn bit_width(&self) -> PyResult<usize> {
        Ok(self.read()?.bit_width())
    }

    fn __repr__(&self) -> PyResult<String> {
        let inner = self.read()?;
        let dim = inner
            .dim_opt()
            .map_or_else(|| "None".to_string(), |dim| dim.to_string());
        Ok(format!(
            "cocoindex.TurboQuantIdMapIndex(dim={}, bit_width={}, n_vectors={})",
            dim,
            inner.bit_width(),
            inner.len()
        ))
    }
}
