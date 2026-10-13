use std::fmt::Display;

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

fn contiguous_slice<'a, T: Element, D: Dimension>(
    array: &'a PyReadonlyArray<'_, T, D>,
    name: &str,
) -> PyResult<&'a [T]> {
    array
        .as_slice()
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

#[pyclass(name = "TurboQuantIdMapIndex")]
pub struct PyTurboQuantIdMapIndex {
    inner: turbovec::IdMapIndex,
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
        Ok(Self { inner })
    }

    fn add_with_ids(
        &mut self,
        vectors: PyReadonlyArray2<f32>,
        ids: PyReadonlyArray1<u64>,
    ) -> PyResult<()> {
        let dim = vectors.as_array().ncols();
        self.inner
            .add_with_ids_2d(
                contiguous_slice(&vectors, "vectors")?,
                dim,
                contiguous_slice(&ids, "ids")?,
            )
            .map_err(py_value_err)
    }

    fn remove(&mut self, id: u64) -> bool {
        self.inner.remove(id)
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
        let query_slice = contiguous_slice(&queries, "queries")?;

        // turbovec rejects an empty allowlist and ids absent from the index, while
        // callers pass allowlists precomputed from metadata that may name ids
        // without stored vectors.
        let allowlist_ids = match &allowlist {
            Some(allowlist) => {
                let ids: Vec<u64> = contiguous_slice(allowlist, "allowlist")?
                    .iter()
                    .copied()
                    .filter(|id| self.inner.contains(*id))
                    .collect();
                if ids.is_empty() {
                    return Ok((
                        to_pyarray_2d(py, num_queries, 0, Vec::new())?,
                        to_pyarray_2d(py, num_queries, 0, Vec::new())?,
                    ));
                }
                Some(ids)
            }
            None => None,
        };

        let results = self
            .inner
            .try_search_with_allowlist(query_slice, k, allowlist_ids.as_deref())
            .map_err(py_value_err)?;
        Ok((
            to_pyarray_2d(py, num_queries, results.k, results.scores)?,
            to_pyarray_2d(py, num_queries, results.k, results.ids)?,
        ))
    }

    fn contains(&self, id: u64) -> bool {
        self.inner.contains(id)
    }

    fn prepare(&self) {
        self.inner.prepare();
    }

    fn write(&self, path: &str) -> PyResult<()> {
        self.inner.write(path).map_err(py_io_err)
    }

    #[classmethod]
    fn load(_cls: &Bound<PyType>, path: &str) -> PyResult<Self> {
        let inner = turbovec::IdMapIndex::load(path).map_err(py_io_err)?;
        Ok(Self { inner })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, id: u64) -> bool {
        self.inner.contains(id)
    }

    #[getter]
    fn dim(&self) -> Option<usize> {
        self.inner.dim_opt()
    }

    #[getter]
    fn bit_width(&self) -> usize {
        self.inner.bit_width()
    }

    fn __repr__(&self) -> String {
        let dim = self
            .inner
            .dim_opt()
            .map_or_else(|| "None".to_string(), |dim| dim.to_string());
        format!(
            "cocoindex.TurboQuantIdMapIndex(dim={}, bit_width={}, n_vectors={})",
            dim,
            self.inner.bit_width(),
            self.inner.len()
        )
    }
}
