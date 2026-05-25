use numpy::{IntoPyArray, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass(name = "TurboQuantIdMapIndex")]
pub struct PyTurboQuantIdMapIndex {
    inner: turbovec::IdMapIndex,
}

#[pymethods]
impl PyTurboQuantIdMapIndex {
    #[new]
    #[pyo3(signature = (dim=None, bit_width=4))]
    fn new(dim: Option<usize>, bit_width: usize) -> Self {
        Self {
            inner: match dim {
                Some(dim) => turbovec::IdMapIndex::new(dim, bit_width),
                None => turbovec::IdMapIndex::new_lazy(bit_width),
            },
        }
    }

    fn add_with_ids(
        &mut self,
        vectors: PyReadonlyArray2<f32>,
        ids: PyReadonlyArray1<u64>,
    ) -> PyResult<()> {
        let vectors = vectors.as_array();
        let dim = vectors.ncols();
        let vector_slice = vectors
            .as_slice()
            .expect("vectors must be C-contiguous float32");
        let ids = ids.as_array();
        let id_slice = ids.as_slice().expect("ids must be contiguous uint64");

        self.inner
            .add_with_ids_2d(vector_slice, dim, id_slice)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
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
        let queries = queries.as_array();
        let num_queries = queries.nrows();
        let query_slice = queries
            .as_slice()
            .expect("queries must be C-contiguous float32");

        let allowlist_array = allowlist.as_ref().map(|allowlist| allowlist.as_array());
        let allowlist_storage: Option<Vec<u64>> = match allowlist_array.as_ref() {
            Some(allowlist) => {
                let slice = allowlist
                    .as_slice()
                    .expect("allowlist must be contiguous uint64");
                let filtered_ids: Vec<u64> = slice
                    .iter()
                    .copied()
                    .filter(|id| self.inner.contains(*id))
                    .collect();
                if filtered_ids.is_empty() {
                    let scores =
                        numpy::ndarray::Array2::from_shape_vec((num_queries, 0), Vec::<f32>::new())
                            .unwrap()
                            .into_pyarray(py);
                    let ids =
                        numpy::ndarray::Array2::from_shape_vec((num_queries, 0), Vec::<u64>::new())
                            .unwrap()
                            .into_pyarray(py);
                    return Ok((scores, ids));
                }
                Some(filtered_ids)
            }
            None => None,
        };
        let allowlist_slice = allowlist_storage.as_deref();

        let (scores, ids) = self
            .inner
            .search_with_allowlist(query_slice, k, allowlist_slice);
        let effective_k = if num_queries == 0 {
            k
        } else {
            scores.len() / num_queries
        };

        let scores = numpy::ndarray::Array2::from_shape_vec((num_queries, effective_k), scores)
            .unwrap()
            .into_pyarray(py);
        let ids = numpy::ndarray::Array2::from_shape_vec((num_queries, effective_k), ids)
            .unwrap()
            .into_pyarray(py);
        Ok((scores, ids))
    }

    fn contains(&self, id: u64) -> bool {
        self.inner.contains(id)
    }

    fn prepare(&self) {
        self.inner.prepare();
    }

    fn write(&self, path: &str) -> PyResult<()> {
        self.inner
            .write(path)
            .map_err(|err| pyo3::exceptions::PyIOError::new_err(err.to_string()))
    }

    #[classmethod]
    fn load(_cls: &Bound<PyType>, path: &str) -> PyResult<Self> {
        let inner = turbovec::IdMapIndex::load(path)
            .map_err(|err| pyo3::exceptions::PyIOError::new_err(err.to_string()))?;
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
