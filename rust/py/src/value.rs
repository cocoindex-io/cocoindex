use cocoindex_core::engine::profile::Persist;
use pyo3::types::PyBytes;

use crate::{prelude::*, runtime::python_objects};

struct PyStoredValueData {
    bytes: Option<bytes::Bytes>,
    object: Option<Py<PyAny>>,
}

// Invariant: at least one of bytes/object is Some.

#[pyclass(frozen, name = "StoredValue")]
#[derive(Clone)]
pub struct PyStoredValue {
    inner: Arc<std::sync::Mutex<PyStoredValueData>>,
}

impl PyStoredValue {
    pub fn new(data: Py<PyAny>) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(PyStoredValueData {
                bytes: None,
                object: Some(data),
            })),
        }
    }
}

#[pymethods]
impl PyStoredValue {
    /// Get the deserialized Python object, lazily deserializing on first access.
    ///
    /// `deserialize_fn` is a `Callable[[bytes], T]` that converts raw bytes to the
    /// desired Python object. It is only called on the first access when the value
    /// has not yet been deserialized (bytes-only). On subsequent calls or when the
    /// value was created from a Python object, returns the cached object immediately.
    fn get(&self, py: Python<'_>, deserialize_fn: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // 1st acquisition: check cache or extract bytes
        let py_bytes = {
            let data = self.inner.lock().unwrap();
            if let Some(ref obj) = data.object {
                return Ok(obj.clone_ref(py)); // cache hit — fast path
            }
            PyBytes::new(py, data.bytes.as_ref().unwrap()).unbind()
        }; // mutex released before calling Python

        // Deserialize (no mutex held — safe even if deserialize_fn releases GIL)
        let result = deserialize_fn.call1((py_bytes.bind(py),))?;

        // 2nd acquisition: cache result
        {
            let mut data = self.inner.lock().unwrap();
            data.object = Some(result.clone().unbind());
        }
        Ok(result.unbind())
    }
}

impl std::fmt::Debug for PyStoredValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.inner.lock().unwrap();
        if let Some(ref obj) = data.object {
            debug_py_any(f, obj)
        } else if data.bytes.is_some() {
            f.write_str("<PyStoredValue: bytes-only>")
        } else {
            f.write_str("<PyStoredValue: empty>")
        }
    }
}

impl Persist for PyStoredValue {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        // Fast path: return cached bytes (mutex only, no GIL)
        {
            let data = self.inner.lock().unwrap();
            if let Some(ref b) = data.bytes {
                return Ok(b.clone());
            }
        } // mutex released before acquiring GIL

        // Slow path: acquire GIL, extract object, release mutex, then serialize
        Python::attach(|py| {
            // 1st acquisition: double-check and extract object reference
            let obj = {
                let data = self.inner.lock().unwrap();
                if let Some(ref b) = data.bytes {
                    return Ok(b.clone());
                }
                data.object.as_ref().unwrap().clone_ref(py)
            }; // mutex released before calling Python

            // Serialize (no mutex held — safe if serialize releases GIL)
            let bytes = python_objects().serialize(py, &obj.bind(py))?;

            // 2nd acquisition: cache result
            {
                let mut data = self.inner.lock().unwrap();
                if data.bytes.is_none() {
                    data.bytes = Some(bytes.clone());
                }
            }
            Ok(bytes)
        })
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(std::sync::Mutex::new(PyStoredValueData {
                bytes: Some(bytes::Bytes::copy_from_slice(data)),
                object: None,
            })),
        })
    }
}

fn debug_py_any(f: &mut std::fmt::Formatter<'_>, value: &Py<PyAny>) -> std::fmt::Result {
    Python::attach(|py| {
        let value = value.bind(py);
        match value.repr() {
            Ok(repr) => match repr.to_str() {
                Ok(repr_str) => f.write_str(repr_str),
                Err(err) => {
                    error!("Error getting repr: {:?}", err);
                    f.write_str("<error getting repr>")
                }
            },
            Err(err) => {
                error!("Error getting repr: {:?}", err);
                f.write_str("<error getting repr>")
            }
        }
    })
}
