//! How the Python profile holds declared target-state values and target
//! actions: encoded by `cocoindex._internal.target_state_codec` when it applies
//! (plain data of a reasonable size), as the Python object otherwise.
//!
//! A value is held from its declaration until its component's pre-commit has
//! reconciled it; an action, from `reconcile()` until its sink has applied it.
//! Both are decoded on demand — the value for `reconcile()`, the action for
//! each sink call — into new objects equal to the ones declared and returned.
//!
//! Past what a component keeps in memory, the engine spills values and actions
//! to disk (`cocoindex_core::engine::spill`). What is held encoded spills as its
//! encoding; an object held as is spills as the codec's encoding of it when it
//! is plain data, whatever its size, and stays in memory otherwise. A spilled
//! value or action is held encoded once read back.

use std::borrow::Cow;

use cocoindex_core::engine::spill::Spillable;
use pyo3::types::{PyBytes, PyString};

use crate::{prelude::*, runtime::python_objects};

/// The codec functions of `cocoindex._internal.target_state_codec`.
pub struct TargetStateCodec {
    encode_value: Py<PyAny>,
    encode_value_for_spill: Py<PyAny>,
    decode_value: Py<PyAny>,
    encode_action: Py<PyAny>,
    encode_action_for_spill: Py<PyAny>,
    decode_action: Py<PyAny>,
}

impl TargetStateCodec {
    pub fn new(module: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            encode_value: module.getattr("encode_value")?.unbind(),
            encode_value_for_spill: module.getattr("encode_value_for_spill")?.unbind(),
            decode_value: module.getattr("decode_value")?.unbind(),
            encode_action: module.getattr("encode_action")?.unbind(),
            encode_action_for_spill: module.getattr("encode_action_for_spill")?.unbind(),
            decode_action: module.getattr("decode_action")?.unbind(),
        })
    }
}

fn codec() -> &'static TargetStateCodec {
    &python_objects().target_state_codec
}

/// A declared target state's value.
pub enum PyTargetStateValue {
    /// The declared object: a value the codec leaves as is.
    Object {
        object: Py<PyAny>,
        /// What to account the object for when deciding to spill: a lower
        /// bound of its encoding's size when the codec could encode it but
        /// the engine doesn't hold it so, a scalar's payload size, 0 for an
        /// object that cannot be spilled anyway.
        size: usize,
    },
    /// The value's encoding, shared with the encoded actions that refer to it.
    Encoded(Arc<[u8]>),
}

impl PyTargetStateValue {
    /// Hold `value` encoded when the codec encodes it, as the object otherwise.
    pub fn new(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        let encoded = codec().encode_value.call1(py, (&value,))?;
        let encoded = encoded.bind(py);
        if let Ok(encoding) = encoded.cast::<PyBytes>() {
            return Ok(Self::Encoded(Arc::from(encoding.as_bytes())));
        }
        let size = if encoded.is_none() {
            let bound = value.bind(py);
            if let Ok(bytes) = bound.cast::<PyBytes>() {
                bytes.len()?
            } else if let Ok(string) = bound.cast::<PyString>() {
                string.len()?
            } else {
                0
            }
        } else {
            encoded.extract::<usize>()?
        };
        Ok(Self::Object {
            object: value,
            size,
        })
    }

    /// Hold `value` as the object itself, as a container's value (its spec) is:
    /// there are few of them, and their actions go on to fulfill child slots.
    pub fn object(value: Py<PyAny>) -> Self {
        Self::Object {
            object: value,
            size: 0,
        }
    }

    /// The value as an object: a new decoding each time when held encoded.
    pub fn to_object(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Object { object, .. } => Ok(object.clone_ref(py)),
            Self::Encoded(encoding) => codec()
                .decode_value
                .call1(py, (PyBytes::new(py, encoding),)),
        }
    }
}

impl Spillable for PyTargetStateValue {
    fn resident_size(&self) -> usize {
        match self {
            Self::Object { size, .. } => *size,
            Self::Encoded(encoding) => encoding.len(),
        }
    }

    fn to_spill_bytes(&self) -> Result<Option<Cow<'_, [u8]>>> {
        match self {
            Self::Encoded(encoding) => Ok(Some(Cow::Borrowed(encoding))),
            Self::Object { object, .. } => Python::attach(|py| -> PyResult<_> {
                let encoded = codec().encode_value_for_spill.call1(py, (object,))?;
                if encoded.is_none(py) {
                    return Ok(None);
                }
                Ok(Some(Cow::Owned(
                    encoded.cast_bound::<PyBytes>(py)?.as_bytes().to_vec(),
                )))
            })
            .from_py_result(),
        }
    }

    fn from_spill_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::Encoded(Arc::from(bytes)))
    }
}

/// A target action `reconcile()` returned.
pub enum PyTargetAction {
    Object(Py<PyAny>),
    Encoded {
        action: Box<[u8]>,
        /// The encoding of the declared value the action refers to, if it does.
        value: Option<Arc<[u8]>>,
    },
}

impl PyTargetAction {
    /// The action as an object for a sink call: a new decoding each time when
    /// held encoded.
    pub fn to_object<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Object(action) => Ok(action.bind(py).clone()),
            Self::Encoded { action, value } => codec().decode_action.bind(py).call1((
                PyBytes::new(py, action),
                value.as_deref().map(|value| PyBytes::new(py, value)),
            )),
        }
    }
}

/// An encoded action spills as a flag byte (whether a value encoding follows),
/// the action encoding's length as 4 little-endian bytes, the action encoding,
/// then the value encoding it refers to, if any.
fn spilled_action_bytes(action: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + action.len() + value.map_or(0, <[u8]>::len));
    bytes.push(value.is_some() as u8);
    bytes.extend_from_slice(&(action.len() as u32).to_le_bytes());
    bytes.extend_from_slice(action);
    if let Some(value) = value {
        bytes.extend_from_slice(value);
    }
    bytes
}

impl Spillable for PyTargetAction {
    fn resident_size(&self) -> usize {
        match self {
            Self::Object(_) => 0,
            Self::Encoded { action, value } => {
                action.len() + value.as_ref().map_or(0, |value| value.len())
            }
        }
    }

    fn to_spill_bytes(&self) -> Result<Option<Cow<'_, [u8]>>> {
        match self {
            Self::Encoded { action, value } => Ok(Some(Cow::Owned(spilled_action_bytes(
                action,
                value.as_deref(),
            )))),
            Self::Object(action) => Python::attach(|py| -> PyResult<_> {
                let encoded = codec().encode_action_for_spill.call1(py, (action,))?;
                if encoded.is_none(py) {
                    return Ok(None);
                }
                Ok(Some(Cow::Owned(spilled_action_bytes(
                    encoded.cast_bound::<PyBytes>(py)?.as_bytes(),
                    None,
                ))))
            })
            .from_py_result(),
        }
    }

    fn from_spill_bytes(bytes: &[u8]) -> Result<Self> {
        let malformed = || internal_error!("malformed spilled target action");
        let (&has_value, rest) = bytes.split_first().ok_or_else(malformed)?;
        let (len, rest) = rest.split_at_checked(4).ok_or_else(malformed)?;
        let len = u32::from_le_bytes(len.try_into().expect("4 bytes")) as usize;
        let (action, value) = rest.split_at_checked(len).ok_or_else(malformed)?;
        Ok(Self::Encoded {
            action: Box::from(action),
            value: (has_value != 0).then(|| Arc::from(value)),
        })
    }
}

/// The value `reconcile()` is called with, and the encoding it was decoded from
/// when held encoded.
pub struct DesiredForReconcile {
    pub object: Py<PyAny>,
    encoding: Option<Arc<[u8]>>,
}

impl DesiredForReconcile {
    /// `value` as an object: a new decoding when held encoded.
    pub fn new(py: Python<'_>, value: &PyTargetStateValue) -> PyResult<Self> {
        Ok(Self {
            object: value.to_object(py)?,
            encoding: match value {
                PyTargetStateValue::Object { .. } => None,
                PyTargetStateValue::Encoded(encoding) => Some(encoding.clone()),
            },
        })
    }

    pub fn non_existence(py: Python<'_>) -> Self {
        Self {
            object: python_objects().non_existence.clone_ref(py),
            encoding: None,
        }
    }

    /// Hold the action `reconcile()` returned for this value.
    ///
    /// For a value held encoded, the action is encoded too, referring to the
    /// value's encoding instead of containing the decoded value, so nothing of
    /// the decoding outlives the call. An action the codec leaves as is keeps
    /// whatever it holds of the decoding, beside the value's encoding until
    /// pre-commit releases the declared values.
    pub fn hold_action(self, py: Python<'_>, action: Py<PyAny>) -> PyResult<PyTargetAction> {
        let Some(encoding) = self.encoding else {
            return Ok(PyTargetAction::Object(action));
        };
        let encoded = codec().encode_action.call1(py, (&action, &self.object))?;
        if encoded.is_none(py) {
            return Ok(PyTargetAction::Object(action));
        }
        let (data, refers_to_value) = encoded.extract::<(Bound<'_, PyBytes>, bool)>(py)?;
        Ok(PyTargetAction::Encoded {
            action: Box::from(data.as_bytes()),
            value: refers_to_value.then_some(encoding),
        })
    }
}
