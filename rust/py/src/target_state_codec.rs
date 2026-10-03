//! How the Python profile holds declared target-state values and target
//! actions: encoded by `cocoindex._internal.target_state_codec` when it applies
//! (plain data), as the Python object otherwise.
//!
//! A value is held from its declaration until its component's pre-commit has
//! reconciled it; an action, from `reconcile()` until its sink has applied it.
//! Both are decoded on demand — the value for `reconcile()`, the action for
//! each sink call — into new objects equal to the ones declared and returned.

use pyo3::types::PyBytes;

use crate::{prelude::*, runtime::python_objects};

/// The codec functions of `cocoindex._internal.target_state_codec`.
pub struct TargetStateCodec {
    encode_value: Py<PyAny>,
    decode_value: Py<PyAny>,
    encode_action: Py<PyAny>,
    decode_action: Py<PyAny>,
}

impl TargetStateCodec {
    pub fn new(module: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            encode_value: module.getattr("encode_value")?.unbind(),
            decode_value: module.getattr("decode_value")?.unbind(),
            encode_action: module.getattr("encode_action")?.unbind(),
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
    Object(Py<PyAny>),
    /// The value's encoding, shared with the encoded actions that refer to it.
    Encoded(Arc<[u8]>),
}

impl PyTargetStateValue {
    /// Hold `value` encoded when the codec encodes it, as the object otherwise.
    pub fn new(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        let encoded = codec().encode_value.call1(py, (&value,))?;
        if encoded.is_none(py) {
            return Ok(Self::Object(value));
        }
        Ok(Self::Encoded(Arc::from(
            encoded.cast_bound::<PyBytes>(py)?.as_bytes(),
        )))
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

/// The value `reconcile()` is called with, and the encoding it was decoded from
/// when held encoded.
pub struct DesiredForReconcile {
    pub object: Py<PyAny>,
    encoding: Option<Arc<[u8]>>,
}

impl DesiredForReconcile {
    /// `value` as an object: a new decoding when held encoded.
    pub fn new(py: Python<'_>, value: &PyTargetStateValue) -> PyResult<Self> {
        match value {
            PyTargetStateValue::Object(object) => Ok(Self {
                object: object.clone_ref(py),
                encoding: None,
            }),
            PyTargetStateValue::Encoded(encoding) => Ok(Self {
                object: codec()
                    .decode_value
                    .call1(py, (PyBytes::new(py, encoding),))?,
                encoding: Some(encoding.clone()),
            }),
        }
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
