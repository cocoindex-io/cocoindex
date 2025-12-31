use crate::prelude::*;

use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};

fn write_py_memo_key(
    fp: &mut utils::fingerprint::Fingerprinter,
    obj: Borrowed<'_, '_, PyAny>,
) -> PyResult<()> {
    if obj.is_none() {
        fp.write_type_tag("");
        return Ok(());
    }

    if obj.is_instance_of::<PyBool>() {
        let v = obj.extract::<bool>()?;
        fp.write_type_tag(if v { "t" } else { "f" });
        return Ok(());
    }

    if obj.is_instance_of::<PyInt>() {
        // Fast-path: if it fits in i64, encode identically to the Serde serializer ("i8").
        if let Ok(v) = obj.extract::<i64>() {
            fp.write_type_tag("i8");
            fp.write_raw_bytes(&v.to_le_bytes());
            return Ok(());
        }

        // Slow-path: Python ints are unbounded; encode sign + little-endian magnitude bytes.
        // This is deterministic and avoids truncation.
        //
        // Note: this calls a couple of Python methods, but only for huge ints.
        let is_neg = obj.call_method1("__lt__", (0,))?.extract::<bool>()?;
        let abs_obj = obj.call_method0("__abs__")?;
        let nbits = abs_obj.call_method0("bit_length")?.extract::<usize>()?;
        let nbytes = std::cmp::max(1, (nbits + 7) / 8);
        let mag: Bound<'_, PyBytes> = abs_obj
            .call_method1("to_bytes", (nbytes, "little"))?
            .extract()?;

        fp.write_type_tag("pyi");
        fp.write_raw_bytes(&[if is_neg { 1u8 } else { 0u8 }]);
        fp.write_varlen_bytes(mag.as_bytes());
        return Ok(());
    }

    if obj.is_instance_of::<PyFloat>() {
        let v = obj.extract::<f64>()?;
        if v.is_nan() {
            fp.write_type_tag("nan");
        } else {
            fp.write_type_tag("f8");
            fp.write_raw_bytes(&v.to_le_bytes());
        }
        return Ok(());
    }

    if obj.is_instance_of::<PyString>() {
        let s = obj.extract::<&str>()?;
        fp.write_type_tag("s");
        fp.write_varlen_bytes(s.as_bytes());
        return Ok(());
    }

    if obj.is_instance_of::<PyBytes>() {
        let b = obj.extract::<&[u8]>()?;
        fp.write_type_tag("b");
        fp.write_varlen_bytes(b);
        return Ok(());
    }

    if obj.is_instance_of::<PyTuple>() || obj.is_instance_of::<PyList>() {
        // The Python canonicalizer should only produce tuples for nested structure,
        // but accept list as well for robustness.
        fp.write_type_tag("T");
        for item in obj.try_iter()? {
            write_py_memo_key(fp, item?.as_borrowed())?;
        }
        fp.write_end_tag();
        return Ok(());
    }

    Err(PyTypeError::new_err(
        "Unsupported type for memoization fingerprint. Expected a tree of None/bool/int/float/str/bytes/tuple.",
    ))
}

#[pyfunction]
pub fn fingerprint_memo_key<'py>(
    _py: Python<'py>,
    obj: Bound<'py, PyAny>,
) -> PyResult<crate::fingerprint::PyFingerprint> {
    let mut fp = utils::fingerprint::Fingerprinter::default();
    write_py_memo_key(&mut fp, obj.as_borrowed())?;
    let digest = fp.into_fingerprint();
    Ok(crate::fingerprint::PyFingerprint(digest))
}
