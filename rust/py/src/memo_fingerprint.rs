use crate::fingerprint::PyFingerprint;
use crate::prelude::*;

use pyo3::exceptions::PyTypeError;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyMapping, PySequence, PySet, PyString,
};

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

    if obj.is_instance_of::<PySequence>() {
        // The Python canonicalizer should only produce tuples for nested structure,
        // but accept list as well for robustness.
        fp.write_type_tag("T");
        for item in obj.try_iter()? {
            write_py_memo_key(fp, item?.as_borrowed())?;
        }
        fp.write_end_tag();
        return Ok(());
    }

    if obj.is_instance_of::<PyMapping>() {
        fp.write_type_tag("M");
        if obj.is_instance_of::<PyDict>() {
            // Special optimization for dicts.
            let mapping: Bound<'_, PyDict> = obj.extract()?;
            for (key, value) in mapping.iter() {
                write_py_memo_key(fp, key.as_borrowed())?;
                write_py_memo_key(fp, value.as_borrowed())?;
            }
        } else {
            let mapping: Bound<'_, PyMapping> = obj.extract()?;
            let items = mapping.items()?;
            for kv in items {
                let (key, value) = kv.extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
                write_py_memo_key(fp, key.as_borrowed())?;
                write_py_memo_key(fp, value.as_borrowed())?;
            }
        }
        fp.write_end_tag();
        return Ok(());
    }

    if obj.is_instance_of::<PySet>() {
        fp.write_type_tag("S");
        let set: Bound<'_, PySet> = obj.extract()?;
        for item in set.iter() {
            write_py_memo_key(fp, item.as_borrowed())?;
        }
        fp.write_end_tag();
        return Ok(());
    }

    if obj.is_instance_of::<PyFingerprint>() {
        let f = obj.extract::<PyFingerprint>()?;
        fp.write_type_tag("fp");
        fp.write_raw_bytes(f.0.as_slice());
        return Ok(());
    }

    if let Ok(uuid_value) = obj.extract::<uuid::Uuid>() {
        fp.write_type_tag("uuid");
        fp.write_raw_bytes(uuid_value.as_bytes());
        return Ok(());
    }

    Err(PyTypeError::new_err(
        "Unsupported type for memoization fingerprint.",
    ))
}

#[pyfunction]
pub fn fingerprint_simple_object<'py>(
    obj: Bound<'py, PyAny>,
) -> PyResult<crate::fingerprint::PyFingerprint> {
    let mut fp = utils::fingerprint::Fingerprinter::default();
    write_py_memo_key(&mut fp, obj.as_borrowed())?;
    let digest = fp.into_fingerprint();
    Ok(crate::fingerprint::PyFingerprint(digest))
}

#[pyfunction]
pub fn fingerprint_bytes<'py>(data: &Bound<'py, PyBytes>) -> crate::fingerprint::PyFingerprint {
    let digest = utils::fingerprint::Fingerprint::from_bytes(data.as_bytes());
    crate::fingerprint::PyFingerprint(digest)
}

#[pyfunction]
pub fn fingerprint_str<'py>(
    s: &Bound<'py, PyString>,
) -> PyResult<crate::fingerprint::PyFingerprint> {
    let digest = utils::fingerprint::Fingerprint::from_bytes(s.to_str()?.as_bytes());
    Ok(crate::fingerprint::PyFingerprint(digest))
}
