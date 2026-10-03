use crate::fingerprint::PyFingerprint;
use crate::prelude::*;

use pyo3::exceptions::PyTypeError;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMapping, PySequence, PySet, PyString,
    PyTuple,
};
use std::collections::HashSet;
use utils::fingerprint::Fingerprinter;

fn write_none(fp: &mut Fingerprinter) {
    fp.write_type_tag("");
}

fn write_bool(fp: &mut Fingerprinter, v: bool) {
    fp.write_type_tag(if v { "t" } else { "f" });
}

fn write_int(fp: &mut Fingerprinter, obj: Borrowed<'_, '_, PyAny>) -> PyResult<()> {
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
    Ok(())
}

fn write_float(fp: &mut Fingerprinter, v: f64) {
    if v.is_nan() {
        fp.write_type_tag("nan");
    } else {
        fp.write_type_tag("f8");
        fp.write_raw_bytes(&v.to_le_bytes());
    }
}

fn write_str(fp: &mut Fingerprinter, s: &str) {
    fp.write_type_tag("s");
    fp.write_varlen_bytes(s.as_bytes());
}

fn write_bytes(fp: &mut Fingerprinter, b: &[u8]) {
    fp.write_type_tag("b");
    fp.write_varlen_bytes(b);
}

fn write_py_memo_key(fp: &mut Fingerprinter, obj: Borrowed<'_, '_, PyAny>) -> PyResult<()> {
    if obj.is_none() {
        write_none(fp);
        return Ok(());
    }

    if obj.is_instance_of::<PyBool>() {
        write_bool(fp, obj.extract::<bool>()?);
        return Ok(());
    }

    if obj.is_instance_of::<PyInt>() {
        return write_int(fp, obj);
    }

    if obj.is_instance_of::<PyFloat>() {
        write_float(fp, obj.extract::<f64>()?);
        return Ok(());
    }

    if obj.is_instance_of::<PyString>() {
        write_str(fp, obj.extract::<&str>()?);
        return Ok(());
    }

    if obj.is_instance_of::<PyBytes>() {
        write_bytes(fp, obj.extract::<&[u8]>()?);
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
    let mut fp = Fingerprinter::default();
    write_py_memo_key(&mut fp, obj.as_borrowed())?;
    let digest = fp.into_fingerprint();
    Ok(crate::fingerprint::PyFingerprint(digest))
}

/// Nesting depth past which the plain-data walker defers to the Python
/// canonicalizer, so that native recursion stays bounded.
const MAX_PLAIN_DEPTH: usize = 64;

/// Writes the elements of a plain `list` / `tuple` as the canonical `("seq", (...))`.
fn write_plain_seq<'py>(
    fp: &mut Fingerprinter,
    elements: impl Iterator<Item = Bound<'py, PyAny>>,
    seen: &mut HashSet<usize>,
    depth: usize,
) -> PyResult<bool> {
    fp.write_type_tag("T");
    write_str(fp, "seq");
    fp.write_type_tag("T");
    for element in elements {
        if !write_plain(fp, &element, seen, depth)? {
            return Ok(false);
        }
    }
    fp.write_end_tag();
    fp.write_end_tag();
    Ok(true)
}

/// Writes a plain `dict` with `str` keys as the canonical `("map", ((key, value), ...))`.
fn write_plain_map(
    fp: &mut Fingerprinter,
    dict: &Bound<'_, PyDict>,
    seen: &mut HashSet<usize>,
    depth: usize,
) -> PyResult<bool> {
    let mut items = Vec::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let Ok(key) = key.cast_into_exact::<PyString>() else {
            return Ok(false);
        };
        items.push((key, value));
    }
    let mut entries = items
        .iter()
        .map(|(key, value)| Ok((key.to_str()?, value)))
        .collect::<PyResult<Vec<_>>>()?;
    // The canonical item order is by key. UTF-8 byte order is code point order,
    // which is how Python orders the strings; keys are distinct, so values never
    // take part in the comparison.
    entries.sort_unstable_by_key(|(key, _)| *key);

    fp.write_type_tag("T");
    write_str(fp, "map");
    fp.write_type_tag("T");
    for (key, value) in entries {
        fp.write_type_tag("T");
        write_str(fp, key);
        if !write_plain(fp, value, seen, depth)? {
            return Ok(false);
        }
        fp.write_end_tag();
    }
    fp.write_end_tag();
    fp.write_end_tag();
    Ok(true)
}

/// Writes `obj` exactly as `write_py_memo_key` writes the Python canonical form
/// of `obj`, provided `obj` is plain data.
///
/// Returns `false`, leaving `fp` in an unspecified state, when it is not: a type
/// other than exactly `None` / `bool` / `int` / `float` / `str` / `bytes` /
/// `list` / `tuple` / `dict`, a `dict` key that is not a `str`, a container
/// reached twice (the canonical form encodes those as ordinal back-references),
/// or nesting deeper than `MAX_PLAIN_DEPTH`.
fn write_plain(
    fp: &mut Fingerprinter,
    obj: &Bound<'_, PyAny>,
    seen: &mut HashSet<usize>,
    depth: usize,
) -> PyResult<bool> {
    if let Ok(s) = obj.cast_exact::<PyString>() {
        write_str(fp, s.to_str()?);
        return Ok(true);
    }
    if obj.is_none() {
        write_none(fp);
        return Ok(true);
    }
    // `bool` cannot be subclassed, and is not exactly an `int`.
    if let Ok(b) = obj.cast::<PyBool>() {
        write_bool(fp, b.is_true());
        return Ok(true);
    }
    if obj.is_exact_instance_of::<PyInt>() {
        write_int(fp, obj.as_borrowed())?;
        return Ok(true);
    }
    if let Ok(f) = obj.cast_exact::<PyFloat>() {
        write_float(fp, f.value());
        return Ok(true);
    }
    if let Ok(b) = obj.cast_exact::<PyBytes>() {
        write_bytes(fp, b.as_bytes());
        return Ok(true);
    }

    // Containers: each may be entered once, and only down to `MAX_PLAIN_DEPTH`.
    let mut enter = || depth < MAX_PLAIN_DEPTH && seen.insert(obj.as_ptr() as usize);
    if let Ok(list) = obj.cast_exact::<PyList>() {
        return Ok(enter() && write_plain_seq(fp, list.iter(), seen, depth + 1)?);
    }
    if let Ok(tuple) = obj.cast_exact::<PyTuple>() {
        return Ok(enter() && write_plain_seq(fp, tuple.iter(), seen, depth + 1)?);
    }
    if let Ok(dict) = obj.cast_exact::<PyDict>() {
        return Ok(enter() && write_plain_map(fp, dict, seen, depth + 1)?);
    }
    Ok(false)
}

/// Fingerprints `obj` without going through the Python canonicalizer, when `obj`
/// is plain data (see `write_plain`). The result equals
/// `fingerprint_simple_object` of the Python canonical form of `obj`.
///
/// Returns `None` when `obj` is not plain data.
#[pyfunction]
pub fn fingerprint_plain_object(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<crate::fingerprint::PyFingerprint>> {
    let mut fp = Fingerprinter::default();
    let mut seen = HashSet::new();
    Ok(write_plain(&mut fp, obj, &mut seen, 0)?
        .then(|| crate::fingerprint::PyFingerprint(fp.into_fingerprint())))
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
