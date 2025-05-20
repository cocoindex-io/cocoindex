use bytes::Bytes;
use pyo3::IntoPyObjectExt;
use pyo3::types::{PyList, PyTuple};
use pyo3::{exceptions::PyException, prelude::*};
use pythonize::{depythonize, pythonize};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use super::IntoPyResult;
use crate::base::{schema, value};

pub struct Pythonized<T>(pub T);

impl<'py, T: DeserializeOwned> FromPyObject<'py> for Pythonized<T> {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Pythonized(depythonize(obj).into_py_result()?))
    }
}

impl<'py, T: Serialize> IntoPyObject<'py> for &Pythonized<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        pythonize(py, &self.0).into_py_result()
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

fn basic_value_to_py_object<'py>(
    py: Python<'py>,
    v: &value::BasicValue,
) -> PyResult<Bound<'py, PyAny>> {
    let result = match v {
        value::BasicValue::Bytes(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Str(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Bool(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Int64(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Float32(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Float64(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Range(v) => pythonize(py, v).into_py_result()?,
        value::BasicValue::Uuid(v) => v.as_bytes().into_bound_py_any(py)?,
        value::BasicValue::Date(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Time(v) => v.into_bound_py_any(py)?,
        value::BasicValue::LocalDateTime(v) => v.into_bound_py_any(py)?,
        value::BasicValue::OffsetDateTime(v) => v.into_bound_py_any(py)?,
        value::BasicValue::TimeDelta(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Json(v) => pythonize(py, v).into_py_result()?,
        value::BasicValue::Vector(v) => v
            .iter()
            .map(|v| basic_value_to_py_object(py, v))
            .collect::<PyResult<Vec<_>>>()?
            .into_bound_py_any(py)?,
    };
    Ok(result)
}

fn field_values_to_py_object<'py, 'a>(
    py: Python<'py>,
    values: impl Iterator<Item = &'a value::Value>,
) -> PyResult<Bound<'py, PyAny>> {
    let fields = values
        .map(|v| value_to_py_object(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, fields)?.into_any())
}

pub fn value_to_py_object<'py>(py: Python<'py>, v: &value::Value) -> PyResult<Bound<'py, PyAny>> {
    let result = match v {
        value::Value::Null => py.None().into_bound(py),
        value::Value::Basic(v) => basic_value_to_py_object(py, v)?,
        value::Value::Struct(v) => field_values_to_py_object(py, v.fields.iter())?,
        value::Value::UTable(v) | value::Value::LTable(v) => {
            let rows = v
                .iter()
                .map(|v| field_values_to_py_object(py, v.0.fields.iter()))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, rows)?.into_any()
        }
        value::Value::KTable(v) => {
            let rows = v
                .iter()
                .map(|(k, v)| {
                    field_values_to_py_object(
                        py,
                        std::iter::once(&value::Value::from(k.clone())).chain(v.0.fields.iter()),
                    )
                })
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, rows)?.into_any()
        }
    };
    Ok(result)
}

fn basic_value_from_py_object<'py>(
    typ: &schema::BasicValueType,
    v: &Bound<'py, PyAny>,
) -> PyResult<value::BasicValue> {
    let result = match typ {
        schema::BasicValueType::Bytes => {
            value::BasicValue::Bytes(Bytes::from(v.extract::<Vec<u8>>()?))
        }
        schema::BasicValueType::Str => value::BasicValue::Str(Arc::from(v.extract::<String>()?)),
        schema::BasicValueType::Bool => value::BasicValue::Bool(v.extract::<bool>()?),
        schema::BasicValueType::Int64 => value::BasicValue::Int64(v.extract::<i64>()?),
        schema::BasicValueType::Float32 => value::BasicValue::Float32(v.extract::<f32>()?),
        schema::BasicValueType::Float64 => value::BasicValue::Float64(v.extract::<f64>()?),
        schema::BasicValueType::Range => value::BasicValue::Range(depythonize(v)?),
        schema::BasicValueType::Uuid => {
            value::BasicValue::Uuid(uuid::Uuid::from_bytes(v.extract::<uuid::Bytes>()?))
        }
        schema::BasicValueType::Date => value::BasicValue::Date(v.extract::<chrono::NaiveDate>()?),
        schema::BasicValueType::Time => value::BasicValue::Time(v.extract::<chrono::NaiveTime>()?),
        schema::BasicValueType::LocalDateTime => {
            value::BasicValue::LocalDateTime(v.extract::<chrono::NaiveDateTime>()?)
        }
        schema::BasicValueType::OffsetDateTime => {
            value::BasicValue::OffsetDateTime(v.extract::<chrono::DateTime<chrono::FixedOffset>>()?)
        }
        schema::BasicValueType::TimeDelta => {
            value::BasicValue::TimeDelta(v.extract::<chrono::TimeDelta>()?)
        }
        schema::BasicValueType::Json => {
            value::BasicValue::Json(Arc::from(depythonize::<serde_json::Value>(v)?))
        }
        schema::BasicValueType::Vector(elem) => value::BasicValue::Vector(Arc::from(
            v.extract::<Vec<Bound<'py, PyAny>>>()?
                .into_iter()
                .map(|v| basic_value_from_py_object(&elem.element_type, &v))
                .collect::<PyResult<Vec<_>>>()?,
        )),
    };
    Ok(result)
}

fn field_values_from_py_object<'py>(
    schema: &schema::StructSchema,
    v: &Bound<'py, PyAny>,
) -> PyResult<value::FieldValues> {
    let list = v.extract::<Vec<Bound<'py, PyAny>>>()?;
    if list.len() != schema.fields.len() {
        return Err(PyException::new_err(format!(
            "struct field number mismatch, expected {}, got {}",
            schema.fields.len(),
            list.len()
        )));
    }
    Ok(value::FieldValues {
        fields: schema
            .fields
            .iter()
            .zip(list.into_iter())
            .map(|(f, v)| value_from_py_object(&f.value_type.typ, &v))
            .collect::<PyResult<Vec<_>>>()?,
    })
}

pub fn value_from_py_object<'py>(
    typ: &schema::ValueType,
    v: &Bound<'py, PyAny>,
) -> PyResult<value::Value> {
    let result = if v.is_none() {
        value::Value::Null
    } else {
        match typ {
            schema::ValueType::Basic(typ) => {
                value::Value::Basic(basic_value_from_py_object(typ, v)?)
            }
            schema::ValueType::Struct(schema) => {
                value::Value::Struct(field_values_from_py_object(schema, v)?)
            }
            schema::ValueType::Table(schema) => {
                let list = v.extract::<Vec<Bound<'py, PyAny>>>()?;
                let values = list
                    .into_iter()
                    .map(|v| field_values_from_py_object(&schema.row, &v))
                    .collect::<PyResult<Vec<_>>>()?;
                match schema.kind {
                    schema::TableKind::UTable => {
                        value::Value::UTable(values.into_iter().map(|v| v.into()).collect())
                    }
                    schema::TableKind::LTable => {
                        value::Value::LTable(values.into_iter().map(|v| v.into()).collect())
                    }
                    schema::TableKind::KTable => value::Value::KTable(
                        values
                            .into_iter()
                            .map(|v| {
                                let mut iter = v.fields.into_iter();
                                let key = iter.next().unwrap().into_key().into_py_result()?;
                                Ok((
                                    key,
                                    value::ScopeValue(value::FieldValues {
                                        fields: iter.collect::<Vec<_>>(),
                                    }),
                                ))
                            })
                            .collect::<PyResult<BTreeMap<_, _>>>()?,
                    ),
                }
            }
        }
    };
    Ok(result)
}
