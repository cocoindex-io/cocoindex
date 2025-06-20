use bytes::Bytes;
use numpy::{PyArray1, PyArrayDyn, PyArrayMethods};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::types::PyAny;
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

#[derive(Debug)]
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
        value::BasicValue::Vector(v) => handle_vector_to_py(py, v)?,
        value::BasicValue::UnionVariant { tag_id, value } => {
            (*tag_id, basic_value_to_py_object(py, &value)?).into_bound_py_any(py)?
        }
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
        schema::BasicValueType::Vector(elem) => {
            if let Some(vector) = handle_ndarray_from_py(&elem.element_type, v)? {
                vector
            } else {
                // Fallback to list
                value::BasicValue::Vector(Arc::from(
                    v.extract::<Vec<Bound<'py, PyAny>>>()?
                        .into_iter()
                        .map(|v| basic_value_from_py_object(&elem.element_type, &v))
                        .collect::<PyResult<Vec<_>>>()?,
                ))
            }
        }
        schema::BasicValueType::Union(s) => {
            let mut valid_value = None;

            // Try parsing the value
            for (i, typ) in s.types.iter().enumerate() {
                if let Ok(value) = basic_value_from_py_object(typ, v) {
                    valid_value = Some(value::BasicValue::UnionVariant {
                        tag_id: i,
                        value: Box::new(value),
                    });
                    break;
                }
            }

            valid_value.ok_or_else(|| {
                PyErr::new::<PyTypeError, _>(format!(
                    "invalid union value: {}, available types: {:?}",
                    v, s.types
                ))
            })?
        }
    };
    Ok(result)
}

// Helper function to convert PyAny to BasicValue for NDArray
fn handle_ndarray_from_py<'py>(
    elem_type: &schema::BasicValueType,
    v: &Bound<'py, PyAny>,
) -> PyResult<Option<value::BasicValue>> {
    macro_rules! try_convert {
        ($t:ty, $cast:expr) => {
            if let Ok(array) = v.downcast::<PyArrayDyn<$t>>() {
                let data = array.readonly().as_slice()?.to_vec();
                let vec = data.into_iter().map($cast).collect::<Vec<_>>();
                return Ok(Some(value::BasicValue::Vector(Arc::from(vec))));
            }
        };
    }

    match elem_type {
        &schema::BasicValueType::Float32 => try_convert!(f32, value::BasicValue::Float32),
        &schema::BasicValueType::Float64 => try_convert!(f64, value::BasicValue::Float64),
        &schema::BasicValueType::Int64 => try_convert!(i64, value::BasicValue::Int64),
        _ => {}
    }

    Ok(None)
}

// Helper function to convert BasicValue::Vector to PyAny
fn handle_vector_to_py<'py>(
    py: Python<'py>,
    v: &[value::BasicValue],
) -> PyResult<Bound<'py, PyAny>> {
    match v.first() {
        Some(value::BasicValue::Float32(_)) => {
            let data = v
                .iter()
                .map(|x| match x {
                    value::BasicValue::Float32(f) => Ok(*f),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected all elements to be Float32",
                    )),
                })
                .collect::<PyResult<Vec<_>>>()?;

            Ok(PyArray1::from_vec(py, data).into_any())
        }
        Some(value::BasicValue::Float64(_)) => {
            let data = v
                .iter()
                .map(|x| match x {
                    value::BasicValue::Float64(f) => Ok(*f),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected all elements to be Float64",
                    )),
                })
                .collect::<PyResult<Vec<_>>>()?;

            Ok(PyArray1::from_vec(py, data).into_any())
        }
        Some(value::BasicValue::Int64(_)) => {
            let data = v
                .iter()
                .map(|x| match x {
                    value::BasicValue::Int64(i) => Ok(*i),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected all elements to be Int64",
                    )),
                })
                .collect::<PyResult<Vec<_>>>()?;

            Ok(PyArray1::from_vec(py, data).into_any())
        }
        _ => Ok(v
            .iter()
            .map(|v| basic_value_to_py_object(py, v))
            .collect::<PyResult<Vec<_>>>()?
            .into_bound_py_any(py)?),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::schema;
    use crate::base::value;
    use crate::base::value::ScopeValue;
    use pyo3::Python;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn assert_roundtrip_conversion(original_value: &value::Value, value_type: &schema::ValueType) {
        Python::with_gil(|py| {
            // Convert Rust value to Python object using value_to_py_object
            let py_object = value_to_py_object(py, original_value)
                .expect("Failed to convert Rust value to Python object");

            println!("Python object: {:?}", py_object);
            let roundtripped_value = value_from_py_object(value_type, &py_object)
                .expect("Failed to convert Python object back to Rust value");

            println!("Roundtripped value: {:?}", roundtripped_value);
            assert_eq!(
                original_value, &roundtripped_value,
                "Value mismatch after roundtrip"
            );
        });
    }

    #[test]
    fn test_roundtrip_basic_values() {
        let values_and_types = vec![
            (
                value::Value::Basic(value::BasicValue::Int64(42)),
                schema::ValueType::Basic(schema::BasicValueType::Int64),
            ),
            (
                value::Value::Basic(value::BasicValue::Float64(3.14)),
                schema::ValueType::Basic(schema::BasicValueType::Float64),
            ),
            (
                value::Value::Basic(value::BasicValue::Str(Arc::from("hello"))),
                schema::ValueType::Basic(schema::BasicValueType::Str),
            ),
            (
                value::Value::Basic(value::BasicValue::Bool(true)),
                schema::ValueType::Basic(schema::BasicValueType::Bool),
            ),
        ];

        for (val, typ) in values_and_types {
            assert_roundtrip_conversion(&val, &typ);
        }
    }

    #[test]
    fn test_roundtrip_struct() {
        let struct_schema = schema::StructSchema {
            description: Some(Arc::from("Test struct description")),
            fields: Arc::new(vec![
                schema::FieldSchema {
                    name: "a".to_string(),
                    value_type: schema::EnrichedValueType {
                        typ: schema::ValueType::Basic(schema::BasicValueType::Int64),
                        nullable: false,
                        attrs: Default::default(),
                    },
                },
                schema::FieldSchema {
                    name: "b".to_string(),
                    value_type: schema::EnrichedValueType {
                        typ: schema::ValueType::Basic(schema::BasicValueType::Str),
                        nullable: false,
                        attrs: Default::default(),
                    },
                },
            ]),
        };

        let struct_val_data = value::FieldValues {
            fields: vec![
                value::Value::Basic(value::BasicValue::Int64(10)),
                value::Value::Basic(value::BasicValue::Str(Arc::from("world"))),
            ],
        };

        let struct_val = value::Value::Struct(struct_val_data);
        let struct_typ = schema::ValueType::Struct(struct_schema); // No clone needed

        assert_roundtrip_conversion(&struct_val, &struct_typ);
    }

    #[test]
    fn test_roundtrip_table_types() {
        let row_schema_struct = Arc::new(schema::StructSchema {
            description: Some(Arc::from("Test table row description")),
            fields: Arc::new(vec![
                schema::FieldSchema {
                    name: "key_col".to_string(), // Will be used as key for KTable implicitly
                    value_type: schema::EnrichedValueType {
                        typ: schema::ValueType::Basic(schema::BasicValueType::Int64),
                        nullable: false,
                        attrs: Default::default(),
                    },
                },
                schema::FieldSchema {
                    name: "data_col_1".to_string(),
                    value_type: schema::EnrichedValueType {
                        typ: schema::ValueType::Basic(schema::BasicValueType::Str),
                        nullable: false,
                        attrs: Default::default(),
                    },
                },
                schema::FieldSchema {
                    name: "data_col_2".to_string(),
                    value_type: schema::EnrichedValueType {
                        typ: schema::ValueType::Basic(schema::BasicValueType::Bool),
                        nullable: false,
                        attrs: Default::default(),
                    },
                },
            ]),
        });

        let row1_fields = value::FieldValues {
            fields: vec![
                value::Value::Basic(value::BasicValue::Int64(1)),
                value::Value::Basic(value::BasicValue::Str(Arc::from("row1_data"))),
                value::Value::Basic(value::BasicValue::Bool(true)),
            ],
        };
        let row1_scope_val: value::ScopeValue = row1_fields.into();

        let row2_fields = value::FieldValues {
            fields: vec![
                value::Value::Basic(value::BasicValue::Int64(2)),
                value::Value::Basic(value::BasicValue::Str(Arc::from("row2_data"))),
                value::Value::Basic(value::BasicValue::Bool(false)),
            ],
        };
        let row2_scope_val: value::ScopeValue = row2_fields.into();

        // UTable
        let utable_schema = schema::TableSchema {
            kind: schema::TableKind::UTable,
            row: (*row_schema_struct).clone(),
        };
        let utable_val = value::Value::UTable(vec![row1_scope_val.clone(), row2_scope_val.clone()]);
        let utable_typ = schema::ValueType::Table(utable_schema);
        assert_roundtrip_conversion(&utable_val, &utable_typ);

        // LTable
        let ltable_schema = schema::TableSchema {
            kind: schema::TableKind::LTable,
            row: (*row_schema_struct).clone(),
        };
        let ltable_val = value::Value::LTable(vec![row1_scope_val.clone(), row2_scope_val.clone()]);
        let ltable_typ = schema::ValueType::Table(ltable_schema);
        assert_roundtrip_conversion(&ltable_val, &ltable_typ);

        // KTable
        let ktable_schema = schema::TableSchema {
            kind: schema::TableKind::KTable,
            row: (*row_schema_struct).clone(),
        };
        let mut ktable_data = BTreeMap::new();

        // Create KTable entries where the ScopeValue doesn't include the key field
        // This matches how the Python code will serialize/deserialize
        let row1_fields = value::FieldValues {
            fields: vec![
                value::Value::Basic(value::BasicValue::Str(Arc::from("row1_data"))),
                value::Value::Basic(value::BasicValue::Bool(true)),
            ],
        };
        let row1_scope_val: value::ScopeValue = row1_fields.into();

        let row2_fields = value::FieldValues {
            fields: vec![
                value::Value::Basic(value::BasicValue::Str(Arc::from("row2_data"))),
                value::Value::Basic(value::BasicValue::Bool(false)),
            ],
        };
        let row2_scope_val: value::ScopeValue = row2_fields.into();

        // For KTable, the key is extracted from the first field of ScopeValue based on current serialization
        let key1 = value::Value::<ScopeValue>::Basic(value::BasicValue::Int64(1))
            .into_key()
            .unwrap();
        let key2 = value::Value::<ScopeValue>::Basic(value::BasicValue::Int64(2))
            .into_key()
            .unwrap();

        ktable_data.insert(key1, row1_scope_val.clone());
        ktable_data.insert(key2, row2_scope_val.clone());

        let ktable_val = value::Value::KTable(ktable_data);
        let ktable_typ = schema::ValueType::Table(ktable_schema);
        assert_roundtrip_conversion(&ktable_val, &ktable_typ);
    }
}
