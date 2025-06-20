use super::schema::*;
use crate::base::duration::parse_duration;
use crate::prelude::invariance_violation;
use crate::{api_bail, api_error};
use anyhow::Result;
use base64::prelude::*;
use bytes::Bytes;
use chrono::Offset;
use log::warn;
use serde::{
    Deserialize, Serialize,
    de::{SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq, SerializeTuple},
};
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RangeValue {
    pub start: usize,
    pub end: usize,
}

impl RangeValue {
    pub fn new(start: usize, end: usize) -> Self {
        RangeValue { start, end }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn extract_str<'s>(&self, s: &'s (impl AsRef<str> + ?Sized)) -> &'s str {
        let s = s.as_ref();
        &s[self.start..self.end]
    }
}

impl Serialize for RangeValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&self.start)?;
        tuple.serialize_element(&self.end)?;
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for RangeValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RangeVisitor;

        impl<'de> Visitor<'de> for RangeVisitor {
            type Value = RangeValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a tuple of two u64")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let start = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::missing_field("missing begin"))?;
                let end = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::missing_field("missing end"))?;
                Ok(RangeValue { start, end })
            }
        }
        deserializer.deserialize_tuple(2, RangeVisitor)
    }
}

/// Value of key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub enum KeyValue {
    Bytes(Bytes),
    Str(Arc<str>),
    Bool(bool),
    Int64(i64),
    Range(RangeValue),
    Uuid(uuid::Uuid),
    Date(chrono::NaiveDate),
    Struct(Vec<KeyValue>),
}

impl From<Bytes> for KeyValue {
    fn from(value: Bytes) -> Self {
        KeyValue::Bytes(value)
    }
}

impl From<Vec<u8>> for KeyValue {
    fn from(value: Vec<u8>) -> Self {
        KeyValue::Bytes(Bytes::from(value))
    }
}

impl From<Arc<str>> for KeyValue {
    fn from(value: Arc<str>) -> Self {
        KeyValue::Str(value)
    }
}

impl From<String> for KeyValue {
    fn from(value: String) -> Self {
        KeyValue::Str(Arc::from(value))
    }
}

impl From<bool> for KeyValue {
    fn from(value: bool) -> Self {
        KeyValue::Bool(value)
    }
}

impl From<i64> for KeyValue {
    fn from(value: i64) -> Self {
        KeyValue::Int64(value)
    }
}

impl From<RangeValue> for KeyValue {
    fn from(value: RangeValue) -> Self {
        KeyValue::Range(value)
    }
}

impl From<uuid::Uuid> for KeyValue {
    fn from(value: uuid::Uuid) -> Self {
        KeyValue::Uuid(value)
    }
}

impl From<chrono::NaiveDate> for KeyValue {
    fn from(value: chrono::NaiveDate) -> Self {
        KeyValue::Date(value)
    }
}

impl From<Vec<KeyValue>> for KeyValue {
    fn from(value: Vec<KeyValue>) -> Self {
        KeyValue::Struct(value)
    }
}

impl serde::Serialize for KeyValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Value::from(self.clone()).serialize(serializer)
    }
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyValue::Bytes(v) => write!(f, "{}", BASE64_STANDARD.encode(v)),
            KeyValue::Str(v) => write!(f, "\"{}\"", v.escape_default()),
            KeyValue::Bool(v) => write!(f, "{}", v),
            KeyValue::Int64(v) => write!(f, "{}", v),
            KeyValue::Range(v) => write!(f, "[{}, {})", v.start, v.end),
            KeyValue::Uuid(v) => write!(f, "{}", v),
            KeyValue::Date(v) => write!(f, "{}", v),
            KeyValue::Struct(v) => {
                write!(
                    f,
                    "[{}]",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

impl KeyValue {
    pub fn from_json(value: serde_json::Value, fields_schema: &[FieldSchema]) -> Result<Self> {
        let value = if fields_schema.len() == 1 {
            Value::from_json(value, &fields_schema[0].value_type.typ)?
        } else {
            let field_values: FieldValues = FieldValues::from_json(value, fields_schema)?;
            Value::Struct(field_values)
        };
        Ok(value.as_key()?)
    }

    pub fn from_values<'a>(values: impl ExactSizeIterator<Item = &'a Value>) -> Result<Self> {
        let key = if values.len() == 1 {
            let mut values = values;
            values.next().ok_or_else(invariance_violation)?.as_key()?
        } else {
            KeyValue::Struct(values.map(|v| v.as_key()).collect::<Result<Vec<_>>>()?)
        };
        Ok(key)
    }

    pub fn fields_iter(&self, num_fields: usize) -> Result<impl Iterator<Item = &KeyValue>> {
        let slice = if num_fields == 1 {
            std::slice::from_ref(self)
        } else {
            match self {
                KeyValue::Struct(v) => v,
                _ => api_bail!("Invalid key value type"),
            }
        };
        Ok(slice.iter())
    }

    fn parts_from_str(
        values_iter: &mut impl Iterator<Item = String>,
        schema: &ValueType,
    ) -> Result<Self> {
        let result = match schema {
            ValueType::Basic(basic_type) => {
                let v = values_iter
                    .next()
                    .ok_or_else(|| api_error!("Key parts less than expected"))?;
                match basic_type {
                    BasicValueType::Bytes { .. } => {
                        KeyValue::Bytes(Bytes::from(BASE64_STANDARD.decode(v)?))
                    }
                    BasicValueType::Str { .. } => KeyValue::Str(Arc::from(v)),
                    BasicValueType::Bool => KeyValue::Bool(v.parse()?),
                    BasicValueType::Int64 => KeyValue::Int64(v.parse()?),
                    BasicValueType::Range => {
                        let v2 = values_iter
                            .next()
                            .ok_or_else(|| api_error!("Key parts less than expected"))?;
                        KeyValue::Range(RangeValue {
                            start: v.parse()?,
                            end: v2.parse()?,
                        })
                    }
                    BasicValueType::Uuid => KeyValue::Uuid(v.parse()?),
                    BasicValueType::Date => KeyValue::Date(v.parse()?),
                    schema => api_bail!("Invalid key type {schema}"),
                }
            }
            ValueType::Struct(s) => KeyValue::Struct(
                s.fields
                    .iter()
                    .map(|f| KeyValue::parts_from_str(values_iter, &f.value_type.typ))
                    .collect::<Result<Vec<_>>>()?,
            ),
            _ => api_bail!("Invalid key type {schema}"),
        };
        Ok(result)
    }

    fn parts_to_strs(&self, output: &mut Vec<String>) {
        match self {
            KeyValue::Bytes(v) => output.push(BASE64_STANDARD.encode(v)),
            KeyValue::Str(v) => output.push(v.to_string()),
            KeyValue::Bool(v) => output.push(v.to_string()),
            KeyValue::Int64(v) => output.push(v.to_string()),
            KeyValue::Range(v) => {
                output.push(v.start.to_string());
                output.push(v.end.to_string());
            }
            KeyValue::Uuid(v) => output.push(v.to_string()),
            KeyValue::Date(v) => output.push(v.to_string()),
            KeyValue::Struct(v) => {
                for part in v {
                    part.parts_to_strs(output);
                }
            }
        }
    }

    pub fn from_strs(value: impl IntoIterator<Item = String>, schema: &ValueType) -> Result<Self> {
        let mut values_iter = value.into_iter();
        let result = Self::parts_from_str(&mut values_iter, schema)?;
        if values_iter.next().is_some() {
            api_bail!("Key parts more than expected");
        }
        Ok(result)
    }

    pub fn to_strs(&self) -> Vec<String> {
        let mut output = Vec::with_capacity(self.num_parts());
        self.parts_to_strs(&mut output);
        output
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            KeyValue::Bytes(_) => "bytes",
            KeyValue::Str(_) => "str",
            KeyValue::Bool(_) => "bool",
            KeyValue::Int64(_) => "int64",
            KeyValue::Range { .. } => "range",
            KeyValue::Uuid(_) => "uuid",
            KeyValue::Date(_) => "date",
            KeyValue::Struct(_) => "struct",
        }
    }

    pub fn bytes_value(&self) -> Result<&Bytes> {
        match self {
            KeyValue::Bytes(v) => Ok(v),
            _ => anyhow::bail!("expected bytes value, but got {}", self.kind_str()),
        }
    }

    pub fn str_value(&self) -> Result<&Arc<str>> {
        match self {
            KeyValue::Str(v) => Ok(v),
            _ => anyhow::bail!("expected str value, but got {}", self.kind_str()),
        }
    }

    pub fn bool_value(&self) -> Result<bool> {
        match self {
            KeyValue::Bool(v) => Ok(*v),
            _ => anyhow::bail!("expected bool value, but got {}", self.kind_str()),
        }
    }

    pub fn int64_value(&self) -> Result<i64> {
        match self {
            KeyValue::Int64(v) => Ok(*v),
            _ => anyhow::bail!("expected int64 value, but got {}", self.kind_str()),
        }
    }

    pub fn range_value(&self) -> Result<RangeValue> {
        match self {
            KeyValue::Range(v) => Ok(*v),
            _ => anyhow::bail!("expected range value, but got {}", self.kind_str()),
        }
    }

    pub fn uuid_value(&self) -> Result<uuid::Uuid> {
        match self {
            KeyValue::Uuid(v) => Ok(*v),
            _ => anyhow::bail!("expected uuid value, but got {}", self.kind_str()),
        }
    }

    pub fn date_value(&self) -> Result<chrono::NaiveDate> {
        match self {
            KeyValue::Date(v) => Ok(*v),
            _ => anyhow::bail!("expected date value, but got {}", self.kind_str()),
        }
    }

    pub fn struct_value(&self) -> Result<&Vec<KeyValue>> {
        match self {
            KeyValue::Struct(v) => Ok(v),
            _ => anyhow::bail!("expected struct value, but got {}", self.kind_str()),
        }
    }

    pub fn num_parts(&self) -> usize {
        match self {
            KeyValue::Range(_) => 2,
            KeyValue::Struct(v) => v.iter().map(|v| v.num_parts()).sum(),
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum BasicValue {
    Bytes(Bytes),
    Str(Arc<str>),
    Bool(bool),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Range(RangeValue),
    Uuid(uuid::Uuid),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    LocalDateTime(chrono::NaiveDateTime),
    OffsetDateTime(chrono::DateTime<chrono::FixedOffset>),
    TimeDelta(chrono::Duration),
    Json(Arc<serde_json::Value>),
    Vector(Arc<[BasicValue]>),
    UnionVariant {
        tag_id: usize,
        value: Box<BasicValue>,
    },
}

impl From<Bytes> for BasicValue {
    fn from(value: Bytes) -> Self {
        BasicValue::Bytes(value)
    }
}

impl From<Vec<u8>> for BasicValue {
    fn from(value: Vec<u8>) -> Self {
        BasicValue::Bytes(Bytes::from(value))
    }
}

impl From<Arc<str>> for BasicValue {
    fn from(value: Arc<str>) -> Self {
        BasicValue::Str(value)
    }
}

impl From<String> for BasicValue {
    fn from(value: String) -> Self {
        BasicValue::Str(Arc::from(value))
    }
}

impl From<bool> for BasicValue {
    fn from(value: bool) -> Self {
        BasicValue::Bool(value)
    }
}

impl From<i64> for BasicValue {
    fn from(value: i64) -> Self {
        BasicValue::Int64(value)
    }
}

impl From<f32> for BasicValue {
    fn from(value: f32) -> Self {
        BasicValue::Float32(value)
    }
}

impl From<f64> for BasicValue {
    fn from(value: f64) -> Self {
        BasicValue::Float64(value)
    }
}

impl From<uuid::Uuid> for BasicValue {
    fn from(value: uuid::Uuid) -> Self {
        BasicValue::Uuid(value)
    }
}

impl From<chrono::NaiveDate> for BasicValue {
    fn from(value: chrono::NaiveDate) -> Self {
        BasicValue::Date(value)
    }
}

impl From<chrono::NaiveTime> for BasicValue {
    fn from(value: chrono::NaiveTime) -> Self {
        BasicValue::Time(value)
    }
}

impl From<chrono::NaiveDateTime> for BasicValue {
    fn from(value: chrono::NaiveDateTime) -> Self {
        BasicValue::LocalDateTime(value)
    }
}

impl From<chrono::DateTime<chrono::FixedOffset>> for BasicValue {
    fn from(value: chrono::DateTime<chrono::FixedOffset>) -> Self {
        BasicValue::OffsetDateTime(value)
    }
}

impl From<chrono::Duration> for BasicValue {
    fn from(value: chrono::Duration) -> Self {
        BasicValue::TimeDelta(value)
    }
}

impl From<serde_json::Value> for BasicValue {
    fn from(value: serde_json::Value) -> Self {
        BasicValue::Json(Arc::from(value))
    }
}

impl<T: Into<BasicValue>> From<Vec<T>> for BasicValue {
    fn from(value: Vec<T>) -> Self {
        BasicValue::Vector(Arc::from(
            value.into_iter().map(|v| v.into()).collect::<Vec<_>>(),
        ))
    }
}

impl BasicValue {
    pub fn into_key(self) -> Result<KeyValue> {
        let result = match self {
            BasicValue::Bytes(v) => KeyValue::Bytes(v),
            BasicValue::Str(v) => KeyValue::Str(v),
            BasicValue::Bool(v) => KeyValue::Bool(v),
            BasicValue::Int64(v) => KeyValue::Int64(v),
            BasicValue::Range(v) => KeyValue::Range(v),
            BasicValue::Uuid(v) => KeyValue::Uuid(v),
            BasicValue::Date(v) => KeyValue::Date(v),
            BasicValue::Float32(_)
            | BasicValue::Float64(_)
            | BasicValue::Time(_)
            | BasicValue::LocalDateTime(_)
            | BasicValue::OffsetDateTime(_)
            | BasicValue::TimeDelta(_)
            | BasicValue::Json(_)
            | BasicValue::Vector(_)
            | BasicValue::UnionVariant { .. } => api_bail!("invalid key value type"),
        };
        Ok(result)
    }

    pub fn as_key(&self) -> Result<KeyValue> {
        let result = match self {
            BasicValue::Bytes(v) => KeyValue::Bytes(v.clone()),
            BasicValue::Str(v) => KeyValue::Str(v.clone()),
            BasicValue::Bool(v) => KeyValue::Bool(*v),
            BasicValue::Int64(v) => KeyValue::Int64(*v),
            BasicValue::Range(v) => KeyValue::Range(*v),
            BasicValue::Uuid(v) => KeyValue::Uuid(*v),
            BasicValue::Date(v) => KeyValue::Date(*v),
            BasicValue::Float32(_)
            | BasicValue::Float64(_)
            | BasicValue::Time(_)
            | BasicValue::LocalDateTime(_)
            | BasicValue::OffsetDateTime(_)
            | BasicValue::TimeDelta(_)
            | BasicValue::Json(_)
            | BasicValue::Vector(_)
            | BasicValue::UnionVariant { .. } => api_bail!("invalid key value type"),
        };
        Ok(result)
    }

    pub fn kind(&self) -> &'static str {
        match &self {
            BasicValue::Bytes(_) => "bytes",
            BasicValue::Str(_) => "str",
            BasicValue::Bool(_) => "bool",
            BasicValue::Int64(_) => "int64",
            BasicValue::Float32(_) => "float32",
            BasicValue::Float64(_) => "float64",
            BasicValue::Range(_) => "range",
            BasicValue::Uuid(_) => "uuid",
            BasicValue::Date(_) => "date",
            BasicValue::Time(_) => "time",
            BasicValue::LocalDateTime(_) => "local_datetime",
            BasicValue::OffsetDateTime(_) => "offset_datetime",
            BasicValue::TimeDelta(_) => "timedelta",
            BasicValue::Json(_) => "json",
            BasicValue::Vector(_) => "vector",
            BasicValue::UnionVariant { .. } => "union",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub enum Value<VS = ScopeValue> {
    #[default]
    Null,
    Basic(BasicValue),
    Struct(FieldValues<VS>),
    UTable(Vec<VS>),
    KTable(BTreeMap<KeyValue, VS>),
    LTable(Vec<VS>),
}

impl<T: Into<BasicValue>> From<T> for Value {
    fn from(value: T) -> Self {
        Value::Basic(value.into())
    }
}

impl From<KeyValue> for Value {
    fn from(value: KeyValue) -> Self {
        match value {
            KeyValue::Bytes(v) => Value::Basic(BasicValue::Bytes(v)),
            KeyValue::Str(v) => Value::Basic(BasicValue::Str(v)),
            KeyValue::Bool(v) => Value::Basic(BasicValue::Bool(v)),
            KeyValue::Int64(v) => Value::Basic(BasicValue::Int64(v)),
            KeyValue::Range(v) => Value::Basic(BasicValue::Range(v)),
            KeyValue::Uuid(v) => Value::Basic(BasicValue::Uuid(v)),
            KeyValue::Date(v) => Value::Basic(BasicValue::Date(v)),
            KeyValue::Struct(v) => Value::Struct(FieldValues {
                fields: v.into_iter().map(Value::from).collect(),
            }),
        }
    }
}

impl From<&KeyValue> for Value {
    fn from(value: &KeyValue) -> Self {
        match value {
            KeyValue::Bytes(v) => Value::Basic(BasicValue::Bytes(v.clone())),
            KeyValue::Str(v) => Value::Basic(BasicValue::Str(v.clone())),
            KeyValue::Bool(v) => Value::Basic(BasicValue::Bool(*v)),
            KeyValue::Int64(v) => Value::Basic(BasicValue::Int64(*v)),
            KeyValue::Range(v) => Value::Basic(BasicValue::Range(*v)),
            KeyValue::Uuid(v) => Value::Basic(BasicValue::Uuid(*v)),
            KeyValue::Date(v) => Value::Basic(BasicValue::Date(*v)),
            KeyValue::Struct(v) => Value::Struct(FieldValues {
                fields: v.iter().map(Value::from).collect(),
            }),
        }
    }
}

impl From<FieldValues> for Value {
    fn from(value: FieldValues) -> Self {
        Value::Struct(value)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl<VS> Value<VS> {
    pub fn from_alternative<AltVS>(value: Value<AltVS>) -> Self
    where
        AltVS: Into<VS>,
    {
        match value {
            Value::Null => Value::Null,
            Value::Basic(v) => Value::Basic(v),
            Value::Struct(v) => Value::Struct(FieldValues::<VS> {
                fields: v
                    .fields
                    .into_iter()
                    .map(|v| Value::<VS>::from_alternative(v))
                    .collect(),
            }),
            Value::UTable(v) => Value::UTable(v.into_iter().map(|v| v.into()).collect()),
            Value::KTable(v) => {
                Value::KTable(v.into_iter().map(|(k, v)| (k.clone(), v.into())).collect())
            }
            Value::LTable(v) => Value::LTable(v.into_iter().map(|v| v.into()).collect()),
        }
    }

    pub fn from_alternative_ref<AltVS>(value: &Value<AltVS>) -> Self
    where
        for<'a> &'a AltVS: Into<VS>,
    {
        match value {
            Value::Null => Value::Null,
            Value::Basic(v) => Value::Basic(v.clone()),
            Value::Struct(v) => Value::Struct(FieldValues::<VS> {
                fields: v
                    .fields
                    .iter()
                    .map(|v| Value::<VS>::from_alternative_ref(v))
                    .collect(),
            }),
            Value::UTable(v) => Value::UTable(v.iter().map(|v| v.into()).collect()),
            Value::KTable(v) => {
                Value::KTable(v.iter().map(|(k, v)| (k.clone(), v.into())).collect())
            }
            Value::LTable(v) => Value::LTable(v.iter().map(|v| v.into()).collect()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn into_key(self) -> Result<KeyValue> {
        let result = match self {
            Value::Basic(v) => v.into_key()?,
            Value::Struct(v) => KeyValue::Struct(
                v.fields
                    .into_iter()
                    .map(|v| v.into_key())
                    .collect::<Result<Vec<_>>>()?,
            ),
            Value::Null | Value::UTable(_) | Value::KTable(_) | Value::LTable(_) => {
                anyhow::bail!("invalid key value type")
            }
        };
        Ok(result)
    }

    pub fn as_key(&self) -> Result<KeyValue> {
        let result = match self {
            Value::Basic(v) => v.as_key()?,
            Value::Struct(v) => KeyValue::Struct(
                v.fields
                    .iter()
                    .map(|v| v.as_key())
                    .collect::<Result<Vec<_>>>()?,
            ),
            Value::Null | Value::UTable(_) | Value::KTable(_) | Value::LTable(_) => {
                anyhow::bail!("invalid key value type")
            }
        };
        Ok(result)
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Basic(v) => v.kind(),
            Value::Struct(_) => "Struct",
            Value::UTable(_) => "UTable",
            Value::KTable(_) => "KTable",
            Value::LTable(_) => "LTable",
        }
    }

    pub fn optional(&self) -> Option<&Self> {
        match self {
            Value::Null => None,
            _ => Some(self),
        }
    }

    pub fn as_bytes(&self) -> Result<&Bytes> {
        match self {
            Value::Basic(BasicValue::Bytes(v)) => Ok(v),
            _ => anyhow::bail!("expected bytes value, but got {}", self.kind()),
        }
    }

    pub fn as_str(&self) -> Result<&Arc<str>> {
        match self {
            Value::Basic(BasicValue::Str(v)) => Ok(v),
            _ => anyhow::bail!("expected str value, but got {}", self.kind()),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            Value::Basic(BasicValue::Bool(v)) => Ok(*v),
            _ => anyhow::bail!("expected bool value, but got {}", self.kind()),
        }
    }

    pub fn as_int64(&self) -> Result<i64> {
        match self {
            Value::Basic(BasicValue::Int64(v)) => Ok(*v),
            _ => anyhow::bail!("expected int64 value, but got {}", self.kind()),
        }
    }

    pub fn as_float32(&self) -> Result<f32> {
        match self {
            Value::Basic(BasicValue::Float32(v)) => Ok(*v),
            _ => anyhow::bail!("expected float32 value, but got {}", self.kind()),
        }
    }

    pub fn as_float64(&self) -> Result<f64> {
        match self {
            Value::Basic(BasicValue::Float64(v)) => Ok(*v),
            _ => anyhow::bail!("expected float64 value, but got {}", self.kind()),
        }
    }

    pub fn as_range(&self) -> Result<RangeValue> {
        match self {
            Value::Basic(BasicValue::Range(v)) => Ok(*v),
            _ => anyhow::bail!("expected range value, but got {}", self.kind()),
        }
    }

    pub fn as_json(&self) -> Result<&Arc<serde_json::Value>> {
        match self {
            Value::Basic(BasicValue::Json(v)) => Ok(v),
            _ => anyhow::bail!("expected json value, but got {}", self.kind()),
        }
    }

    pub fn as_vector(&self) -> Result<&Arc<[BasicValue]>> {
        match self {
            Value::Basic(BasicValue::Vector(v)) => Ok(v),
            _ => anyhow::bail!("expected vector value, but got {}", self.kind()),
        }
    }

    pub fn as_struct(&self) -> Result<&FieldValues<VS>> {
        match self {
            Value::Struct(v) => Ok(v),
            _ => anyhow::bail!("expected struct value, but got {}", self.kind()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FieldValues<VS = ScopeValue> {
    pub fields: Vec<Value<VS>>,
}

impl serde::Serialize for FieldValues {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.fields.serialize(serializer)
    }
}

impl<VS: Clone> FieldValues<VS>
where
    FieldValues<VS>: Into<VS>,
{
    pub fn new(num_fields: usize) -> Self {
        let mut fields = Vec::with_capacity(num_fields);
        fields.resize(num_fields, Value::<VS>::Null);
        Self { fields }
    }

    fn from_json_values<'a>(
        fields: impl Iterator<Item = (&'a FieldSchema, serde_json::Value)>,
    ) -> Result<Self> {
        Ok(Self {
            fields: fields
                .map(|(s, v)| {
                    let value = Value::<VS>::from_json(v, &s.value_type.typ)?;
                    if value.is_null() && !s.value_type.nullable {
                        api_bail!("expected non-null value for `{}`", s.name);
                    }
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_json_object<'a>(
        values: serde_json::Map<String, serde_json::Value>,
        fields_schema: impl Iterator<Item = &'a FieldSchema>,
    ) -> Result<Self> {
        let mut values = values;
        Ok(Self {
            fields: fields_schema
                .map(|field| {
                    let value = match values.get_mut(&field.name) {
                        Some(v) => {
                            Value::<VS>::from_json(std::mem::take(v), &field.value_type.typ)?
                        }
                        None => Value::<VS>::default(),
                    };
                    if value.is_null() && !field.value_type.nullable {
                        api_bail!("expected non-null value for `{}`", field.name);
                    }
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn from_json(value: serde_json::Value, fields_schema: &[FieldSchema]) -> Result<Self> {
        match value {
            serde_json::Value::Array(v) => {
                if v.len() != fields_schema.len() {
                    api_bail!("unmatched value length");
                }
                Self::from_json_values(fields_schema.iter().zip(v))
            }
            serde_json::Value::Object(v) => Self::from_json_object(v, fields_schema.iter()),
            _ => api_bail!("invalid value type"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScopeValue(pub FieldValues);

impl Deref for ScopeValue {
    type Target = FieldValues;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<FieldValues> for ScopeValue {
    fn from(value: FieldValues) -> Self {
        Self(value)
    }
}

impl serde::Serialize for BasicValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            BasicValue::Bytes(v) => serializer.serialize_str(&BASE64_STANDARD.encode(v)),
            BasicValue::Str(v) => serializer.serialize_str(v),
            BasicValue::Bool(v) => serializer.serialize_bool(*v),
            BasicValue::Int64(v) => serializer.serialize_i64(*v),
            BasicValue::Float32(v) => serializer.serialize_f32(*v),
            BasicValue::Float64(v) => serializer.serialize_f64(*v),
            BasicValue::Range(v) => v.serialize(serializer),
            BasicValue::Uuid(v) => serializer.serialize_str(&v.to_string()),
            BasicValue::Date(v) => serializer.serialize_str(&v.to_string()),
            BasicValue::Time(v) => serializer.serialize_str(&v.to_string()),
            BasicValue::LocalDateTime(v) => {
                serializer.serialize_str(&v.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
            }
            BasicValue::OffsetDateTime(v) => {
                serializer.serialize_str(&v.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
            }
            BasicValue::TimeDelta(v) => serializer.serialize_str(&v.to_string()),
            BasicValue::Json(v) => v.serialize(serializer),
            BasicValue::Vector(v) => v.serialize(serializer),
            BasicValue::UnionVariant { tag_id, value } => {
                let mut s = serializer.serialize_tuple(2)?;
                s.serialize_element(tag_id)?;
                s.serialize_element(value)?;
                s.end()
            }
        }
    }
}

impl BasicValue {
    pub fn from_json(value: serde_json::Value, schema: &BasicValueType) -> Result<Self> {
        let result = match (value, schema) {
            (serde_json::Value::String(v), BasicValueType::Bytes { .. }) => {
                BasicValue::Bytes(Bytes::from(BASE64_STANDARD.decode(v)?))
            }
            (serde_json::Value::String(v), BasicValueType::Str { .. }) => {
                BasicValue::Str(Arc::from(v))
            }
            (serde_json::Value::Bool(v), BasicValueType::Bool) => BasicValue::Bool(v),
            (serde_json::Value::Number(v), BasicValueType::Int64) => BasicValue::Int64(
                v.as_i64()
                    .ok_or_else(|| anyhow::anyhow!("invalid int64 value {v}"))?,
            ),
            (serde_json::Value::Number(v), BasicValueType::Float32) => BasicValue::Float32(
                v.as_f64()
                    .ok_or_else(|| anyhow::anyhow!("invalid fp32 value {v}"))?
                    as f32,
            ),
            (serde_json::Value::Number(v), BasicValueType::Float64) => BasicValue::Float64(
                v.as_f64()
                    .ok_or_else(|| anyhow::anyhow!("invalid fp64 value {v}"))?,
            ),
            (v, BasicValueType::Range) => BasicValue::Range(serde_json::from_value(v)?),
            (serde_json::Value::String(v), BasicValueType::Uuid) => BasicValue::Uuid(v.parse()?),
            (serde_json::Value::String(v), BasicValueType::Date) => BasicValue::Date(v.parse()?),
            (serde_json::Value::String(v), BasicValueType::Time) => BasicValue::Time(v.parse()?),
            (serde_json::Value::String(v), BasicValueType::LocalDateTime) => {
                BasicValue::LocalDateTime(v.parse()?)
            }
            (serde_json::Value::String(v), BasicValueType::OffsetDateTime) => {
                match chrono::DateTime::parse_from_rfc3339(&v) {
                    Ok(dt) => BasicValue::OffsetDateTime(dt),
                    Err(e) => {
                        if let Ok(dt) = v.parse::<chrono::NaiveDateTime>() {
                            warn!("Datetime without timezone offset, assuming UTC");
                            BasicValue::OffsetDateTime(chrono::DateTime::from_naive_utc_and_offset(
                                dt,
                                chrono::Utc.fix(),
                            ))
                        } else {
                            Err(e)?
                        }
                    }
                }
            }
            (serde_json::Value::String(v), BasicValueType::TimeDelta) => {
                BasicValue::TimeDelta(parse_duration(&v)?)
            }
            (v, BasicValueType::Json) => BasicValue::Json(Arc::from(v)),
            (
                serde_json::Value::Array(v),
                BasicValueType::Vector(VectorTypeSchema { element_type, .. }),
            ) => {
                let vec = v
                    .into_iter()
                    .map(|v| BasicValue::from_json(v, element_type))
                    .collect::<Result<Vec<_>>>()?;
                BasicValue::Vector(Arc::from(vec))
            }
            (v, BasicValueType::Union(typ)) => {
                let arr = match v {
                    serde_json::Value::Array(arr) => arr,
                    _ => anyhow::bail!("Invalid JSON value for union, expect array"),
                };

                if arr.len() != 2 {
                    anyhow::bail!(
                        "Invalid union tuple: expect 2 values, received {}",
                        arr.len()
                    );
                }

                let mut obj_iter = arr.into_iter();

                // Take first element
                let tag_id = obj_iter
                    .next()
                    .and_then(|value| value.as_u64().map(|num_u64| num_u64 as usize))
                    .unwrap();

                // Take second element
                let value = obj_iter.next().unwrap();

                let cur_type = typ
                    .types
                    .get(tag_id)
                    .ok_or_else(|| anyhow::anyhow!("No type in `tag_id` \"{tag_id}\" found"))?;

                BasicValue::UnionVariant {
                    tag_id,
                    value: Box::new(BasicValue::from_json(value, cur_type)?),
                }
            }
            (v, t) => {
                anyhow::bail!("Value and type not matched.\nTarget type {t:?}\nJSON value: {v}\n")
            }
        };
        Ok(result)
    }
}

struct TableEntry<'a>(&'a KeyValue, &'a ScopeValue);

impl serde::Serialize for Value<ScopeValue> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => serializer.serialize_none(),
            Value::Basic(v) => v.serialize(serializer),
            Value::Struct(v) => v.serialize(serializer),
            Value::UTable(v) => v.serialize(serializer),
            Value::KTable(m) => {
                let mut seq = serializer.serialize_seq(Some(m.len()))?;
                for (k, v) in m.iter() {
                    seq.serialize_element(&TableEntry(k, v))?;
                }
                seq.end()
            }
            Value::LTable(v) => v.serialize(serializer),
        }
    }
}

impl serde::Serialize for TableEntry<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let &TableEntry(key, value) = self;
        let mut seq = serializer.serialize_seq(Some(value.0.fields.len() + 1))?;
        seq.serialize_element(key)?;
        for item in value.0.fields.iter() {
            seq.serialize_element(item)?;
        }
        seq.end()
    }
}

impl<VS: Clone> Value<VS>
where
    FieldValues<VS>: Into<VS>,
{
    pub fn from_json(value: serde_json::Value, schema: &ValueType) -> Result<Self> {
        let result = match (value, schema) {
            (serde_json::Value::Null, _) => Value::<VS>::Null,
            (v, ValueType::Basic(t)) => Value::<VS>::Basic(BasicValue::from_json(v, t)?),
            (v, ValueType::Struct(s)) => {
                Value::<VS>::Struct(FieldValues::<VS>::from_json(v, &s.fields)?)
            }
            (serde_json::Value::Array(v), ValueType::Table(s)) => match s.kind {
                TableKind::UTable => {
                    let rows = v
                        .into_iter()
                        .map(|v| Ok(FieldValues::from_json(v, &s.row.fields)?.into()))
                        .collect::<Result<Vec<_>>>()?;
                    Value::LTable(rows)
                }
                TableKind::KTable => {
                    let rows = v
                        .into_iter()
                        .map(|v| {
                            let mut fields_iter = s.row.fields.iter();
                            let key_field = fields_iter
                                .next()
                                .ok_or_else(|| api_error!("Empty struct field values"))?;

                            match v {
                                serde_json::Value::Array(v) => {
                                    let mut field_vals_iter = v.into_iter();
                                    let key = Self::from_json(
                                        field_vals_iter.next().ok_or_else(|| {
                                            api_error!("Empty struct field values")
                                        })?,
                                        &key_field.value_type.typ,
                                    )?
                                    .into_key()?;
                                    let values = FieldValues::from_json_values(
                                        fields_iter.zip(field_vals_iter),
                                    )?;
                                    Ok((key, values.into()))
                                }
                                serde_json::Value::Object(mut v) => {
                                    let key = Self::from_json(
                                        std::mem::take(v.get_mut(&key_field.name).ok_or_else(
                                            || {
                                                api_error!(
                                                    "key field `{}` doesn't exist in value",
                                                    key_field.name
                                                )
                                            },
                                        )?),
                                        &key_field.value_type.typ,
                                    )?
                                    .into_key()?;
                                    let values = FieldValues::from_json_object(v, fields_iter)?;
                                    Ok((key, values.into()))
                                }
                                _ => api_bail!("Table value must be a JSON array or object"),
                            }
                        })
                        .collect::<Result<BTreeMap<_, _>>>()?;
                    Value::KTable(rows)
                }
                TableKind::LTable => {
                    let rows = v
                        .into_iter()
                        .map(|v| Ok(FieldValues::from_json(v, &s.row.fields)?.into()))
                        .collect::<Result<Vec<_>>>()?;
                    Value::LTable(rows)
                }
            },
            (v, t) => {
                anyhow::bail!("Value and type not matched.\nTarget type {t:?}\nJSON value: {v}\n")
            }
        };
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TypedValue<'a> {
    pub t: &'a ValueType,
    pub v: &'a Value,
}

impl Serialize for TypedValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match (self.t, self.v) {
            (_, Value::Null) => serializer.serialize_none(),
            (ValueType::Basic(t), v) => match t {
                BasicValueType::Union(_) => match v {
                    Value::Basic(BasicValue::UnionVariant { value, .. }) => {
                        value.serialize(serializer)
                    }
                    _ => Err(serde::ser::Error::custom(
                        "Unmatched union type and value for `TypedValue`",
                    )),
                },
                _ => v.serialize(serializer),
            },
            (ValueType::Struct(s), Value::Struct(field_values)) => TypedFieldsValue {
                schema: &s.fields,
                values_iter: field_values.fields.iter(),
            }
            .serialize(serializer),
            (ValueType::Table(c), Value::UTable(rows) | Value::LTable(rows)) => {
                let mut seq = serializer.serialize_seq(Some(rows.len()))?;
                for row in rows {
                    seq.serialize_element(&TypedFieldsValue {
                        schema: &c.row.fields,
                        values_iter: row.fields.iter(),
                    })?;
                }
                seq.end()
            }
            (ValueType::Table(c), Value::KTable(rows)) => {
                let mut seq = serializer.serialize_seq(Some(rows.len()))?;
                for (k, v) in rows {
                    seq.serialize_element(&TypedFieldsValue {
                        schema: &c.row.fields,
                        values_iter: std::iter::once(&Value::from(k.clone()))
                            .chain(v.fields.iter()),
                    })?;
                }
                seq.end()
            }
            _ => Err(serde::ser::Error::custom(format!(
                "Incompatible value type: {:?} {:?}",
                self.t, self.v
            ))),
        }
    }
}

pub struct TypedFieldsValue<'a, I: Iterator<Item = &'a Value> + Clone> {
    pub schema: &'a [FieldSchema],
    pub values_iter: I,
}

impl<'a, I: Iterator<Item = &'a Value> + Clone> Serialize for TypedFieldsValue<'a, I> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.schema.len()))?;
        let values_iter = self.values_iter.clone();
        for (field, value) in self.schema.iter().zip(values_iter) {
            map.serialize_entry(
                &field.name,
                &TypedValue {
                    t: &field.value_type.typ,
                    v: value,
                },
            )?;
        }
        map.end()
    }
}

pub mod test_util {
    use super::*;

    pub fn seder_roundtrip(value: &Value, typ: &ValueType) -> Result<Value> {
        let json_value = serde_json::to_value(value)?;
        let roundtrip_value = Value::from_json(json_value, typ)?;
        Ok(roundtrip_value)
    }
}
