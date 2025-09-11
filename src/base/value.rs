use crate::prelude::*;

use super::schema::*;
use crate::base::duration::parse_duration;
use base64::prelude::*;
use bytes::Bytes;
use chrono::Offset;
use log::warn;
use serde::{
    de::{SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq, SerializeTuple},
};
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

pub trait EstimatedByteSize: Sized {
    fn estimated_detached_byte_size(&self) -> usize;

    fn estimated_byte_size(&self) -> usize {
        self.estimated_detached_byte_size() + std::mem::size_of::<Self>()
    }
}

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
pub enum KeyPart {
    Bytes(Bytes),
    Str(Arc<str>),
    Bool(bool),
    Int64(i64),
    Range(RangeValue),
    Uuid(uuid::Uuid),
    Date(chrono::NaiveDate),
    Struct(Vec<KeyPart>),
}

impl From<Bytes> for KeyPart {
    fn from(value: Bytes) -> Self {
        KeyPart::Bytes(value)
    }
}

impl From<Vec<u8>> for KeyPart {
    fn from(value: Vec<u8>) -> Self {
        KeyPart::Bytes(Bytes::from(value))
    }
}

impl From<Arc<str>> for KeyPart {
    fn from(value: Arc<str>) -> Self {
        KeyPart::Str(value)
    }
}

impl From<String> for KeyPart {
    fn from(value: String) -> Self {
        KeyPart::Str(Arc::from(value))
    }
}

impl From<bool> for KeyPart {
    fn from(value: bool) -> Self {
        KeyPart::Bool(value)
    }
}

impl From<i64> for KeyPart {
    fn from(value: i64) -> Self {
        KeyPart::Int64(value)
    }
}

impl From<RangeValue> for KeyPart {
    fn from(value: RangeValue) -> Self {
        KeyPart::Range(value)
    }
}

impl From<uuid::Uuid> for KeyPart {
    fn from(value: uuid::Uuid) -> Self {
        KeyPart::Uuid(value)
    }
}

impl From<chrono::NaiveDate> for KeyPart {
    fn from(value: chrono::NaiveDate) -> Self {
        KeyPart::Date(value)
    }
}

impl From<Vec<KeyPart>> for KeyPart {
    fn from(value: Vec<KeyPart>) -> Self {
        KeyPart::Struct(value)
    }
}

impl serde::Serialize for KeyPart {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Value::from(self.clone()).serialize(serializer)
    }
}

impl std::fmt::Display for KeyPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyPart::Bytes(v) => write!(f, "{}", BASE64_STANDARD.encode(v)),
            KeyPart::Str(v) => write!(f, "\"{}\"", v.escape_default()),
            KeyPart::Bool(v) => write!(f, "{v}"),
            KeyPart::Int64(v) => write!(f, "{v}"),
            KeyPart::Range(v) => write!(f, "[{}, {})", v.start, v.end),
            KeyPart::Uuid(v) => write!(f, "{v}"),
            KeyPart::Date(v) => write!(f, "{v}"),
            KeyPart::Struct(v) => {
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

impl KeyPart {
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
                    BasicValueType::Bytes => {
                        KeyPart::Bytes(Bytes::from(BASE64_STANDARD.decode(v)?))
                    }
                    BasicValueType::Str => KeyPart::Str(Arc::from(v)),
                    BasicValueType::Bool => KeyPart::Bool(v.parse()?),
                    BasicValueType::Int64 => KeyPart::Int64(v.parse()?),
                    BasicValueType::Range => {
                        let v2 = values_iter
                            .next()
                            .ok_or_else(|| api_error!("Key parts less than expected"))?;
                        KeyPart::Range(RangeValue {
                            start: v.parse()?,
                            end: v2.parse()?,
                        })
                    }
                    BasicValueType::Uuid => KeyPart::Uuid(v.parse()?),
                    BasicValueType::Date => KeyPart::Date(v.parse()?),
                    schema => api_bail!("Invalid key type {schema}"),
                }
            }
            ValueType::Struct(s) => KeyPart::Struct(
                s.fields
                    .iter()
                    .map(|f| KeyPart::parts_from_str(values_iter, &f.value_type.typ))
                    .collect::<Result<Vec<_>>>()?,
            ),
            _ => api_bail!("Invalid key type {schema}"),
        };
        Ok(result)
    }

    fn parts_to_strs(&self, output: &mut Vec<String>) {
        match self {
            KeyPart::Bytes(v) => output.push(BASE64_STANDARD.encode(v)),
            KeyPart::Str(v) => output.push(v.to_string()),
            KeyPart::Bool(v) => output.push(v.to_string()),
            KeyPart::Int64(v) => output.push(v.to_string()),
            KeyPart::Range(v) => {
                output.push(v.start.to_string());
                output.push(v.end.to_string());
            }
            KeyPart::Uuid(v) => output.push(v.to_string()),
            KeyPart::Date(v) => output.push(v.to_string()),
            KeyPart::Struct(v) => {
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
            KeyPart::Bytes(_) => "bytes",
            KeyPart::Str(_) => "str",
            KeyPart::Bool(_) => "bool",
            KeyPart::Int64(_) => "int64",
            KeyPart::Range { .. } => "range",
            KeyPart::Uuid(_) => "uuid",
            KeyPart::Date(_) => "date",
            KeyPart::Struct(_) => "struct",
        }
    }

    pub fn bytes_value(&self) -> Result<&Bytes> {
        match self {
            KeyPart::Bytes(v) => Ok(v),
            _ => anyhow::bail!("expected bytes value, but got {}", self.kind_str()),
        }
    }

    pub fn str_value(&self) -> Result<&Arc<str>> {
        match self {
            KeyPart::Str(v) => Ok(v),
            _ => anyhow::bail!("expected str value, but got {}", self.kind_str()),
        }
    }

    pub fn bool_value(&self) -> Result<bool> {
        match self {
            KeyPart::Bool(v) => Ok(*v),
            _ => anyhow::bail!("expected bool value, but got {}", self.kind_str()),
        }
    }

    pub fn int64_value(&self) -> Result<i64> {
        match self {
            KeyPart::Int64(v) => Ok(*v),
            _ => anyhow::bail!("expected int64 value, but got {}", self.kind_str()),
        }
    }

    pub fn range_value(&self) -> Result<RangeValue> {
        match self {
            KeyPart::Range(v) => Ok(*v),
            _ => anyhow::bail!("expected range value, but got {}", self.kind_str()),
        }
    }

    pub fn uuid_value(&self) -> Result<uuid::Uuid> {
        match self {
            KeyPart::Uuid(v) => Ok(*v),
            _ => anyhow::bail!("expected uuid value, but got {}", self.kind_str()),
        }
    }

    pub fn date_value(&self) -> Result<chrono::NaiveDate> {
        match self {
            KeyPart::Date(v) => Ok(*v),
            _ => anyhow::bail!("expected date value, but got {}", self.kind_str()),
        }
    }

    pub fn struct_value(&self) -> Result<&Vec<KeyPart>> {
        match self {
            KeyPart::Struct(v) => Ok(v),
            _ => anyhow::bail!("expected struct value, but got {}", self.kind_str()),
        }
    }

    pub fn num_parts(&self) -> usize {
        match self {
            KeyPart::Range(_) => 2,
            KeyPart::Struct(v) => v.iter().map(|v| v.num_parts()).sum(),
            _ => 1,
        }
    }

    fn estimated_detached_byte_size(&self) -> usize {
        match self {
            KeyPart::Bytes(v) => v.len(),
            KeyPart::Str(v) => v.len(),
            KeyPart::Struct(v) => {
                v.iter()
                    .map(KeyPart::estimated_detached_byte_size)
                    .sum::<usize>()
                    + v.len() * std::mem::size_of::<KeyPart>()
            }
            KeyPart::Bool(_)
            | KeyPart::Int64(_)
            | KeyPart::Range(_)
            | KeyPart::Uuid(_)
            | KeyPart::Date(_) => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyValue(pub Box<[KeyPart]>);

impl<T: Into<Box<[KeyPart]>>> From<T> for KeyValue {
    fn from(value: T) -> Self {
        KeyValue(value.into())
    }
}

impl IntoIterator for KeyValue {
    type Item = KeyPart;
    type IntoIter = std::vec::IntoIter<KeyPart>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a KeyValue {
    type Item = &'a KeyPart;
    type IntoIter = std::slice::Iter<'a, KeyPart>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Deref for KeyValue {
    type Target = [KeyPart];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.0
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl Serialize for KeyValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.len() == 1 && !matches!(self.0[0], KeyPart::Struct(_)) {
            self.0[0].serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl KeyValue {
    pub fn from_single_part<V: Into<KeyPart>>(value: V) -> Self {
        Self(Box::new([value.into()]))
    }

    pub fn iter(&self) -> impl Iterator<Item = &KeyPart> {
        self.0.iter()
    }

    pub fn from_json(value: serde_json::Value, schema: &[FieldSchema]) -> Result<Self> {
        let field_values = if schema.len() == 1
            && matches!(schema[0].value_type.typ, ValueType::Basic(_))
        {
            let val = Value::<ScopeValue>::from_json(value, &schema[0].value_type.typ)?;
            Box::from([val.into_key()?])
        } else {
            match value {
                serde_json::Value::Array(arr) => std::iter::zip(arr.into_iter(), schema)
                    .map(|(v, s)| Value::<ScopeValue>::from_json(v, &s.value_type.typ)?.into_key())
                    .collect::<Result<Box<[_]>>>()?,
                _ => anyhow::bail!("expected array value, but got {}", value),
            }
        };
        Ok(Self(field_values))
    }

    pub fn encode_to_strs(&self) -> Vec<String> {
        let capacity = self.0.iter().map(|k| k.num_parts()).sum();
        let mut output = Vec::with_capacity(capacity);
        for part in self.0.iter() {
            part.parts_to_strs(&mut output);
        }
        output
    }

    pub fn decode_from_strs(
        value: impl IntoIterator<Item = String>,
        schema: &[FieldSchema],
    ) -> Result<Self> {
        let mut values_iter = value.into_iter();
        let keys: Box<[KeyPart]> = schema
            .iter()
            .map(|f| KeyPart::parts_from_str(&mut values_iter, &f.value_type.typ))
            .collect::<Result<Box<[_]>>>()?;
        if values_iter.next().is_some() {
            api_bail!("Key parts more than expected");
        }
        Ok(Self(keys))
    }

    pub fn to_values(&self) -> Box<[Value]> {
        self.0.iter().map(|v| v.into()).collect()
    }

    pub fn single_part(&self) -> Result<&KeyPart> {
        if self.0.len() != 1 {
            api_bail!("expected single value, but got {}", self.0.len());
        }
        Ok(&self.0[0])
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
    pub fn into_key(self) -> Result<KeyPart> {
        let result = match self {
            BasicValue::Bytes(v) => KeyPart::Bytes(v),
            BasicValue::Str(v) => KeyPart::Str(v),
            BasicValue::Bool(v) => KeyPart::Bool(v),
            BasicValue::Int64(v) => KeyPart::Int64(v),
            BasicValue::Range(v) => KeyPart::Range(v),
            BasicValue::Uuid(v) => KeyPart::Uuid(v),
            BasicValue::Date(v) => KeyPart::Date(v),
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

    pub fn as_key(&self) -> Result<KeyPart> {
        let result = match self {
            BasicValue::Bytes(v) => KeyPart::Bytes(v.clone()),
            BasicValue::Str(v) => KeyPart::Str(v.clone()),
            BasicValue::Bool(v) => KeyPart::Bool(*v),
            BasicValue::Int64(v) => KeyPart::Int64(*v),
            BasicValue::Range(v) => KeyPart::Range(*v),
            BasicValue::Uuid(v) => KeyPart::Uuid(*v),
            BasicValue::Date(v) => KeyPart::Date(*v),
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

    /// Returns the estimated byte size of the value, for detached data (i.e. allocated on heap).
    fn estimated_detached_byte_size(&self) -> usize {
        fn json_estimated_detached_byte_size(val: &serde_json::Value) -> usize {
            match val {
                serde_json::Value::String(s) => s.len(),
                serde_json::Value::Array(arr) => {
                    arr.iter()
                        .map(json_estimated_detached_byte_size)
                        .sum::<usize>()
                        + arr.len() * std::mem::size_of::<serde_json::Value>()
                }
                serde_json::Value::Object(map) => map
                    .iter()
                    .map(|(k, v)| {
                        std::mem::size_of::<serde_json::map::Entry>()
                            + k.len()
                            + json_estimated_detached_byte_size(v)
                    })
                    .sum(),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_) => 0,
            }
        }
        match self {
            BasicValue::Bytes(v) => v.len(),
            BasicValue::Str(v) => v.len(),
            BasicValue::Json(v) => json_estimated_detached_byte_size(v),
            BasicValue::Vector(v) => {
                v.iter()
                    .map(BasicValue::estimated_detached_byte_size)
                    .sum::<usize>()
                    + v.len() * std::mem::size_of::<BasicValue>()
            }
            BasicValue::UnionVariant { value, .. } => {
                value.estimated_detached_byte_size() + std::mem::size_of::<BasicValue>()
            }
            BasicValue::Bool(_)
            | BasicValue::Int64(_)
            | BasicValue::Float32(_)
            | BasicValue::Float64(_)
            | BasicValue::Range(_)
            | BasicValue::Uuid(_)
            | BasicValue::Date(_)
            | BasicValue::Time(_)
            | BasicValue::LocalDateTime(_)
            | BasicValue::OffsetDateTime(_)
            | BasicValue::TimeDelta(_) => 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
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

impl From<KeyPart> for Value {
    fn from(value: KeyPart) -> Self {
        match value {
            KeyPart::Bytes(v) => Value::Basic(BasicValue::Bytes(v)),
            KeyPart::Str(v) => Value::Basic(BasicValue::Str(v)),
            KeyPart::Bool(v) => Value::Basic(BasicValue::Bool(v)),
            KeyPart::Int64(v) => Value::Basic(BasicValue::Int64(v)),
            KeyPart::Range(v) => Value::Basic(BasicValue::Range(v)),
            KeyPart::Uuid(v) => Value::Basic(BasicValue::Uuid(v)),
            KeyPart::Date(v) => Value::Basic(BasicValue::Date(v)),
            KeyPart::Struct(v) => Value::Struct(FieldValues {
                fields: v.into_iter().map(Value::from).collect(),
            }),
        }
    }
}

impl From<&KeyPart> for Value {
    fn from(value: &KeyPart) -> Self {
        match value {
            KeyPart::Bytes(v) => Value::Basic(BasicValue::Bytes(v.clone())),
            KeyPart::Str(v) => Value::Basic(BasicValue::Str(v.clone())),
            KeyPart::Bool(v) => Value::Basic(BasicValue::Bool(*v)),
            KeyPart::Int64(v) => Value::Basic(BasicValue::Int64(*v)),
            KeyPart::Range(v) => Value::Basic(BasicValue::Range(*v)),
            KeyPart::Uuid(v) => Value::Basic(BasicValue::Uuid(*v)),
            KeyPart::Date(v) => Value::Basic(BasicValue::Date(*v)),
            KeyPart::Struct(v) => Value::Struct(FieldValues {
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
            Value::KTable(v) => Value::KTable(v.into_iter().map(|(k, v)| (k, v.into())).collect()),
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

    pub fn into_key(self) -> Result<KeyPart> {
        let result = match self {
            Value::Basic(v) => v.into_key()?,
            Value::Struct(v) => KeyPart::Struct(
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

    pub fn as_key(&self) -> Result<KeyPart> {
        let result = match self {
            Value::Basic(v) => v.as_key()?,
            Value::Struct(v) => KeyPart::Struct(
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

impl<VS: EstimatedByteSize> Value<VS> {
    pub fn estimated_byte_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Value::Null => 0,
                Value::Basic(v) => v.estimated_detached_byte_size(),
                Value::Struct(v) => v.estimated_detached_byte_size(),
                Value::UTable(v) | Value::LTable(v) => {
                    v.iter()
                        .map(|v| v.estimated_detached_byte_size())
                        .sum::<usize>()
                        + v.len() * std::mem::size_of::<ScopeValue>()
                }
                Value::KTable(v) => {
                    v.iter()
                        .map(|(k, v)| {
                            k.iter()
                                .map(|k| k.estimated_detached_byte_size())
                                .sum::<usize>()
                                + v.estimated_detached_byte_size()
                        })
                        .sum::<usize>()
                        + v.len() * std::mem::size_of::<(String, ScopeValue)>()
                }
            }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldValues<VS = ScopeValue> {
    pub fields: Vec<Value<VS>>,
}

impl<VS: EstimatedByteSize> EstimatedByteSize for FieldValues<VS> {
    fn estimated_detached_byte_size(&self) -> usize {
        self.fields
            .iter()
            .map(Value::<VS>::estimated_byte_size)
            .sum::<usize>()
            + self.fields.len() * std::mem::size_of::<Value<VS>>()
    }
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
                    let value = Value::<VS>::from_json(v, &s.value_type.typ)
                        .with_context(|| format!("while deserializing field `{}`", s.name))?;
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
                        Some(v) => Value::<VS>::from_json(std::mem::take(v), &field.value_type.typ)
                            .with_context(|| {
                                format!("while deserializing field `{}`", field.name)
                            })?,
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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ScopeValue(pub FieldValues);

impl EstimatedByteSize for ScopeValue {
    fn estimated_detached_byte_size(&self) -> usize {
        self.0.estimated_detached_byte_size()
    }
}

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
            (serde_json::Value::String(v), BasicValueType::Bytes) => {
                BasicValue::Bytes(Bytes::from(BASE64_STANDARD.decode(v)?))
            }
            (serde_json::Value::String(v), BasicValueType::Str) => BasicValue::Str(Arc::from(v)),
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
            (v, BasicValueType::Range) => BasicValue::Range(utils::deser::from_json_value(v)?),
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
                    .enumerate()
                    .map(|(i, v)| {
                        BasicValue::from_json(v, element_type)
                            .with_context(|| format!("while deserializing Vector element #{i}"))
                    })
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

struct TableEntry<'a>(&'a [KeyPart], &'a ScopeValue);

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
        let mut seq = serializer.serialize_seq(Some(key.len() + value.0.fields.len()))?;
        for item in key.iter() {
            seq.serialize_element(item)?;
        }
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
            (serde_json::Value::Array(v), ValueType::Table(s)) => {
                match s.kind {
                    TableKind::UTable => {
                        let rows = v
                            .into_iter()
                            .map(|v| {
                                Ok(FieldValues::from_json(v, &s.row.fields)
                                    .with_context(|| format!("while deserializing UTable row"))?
                                    .into())
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Value::LTable(rows)
                    }
                    TableKind::KTable(info) => {
                        let num_key_parts = info.num_key_parts;
                        let rows =
                        v.into_iter()
                            .map(|v| {
                                if s.row.fields.len() < num_key_parts {
                                    anyhow::bail!("Invalid KTable schema: expect at least {} fields, got {}", num_key_parts, s.row.fields.len());
                                }
                                let mut fields_iter = s.row.fields.iter();
                                match v {
                                    serde_json::Value::Array(v) => {
                                        if v.len() != fields_iter.len() {
                                            anyhow::bail!("Invalid KTable value: expect {} values, received {}", fields_iter.len(), v.len());
                                        }

                                        let mut field_vals_iter = v.into_iter();
                                        let keys: Box<[KeyPart]> = (0..num_key_parts)
                                            .map(|_| {
                                                let field_schema = fields_iter.next().unwrap();
                                                Self::from_json(
                                                    field_vals_iter.next().unwrap(),
                                                    &field_schema.value_type.typ,
                                                ).with_context(|| {
                                                    format!("while deserializing key part `{}`", field_schema.name)
                                                })?
                                                .into_key()
                                            })
                                            .collect::<Result<_>>()?;

                                        let values = FieldValues::from_json_values(
                                            std::iter::zip(fields_iter, field_vals_iter),
                                        )?;
                                        Ok((KeyValue(keys), values.into()))
                                    }
                                    serde_json::Value::Object(mut v) => {
                                        let keys: Box<[KeyPart]> = (0..num_key_parts).map(|_| {
                                            let f = fields_iter.next().unwrap();
                                            Self::from_json(
                                                std::mem::take(v.get_mut(&f.name).ok_or_else(
                                                || {
                                                    api_error!(
                                                        "key field `{}` doesn't exist in value",
                                                        f.name
                                                    )
                                                },
                                            )?),
                                            &f.value_type.typ)?.into_key()
                                        }).collect::<Result<_>>()?;
                                        let values = FieldValues::from_json_object(v, fields_iter)?;
                                        Ok((KeyValue(keys), values.into()))
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
                            .enumerate()
                            .map(|(i, v)| {
                                Ok(FieldValues::from_json(v, &s.row.fields)
                                    .with_context(|| {
                                        format!("while deserializing LTable row #{i}")
                                    })?
                                    .into())
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Value::LTable(rows)
                    }
                }
            }
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
                    let keys: Box<[Value]> = k.iter().map(|k| Value::from(k.clone())).collect();
                    seq.serialize_element(&TypedFieldsValue {
                        schema: &c.row.fields,
                        values_iter: keys.iter().chain(v.fields.iter()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_estimated_byte_size_null() {
        let value = Value::<ScopeValue>::Null;
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());
    }

    #[test]
    fn test_estimated_byte_size_basic_primitive() {
        // Test primitives that should have 0 detached byte size
        let value = Value::<ScopeValue>::Basic(BasicValue::Bool(true));
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());

        let value = Value::<ScopeValue>::Basic(BasicValue::Int64(42));
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());

        let value = Value::<ScopeValue>::Basic(BasicValue::Float64(3.14));
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());
    }

    #[test]
    fn test_estimated_byte_size_basic_string() {
        let test_str = "hello world";
        let value = Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(test_str)));
        let size = value.estimated_byte_size();

        let expected_size = std::mem::size_of::<Value<ScopeValue>>() + test_str.len();
        assert_eq!(size, expected_size);
    }

    #[test]
    fn test_estimated_byte_size_basic_bytes() {
        let test_bytes = b"hello world";
        let value = Value::<ScopeValue>::Basic(BasicValue::Bytes(Bytes::from(test_bytes.to_vec())));
        let size = value.estimated_byte_size();

        let expected_size = std::mem::size_of::<Value<ScopeValue>>() + test_bytes.len();
        assert_eq!(size, expected_size);
    }

    #[test]
    fn test_estimated_byte_size_basic_json() {
        let json_val = serde_json::json!({"key": "value", "number": 42});
        let value = Value::<ScopeValue>::Basic(BasicValue::Json(Arc::from(json_val)));
        let size = value.estimated_byte_size();

        // Should include the size of the JSON structure
        // The exact size depends on the internal JSON representation
        assert!(size > std::mem::size_of::<Value<ScopeValue>>());
    }

    #[test]
    fn test_estimated_byte_size_basic_vector() {
        let vec_elements = vec![
            BasicValue::Str(Arc::from("hello")),
            BasicValue::Str(Arc::from("world")),
            BasicValue::Int64(42),
        ];
        let value = Value::<ScopeValue>::Basic(BasicValue::Vector(Arc::from(vec_elements)));
        let size = value.estimated_byte_size();

        // Should include the size of the vector elements
        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "hello".len()
            + "world".len()
            + 3 * std::mem::size_of::<BasicValue>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_struct() {
        let fields = vec![
            Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from("test"))),
            Value::<ScopeValue>::Basic(BasicValue::Int64(123)),
        ];
        let field_values = FieldValues { fields };
        let value = Value::<ScopeValue>::Struct(field_values);
        let size = value.estimated_byte_size();

        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "test".len()
            + 2 * std::mem::size_of::<Value<ScopeValue>>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_utable() {
        let scope_values = vec![
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "item1",
                )))],
            }),
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "item2",
                )))],
            }),
        ];
        let value = Value::<ScopeValue>::UTable(scope_values);
        let size = value.estimated_byte_size();

        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "item1".len()
            + "item2".len()
            + 2 * std::mem::size_of::<ScopeValue>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_ltable() {
        let scope_values = vec![
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "list1",
                )))],
            }),
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "list2",
                )))],
            }),
        ];
        let value = Value::<ScopeValue>::LTable(scope_values);
        let size = value.estimated_byte_size();

        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "list1".len()
            + "list2".len()
            + 2 * std::mem::size_of::<ScopeValue>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_ktable() {
        let mut map = BTreeMap::new();
        map.insert(
            KeyValue(Box::from([KeyPart::Str(Arc::from("key1"))])),
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "value1",
                )))],
            }),
        );
        map.insert(
            KeyValue(Box::from([KeyPart::Str(Arc::from("key2"))])),
            ScopeValue(FieldValues {
                fields: vec![Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from(
                    "value2",
                )))],
            }),
        );
        let value = Value::<ScopeValue>::KTable(map);
        let size = value.estimated_byte_size();

        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "key1".len()
            + "key2".len()
            + "value1".len()
            + "value2".len()
            + 2 * std::mem::size_of::<(String, ScopeValue)>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_nested_struct() {
        let inner_struct = Value::<ScopeValue>::Struct(FieldValues {
            fields: vec![
                Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from("inner"))),
                Value::<ScopeValue>::Basic(BasicValue::Int64(456)),
            ],
        });

        let outer_struct = Value::<ScopeValue>::Struct(FieldValues {
            fields: vec![
                Value::<ScopeValue>::Basic(BasicValue::Str(Arc::from("outer"))),
                inner_struct,
            ],
        });

        let size = outer_struct.estimated_byte_size();

        let expected_min_size = std::mem::size_of::<Value<ScopeValue>>()
            + "outer".len()
            + "inner".len()
            + 4 * std::mem::size_of::<Value<ScopeValue>>();
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_estimated_byte_size_empty_collections() {
        // Empty UTable
        let value = Value::<ScopeValue>::UTable(vec![]);
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());

        // Empty LTable
        let value = Value::<ScopeValue>::LTable(vec![]);
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());

        // Empty KTable
        let value = Value::<ScopeValue>::KTable(BTreeMap::new());
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());

        // Empty Struct
        let value = Value::<ScopeValue>::Struct(FieldValues { fields: vec![] });
        let size = value.estimated_byte_size();
        assert_eq!(size, std::mem::size_of::<Value<ScopeValue>>());
    }
}
