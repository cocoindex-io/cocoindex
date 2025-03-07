use crate::builder::plan::AnalyzedValueMapping;

use super::spec::*;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorTypeSchema {
    pub element_type: Box<BasicValueType>,
    pub dimension: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BasicValueType {
    /// A sequence of bytes in binary.
    Bytes,

    /// String encoded in UTF-8.
    Str,

    /// A boolean value.
    Bool,

    /// 64-bit integer.
    Int64,

    /// 32-bit floating point number.
    Float32,

    /// 64-bit floating point number.
    Float64,

    /// A range, with a start offset and a length.
    Range,

    /// A JSON value.
    Json,

    /// A vector of values (usually numbers, for embeddings).
    Vector(VectorTypeSchema),
}

impl std::fmt::Display for BasicValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BasicValueType::Bytes => write!(f, "bytes"),
            BasicValueType::Str => write!(f, "str"),
            BasicValueType::Bool => write!(f, "bool"),
            BasicValueType::Int64 => write!(f, "int64"),
            BasicValueType::Float32 => write!(f, "float32"),
            BasicValueType::Float64 => write!(f, "float64"),
            BasicValueType::Range => write!(f, "range"),
            BasicValueType::Json => write!(f, "json"),
            BasicValueType::Vector(s) => write!(
                f,
                "vector({}, {})",
                s.dimension
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "*".to_string()),
                s.element_type
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructSchema {
    pub fields: Arc<Vec<FieldSchema>>,
}

impl StructSchema {
    pub fn without_attrs(&self) -> Self {
        Self {
            fields: Arc::new(self.fields.iter().map(|f| f.without_attrs()).collect()),
        }
    }
}

impl std::fmt::Display for StructSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Struct(")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CollectionKind {
    /// A generic collection can have any row type.
    Collection,
    /// A table's first field is the key.
    Table,
    /// A list is a table whose key type is int64 starting from 0 continuously..
    List,
}

impl std::fmt::Display for CollectionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionKind::Collection => write!(f, "Collection"),
            CollectionKind::Table => write!(f, "Table"),
            CollectionKind::List => write!(f, "List"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionSchema {
    pub kind: CollectionKind,
    pub row: StructSchema,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub collectors: Vec<NamedSpec<StructSchema>>,
}

impl CollectionSchema {
    pub fn has_key(&self) -> bool {
        match self.kind {
            CollectionKind::Table => true,
            CollectionKind::Collection | CollectionKind::List => false,
        }
    }

    pub fn key_type(&self) -> Option<&EnrichedValueType> {
        match self.kind {
            CollectionKind::Table => self
                .row
                .fields
                .first()
                .as_ref()
                .map(|field| &field.value_type),
            CollectionKind::Collection | CollectionKind::List => None,
        }
    }

    pub fn without_attrs(&self) -> Self {
        Self {
            kind: self.kind,
            row: self.row.without_attrs(),
            collectors: self
                .collectors
                .iter()
                .map(|c| NamedSpec {
                    name: c.name.clone(),
                    spec: c.spec.without_attrs(),
                })
                .collect(),
        }
    }
}

impl std::fmt::Display for CollectionSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({}", self.kind, self.row)?;
        for collector in self.collectors.iter() {
            write!(f, "; COLLECTOR {} ({})", collector.name, collector.spec)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl CollectionSchema {
    pub fn new(kind: CollectionKind, fields: Vec<FieldSchema>) -> Self {
        Self {
            kind,
            row: StructSchema {
                fields: Arc::new(fields),
            },
            collectors: Default::default(),
        }
    }

    pub fn key_field<'a>(&'a self) -> Option<&'a FieldSchema> {
        match self.kind {
            CollectionKind::Table => Some(self.row.fields.first().unwrap()),
            CollectionKind::Collection | CollectionKind::List => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum ValueType {
    Struct(StructSchema),

    #[serde(untagged)]
    Basic(BasicValueType),

    #[serde(untagged)]
    Collection(CollectionSchema),
}

impl ValueType {
    pub fn key_type(&self) -> Option<&EnrichedValueType> {
        match self {
            ValueType::Basic(_) => None,
            ValueType::Struct(_) => None,
            ValueType::Collection(c) => c.key_type(),
        }
    }

    // Type equality, ignoring attributes.
    pub fn without_attrs(&self) -> Self {
        match self {
            ValueType::Basic(a) => ValueType::Basic(a.clone()),
            ValueType::Struct(a) => ValueType::Struct(a.without_attrs()),
            ValueType::Collection(a) => ValueType::Collection(a.without_attrs()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnrichedValueType<DataType = ValueType> {
    #[serde(rename = "type")]
    pub typ: DataType,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nullable: bool,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attrs: Arc<BTreeMap<String, serde_json::Value>>,
}

impl EnrichedValueType {
    pub fn without_attrs(&self) -> Self {
        Self {
            typ: self.typ.without_attrs(),
            nullable: self.nullable,
            attrs: Default::default(),
        }
    }
}

impl<DataType> EnrichedValueType<DataType> {
    pub fn from_alternative<AltDataType>(
        value_type: &EnrichedValueType<AltDataType>,
    ) -> Result<Self>
    where
        for<'a> &'a AltDataType: TryInto<DataType, Error = anyhow::Error>,
    {
        Ok(Self {
            typ: (&value_type.typ).try_into()?,
            nullable: value_type.nullable,
            attrs: value_type.attrs.clone(),
        })
    }

    pub fn with_attr(mut self, key: &str, value: serde_json::Value) -> Self {
        Arc::make_mut(&mut self.attrs).insert(key.to_string(), value);
        self
    }
}

impl std::fmt::Display for EnrichedValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.typ)?;
        if self.nullable {
            write!(f, "?")?;
        }
        if !self.attrs.is_empty() {
            write!(
                f,
                " [{}]",
                self.attrs
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Basic(b) => write!(f, "{}", b),
            ValueType::Struct(s) => write!(f, "{}", s),
            ValueType::Collection(c) => write!(f, "{}", c),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldSchema<DataType = ValueType> {
    /// ID is used to identify the field in the schema.
    pub name: FieldName,

    #[serde(flatten)]
    pub value_type: EnrichedValueType<DataType>,
}

impl FieldSchema {
    pub fn new(name: impl ToString, value_type: EnrichedValueType) -> Self {
        Self {
            name: name.to_string(),
            value_type,
        }
    }

    pub fn without_attrs(&self) -> Self {
        Self {
            name: self.name.clone(),
            value_type: self.value_type.without_attrs(),
        }
    }
}

impl<DataType> FieldSchema<DataType> {
    pub fn from_alternative<AltDataType>(field: &FieldSchema<AltDataType>) -> Result<Self>
    where
        for<'a> &'a AltDataType: TryInto<DataType, Error = anyhow::Error>,
    {
        Ok(Self {
            name: field.name.clone(),
            value_type: EnrichedValueType::from_alternative(&field.value_type)?,
        })
    }
}

impl std::fmt::Display for FieldSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.value_type)
    }
}

/// Top-level schema for a flow instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSchema {
    pub schema: StructSchema,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub collectors: Vec<NamedSpec<StructSchema>>,
}

impl Deref for DataSchema {
    type Target = StructSchema;

    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}

pub struct OpArgSchema {
    pub name: OpArgName,
    pub value_type: EnrichedValueType,
    pub analyzed_value: AnalyzedValueMapping,
}
