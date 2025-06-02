use crate::prelude::*;

use super::spec::*;
use crate::builder::plan::AnalyzedValueMapping;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorTypeSchema {
    pub element_type: Box<BasicValueType>,
    pub dimension: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnionTypeSchema {
    pub types: Vec<BasicValueType>,
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

    /// A UUID.
    Uuid,

    /// Date (without time within the current day).
    Date,

    /// Time of the day.
    Time,

    /// Local date and time, without timezone.
    LocalDateTime,

    /// Date and time with timezone.
    OffsetDateTime,

    /// A time duration.
    TimeDelta,

    /// A JSON value.
    Json,

    /// A vector of values (usually numbers, for embeddings).
    Vector(VectorTypeSchema),

    /// A union
    Union(UnionTypeSchema),
}

impl std::fmt::Display for BasicValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BasicValueType::Bytes => write!(f, "Bytes"),
            BasicValueType::Str => write!(f, "Str"),
            BasicValueType::Bool => write!(f, "Bool"),
            BasicValueType::Int64 => write!(f, "Int64"),
            BasicValueType::Float32 => write!(f, "Float32"),
            BasicValueType::Float64 => write!(f, "Float64"),
            BasicValueType::Range => write!(f, "Range"),
            BasicValueType::Uuid => write!(f, "Uuid"),
            BasicValueType::Date => write!(f, "Date"),
            BasicValueType::Time => write!(f, "Time"),
            BasicValueType::LocalDateTime => write!(f, "LocalDateTime"),
            BasicValueType::OffsetDateTime => write!(f, "OffsetDateTime"),
            BasicValueType::TimeDelta => write!(f, "TimeDelta"),
            BasicValueType::Json => write!(f, "Json"),
            BasicValueType::Vector(s) => {
                write!(f, "Vector[{}", s.element_type)?;
                if let Some(dimension) = s.dimension {
                    write!(f, ", {}", dimension)?;
                }
                write!(f, "]")
            }
            BasicValueType::Union(s) => {
                write!(f, "Union[")?;
                for (i, typ) in s.types.iter().enumerate() {
                    if i > 0 {
                        // Add type delimiter
                        write!(f, " | ")?;
                    }
                    write!(f, "{}", typ)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StructSchema {
    pub fields: Arc<Vec<FieldSchema>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Arc<str>>,
}

impl StructSchema {
    pub fn without_attrs(&self) -> Self {
        Self {
            fields: Arc::new(self.fields.iter().map(|f| f.without_attrs()).collect()),
            description: None,
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
pub enum TableKind {
    /// An table with unordered rows, without key.
    UTable,
    /// A table's first field is the key.
    #[serde(alias = "Table")]
    KTable,
    /// A table whose rows orders are preserved.
    #[serde(alias = "List")]
    LTable,
}

impl std::fmt::Display for TableKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableKind::UTable => write!(f, "Table"),
            TableKind::KTable => write!(f, "KTable"),
            TableKind::LTable => write!(f, "LTable"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    pub kind: TableKind,
    pub row: StructSchema,
}

impl TableSchema {
    pub fn has_key(&self) -> bool {
        match self.kind {
            TableKind::KTable => true,
            TableKind::UTable | TableKind::LTable => false,
        }
    }

    pub fn key_type(&self) -> Option<&EnrichedValueType> {
        match self.kind {
            TableKind::KTable => self
                .row
                .fields
                .first()
                .as_ref()
                .map(|field| &field.value_type),
            TableKind::UTable | TableKind::LTable => None,
        }
    }

    pub fn without_attrs(&self) -> Self {
        Self {
            kind: self.kind,
            row: self.row.without_attrs(),
        }
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.kind, self.row)
    }
}

impl TableSchema {
    pub fn new(kind: TableKind, row: StructSchema) -> Self {
        Self { kind, row }
    }

    pub fn key_field(&self) -> Option<&FieldSchema> {
        match self.kind {
            TableKind::KTable => Some(self.row.fields.first().unwrap()),
            TableKind::UTable | TableKind::LTable => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum ValueType {
    Struct(StructSchema),

    #[serde(untagged)]
    Basic(BasicValueType),

    #[serde(untagged)]
    Table(TableSchema),
}

impl ValueType {
    pub fn key_type(&self) -> Option<&EnrichedValueType> {
        match self {
            ValueType::Basic(_) => None,
            ValueType::Struct(_) => None,
            ValueType::Table(c) => c.key_type(),
        }
    }

    // Type equality, ignoring attributes.
    pub fn without_attrs(&self) -> Self {
        match self {
            ValueType::Basic(a) => ValueType::Basic(a.clone()),
            ValueType::Struct(a) => ValueType::Struct(a.without_attrs()),
            ValueType::Table(a) => ValueType::Table(a.without_attrs()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
            ValueType::Table(c) => write!(f, "{}", c),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorSchema {
    pub fields: Vec<FieldSchema>,
    /// If specified, the collector will have an automatically generated UUID field with the given index.
    pub auto_uuid_field_idx: Option<usize>,
}

impl std::fmt::Display for CollectorSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Collector(")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, ")")
    }
}

impl CollectorSchema {
    pub fn from_fields(fields: Vec<FieldSchema>, auto_uuid_field: Option<FieldName>) -> Self {
        let mut fields = fields;
        let auto_uuid_field_idx = if let Some(auto_uuid_field) = auto_uuid_field {
            fields.insert(
                0,
                FieldSchema::new(
                    auto_uuid_field,
                    EnrichedValueType {
                        typ: ValueType::Basic(BasicValueType::Uuid),
                        nullable: false,
                        attrs: Default::default(),
                    },
                ),
            );
            Some(0)
        } else {
            None
        };
        Self {
            fields,
            auto_uuid_field_idx,
        }
    }
    pub fn without_attrs(&self) -> Self {
        Self {
            fields: self.fields.iter().map(|f| f.without_attrs()).collect(),
            auto_uuid_field_idx: self.auto_uuid_field_idx,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpScopeSchema {
    /// Output schema for transform ops.
    pub op_output_types: HashMap<FieldName, EnrichedValueType>,

    /// Child op scope for foreach ops.
    pub op_scopes: HashMap<String, Arc<OpScopeSchema>>,

    /// Collectors for the current scope.
    pub collectors: Vec<NamedSpec<Arc<CollectorSchema>>>,
}

/// Top-level schema for a flow instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSchema {
    pub schema: StructSchema,

    pub root_op_scope: OpScopeSchema,
}

impl std::ops::Deref for FlowSchema {
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
