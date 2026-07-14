//! Derive-based table-schema construction — the Rust analogue of Python's
//! `TableSchema.from_class`.
//!
//! A row struct deriving [`SchemaFields`](cocoindex_macros::SchemaFields) reports
//! its columns as connector-agnostic [`LogicalType`]s; each target connector's
//! `TableSchema::from_row::<T>(primary_key)` maps those to its own SQL types
//! (mirroring the per-connector `_LEAF_TYPE_MAPPINGS` in Python's `from_class`).
//!
//! ```ignore
//! #[derive(serde::Serialize, cocoindex::SchemaFields)]
//! struct Doc {
//!     id: String,
//!     title: Option<String>,
//!     views: i64,
//!     #[coco(vector = 384)]
//!     embedding: Vec<f32>,
//! }
//! // Postgres:  id text NOT NULL, title text, views bigint NOT NULL,
//! //            embedding vector(384) NOT NULL
//! let schema = cocoindex::connectors::postgres::TableSchema::from_row::<Doc>(["id"])?;
//! ```
//!
//! Field attributes:
//! * `#[coco(vector = N)]` — a dense `f32` vector column of dimension `N`.
//! * `#[coco(vector = N, half)]` — a 16-bit (half-precision) vector column.
//! * `#[coco(type = "…")]` — a raw connector SQL type, used verbatim (the escape
//!   hatch matching Python's `PgType`/`SqliteType`/`DorisType`).
//! * `#[coco(json)]` — force JSON storage for a field.
//! * `#[coco(rename = "…")]` — use a different column name.

/// A connector-agnostic column type derived from a Rust field type. Each target
/// connector maps these to its own SQL type strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Text,
    Bytes,
    Uuid,
    Date,
    Time,
    DateTime,
    Duration,
    /// A complex value (collection / map / nested struct / `Any`) stored as JSON.
    Json,
    /// A dense float vector of fixed dimension (`half` → 16-bit element type).
    Vector {
        dim: u32,
        half: bool,
    },
    /// A raw, connector-specific SQL type string (`#[coco(type = "…")]`), used
    /// verbatim by each connector.
    Custom(String),
}

/// One column derived from a row-struct field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaField {
    /// Column name (the field name, or `#[coco(rename = "…")]`).
    pub name: String,
    /// Connector-agnostic column type.
    pub logical_type: LogicalType,
    /// Whether the column allows `NULL` (true for `Option<T>` fields).
    pub nullable: bool,
}

/// Implemented by `#[derive(SchemaFields)]` row structs: reports each field as a
/// connector-agnostic column. Pass an implementor's type to a connector's
/// `TableSchema::from_row::<T>()`.
pub trait SchemaFields {
    fn schema_fields() -> Vec<SchemaField>;
}
