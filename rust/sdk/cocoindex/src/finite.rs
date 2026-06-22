//! A no-op `serde::Serializer` that rejects non-finite floats (`NaN`/`±Inf`).
//!
//! Target connectors convert a row to `serde_json::Value` to build SQL literals.
//! `serde_json` silently maps a non-finite `f32`/`f64` to JSON `null`, so a real
//! (but non-finite) value would become a SQL `NULL` (silent data loss) — or, for
//! a `NOT NULL` column, fail with a misleading "got null" error. Running a row
//! through [`ensure_finite`] first turns that into a clear, early error.

use std::fmt::{self, Display};

use serde::{Serialize, ser};

/// Error carrying a human-readable message about a non-finite float.
#[derive(Debug)]
pub struct NonFinite(String);

impl Display for NonFinite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for NonFinite {}
impl ser::Error for NonFinite {
    fn custom<T: Display>(msg: T) -> Self {
        NonFinite(msg.to_string())
    }
}

/// Return `Err(message)` if `value` contains any non-finite `f32`/`f64`.
pub fn ensure_finite<T: Serialize + ?Sized>(value: &T) -> Result<(), String> {
    value.serialize(FiniteChecker).map_err(|e| e.0)
}

#[derive(Clone, Copy)]
struct FiniteChecker;

fn check_f64(v: f64) -> Result<(), NonFinite> {
    if v.is_finite() {
        Ok(())
    } else {
        Err(NonFinite(format!("non-finite floating-point value ({v})")))
    }
}

impl ser::Serializer for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_f32(self, v: f32) -> Result<(), NonFinite> {
        check_f64(v as f64)
    }
    fn serialize_f64(self, v: f64) -> Result<(), NonFinite> {
        check_f64(v)
    }

    // All other scalars are always finite / float-free.
    fn serialize_bool(self, _: bool) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i8(self, _: i8) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i16(self, _: i16) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i32(self, _: i32) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i64(self, _: i64) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_i128(self, _: i128) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u8(self, _: u8) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u16(self, _: u16) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u32(self, _: u32) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u64(self, _: u64) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_u128(self, _: u128) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_str(self, _: &str) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_none(self) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_unit(self) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), NonFinite> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), NonFinite> {
        Ok(())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), NonFinite> {
        v.serialize(self)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<(), NonFinite> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        v: &T,
    ) -> Result<(), NonFinite> {
        v.serialize(self)
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, NonFinite> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, NonFinite> {
        Ok(self)
    }
}

impl ser::SerializeSeq for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeTuple for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeTupleStruct for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeTupleVariant for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeMap for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, k: &T) -> Result<(), NonFinite> {
        k.serialize(FiniteChecker)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeStruct for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        v: &T,
    ) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}
impl ser::SerializeStructVariant for FiniteChecker {
    type Ok = ();
    type Error = NonFinite;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        v: &T,
    ) -> Result<(), NonFinite> {
        v.serialize(FiniteChecker)
    }
    fn end(self) -> Result<(), NonFinite> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_finite;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        id: i64,
        score: f64,
        embedding: Vec<f32>,
        note: Option<String>,
    }

    #[test]
    fn finite_row_ok() {
        let r = Row {
            id: 1,
            score: 0.5,
            embedding: vec![1.0, 2.0],
            note: Some("x".into()),
        };
        assert!(ensure_finite(&r).is_ok());
    }

    #[test]
    fn nan_scalar_rejected() {
        let r = Row {
            id: 1,
            score: f64::NAN,
            embedding: vec![1.0],
            note: None,
        };
        assert!(ensure_finite(&r).is_err());
    }

    #[test]
    fn inf_in_vector_rejected() {
        let r = Row {
            id: 1,
            score: 0.0,
            embedding: vec![1.0, f32::INFINITY],
            note: None,
        };
        assert!(ensure_finite(&r).is_err());
    }
}
