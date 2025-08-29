use std::borrow::Cow;
use std::fmt::Display;

use serde::Serialize;
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use sqlx::Type;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, Postgres};

pub fn strip_zero_code<'a>(s: Cow<'a, str>) -> Cow<'a, str> {
    if s.contains('\0') {
        let mut sanitized = String::with_capacity(s.len());
        for ch in s.chars() {
            if ch != '\0' {
                sanitized.push(ch);
            }
        }
        Cow::Owned(sanitized)
    } else {
        s
    }
}

/// A thin wrapper for sqlx parameter binding that strips NUL (\0) bytes
/// from the wrapped string before encoding.
///
/// Usage: wrap a string reference when binding:
/// `query.bind(ZeroCodeStrippedEncode(my_str))`
#[derive(Copy, Clone, Debug)]
pub struct ZeroCodeStrippedEncode<'a>(pub &'a str);

impl<'a> Type<Postgres> for ZeroCodeStrippedEncode<'a> {
    fn type_info() -> <Postgres as sqlx::Database>::TypeInfo {
        <&'a str as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &<Postgres as sqlx::Database>::TypeInfo) -> bool {
        <&'a str as Type<Postgres>>::compatible(ty)
    }
}

impl<'a> Encode<'a, Postgres> for ZeroCodeStrippedEncode<'a> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let sanitized = strip_zero_code(Cow::Borrowed(self.0));
        <&str as Encode<'a, Postgres>>::encode_by_ref(&sanitized.as_ref(), buf)
    }

    fn size_hint(&self) -> usize {
        self.0.len()
    }
}

/// A wrapper that sanitizes zero bytes from strings during serialization.
///
/// It ensures:
/// - All string values have zero bytes removed
/// - Struct field names are sanitized before being written
/// - Map keys and any nested content are sanitized recursively
pub struct ZeroCodeStrippedSerialize<T>(pub T);

impl<T> Serialize for ZeroCodeStrippedSerialize<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let sanitizing = SanitizingSerializer { inner: serializer };
        self.0.serialize(sanitizing)
    }
}

/// Internal serializer wrapper that strips zero bytes from strings and sanitizes
/// struct field names by routing struct serialization through maps with sanitized keys.
struct SanitizingSerializer<S> {
    inner: S,
}

// Helper newtype to apply sanitizing serializer to any &T during nested serialization
struct SanitizeRef<'a, T: ?Sized>(&'a T);

impl<'a, T> Serialize for SanitizeRef<'a, T>
where
    T: ?Sized + Serialize,
{
    fn serialize<S1>(
        &self,
        serializer: S1,
    ) -> Result<<S1 as serde::Serializer>::Ok, <S1 as serde::Serializer>::Error>
    where
        S1: serde::Serializer,
    {
        let sanitizing = SanitizingSerializer { inner: serializer };
        self.0.serialize(sanitizing)
    }
}

// Seq wrapper to sanitize nested elements
struct SanitizingSerializeSeq<S: serde::Serializer> {
    inner: S::SerializeSeq,
}

impl<S> SerializeSeq for SanitizingSerializeSeq<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_element(&SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

// Tuple wrapper
struct SanitizingSerializeTuple<S: serde::Serializer> {
    inner: S::SerializeTuple,
}

impl<S> SerializeTuple for SanitizingSerializeTuple<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_element(&SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

// Tuple struct wrapper
struct SanitizingSerializeTupleStruct<S: serde::Serializer> {
    inner: S::SerializeTupleStruct,
}

impl<S> SerializeTupleStruct for SanitizingSerializeTupleStruct<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_field(&SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

// Tuple variant wrapper
struct SanitizingSerializeTupleVariant<S: serde::Serializer> {
    inner: S::SerializeTupleVariant,
}

impl<S> SerializeTupleVariant for SanitizingSerializeTupleVariant<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_field(&SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

// Map wrapper; ensures keys and values are sanitized
struct SanitizingSerializeMap<S: serde::Serializer> {
    inner: S::SerializeMap,
}

impl<S> SerializeMap for SanitizingSerializeMap<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_key(&SanitizeRef(key))
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_value(&SanitizeRef(value))
    }

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), Self::Error>
    where
        K: ?Sized + Serialize,
        V: ?Sized + Serialize,
    {
        self.inner
            .serialize_entry(&SanitizeRef(key), &SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

// Struct wrapper: implement via inner map to allow dynamic, sanitized field names
struct SanitizingSerializeStruct<S: serde::Serializer> {
    inner: S::SerializeMap,
}

impl<S> SerializeStruct for SanitizingSerializeStruct<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner
            .serialize_entry(&SanitizeRef(&key), &SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

impl<S> serde::Serializer for SanitizingSerializer<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = SanitizingSerializeSeq<S>;
    type SerializeTuple = SanitizingSerializeTuple<S>;
    type SerializeTupleStruct = SanitizingSerializeTupleStruct<S>;
    type SerializeTupleVariant = SanitizingSerializeTupleVariant<S>;
    type SerializeMap = SanitizingSerializeMap<S>;
    type SerializeStruct = SanitizingSerializeStruct<S>;
    type SerializeStructVariant = SanitizingSerializeStructVariant<S>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_i64(v)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_u64(v)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_f32(v)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_f64(v)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        // A single char cannot contain a NUL; forward directly
        self.inner.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let sanitized = strip_zero_code(Cow::Borrowed(v));
        self.inner.serialize_str(sanitized.as_ref())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_none()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner.serialize_some(&SanitizeRef(value))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        // Type names are not field names; forward
        self.inner.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        // Variant names are not field names; forward
        self.inner
            .serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner
            .serialize_newtype_struct(name, &SanitizeRef(value))
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.inner
            .serialize_newtype_variant(name, variant_index, variant, &SanitizeRef(value))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SanitizingSerializeSeq {
            inner: self.inner.serialize_seq(len)?,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(SanitizingSerializeTuple {
            inner: self.inner.serialize_tuple(len)?,
        })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(SanitizingSerializeTupleStruct {
            inner: self.inner.serialize_tuple_struct(name, len)?,
        })
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SanitizingSerializeTupleVariant {
            inner: self
                .inner
                .serialize_tuple_variant(name, variant_index, variant, len)?,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SanitizingSerializeMap {
            inner: self.inner.serialize_map(len)?,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        // Route through a map so we can provide dynamically sanitized field names
        Ok(SanitizingSerializeStruct {
            inner: self.inner.serialize_map(Some(len))?,
        })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SanitizingSerializeStructVariant {
            inner: self
                .inner
                .serialize_struct_variant(name, variant_index, variant, len)?,
        })
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }

    fn collect_str<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Display,
    {
        let s = value.to_string();
        let sanitized = strip_zero_code(Cow::Owned(s));
        self.inner.serialize_str(sanitized.as_ref())
    }
}

// Struct variant wrapper: sanitize field names and nested values
struct SanitizingSerializeStructVariant<S: serde::Serializer> {
    inner: S::SerializeStructVariant,
}

impl<S> SerializeStructVariant for SanitizingSerializeStructVariant<S>
where
    S: serde::Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Cannot allocate dynamic field names here due to &'static str bound.
        // Sanitize only values.
        self.inner.serialize_field(key, &SanitizeRef(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::{Value, json};
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    #[test]
    fn strip_zero_code_no_change_borrowed() {
        let input = "abc";
        let out = strip_zero_code(Cow::Borrowed(input));
        assert!(matches!(out, Cow::Borrowed(_)));
        assert_eq!(out.as_ref(), "abc");
    }

    #[test]
    fn strip_zero_code_removes_nuls_owned() {
        let input = "a\0b\0c\0".to_string();
        let out = strip_zero_code(Cow::Owned(input));
        assert_eq!(out.as_ref(), "abc");
    }

    #[test]
    fn wrapper_sanitizes_plain_string_value() {
        let s = "he\0ll\0o";
        let v: Value = serde_json::to_value(ZeroCodeStrippedSerialize(s)).unwrap();
        assert_eq!(v, json!("hello"));
    }

    #[test]
    fn wrapper_sanitizes_map_keys_and_values() {
        let mut m = BTreeMap::new();
        m.insert("a\0b".to_string(), "x\0y".to_string());
        m.insert("\0start".to_string(), "en\0d".to_string());
        let v: Value = serde_json::to_value(ZeroCodeStrippedSerialize(&m)).unwrap();
        let obj = v.as_object().unwrap();
        assert_eq!(obj.get("ab").unwrap(), &json!("xy"));
        assert_eq!(obj.get("start").unwrap(), &json!("end"));
        assert!(!obj.contains_key("a\0b"));
        assert!(!obj.contains_key("\0start"));
    }

    #[derive(Serialize)]
    struct TestStruct {
        #[serde(rename = "fi\0eld")] // Intentionally includes NUL
        value: String,
        #[serde(rename = "n\0ested")] // Intentionally includes NUL
        nested: Inner,
    }

    #[derive(Serialize)]
    struct Inner {
        #[serde(rename = "n\0ame")] // Intentionally includes NUL
        name: String,
    }

    #[test]
    fn wrapper_sanitizes_struct_field_names_and_values() {
        let s = TestStruct {
            value: "hi\0!".to_string(),
            nested: Inner {
                name: "al\0ice".to_string(),
            },
        };
        let v: Value = serde_json::to_value(ZeroCodeStrippedSerialize(&s)).unwrap();
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("field"));
        assert!(obj.contains_key("nested"));
        assert_eq!(obj.get("field").unwrap(), &json!("hi!"));
        let nested = obj.get("nested").unwrap().as_object().unwrap();
        assert!(nested.contains_key("name"));
        assert_eq!(nested.get("name").unwrap(), &json!("alice"));
        assert!(!obj.contains_key("fi\0eld"));
    }

    #[derive(Serialize)]
    enum TestEnum {
        Var {
            #[serde(rename = "ke\0y")] // Intentionally includes NUL
            field: String,
        },
    }

    #[test]
    fn wrapper_sanitizes_struct_variant_values_only() {
        let e = TestEnum::Var {
            field: "b\0ar".to_string(),
        };
        let v: Value = serde_json::to_value(ZeroCodeStrippedSerialize(&e)).unwrap();
        // {"Var":{"key":"bar"}}
        let root = v.as_object().unwrap();
        let var = root.get("Var").unwrap().as_object().unwrap();
        // Field name remains unchanged due to &'static str constraint of SerializeStructVariant
        assert!(var.contains_key("ke\0y"));
        assert_eq!(var.get("ke\0y").unwrap(), &json!("bar"));
    }
}
