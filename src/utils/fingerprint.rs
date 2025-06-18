use anyhow::bail;
use base64::prelude::*;
use blake2::digest::typenum;
use blake2::{Blake2b, Digest};
use serde::Deserialize;
use serde::ser::{
    Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant, Serializer,
};

#[derive(Debug)]
pub struct FingerprinterError {
    msg: String,
}

impl std::fmt::Display for FingerprinterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FingerprinterError: {}", self.msg)
    }
}
impl std::error::Error for FingerprinterError {}
impl serde::ser::Error for FingerprinterError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        FingerprinterError {
            msg: format!("{msg}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fingerprint(pub [u8; 16]);

impl Fingerprint {
    pub fn to_base64(self) -> String {
        BASE64_STANDARD.encode(self.0)
    }

    pub fn from_base64(s: &str) -> anyhow::Result<Self> {
        let bytes = match s.len() {
            24 => BASE64_STANDARD.decode(s)?,

            // For backward compatibility. Some old version (<= v0.1.2) is using hex encoding.
            32 => hex::decode(s)?,
            _ => bail!("Encoded fingerprint length is unexpected: {}", s.len()),
        };
        match bytes.try_into() {
            Ok(bytes) => Ok(Fingerprint(bytes)),
            Err(e) => bail!("Fingerprint bytes length is unexpected: {}", e.len()),
        }
    }
}

impl Serialize for Fingerprint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_base64(&s).map_err(serde::de::Error::custom)
    }
}
#[derive(Clone, Default)]
pub struct Fingerprinter {
    hasher: Blake2b<typenum::U16>,
}

impl Fingerprinter {
    pub fn into_fingerprint(self) -> Fingerprint {
        Fingerprint(self.hasher.finalize().into())
    }

    pub fn with<S: Serialize + ?Sized>(self, value: &S) -> Result<Self, FingerprinterError> {
        let mut fingerprinter = self;
        value.serialize(&mut fingerprinter)?;
        Ok(fingerprinter)
    }

    pub fn write<S: Serialize + ?Sized>(&mut self, value: &S) -> Result<(), FingerprinterError> {
        value.serialize(self)
    }

    fn write_type_tag(&mut self, tag: &str) {
        self.hasher.update(tag.as_bytes());
        self.hasher.update(b";");
    }

    fn write_end_tag(&mut self) {
        self.hasher.update(b".");
    }

    fn write_varlen_bytes(&mut self, bytes: &[u8]) {
        self.write_usize(bytes.len());
        self.hasher.update(bytes);
    }

    fn write_usize(&mut self, value: usize) {
        self.hasher.update((value as u32).to_le_bytes());
    }
}

impl Serializer for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<(), Self::Error> {
        self.write_type_tag(if v { "t" } else { "f" });
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), Self::Error> {
        self.write_type_tag("i1");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<(), Self::Error> {
        self.write_type_tag("i2");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<(), Self::Error> {
        self.write_type_tag("i4");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<(), Self::Error> {
        self.write_type_tag("i8");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), Self::Error> {
        self.write_type_tag("u1");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<(), Self::Error> {
        self.write_type_tag("u2");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<(), Self::Error> {
        self.write_type_tag("u4");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<(), Self::Error> {
        self.write_type_tag("u8");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), Self::Error> {
        self.write_type_tag("f4");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), Self::Error> {
        self.write_type_tag("f8");
        self.hasher.update(v.to_le_bytes());
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), Self::Error> {
        self.write_type_tag("c");
        self.write_usize(v as usize);
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<(), Self::Error> {
        self.write_type_tag("s");
        self.write_varlen_bytes(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Self::Error> {
        self.write_type_tag("b");
        self.write_varlen_bytes(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), Self::Error> {
        self.write_type_tag("");
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.write_type_tag("()");
        Ok(())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<(), Self::Error> {
        self.write_type_tag("US");
        self.write_varlen_bytes(name.as_bytes());
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<(), Self::Error> {
        self.write_type_tag("UV");
        self.write_varlen_bytes(name.as_bytes());
        self.write_varlen_bytes(variant.as_bytes());
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.write_type_tag("NS");
        self.write_varlen_bytes(name.as_bytes());
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.write_type_tag("NV");
        self.write_varlen_bytes(name.as_bytes());
        self.write_varlen_bytes(variant.as_bytes());
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.write_type_tag("L");
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.write_type_tag("T");
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.write_type_tag("TS");
        self.write_varlen_bytes(name.as_bytes());
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.write_type_tag("TV");
        self.write_varlen_bytes(name.as_bytes());
        self.write_varlen_bytes(variant.as_bytes());
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.write_type_tag("M");
        Ok(self)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.write_type_tag("S");
        self.write_varlen_bytes(name.as_bytes());
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.write_type_tag("SV");
        self.write_varlen_bytes(name.as_bytes());
        self.write_varlen_bytes(variant.as_bytes());
        Ok(self)
    }
}

impl SerializeSeq for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeTuple for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeTupleVariant for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeMap for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeStruct for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.hasher.update(key.as_bytes());
        self.hasher.update(b"\n");
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}

impl SerializeStructVariant for &mut Fingerprinter {
    type Ok = ();
    type Error = FingerprinterError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.hasher.update(key.as_bytes());
        self.hasher.update(b"\n");
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.write_end_tag();
        Ok(())
    }
}
