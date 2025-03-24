use base64::prelude::*;
use serde::ser::{self, Serialize};
use yaml_rust2::yaml::Yaml;

#[derive(Debug)]
pub struct YamlSerializerError {
    msg: String,
}

impl std::fmt::Display for YamlSerializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "YamlSerializerError: {}", self.msg)
    }
}

impl std::error::Error for YamlSerializerError {}

impl ser::Error for YamlSerializerError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        YamlSerializerError {
            msg: format!("{msg}"),
        }
    }
}

pub struct YamlSerializer;

impl YamlSerializer {
    pub fn serialize<T>(value: &T) -> Result<Yaml, YamlSerializerError>
    where
        T: Serialize,
    {
        value.serialize(YamlSerializer)
    }
}

impl ser::Serializer for YamlSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = SeqSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = MapSerializer;
    type SerializeStructVariant = MapSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Integer(v as i64))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Real(v.to_string()))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Real(v.to_string()))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Real(v.to_string()))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::String(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::String(v.to_owned()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let encoded = BASE64_STANDARD.encode(v);
        Ok(Yaml::String(encoded))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Null)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Hash(Default::default()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Hash(Default::default()))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::String(variant.to_owned()))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let mut hash = yaml_rust2::yaml::Hash::new();
        hash.insert(Yaml::String(variant.to_owned()), value.serialize(self)?);
        Ok(Yaml::Hash(hash))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SeqSerializer {
            vec: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SeqSerializer {
            vec: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapSerializer {
            map: yaml_rust2::yaml::Hash::new(),
            next_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_map(Some(len))
    }
}

pub struct SeqSerializer {
    vec: Vec<Yaml>,
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.vec.push(value.serialize(YamlSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Array(self.vec))
    }
}

impl ser::SerializeTuple for SeqSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for SeqSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleVariant for SeqSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

pub struct MapSerializer {
    map: yaml_rust2::yaml::Hash,
    next_key: Option<Yaml>,
}

impl ser::SerializeMap for MapSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.next_key = Some(key.serialize(YamlSerializer)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let key = self.next_key.take().unwrap();
        self.map.insert(key, value.serialize(YamlSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Yaml::Hash(self.map))
    }
}

impl ser::SerializeStruct for MapSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeMap::end(self)
    }
}

impl ser::SerializeStructVariant for MapSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeMap::end(self)
    }
}
