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
    type SerializeTupleVariant = VariantSeqSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = MapSerializer;
    type SerializeStructVariant = VariantMapSerializer;

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
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(VariantSeqSerializer {
            variant_name: variant.to_owned(),
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
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(VariantMapSerializer {
            variant_name: variant.to_owned(),
            map: yaml_rust2::yaml::Hash::new(),
        })
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

pub struct VariantMapSerializer {
    variant_name: String,
    map: yaml_rust2::yaml::Hash,
}

impl ser::SerializeStructVariant for VariantMapSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.map.insert(
            Yaml::String(key.to_owned()),
            value.serialize(YamlSerializer)?,
        );
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut outer_map = yaml_rust2::yaml::Hash::new();
        outer_map.insert(Yaml::String(self.variant_name), Yaml::Hash(self.map));
        Ok(Yaml::Hash(outer_map))
    }
}

pub struct VariantSeqSerializer {
    variant_name: String,
    vec: Vec<Yaml>,
}

impl ser::SerializeTupleVariant for VariantSeqSerializer {
    type Ok = Yaml;
    type Error = YamlSerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.vec.push(value.serialize(YamlSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut map = yaml_rust2::yaml::Hash::new();
        map.insert(Yaml::String(self.variant_name), Yaml::Array(self.vec));
        Ok(Yaml::Hash(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::Error as SerdeSerError;
    use serde::{Serialize, Serializer};
    use std::collections::BTreeMap;
    use yaml_rust2::yaml::{Hash, Yaml};

    fn assert_yaml_serialization<T: Serialize>(value: T, expected_yaml: Yaml) {
        let result = YamlSerializer::serialize(&value);
        println!(
            "Serialized value: {:?}, Expected value: {:?}",
            result, expected_yaml
        );

        assert!(
            result.is_ok(),
            "Serialization failed when it should have succeeded. Error: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            expected_yaml,
            "Serialized YAML did not match expected YAML."
        );
    }

    #[test]
    fn test_serialize_bool() {
        assert_yaml_serialization(true, Yaml::Boolean(true));
        assert_yaml_serialization(false, Yaml::Boolean(false));
    }

    #[test]
    fn test_serialize_integers() {
        assert_yaml_serialization(42i8, Yaml::Integer(42));
        assert_yaml_serialization(-100i16, Yaml::Integer(-100));
        assert_yaml_serialization(123456i32, Yaml::Integer(123456));
        assert_yaml_serialization(7890123456789i64, Yaml::Integer(7890123456789));
        assert_yaml_serialization(255u8, Yaml::Integer(255));
        assert_yaml_serialization(65535u16, Yaml::Integer(65535));
        assert_yaml_serialization(4000000000u32, Yaml::Integer(4000000000));
        // u64 is serialized as Yaml::Real(String) in your implementation
        assert_yaml_serialization(
            18446744073709551615u64,
            Yaml::Real("18446744073709551615".to_string()),
        );
    }

    #[test]
    fn test_serialize_floats() {
        assert_yaml_serialization(3.14f32, Yaml::Real("3.14".to_string()));
        assert_yaml_serialization(-0.001f64, Yaml::Real("-0.001".to_string()));
        assert_yaml_serialization(1.0e10f64, Yaml::Real("10000000000".to_string()));
    }

    #[test]
    fn test_serialize_char() {
        assert_yaml_serialization('X', Yaml::String("X".to_string()));
        assert_yaml_serialization('✨', Yaml::String("✨".to_string()));
    }

    #[test]
    fn test_serialize_str_and_string() {
        assert_yaml_serialization("hello YAML", Yaml::String("hello YAML".to_string()));
        assert_yaml_serialization("".to_string(), Yaml::String("".to_string()));
    }

    #[test]
    fn test_serialize_raw_bytes() {
        let bytes_slice: &[u8] = &[0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let expected = Yaml::Array(vec![
            Yaml::Integer(72),
            Yaml::Integer(101),
            Yaml::Integer(108),
            Yaml::Integer(108),
            Yaml::Integer(111),
        ]);
        assert_yaml_serialization(bytes_slice, expected.clone());

        let bytes_vec: Vec<u8> = bytes_slice.to_vec();
        assert_yaml_serialization(bytes_vec, expected);

        let empty_bytes_slice: &[u8] = &[];
        assert_yaml_serialization(empty_bytes_slice, Yaml::Array(vec![]));
    }

    struct MyBytesWrapper<'a>(&'a [u8]);

    impl<'a> Serialize for MyBytesWrapper<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(self.0)
        }
    }

    #[test]
    fn test_custom_wrapper_serializes_bytes_as_base64_string() {
        let data: &[u8] = &[72, 101, 108, 108, 111]; // "Hello"
        let wrapped_data = MyBytesWrapper(data);

        let base64_encoded = BASE64_STANDARD.encode(data);
        let expected_yaml = Yaml::String(base64_encoded);

        assert_yaml_serialization(wrapped_data, expected_yaml);

        let empty_data: &[u8] = &[];
        let wrapped_empty_data = MyBytesWrapper(empty_data);
        let empty_base64_encoded = BASE64_STANDARD.encode(empty_data);
        let expected_empty_yaml = Yaml::String(empty_base64_encoded);
        assert_yaml_serialization(wrapped_empty_data, expected_empty_yaml);
    }

    #[test]
    fn test_serialize_option() {
        let val_none: Option<i32> = None;
        assert_yaml_serialization(val_none, Yaml::Null);

        let val_some: Option<String> = Some("has value".to_string());
        assert_yaml_serialization(val_some, Yaml::String("has value".to_string()));
    }

    #[test]
    fn test_serialize_unit() {
        assert_yaml_serialization((), Yaml::Hash(Hash::new()));
    }

    #[test]
    fn test_serialize_unit_struct() {
        #[derive(Serialize)]
        struct MyUnitStruct;

        assert_yaml_serialization(MyUnitStruct, Yaml::Hash(Hash::new()));
    }

    #[test]
    fn test_serialize_newtype_struct() {
        #[derive(Serialize)]
        struct MyNewtypeStruct(u64);

        assert_yaml_serialization(MyNewtypeStruct(12345u64), Yaml::Real("12345".to_string()));
    }

    #[test]
    fn test_serialize_seq() {
        let empty_vec: Vec<i32> = vec![];
        assert_yaml_serialization(empty_vec, Yaml::Array(vec![]));

        let simple_vec = vec![10, 20, 30];
        assert_yaml_serialization(
            simple_vec,
            Yaml::Array(vec![
                Yaml::Integer(10),
                Yaml::Integer(20),
                Yaml::Integer(30),
            ]),
        );

        let string_vec = vec!["a".to_string(), "b".to_string()];
        assert_yaml_serialization(
            string_vec,
            Yaml::Array(vec![
                Yaml::String("a".to_string()),
                Yaml::String("b".to_string()),
            ]),
        );
    }

    #[test]
    fn test_serialize_tuple() {
        let tuple_val = (42i32, "text", false);
        assert_yaml_serialization(
            tuple_val,
            Yaml::Array(vec![
                Yaml::Integer(42),
                Yaml::String("text".to_string()),
                Yaml::Boolean(false),
            ]),
        );
    }

    #[test]
    fn test_serialize_tuple_struct() {
        #[derive(Serialize)]
        struct MyTupleStruct(String, i64);

        assert_yaml_serialization(
            MyTupleStruct("value".to_string(), -500),
            Yaml::Array(vec![Yaml::String("value".to_string()), Yaml::Integer(-500)]),
        );
    }

    #[test]
    fn test_serialize_map() {
        let mut map = BTreeMap::new(); // BTreeMap for ordered keys, matching yaml::Hash
        map.insert("key1".to_string(), 100);
        map.insert("key2".to_string(), 200);

        let mut expected_hash = Hash::new();
        expected_hash.insert(Yaml::String("key1".to_string()), Yaml::Integer(100));
        expected_hash.insert(Yaml::String("key2".to_string()), Yaml::Integer(200));
        assert_yaml_serialization(map, Yaml::Hash(expected_hash));

        let empty_map: BTreeMap<String, i32> = BTreeMap::new();
        assert_yaml_serialization(empty_map, Yaml::Hash(Hash::new()));
    }

    #[derive(Serialize)]
    struct SimpleStruct {
        id: u32,
        name: String,
        is_active: bool,
    }

    #[test]
    fn test_serialize_struct() {
        let s = SimpleStruct {
            id: 101,
            name: "A Struct".to_string(),
            is_active: true,
        };
        let mut expected_hash = Hash::new();
        expected_hash.insert(Yaml::String("id".to_string()), Yaml::Integer(101));
        expected_hash.insert(
            Yaml::String("name".to_string()),
            Yaml::String("A Struct".to_string()),
        );
        expected_hash.insert(Yaml::String("is_active".to_string()), Yaml::Boolean(true));
        assert_yaml_serialization(s, Yaml::Hash(expected_hash));
    }

    #[derive(Serialize)]
    struct NestedStruct {
        description: String,
        data: SimpleStruct,
        tags: Vec<String>,
    }

    #[test]
    fn test_serialize_nested_struct() {
        let ns = NestedStruct {
            description: "Contains another struct and a vec".to_string(),
            data: SimpleStruct {
                id: 202,
                name: "Inner".to_string(),
                is_active: false,
            },
            tags: vec!["nested".to_string(), "complex".to_string()],
        };

        let mut inner_struct_hash = Hash::new();
        inner_struct_hash.insert(Yaml::String("id".to_string()), Yaml::Integer(202));
        inner_struct_hash.insert(
            Yaml::String("name".to_string()),
            Yaml::String("Inner".to_string()),
        );
        inner_struct_hash.insert(Yaml::String("is_active".to_string()), Yaml::Boolean(false));

        let tags_array = Yaml::Array(vec![
            Yaml::String("nested".to_string()),
            Yaml::String("complex".to_string()),
        ]);

        let mut expected_hash = Hash::new();
        expected_hash.insert(
            Yaml::String("description".to_string()),
            Yaml::String("Contains another struct and a vec".to_string()),
        );
        expected_hash.insert(
            Yaml::String("data".to_string()),
            Yaml::Hash(inner_struct_hash),
        );
        expected_hash.insert(Yaml::String("tags".to_string()), tags_array);

        assert_yaml_serialization(ns, Yaml::Hash(expected_hash));
    }

    #[derive(Serialize)]
    enum MyEnum {
        Unit,
        Newtype(i32),
        Tuple(String, bool),
        Struct { field_a: u16, field_b: char },
    }

    #[test]
    fn test_serialize_enum_unit_variant() {
        assert_yaml_serialization(MyEnum::Unit, Yaml::String("Unit".to_string()));
    }

    #[test]
    fn test_serialize_enum_newtype_variant() {
        let mut expected_hash = Hash::new();
        expected_hash.insert(Yaml::String("Newtype".to_string()), Yaml::Integer(999));
        assert_yaml_serialization(MyEnum::Newtype(999), Yaml::Hash(expected_hash));
    }

    #[test]
    fn test_serialize_enum_tuple_variant() {
        let mut expected_hash = Hash::new();
        let inner_array = Yaml::Array(vec![
            Yaml::String("tuple_data".to_string()),
            Yaml::Boolean(true),
        ]);
        expected_hash.insert(Yaml::String("Tuple".to_string()), inner_array);
        assert_yaml_serialization(
            MyEnum::Tuple("tuple_data".to_string(), true),
            Yaml::Hash(expected_hash),
        );
    }

    #[test]
    fn test_serialize_enum_struct_variant() {
        let mut inner_struct_hash = Hash::new();
        inner_struct_hash.insert(Yaml::String("field_a".to_string()), Yaml::Integer(123));
        inner_struct_hash.insert(
            Yaml::String("field_b".to_string()),
            Yaml::String("Z".to_string()),
        );

        let mut expected_hash = Hash::new();
        expected_hash.insert(
            Yaml::String("Struct".to_string()),
            Yaml::Hash(inner_struct_hash),
        );
        assert_yaml_serialization(
            MyEnum::Struct {
                field_a: 123,
                field_b: 'Z',
            },
            Yaml::Hash(expected_hash),
        );
    }

    #[test]
    fn test_yaml_serializer_error_display() {
        let error = YamlSerializerError {
            msg: "A test error message".to_string(),
        };
        assert_eq!(
            format!("{}", error),
            "YamlSerializerError: A test error message"
        );
    }

    #[test]
    fn test_yaml_serializer_error_custom() {
        let error = YamlSerializerError::custom("Custom error detail");
        assert_eq!(error.msg, "Custom error detail");
        assert_eq!(
            format!("{}", error),
            "YamlSerializerError: Custom error detail"
        );
        let _err_trait_obj: Box<dyn std::error::Error> = Box::new(error);
    }
}
