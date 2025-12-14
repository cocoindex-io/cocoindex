use crate::prelude::*;
use std::{fmt::Write as FmtWrite, io::Write};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StateKey {
    Null,
    Bool(bool),
    Int(i64),

    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    Uuid(uuid::Uuid),
    Array(Arc<[StateKey]>),
    Fingerprint(utils::fingerprint::Fingerprint),
}

impl std::fmt::Display for StateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateKey::Null => write!(f, "null"),
            StateKey::Bool(b) => write!(f, "{}", b),
            StateKey::Int(i) => write!(f, "{}", i),
            StateKey::Str(s) => {
                f.write_char('"')?;
                f.write_str(s.escape_default().to_string().as_str())?;
                f.write_char('"')
            }
            StateKey::Bytes(b) => {
                f.write_str("b\"")?;
                for &byte in b.iter() {
                    for esc in std::ascii::escape_default(byte) {
                        f.write_char(esc as char)?;
                    }
                }
                f.write_char('"')
            }
            StateKey::Uuid(u) => write!(f, "{}", u.to_string()),
            StateKey::Array(a) => {
                f.write_char('[')?;
                for (i, part) in a.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    part.fmt(f)?;
                }
                f.write_char(']')
            }
            StateKey::Fingerprint(fp) => write!(f, "{fp}"),
        }
    }
}

impl storekey::Encode for StateKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            StateKey::Null => {
                e.write_u8(2)?;
            }
            StateKey::Bool(false) => {
                e.write_u8(3)?;
            }
            StateKey::Bool(true) => {
                e.write_u8(4)?;
            }
            StateKey::Int(i) => {
                e.write_u8(5)?;
                e.write_i64(*i)?;
            }
            StateKey::Str(s) => {
                e.write_u8(6)?;
                e.write_slice(s.as_bytes())?;
            }
            StateKey::Bytes(b) => {
                e.write_u8(7)?;
                e.write_slice(b.as_ref())?;
            }
            StateKey::Uuid(u) => {
                e.write_u8(8)?;
                e.write_array(*u.as_bytes())?;
            }
            StateKey::Array(a) => {
                e.write_u8(9)?;
                storekey::Encode::encode(a.as_ref(), e)?;
            }
            StateKey::Fingerprint(fp) => {
                e.write_u8(10)?;
                storekey::Encode::encode(fp, e)?;
            }
        }
        Ok(())
    }
}

impl storekey::Decode for StateKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        match d.read_u8()? {
            2 => Ok(StateKey::Null),
            3 => Ok(StateKey::Bool(false)),
            4 => Ok(StateKey::Bool(true)),
            5 => Ok(StateKey::Int(d.read_i64()?)),
            6 => Ok(StateKey::Str(d.read_string()?.into())),
            7 => Ok(StateKey::Bytes(Arc::from(d.read_vec()?))),
            8 => {
                let bytes: [u8; 16] = d.read_array()?;
                Ok(StateKey::Uuid(uuid::Uuid::from_bytes(bytes)))
            }
            9 => {
                let v: Vec<StateKey> = storekey::Decode::decode(d)?;
                Ok(StateKey::Array(Arc::from(v)))
            }
            10 => {
                let fp: utils::fingerprint::Fingerprint = storekey::Decode::decode(d)?;
                Ok(StateKey::Fingerprint(fp))
            }
            _ => Err(storekey::DecodeError::InvalidFormat),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatePathRef<'a>(pub &'a [StateKey]);

impl<'a> std::fmt::Display for StatePathRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_char('/');
        }
        for part in self.0.iter() {
            f.write_str("/")?;
            part.fmt(f)?;
        }
        Ok(())
    }
}

impl<'a> From<&'a [StateKey]> for StatePathRef<'a> {
    fn from(value: &'a [StateKey]) -> Self {
        StatePathRef(value)
    }
}

impl<'a> std::ops::Deref for StatePathRef<'a> {
    type Target = [StateKey];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'p> StatePathRef<'p> {
    pub fn strip_parent(&self, parent: StatePathRef) -> Result<Self> {
        if self.0.len() < parent.0.len() || &self.0[..parent.0.len()] != parent.0 {
            bail!("Path {self} is not a child of parent {parent}");
        }
        Ok(StatePathRef(&self.0[parent.0.len()..]))
    }

    pub fn concat(&self, other: StatePathRef) -> StatePath {
        StatePath(self.0.iter().chain(other.0.iter()).cloned().collect())
    }

    pub fn concat_part(&self, part: StateKey) -> StatePath {
        StatePath(
            self.0
                .iter()
                .cloned()
                .chain(std::iter::once(part))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatePath(pub Arc<[StateKey]>);

impl storekey::Encode for StatePath {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        storekey::Encode::encode(self.0.as_ref(), e)
    }
}

impl storekey::Decode for StatePath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let items: Vec<StateKey> = storekey::Decode::decode(d)?;
        Ok(StatePath(Arc::from(items)))
    }
}

static ROOT_PATH: LazyLock<StatePath> = LazyLock::new(|| StatePath(Arc::new([])));

impl StatePath {
    pub fn root() -> Self {
        ROOT_PATH.clone()
    }

    pub fn concat_part(&self, part: StateKey) -> Self {
        self.as_ref().concat_part(part)
    }

    pub fn concat(&self, other: StatePathRef) -> StatePath {
        self.as_ref().concat(other)
    }

    pub fn as_ref<'a>(&'a self) -> StatePathRef<'a> {
        StatePathRef(self.0.as_ref())
    }
}

impl std::fmt::Display for StatePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        StatePathRef(self.0.as_ref()).fmt(f)
    }
}

impl std::ops::Deref for StatePath {
    type Target = [StateKey];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> std::borrow::Borrow<[StateKey]> for StatePath {
    fn borrow(&self) -> &[StateKey] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip<T>(value: &T) -> T
    where
        T: storekey::Encode + storekey::Decode + PartialEq + std::fmt::Debug,
    {
        let buf = storekey::encode_vec(value).expect("encode");
        let decoded: T = storekey::decode(Cursor::new(&buf)).expect("decode");
        decoded
    }

    #[test]
    fn state_key_roundtrip() {
        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);
        let cases = vec![
            StateKey::Null,
            StateKey::Bool(false),
            StateKey::Bool(true),
            StateKey::Int(0),
            StateKey::Int(-1),
            StateKey::Int(i64::MIN / 2),
            StateKey::Int(i64::MAX / 2),
            StateKey::Str(Arc::from("hello")),
            StateKey::Str(Arc::from("nul\0inside")),
            StateKey::Bytes(Arc::from(&b"bytes\x00with\x01escapes"[..])),
            StateKey::Uuid(uuid),
            StateKey::Array(Arc::from([
                StateKey::Int(1),
                StateKey::Str(Arc::from("a")),
                StateKey::Bytes(Arc::from(&b"\0"[..])),
            ])),
            StateKey::Fingerprint(fp),
        ];

        for original in cases {
            let decoded = roundtrip(&original);
            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn state_path_roundtrip() {
        let path = StatePath(Arc::from(vec![
            StateKey::Int(42),
            StateKey::Str(Arc::from("part")),
            StateKey::Bytes(Arc::from(&b"\0term"[..])),
        ]));
        let decoded = roundtrip(&path);
        assert_eq!(decoded, path);

        let empty = StatePath::root();
        let decoded_empty = roundtrip(&empty);
        assert_eq!(decoded_empty, empty);
    }
}
