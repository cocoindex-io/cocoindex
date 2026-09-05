use crate::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt::Write as FmtWrite, io::Write};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StableKey {
    Null,
    Symbol(Arc<str>),
    Bool(bool),
    Int(i64),
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    Uuid(uuid::Uuid),
    Array(Arc<[StableKey]>),
    Fingerprint(utils::fingerprint::Fingerprint),
}

impl Serialize for StableKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        match self {
            StableKey::Null => serializer.serialize_unit(),
            StableKey::Bool(b) => serializer.serialize_bool(*b),
            StableKey::Int(i) => serializer.serialize_i64(*i),
            StableKey::Str(s) => serializer.serialize_str(s),
            StableKey::Bytes(b) => serializer.serialize_bytes(b.as_ref()),
            StableKey::Uuid(u) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("uuid", u)?;
                map.end()
            }
            StableKey::Array(a) => a.as_ref().serialize(serializer),
            StableKey::Fingerprint(fp) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("fp", fp)?;
                map.end()
            }
            StableKey::Symbol(s) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("sym", s.as_ref())?;
                map.end()
            }
        }
    }
}

/// Decodes one `StableKey` straight off a self-describing format, reading
/// exactly what the format carries: seq -> `Array`, bytes -> `Bytes`,
/// str -> `Str`, one-entry map -> tagged variant (`Uuid` / `Fingerprint` /
/// `Symbol`).
struct StableKeyVisitor;

impl<'de> de::Visitor<'de> for StableKeyVisitor {
    type Value = StableKey;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a stable key")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<StableKey, E> {
        Ok(StableKey::Null)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<StableKey, E> {
        Ok(StableKey::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<StableKey, E> {
        Ok(StableKey::Int(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<StableKey, E> {
        i64::try_from(v)
            .map(StableKey::Int)
            .map_err(|_| de::Error::custom(format!("integer {v} out of range for i64")))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<StableKey, E> {
        Ok(StableKey::Str(Arc::from(v)))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<StableKey, E> {
        Ok(StableKey::Bytes(Arc::from(v)))
    }

    fn visit_seq<A: de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<StableKey, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element::<StableKey>()? {
            items.push(item);
        }
        Ok(StableKey::Array(Arc::from(items)))
    }

    /// Tagged variants are written as a one-entry map. Values are read through
    /// `next_value` so `Uuid`/`Fingerprint` decode against the real format
    /// (msgpack `bin` here, base64 text in human-readable formats).
    fn visit_map<A: de::MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<StableKey, A::Error> {
        const TAGS: &[&str] = &["uuid", "fp", "sym"];
        let Some(tag) = map.next_key::<String>()? else {
            return Err(de::Error::invalid_length(0, &"a one-entry tagged map"));
        };
        let key = match tag.as_str() {
            "uuid" => StableKey::Uuid(map.next_value()?),
            "fp" => StableKey::Fingerprint(map.next_value()?),
            "sym" => StableKey::Symbol(Arc::from(map.next_value::<String>()?)),
            other => return Err(de::Error::unknown_field(other, TAGS)),
        };
        if map.next_key::<de::IgnoredAny>()?.is_some() {
            return Err(de::Error::custom(
                "tagged stable key must have exactly one entry",
            ));
        }
        Ok(key)
    }
}

impl<'de> Deserialize<'de> for StableKey {
    /// Round trip is guaranteed only for formats that natively distinguish
    /// array / bin / str, such as MessagePack, which is what the state store
    /// uses. Formats without a bytes type (JSON) serialize `Bytes` as an
    /// integer sequence and read it back as `Array`; nothing in production
    /// deserializes a `StableKey` from such formats.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(StableKeyVisitor)
    }
}

/// Whether a symbol name can be rendered without quotes, i.e. it can't be
/// confused with the path/array syntax around it (`/`, `,`, `]`) or with a
/// quoted form. Anything else is rendered as `@"..."`.
fn is_bare_symbol_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn write_quoted_str(f: &mut std::fmt::Formatter<'_>, s: &str) -> std::fmt::Result {
    f.write_char('"')?;
    for c in s.chars() {
        for esc in c.escape_default() {
            f.write_char(esc)?;
        }
    }
    f.write_char('"')
}

impl std::fmt::Display for StableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StableKey::Null => write!(f, "null"),
            StableKey::Bool(b) => write!(f, "{}", b),
            StableKey::Int(i) => write!(f, "{}", i),
            StableKey::Str(s) => write_quoted_str(f, s),
            StableKey::Bytes(b) => {
                f.write_str("b\"")?;
                for &byte in b.iter() {
                    for esc in std::ascii::escape_default(byte) {
                        f.write_char(esc as char)?;
                    }
                }
                f.write_char('"')
            }
            StableKey::Uuid(u) => write!(f, "{}", u.to_string()),
            StableKey::Array(a) => {
                f.write_char('[')?;
                for (i, part) in a.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    part.fmt(f)?;
                }
                f.write_char(']')
            }
            StableKey::Fingerprint(fp) => write!(f, "{fp}"),
            StableKey::Symbol(s) => {
                f.write_char('@')?;
                if is_bare_symbol_name(s) {
                    f.write_str(s)
                } else {
                    write_quoted_str(f, s)
                }
            }
        }
    }
}

impl storekey::Encode for StableKey {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        match self {
            StableKey::Null => {
                e.write_u8(2)?;
            }
            StableKey::Symbol(s) => {
                e.write_u8(3)?;
                e.write_slice(s.as_bytes())?;
            }
            StableKey::Bool(false) => {
                e.write_u8(4)?;
                e.write_u8(0)?;
            }
            StableKey::Bool(true) => {
                e.write_u8(4)?;
                e.write_u8(1)?;
            }
            StableKey::Int(i) => {
                e.write_u8(5)?;
                e.write_i64(*i)?;
            }
            StableKey::Str(s) => {
                e.write_u8(6)?;
                e.write_slice(s.as_bytes())?;
            }
            StableKey::Bytes(b) => {
                e.write_u8(7)?;
                e.write_slice(b.as_ref())?;
            }
            StableKey::Uuid(u) => {
                e.write_u8(8)?;
                e.write_array(*u.as_bytes())?;
            }
            StableKey::Array(a) => {
                e.write_u8(9)?;
                storekey::Encode::encode(a.as_ref(), e)?;
            }
            StableKey::Fingerprint(fp) => {
                e.write_u8(10)?;
                storekey::Encode::encode(fp, e)?;
            }
        }
        Ok(())
    }
}

impl storekey::Decode for StableKey {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        match d.read_u8()? {
            2 => Ok(StableKey::Null),
            3 => Ok(StableKey::Symbol(d.read_string()?.into())),
            4 => Ok(StableKey::Bool(d.read_u8()? != 0)),
            5 => Ok(StableKey::Int(d.read_i64()?)),
            6 => Ok(StableKey::Str(d.read_string()?.into())),
            7 => Ok(StableKey::Bytes(Arc::from(d.read_vec()?))),
            8 => {
                let bytes: [u8; 16] = d.read_array()?;
                Ok(StableKey::Uuid(uuid::Uuid::from_bytes(bytes)))
            }
            9 => {
                let v: Vec<StableKey> = storekey::Decode::decode(d)?;
                Ok(StableKey::Array(Arc::from(v)))
            }
            10 => {
                let fp: utils::fingerprint::Fingerprint = storekey::Decode::decode(d)?;
                Ok(StableKey::Fingerprint(fp))
            }
            _ => Err(storekey::DecodeError::InvalidFormat),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StablePathRef<'a>(pub &'a [StableKey]);

impl<'a> std::fmt::Display for StablePathRef<'a> {
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

impl<'a> From<&'a [StableKey]> for StablePathRef<'a> {
    fn from(value: &'a [StableKey]) -> Self {
        StablePathRef(value)
    }
}

impl<'a> std::ops::Deref for StablePathRef<'a> {
    type Target = [StableKey];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'p> StablePathRef<'p> {
    pub fn strip_parent(&self, parent: StablePathRef) -> Result<Self> {
        if self.0.len() < parent.0.len() || &self.0[..parent.0.len()] != parent.0 {
            internal_bail!("Path {self} is not a child of parent {parent}");
        }
        Ok(StablePathRef(&self.0[parent.0.len()..]))
    }

    pub fn concat(&self, other: StablePathRef) -> StablePath {
        StablePath(self.0.iter().chain(other.0.iter()).cloned().collect())
    }

    pub fn concat_part(&self, part: StableKey) -> StablePath {
        StablePath(
            self.0
                .iter()
                .cloned()
                .chain(std::iter::once(part))
                .collect(),
        )
    }

    pub fn split_parent(&self) -> Option<(StablePathRef<'p>, &'p StableKey)> {
        self.0
            .split_last()
            .map(|(last, parent)| (StablePathRef(parent), last))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StablePath(pub Arc<[StableKey]>);

impl storekey::Encode for StablePath {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        storekey::Encode::encode(self.0.as_ref(), e)
    }
}

impl storekey::Decode for StablePath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let items: Vec<StableKey> = storekey::Decode::decode(d)?;
        Ok(StablePath(Arc::from(items)))
    }
}

static ROOT_PATH: LazyLock<StablePath> = LazyLock::new(|| StablePath(Arc::new([])));

impl StablePath {
    pub fn root() -> Self {
        ROOT_PATH.clone()
    }

    pub fn concat_part(&self, part: StableKey) -> Self {
        self.as_ref().concat_part(part)
    }

    pub fn concat(&self, other: StablePathRef) -> StablePath {
        self.as_ref().concat(other)
    }

    pub fn as_ref<'a>(&'a self) -> StablePathRef<'a> {
        StablePathRef(self.0.as_ref())
    }
}

impl<'a> From<StablePathRef<'a>> for StablePath {
    fn from(value: StablePathRef<'a>) -> Self {
        StablePath(value.0.to_owned().into())
    }
}

impl std::fmt::Display for StablePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        StablePathRef(self.0.as_ref()).fmt(f)
    }
}

impl std::ops::Deref for StablePath {
    type Target = [StableKey];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> std::borrow::Borrow<[StableKey]> for StablePath {
    fn borrow(&self) -> &[StableKey] {
        &self.0
    }
}

/// Parser for the textual form that [`StableKey`] and [`StablePath`] render
/// through `Display`, so a path printed by the CLI can be fed back in.
///
/// Every key type renders with a distinguishing marker — `"…"` for strings,
/// `b"…"` for bytes, `@…` for symbols, `#…` for fingerprints, `[…]` for
/// arrays, and bare `null`/`true`/`false`/integer/UUID tokens for the rest —
/// so the grammar stays unambiguous and this parser is the exact inverse of
/// `Display`.
struct StablePathParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> StablePathParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    /// Parse `/`-separated parts. The leading `/` is optional so both the
    /// rendered form (`/@files/"a.md"`) and a bare relative form parse.
    fn parse_path(&mut self) -> Result<StablePath> {
        let mut parts = Vec::new();
        self.eat('/');
        if self.at_end() {
            return Ok(StablePath(Arc::from(parts)));
        }
        loop {
            parts.push(self.parse_key()?);
            if !self.eat('/') {
                break;
            }
        }
        if !self.at_end() {
            client_bail!(
                "Unexpected character {:?} at position {} of stable path {:?}",
                self.peek().unwrap_or_default(),
                self.pos,
                self.input
            );
        }
        Ok(StablePath(Arc::from(parts)))
    }

    fn parse_key(&mut self) -> Result<StableKey> {
        match self.peek() {
            Some('"') => Ok(StableKey::Str(Arc::from(self.parse_quoted_str()?))),
            Some('b') if self.input[self.pos..].starts_with("b\"") => {
                self.pos += 1;
                Ok(StableKey::Bytes(Arc::from(self.parse_quoted_bytes()?)))
            }
            Some('@') => {
                self.pos += 1;
                let name = if self.peek() == Some('"') {
                    self.parse_quoted_str()?
                } else {
                    let bare = self.take_token();
                    if !is_bare_symbol_name(bare) {
                        client_bail!(
                            "Invalid symbol name {:?} at position {} of stable path {:?}; \
                             quote it as @\"...\"",
                            bare,
                            self.pos,
                            self.input
                        );
                    }
                    bare.to_string()
                };
                Ok(StableKey::Symbol(Arc::from(name)))
            }
            Some('[') => {
                self.pos += 1;
                let mut items = Vec::new();
                if !self.eat(']') {
                    loop {
                        items.push(self.parse_key()?);
                        if self.eat(']') {
                            break;
                        }
                        if !self.eat(',') {
                            client_bail!(
                                "Expected ',' or ']' at position {} of stable path {:?}",
                                self.pos,
                                self.input
                            );
                        }
                    }
                }
                Ok(StableKey::Array(Arc::from(items)))
            }
            Some('#') => {
                self.pos += 1;
                let token = self.take_token();
                let fp = parse_fingerprint_hex(token).ok_or_else(|| {
                    client_error!(
                        "Invalid fingerprint {:?} in stable path {:?}; expected 32 hex digits",
                        token,
                        self.input
                    )
                })?;
                Ok(StableKey::Fingerprint(fp))
            }
            _ => {
                let start = self.pos;
                let token = self.take_token();
                if token == "null" {
                    Ok(StableKey::Null)
                } else if token == "true" {
                    Ok(StableKey::Bool(true))
                } else if token == "false" {
                    Ok(StableKey::Bool(false))
                } else if let Ok(i) = token.parse::<i64>() {
                    Ok(StableKey::Int(i))
                } else if let Ok(u) = uuid::Uuid::parse_str(token) {
                    Ok(StableKey::Uuid(u))
                } else if token.is_empty() {
                    client_bail!(
                        "Empty key at position {} of stable path {:?}",
                        start,
                        self.input
                    )
                } else {
                    client_bail!(
                        "Cannot interpret {:?} at position {} of stable path {:?} as a key; \
                         write \"{}\" for a string key or @{} for a symbol key",
                        token,
                        start,
                        self.input,
                        token,
                        token
                    )
                }
            }
        }
    }

    /// Consume characters up to the next structural delimiter (`/`, `,`, `]`).
    fn take_token(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if matches!(c, '/' | ',' | ']') {
                break;
            }
            self.pos += c.len_utf8();
        }
        &self.input[start..self.pos]
    }

    /// Parse a `"..."` literal escaped by `char::escape_default` (the escaping
    /// `Display` applies to strings and non-bare symbol names).
    fn parse_quoted_str(&mut self) -> Result<String> {
        self.expect_quote()?;
        let mut out = String::new();
        loop {
            match self.bump() {
                None => client_bail!("Unterminated quoted string in stable path {:?}", self.input),
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('\\') => out.push('\\'),
                    Some('\'') => out.push('\''),
                    Some('"') => out.push('"'),
                    Some('0') => out.push('\0'),
                    Some('u') => out.push(self.parse_unicode_escape()?),
                    other => client_bail!(
                        "Invalid escape \\{} in stable path {:?}",
                        other.unwrap_or_default(),
                        self.input
                    ),
                },
                Some(c) => out.push(c),
            }
        }
    }

    /// Parse the `"..."` part of a `b"..."` literal, escaped by
    /// `std::ascii::escape_default`.
    fn parse_quoted_bytes(&mut self) -> Result<Vec<u8>> {
        self.expect_quote()?;
        let mut out = Vec::new();
        loop {
            match self.bump() {
                None => client_bail!("Unterminated byte string in stable path {:?}", self.input),
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('n') => out.push(b'\n'),
                    Some('r') => out.push(b'\r'),
                    Some('t') => out.push(b'\t'),
                    Some('\\') => out.push(b'\\'),
                    Some('\'') => out.push(b'\''),
                    Some('"') => out.push(b'"'),
                    Some('0') => out.push(0),
                    Some('x') => out.push(self.parse_hex_byte()?),
                    other => client_bail!(
                        "Invalid escape \\{} in stable path {:?}",
                        other.unwrap_or_default(),
                        self.input
                    ),
                },
                // `escape_default` only emits ASCII, so anything else is a
                // literal byte the writer passed through unescaped.
                Some(c) if c.is_ascii() => out.push(c as u8),
                Some(c) => client_bail!(
                    "Non-ASCII character {:?} in byte string of stable path {:?}",
                    c,
                    self.input
                ),
            }
        }
    }

    fn expect_quote(&mut self) -> Result<()> {
        if !self.eat('"') {
            client_bail!(
                "Expected '\"' at position {} of stable path {:?}",
                self.pos,
                self.input
            );
        }
        Ok(())
    }

    fn parse_unicode_escape(&mut self) -> Result<char> {
        if !self.eat('{') {
            client_bail!(
                "Expected '{{' after \\u at position {} of stable path {:?}",
                self.pos,
                self.input
            );
        }
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_hexdigit()) {
            self.pos += 1;
        }
        let digits = &self.input[start..self.pos];
        if !self.eat('}') {
            client_bail!(
                "Unterminated \\u escape at position {} of stable path {:?}",
                self.pos,
                self.input
            );
        }
        u32::from_str_radix(digits, 16)
            .ok()
            .and_then(char::from_u32)
            .ok_or_else(|| {
                client_error!(
                    "Invalid unicode escape \\u{{{}}} in stable path {:?}",
                    digits,
                    self.input
                )
            })
    }

    fn parse_hex_byte(&mut self) -> Result<u8> {
        let start = self.pos;
        for _ in 0..2 {
            match self.peek() {
                Some(c) if c.is_ascii_hexdigit() => self.pos += 1,
                _ => client_bail!(
                    "Expected two hex digits after \\x at position {} of stable path {:?}",
                    self.pos,
                    self.input
                ),
            }
        }
        Ok(
            u8::from_str_radix(&self.input[start..self.pos], 16).map_err(|e| {
                client_error!("Invalid \\x escape in stable path {:?}: {e}", self.input)
            })?,
        )
    }
}

/// Decode the 32 hex digits that follow the `#` in a rendered fingerprint.
fn parse_fingerprint_hex(token: &str) -> Option<utils::fingerprint::Fingerprint> {
    if token.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(token.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(utils::fingerprint::Fingerprint(bytes))
}

impl std::str::FromStr for StablePath {
    type Err = utils::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        StablePathParser::new(s.trim()).parse_path()
    }
}

impl StablePath {
    /// Parse a path from the textual form rendered by [`Display`].
    ///
    /// [`Display`]: std::fmt::Display
    pub fn parse(s: &str) -> Result<Self> {
        s.parse()
    }
}

#[derive(Debug, Default)]
pub struct StablePathPrefix<'a>(StablePathRef<'a>);

impl<'a> storekey::Encode for StablePathPrefix<'a> {
    fn encode<W: Write>(&self, e: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
        for part in self.0.iter() {
            part.encode(e)?;
        }
        Ok(())
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
    fn stable_key_roundtrip() {
        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);
        let cases = vec![
            StableKey::Null,
            StableKey::Bool(false),
            StableKey::Bool(true),
            StableKey::Int(0),
            StableKey::Int(-1),
            StableKey::Int(i64::MIN / 2),
            StableKey::Int(i64::MAX / 2),
            StableKey::Str(Arc::from("hello")),
            StableKey::Str(Arc::from("nul\0inside")),
            StableKey::Bytes(Arc::from(&b"bytes\x00with\x01escapes"[..])),
            StableKey::Uuid(uuid),
            StableKey::Array(Arc::from([
                StableKey::Int(1),
                StableKey::Str(Arc::from("a")),
                StableKey::Bytes(Arc::from(&b"\0"[..])),
            ])),
            StableKey::Fingerprint(fp),
            StableKey::Symbol(Arc::from("cocoindex/setup")),
        ];

        for original in cases {
            let decoded = roundtrip(&original);
            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn stable_path_roundtrip() {
        let path = StablePath(Arc::from(vec![
            StableKey::Int(42),
            StableKey::Str(Arc::from("part")),
            StableKey::Bytes(Arc::from(&b"\0term"[..])),
        ]));
        let decoded = roundtrip(&path);
        assert_eq!(decoded, path);

        let empty = StablePath::root();
        let decoded_empty = roundtrip(&empty);
        assert_eq!(decoded_empty, empty);
    }

    /// Every key type must survive `Display` -> `parse`, so a path copied
    /// out of `cocoindex show` can be pasted back in as an argument.
    #[test]
    fn stable_path_display_parse_roundtrip() {
        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);
        let keys = vec![
            StableKey::Null,
            StableKey::Bool(true),
            StableKey::Bool(false),
            StableKey::Int(0),
            StableKey::Int(-42),
            StableKey::Int(i64::MIN),
            StableKey::Int(i64::MAX),
            StableKey::Str(Arc::from("rfc8259.md")),
            // Strings that would break a naive split on '/' or on quotes.
            StableKey::Str(Arc::from("a/b/c")),
            StableKey::Str(Arc::from("with \"quotes\" and \\ backslash")),
            StableKey::Str(Arc::from("nul\0and\ttabs\n")),
            StableKey::Str(Arc::from("café ☕")),
            StableKey::Str(Arc::from("")),
            // A string that looks exactly like every other rendered form.
            StableKey::Str(Arc::from("null")),
            StableKey::Str(Arc::from("true")),
            StableKey::Str(Arc::from("13")),
            StableKey::Str(Arc::from("@process_files")),
            StableKey::Str(Arc::from(uuid.to_string())),
            StableKey::Bytes(Arc::from(&b"bytes\x00with\x01escapes/and\"quote"[..])),
            StableKey::Bytes(Arc::from(&b""[..])),
            StableKey::Uuid(uuid),
            StableKey::Fingerprint(fp),
            StableKey::Symbol(Arc::from("process_files")),
            // Symbols carrying '/' must round-trip too — several internal
            // symbols (e.g. "cocoindex/mount_target") contain one.
            StableKey::Symbol(Arc::from("cocoindex/mount_target")),
            StableKey::Symbol(Arc::from("")),
            StableKey::Array(Arc::from([])),
            StableKey::Array(Arc::from([
                StableKey::Int(13),
                StableKey::Str(Arc::from("a,b]c/d")),
                StableKey::Symbol(Arc::from("sys/state")),
                StableKey::Array(Arc::from([
                    StableKey::Null,
                    StableKey::Bytes(Arc::from(&b"\0"[..])),
                ])),
            ])),
        ];

        for key in &keys {
            let path = StablePath(Arc::from(vec![key.clone()]));
            let rendered = path.to_string();
            let parsed = StablePath::parse(&rendered)
                .unwrap_or_else(|e| panic!("parse {rendered:?} failed: {e}"));
            assert_eq!(parsed, path, "roundtrip failed for {rendered}");
        }

        // The whole set as one deep path.
        let path = StablePath(Arc::from(keys));
        assert_eq!(StablePath::parse(&path.to_string()).unwrap(), path);

        // Root.
        assert_eq!(StablePath::parse("/").unwrap(), StablePath::root());
        assert_eq!(StablePath::parse("").unwrap(), StablePath::root());
    }

    #[test]
    fn stable_key_display_is_unambiguous() {
        assert_eq!(
            StablePath(Arc::from(vec![
                StableKey::Symbol(Arc::from("process_files")),
                StableKey::Str(Arc::from("rfc8259.md")),
            ]))
            .to_string(),
            "/@process_files/\"rfc8259.md\""
        );
        // A symbol that isn't a bare token gets quoted, so it stays one segment.
        assert_eq!(
            StableKey::Symbol(Arc::from("cocoindex/mount_target")).to_string(),
            "@\"cocoindex/mount_target\""
        );
    }

    #[test]
    fn stable_path_parse_rejects_ambiguous_input() {
        // A bare word is neither a string nor a symbol — the caller has to say
        // which, instead of it silently becoming a string (issue #2297).
        let err = StablePath::parse("/process_files").unwrap_err().to_string();
        assert!(err.contains("process_files"), "unexpected error: {err}");

        for bad in [
            "/\"unterminated",
            "/b\"unterminated",
            "/[1,2",
            "/#abc",
            "/@bad name",
            "//",
            "/\"a\"x",
        ] {
            assert!(
                StablePath::parse(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    /// JSON has no bytes type, so `Bytes` serializes as an integer sequence
    /// and decodes as `Array`; every other variant round-trips unchanged.
    #[test]
    fn stable_key_serde_json_shape() {
        use serde_json::{Value, json};

        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);

        // (key, expected JSON, expected decoded key)
        let cases: Vec<(StableKey, Value, StableKey)> = vec![
            (StableKey::Null, Value::Null, StableKey::Null),
            (StableKey::Bool(true), json!(true), StableKey::Bool(true)),
            (StableKey::Int(-7), json!(-7), StableKey::Int(-7)),
            (
                StableKey::Str(Arc::from("hi")),
                json!("hi"),
                StableKey::Str(Arc::from("hi")),
            ),
            (
                StableKey::Bytes(Arc::from(&b"\x00\x01\xff"[..])),
                json!([0, 1, 255]),
                StableKey::Array(Arc::from([
                    StableKey::Int(0),
                    StableKey::Int(1),
                    StableKey::Int(255),
                ])),
            ),
            (
                StableKey::Uuid(uuid),
                json!({ "uuid": uuid.to_string() }),
                StableKey::Uuid(uuid),
            ),
            (
                StableKey::Fingerprint(fp),
                json!({ "fp": serde_json::to_value(fp).expect("fp to value") }),
                StableKey::Fingerprint(fp),
            ),
            (
                StableKey::Symbol(Arc::from("cocoindex/setup")),
                json!({ "sym": "cocoindex/setup" }),
                StableKey::Symbol(Arc::from("cocoindex/setup")),
            ),
            (
                StableKey::Array(Arc::from([
                    StableKey::Int(1),
                    StableKey::Str(Arc::from("a")),
                ])),
                json!([1, "a"]),
                StableKey::Array(Arc::from([
                    StableKey::Int(1),
                    StableKey::Str(Arc::from("a")),
                ])),
            ),
        ];

        for (key, expected, expected_decoded) in cases {
            let got = serde_json::to_value(&key).expect("serialize");
            assert_eq!(got, expected);
            let decoded: StableKey = serde_json::from_value(got).expect("deserialize");
            assert_eq!(decoded, expected_decoded, "decoded shape for {key:?}");
        }
    }

    #[test]
    fn stable_path_serde_json_shape() {
        use serde_json::json;

        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);

        let path = StablePath(Arc::from(vec![
            StableKey::Int(42),
            StableKey::Bytes(Arc::from(&b"\0term"[..])),
            StableKey::Uuid(uuid),
            StableKey::Fingerprint(fp),
        ]));

        let got = serde_json::to_value(&path).expect("serialize");
        let expected = json!([
            42,
            [0, 116, 101, 114, 109],
            { "uuid": uuid.to_string() },
            { "fp": serde_json::to_value(fp).expect("fp to value") },
        ]);
        assert_eq!(got, expected);

        // The `Bytes` segment comes back as an `Array` of `Int`: JSON carries
        // no bytes type, so the decoder reads the integer sequence it finds.
        let expected_decoded = StablePath(Arc::from(vec![
            StableKey::Int(42),
            StableKey::Array(Arc::from([
                StableKey::Int(0),
                StableKey::Int(116),
                StableKey::Int(101),
                StableKey::Int(114),
                StableKey::Int(109),
            ])),
            StableKey::Uuid(uuid),
            StableKey::Fingerprint(fp),
        ]));
        let decoded: StablePath = serde_json::from_value(got).expect("deserialize");
        assert_eq!(decoded, expected_decoded);
    }

    #[test]
    fn serde_msgpack_every_variant_roundtrip() {
        // Every variant must survive the state store's msgpack encoding
        // unchanged: ownership guards compare decoded component paths, so a
        // variant swap (array->bytes, bytes->str) silently retargets them.
        let arr = |items: Vec<StableKey>| StableKey::Array(Arc::from(items));
        let cases = vec![
            StableKey::Null,
            StableKey::Bool(false),
            StableKey::Bool(true),
            StableKey::Int(0),
            StableKey::Int(-1),
            StableKey::Int(i64::MIN),
            StableKey::Int(i64::MAX),
            StableKey::Int(255),
            StableKey::Int(256),
            StableKey::Str(Arc::from("")),
            StableKey::Str(Arc::from("process")),
            StableKey::Str(Arc::from("\u{4e2d}\u{6587}/caf\u{e9}")),
            StableKey::Str(Arc::from("nul\0inside")),
            StableKey::Bytes(Arc::from(&b""[..])),
            // Valid UTF-8 bytes must not decode as `Str`.
            StableKey::Bytes(Arc::from(&b"abc"[..])),
            StableKey::Bytes(Arc::from("\u{4e2d}\u{6587}".as_bytes())),
            StableKey::Bytes(Arc::from(&b"nul\0inside"[..])),
            StableKey::Bytes(Arc::from(&b"\x00\x01\xff"[..])),
            StableKey::Uuid(uuid::Uuid::from_bytes([3u8; 16])),
            StableKey::Fingerprint(utils::fingerprint::Fingerprint([7u8; 16])),
            StableKey::Symbol(Arc::from("cocoindex/setup")),
            // Arrays whose elements all fit in a byte must not decode as `Bytes`.
            arr(vec![]),
            arr(vec![StableKey::Int(1)]),
            arr(vec![StableKey::Int(1), StableKey::Int(1)]),
            arr(vec![StableKey::Int(1), StableKey::Int(2)]),
            // Reordering must stay distinguishable from the case above.
            arr(vec![StableKey::Int(2), StableKey::Int(1)]),
            arr(vec![StableKey::Int(0), StableKey::Int(255)]),
            arr(vec![StableKey::Int(-1), StableKey::Int(256)]),
            arr(vec![
                StableKey::Str(Arc::from("process")),
                arr(vec![StableKey::Int(1), StableKey::Int(2)]),
            ]),
            // A nested array carrying a symbol, strings, and a raw-bytes key.
            arr(vec![
                arr(vec![
                    StableKey::Symbol(Arc::from("obj")),
                    StableKey::Str(Arc::from("tenant")),
                ]),
                StableKey::Str(Arc::from("src/main.rs")),
                StableKey::Bytes(Arc::from(&[0xde, 0xad, 0xbe, 0xef][..])),
            ]),
            arr(vec![
                StableKey::Null,
                StableKey::Bool(true),
                StableKey::Bytes(Arc::from(&b"abc"[..])),
                StableKey::Uuid(uuid::Uuid::from_bytes([9u8; 16])),
                StableKey::Fingerprint(utils::fingerprint::Fingerprint([1u8; 16])),
                StableKey::Symbol(Arc::from("sym")),
                arr(vec![arr(vec![])]),
            ]),
        ];

        for key in cases {
            let bytes = rmp_serde::to_vec_named(&key).expect("encode");
            // Same wrapper the state store reads through.
            let decoded: StableKey =
                utils::deser::from_msgpack_slice(&bytes).expect("decode stable key");
            assert_eq!(decoded, key, "round-trip changed variant for {key:?}");
        }
    }

    /// The deserializer must read bytes the *unchanged* serializer produces, so
    /// pin the wire shapes rather than only round-tripping through it.
    #[test]
    fn serde_msgpack_decodes_serializer_fixtures() {
        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);
        let cases: Vec<(StableKey, Vec<u8>)> = vec![
            (StableKey::Null, vec![0xc0]),
            (StableKey::Bool(true), vec![0xc3]),
            (StableKey::Int(1), vec![0x01]),
            (StableKey::Int(-1), vec![0xff]),
            // fixstr "ab"
            (StableKey::Str(Arc::from("ab")), vec![0xa2, b'a', b'b']),
            // bin8 of b"ab" -- distinct from the fixstr above.
            (
                StableKey::Bytes(Arc::from(&b"ab"[..])),
                vec![0xc4, 0x02, b'a', b'b'],
            ),
            // fixarray of 2 ints -- the shape from the issue report.
            (
                StableKey::Array(Arc::from(vec![StableKey::Int(1), StableKey::Int(2)])),
                vec![0x92, 0x01, 0x02],
            ),
            (StableKey::Array(Arc::from(vec![])), vec![0x90]),
            (
                StableKey::Uuid(uuid),
                [
                    &[0x81, 0xa4, b'u', b'u', b'i', b'd', 0xc4, 0x10][..],
                    &[3u8; 16][..],
                ]
                .concat(),
            ),
            (
                StableKey::Fingerprint(fp),
                [&[0x81, 0xa2, b'f', b'p', 0xc4, 0x10][..], &[7u8; 16][..]].concat(),
            ),
            (
                StableKey::Symbol(Arc::from("a")),
                vec![0x81, 0xa3, b's', b'y', b'm', 0xa1, b'a'],
            ),
        ];

        for (key, wire) in cases {
            assert_eq!(
                rmp_serde::to_vec_named(&key).expect("encode"),
                wire,
                "serializer shape changed for {key:?}"
            );
            let decoded: StableKey =
                utils::deser::from_msgpack_slice(&wire).expect("decode fixture");
            assert_eq!(decoded, key);
        }
    }

    /// The root path and a path holding one empty-array segment are different
    /// paths; msgpack must not flatten one into the other.
    #[test]
    fn serde_msgpack_root_path_differs_from_empty_array_segment() {
        let root = StablePath::root();
        let empty_segment = StablePath(Arc::from(vec![StableKey::Array(Arc::from(vec![]))]));
        assert_ne!(root, empty_segment);

        let root_bytes = rmp_serde::to_vec_named(&root).expect("encode root");
        let segment_bytes = rmp_serde::to_vec_named(&empty_segment).expect("encode segment");
        assert_ne!(root_bytes, segment_bytes);

        assert_eq!(
            utils::deser::from_msgpack_slice::<StablePath>(&root_bytes).expect("decode root"),
            root
        );
        assert_eq!(
            utils::deser::from_msgpack_slice::<StablePath>(&segment_bytes).expect("decode segment"),
            empty_segment
        );
    }

    /// Hand-rolled msgpack so the test can build shapes the serializer never
    /// emits (duplicate keys, extra entries, malformed payloads).
    fn msgpack_fixstr(s: &str) -> Vec<u8> {
        assert!(s.len() < 32);
        let mut out = vec![0xa0 | s.len() as u8];
        out.extend_from_slice(s.as_bytes());
        out
    }

    fn msgpack_bin(v: &[u8]) -> Vec<u8> {
        assert!(v.len() < 256);
        let mut out = vec![0xc4, v.len() as u8];
        out.extend_from_slice(v);
        out
    }

    fn msgpack_map(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        assert!(entries.len() < 16);
        let mut out = vec![0x80 | entries.len() as u8];
        for (k, v) in entries {
            out.extend_from_slice(&msgpack_fixstr(k));
            out.extend_from_slice(v);
        }
        out
    }

    #[test]
    fn serde_msgpack_tagged_map_accepts_only_one_known_tag() {
        let uuid_payload = msgpack_bin(&[3u8; 16]);

        // Each recognized tag alone decodes.
        let accepted: Vec<(Vec<u8>, StableKey)> = vec![
            (
                msgpack_map(&[("uuid", uuid_payload.clone())]),
                StableKey::Uuid(uuid::Uuid::from_bytes([3u8; 16])),
            ),
            (
                msgpack_map(&[("fp", msgpack_bin(&[7u8; 16]))]),
                StableKey::Fingerprint(utils::fingerprint::Fingerprint([7u8; 16])),
            ),
            (
                msgpack_map(&[("sym", msgpack_fixstr("a"))]),
                StableKey::Symbol(Arc::from("a")),
            ),
        ];
        for (bytes, expected) in accepted {
            assert_eq!(
                utils::deser::from_msgpack_slice::<StableKey>(&bytes).expect("decode tagged"),
                expected
            );
        }

        let rejected: Vec<(&str, Vec<u8>)> = vec![
            ("unknown tag", msgpack_map(&[("nope", vec![0x01])])),
            ("missing tag (empty map)", msgpack_map(&[])),
            // A recognized tag plus an extra entry is ambiguous; the previous
            // decoder silently resolved such maps by enum priority.
            (
                "known tag plus extra",
                msgpack_map(&[("sym", msgpack_fixstr("a")), ("zextra", vec![0x01])]),
            ),
            (
                "extra before known tag",
                msgpack_map(&[("zextra", vec![0x01]), ("sym", msgpack_fixstr("a"))]),
            ),
            (
                "two recognized tags",
                msgpack_map(&[("uuid", uuid_payload.clone()), ("sym", msgpack_fixstr("a"))]),
            ),
            (
                "two recognized tags reversed",
                msgpack_map(&[("sym", msgpack_fixstr("a")), ("uuid", uuid_payload)]),
            ),
            (
                "duplicate tag",
                msgpack_map(&[("sym", msgpack_fixstr("a")), ("sym", msgpack_fixstr("b"))]),
            ),
            // A malformed or null payload must fail even when a valid tagged
            // entry follows it; nothing falls through to the next entry.
            (
                "malformed payload before known tag",
                msgpack_map(&[
                    ("uuid", msgpack_fixstr("invalid")),
                    ("sym", msgpack_fixstr("a")),
                ]),
            ),
            (
                "null payload before known tag",
                msgpack_map(&[("uuid", vec![0xc0]), ("sym", msgpack_fixstr("a"))]),
            ),
            ("uuid null payload", msgpack_map(&[("uuid", vec![0xc0])])),
            (
                "fingerprint null payload",
                msgpack_map(&[("fp", vec![0xc0])]),
            ),
            ("symbol null payload", msgpack_map(&[("sym", vec![0xc0])])),
            (
                "uuid payload wrong length",
                msgpack_map(&[("uuid", msgpack_bin(&[1u8; 4]))]),
            ),
            (
                "uuid payload not bytes",
                msgpack_map(&[("uuid", msgpack_fixstr("invalid"))]),
            ),
            (
                "fingerprint payload wrong length",
                msgpack_map(&[("fp", msgpack_bin(&[1u8; 4]))]),
            ),
            (
                "fingerprint payload not bytes",
                msgpack_map(&[("fp", msgpack_fixstr("invalid!"))]),
            ),
            (
                "symbol payload not a string",
                msgpack_map(&[("sym", vec![0x01])]),
            ),
        ];

        for (label, bytes) in rejected {
            assert!(
                utils::deser::from_msgpack_slice::<StableKey>(&bytes).is_err(),
                "{label} must be rejected"
            );
        }
    }

    #[test]
    fn serde_msgpack_rejects_unrepresentable_and_malformed_input() {
        // No `StableKey` variant holds an unsigned value above `i64::MAX` or a
        // float, so both must fail rather than silently truncate.
        let too_large = rmp_serde::to_vec_named(&u64::MAX).expect("encode u64");
        assert!(utils::deser::from_msgpack_slice::<StableKey>(&too_large).is_err());
        let float = rmp_serde::to_vec_named(&1.5f64).expect("encode f64");
        assert!(utils::deser::from_msgpack_slice::<StableKey>(&float).is_err());
        // 0xc1 is reserved and cannot begin a MessagePack value, including
        // inside an otherwise valid array.
        assert!(utils::deser::from_msgpack_slice::<StableKey>(&[0xc1]).is_err());
        assert!(utils::deser::from_msgpack_slice::<StableKey>(&[0x91, 0xc1]).is_err());

        // `i64::MAX` itself encodes as a uint64 and must still decode.
        let max = rmp_serde::to_vec_named(&StableKey::Int(i64::MAX)).expect("encode i64::MAX");
        assert_eq!(
            utils::deser::from_msgpack_slice::<StableKey>(&max).expect("decode i64::MAX"),
            StableKey::Int(i64::MAX)
        );

        // Truncated input at every prefix of a nested array.
        let key = StableKey::Array(Arc::from(vec![
            StableKey::Str(Arc::from("process")),
            StableKey::Bytes(Arc::from(&b"\x00\x01"[..])),
        ]));
        let full = rmp_serde::to_vec_named(&key).expect("encode");
        for len in 1..full.len() {
            assert!(
                utils::deser::from_msgpack_slice::<StableKey>(&full[..len]).is_err(),
                "truncation to {len} bytes must be rejected"
            );
        }
        assert!(utils::deser::from_msgpack_slice::<StableKey>(&[]).is_err());
    }
}
