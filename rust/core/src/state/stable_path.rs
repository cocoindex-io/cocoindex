use crate::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt::Write as FmtWrite, io::Write};

// ---------------------------------------------------------------------------
// Selector (fnmatch-style) path matching
// ---------------------------------------------------------------------------

/// Returns `true` when *s* contains any fnmatch wildcard character.
fn has_glob(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

/// Simple fnmatch implementation supporting `*`, `?`, and `[...]` character classes.
fn fnmatch_bytes(s: &[u8], p: &[u8]) -> bool {
    // Backtracking state for `*` — store where we were in s and p after the star.
    let mut star_s: Option<usize> = None;
    let mut star_p: Option<usize> = None;
    let mut si: usize = 0;
    let mut pi: usize = 0;

    loop {
        if pi == p.len() {
            // Pattern exhausted — match iff we consumed all of s.
            return si == s.len();
        }
        match p[pi] {
            b'*' => {
                // Record state so we can backtrack: try matching zero chars first.
                star_s = Some(si);
                star_p = Some(pi);
                pi += 1;
            }
            b'?' => {
                if si < s.len() {
                    si += 1;
                    pi += 1;
                } else {
                    // Backtrack if possible; otherwise fail.
                    match (star_s, star_p) {
                        (Some(ss), Some(sp)) if ss < s.len() => {
                            si = ss + 1;
                            pi = sp + 1;
                            star_s = Some(si);
                        }
                        _ => return false,
                    }
                }
            }
            b'[' => {
                // Find the closing `]`.
                let close = p[pi..].iter().position(|&c| c == b']');
                if let Some(end) = close {
                    let class_end = pi + end;
                    if si < s.len() {
                        let c = s[si];
                        let negate = p[pi + 1] == b'!';
                        let class_start = if negate { pi + 2 } else { pi + 1 };
                        let in_class = p[class_start..class_end].contains(&c);
                        if (negate && !in_class) || (!negate && in_class) {
                            si += 1;
                            pi = class_end + 1;
                            continue;
                        }
                    }
                    // Character didn't match class — backtrack or fail.
                    match (star_s, star_p) {
                        (Some(ss), Some(sp)) if ss < s.len() => {
                            si = ss + 1;
                            pi = sp + 1;
                            star_s = Some(si);
                        }
                        _ => return false,
                    }
                } else {
                    // Malformed `[` — treat as literal.
                    if si < s.len() && s[si] == b'[' {
                        si += 1;
                        pi += 1;
                    } else {
                        match (star_s, star_p) {
                            (Some(ss), Some(sp)) if ss < s.len() => {
                                si = ss + 1;
                                pi = sp + 1;
                                star_s = Some(si);
                            }
                            _ => return false,
                        }
                    }
                }
            }
            _ => {
                if si < s.len() && s[si] == p[pi] {
                    si += 1;
                    pi += 1;
                } else {
                    match (star_s, star_p) {
                        (Some(ss), Some(sp)) if ss < s.len() => {
                            si = ss + 1;
                            pi = sp + 1;
                            star_s = Some(si);
                        }
                        _ => return false,
                    }
                }
            }
        }
    }
}

/// Convert a [`StableKey`] to a selector-friendly string for matching.
fn stable_key_to_selector_part(key: &StableKey) -> String {
    match key {
        StableKey::Null => "null".to_string(),
        StableKey::Bool(b) => b.to_string(),
        StableKey::Int(i) => i.to_string(),
        StableKey::Str(s) => s.to_string(),
        StableKey::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        StableKey::Uuid(u) => u.to_string(),
        StableKey::Symbol(s) => s.to_string(),
        StableKey::Array(a) => {
            let parts: Vec<String> = a.iter().map(stable_key_to_selector_part).collect();
            format!("[{}]", parts.join(","))
        }
        StableKey::Fingerprint(fp) => fp.to_string(),
    }
}

/// Check if a single path part matches a selector part.
///
/// String selector parts may contain fnmatch glob patterns (`*`, `?`, `[...]`).
/// A `str` and `Symbol` are treated as matching when the string equals the
/// symbol's name (the two representations are interchangeable in CocoIndex paths).
fn selector_part_matches(path_part: &StableKey, sel_part: &StableKey) -> bool {
    // Glob matching for string selector parts.
    if let StableKey::Str(sel_str) = sel_part {
        if has_glob(sel_str) {
            let path_str = stable_key_to_selector_part(path_part);
            return fnmatch_bytes(path_str.as_bytes(), sel_str.as_bytes());
        }
    }

    // Cross-type: str ↔ Symbol (interchangeable in paths).
    match (path_part, sel_part) {
        (StableKey::Str(p), StableKey::Symbol(s)) => return p.as_ref() == s.as_ref(),
        (StableKey::Symbol(p), StableKey::Str(s)) => return p.as_ref() == s.as_ref(),
        _ => {}
    }

    // Exact match: types must be the same.
    if std::mem::discriminant(path_part) != std::mem::discriminant(sel_part) {
        return false;
    }

    match (path_part, sel_part) {
        (StableKey::Symbol(p), StableKey::Symbol(s)) => p.as_ref() == s.as_ref(),
        (StableKey::Array(p), StableKey::Array(s)) => {
            if p.len() != s.len() {
                return false;
            }
            p.iter()
                .zip(s.iter())
                .all(|(a, b)| selector_part_matches(a, b))
        }
        _ => path_part == sel_part,
    }
}

/// Check whether *path* matches any entry in the component *selector*.
///
/// Returns `true` when *selector* is `None` (meaning "run everything").
/// Each selector entry is compared part-by-part against *path*; string
/// selector parts may use fnmatch glob patterns.
pub fn is_path_selected(path: &StablePath, selector: Option<&[StablePath]>) -> bool {
    let Some(sel) = selector else {
        return true;
    };
    let path_parts: &[StableKey] = path;
    for sel_path in sel {
        let sel_parts: &[StableKey] = sel_path;
        if path_parts.len() != sel_parts.len() {
            continue;
        }
        if path_parts
            .iter()
            .zip(sel_parts.iter())
            .all(|(p, s)| selector_part_matches(p, s))
        {
            return true;
        }
    }
    false
}

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

impl<'de> Deserialize<'de> for StableKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Null(()),
            Bool(bool),
            Int(i64),
            Str(String),
            // Intentionally before Array to preserve StableKey::Bytes roundtrip in formats
            // (like JSON) where bytes can be represented as a sequence of integers.
            // `serde_bytes` makes this consume a native byte string too (msgpack
            // `bin`, the state-store format) — a plain `Vec<u8>` only deserializes
            // via `deserialize_seq`, so a `bin` would fail to match any variant.
            #[serde(with = "serde_bytes")]
            Bytes(Vec<u8>),
            Uuid {
                uuid: uuid::Uuid,
            },
            Fp {
                fp: utils::fingerprint::Fingerprint,
            },
            Sym {
                sym: String,
            },
            Array(Vec<Repr>),
        }

        impl Repr {
            fn into_stable_key(self) -> StableKey {
                match self {
                    Repr::Null(()) => StableKey::Null,
                    Repr::Bool(b) => StableKey::Bool(b),
                    Repr::Int(i) => StableKey::Int(i),
                    Repr::Str(s) => StableKey::Str(Arc::from(s)),
                    Repr::Bytes(b) => StableKey::Bytes(Arc::from(b)),
                    Repr::Uuid { uuid } => StableKey::Uuid(uuid),
                    Repr::Fp { fp } => StableKey::Fingerprint(fp),
                    Repr::Sym { sym } => StableKey::Symbol(Arc::from(sym)),
                    Repr::Array(items) => StableKey::Array(Arc::from(
                        items
                            .into_iter()
                            .map(Repr::into_stable_key)
                            .collect::<Vec<_>>(),
                    )),
                }
            }
        }

        Ok(Repr::deserialize(deserializer)?.into_stable_key())
    }
}

impl std::fmt::Display for StableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StableKey::Null => write!(f, "null"),
            StableKey::Bool(b) => write!(f, "{}", b),
            StableKey::Int(i) => write!(f, "{}", i),
            StableKey::Str(s) => {
                f.write_char('"')?;
                f.write_str(s.escape_default().to_string().as_str())?;
                f.write_char('"')
            }
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
            StableKey::Symbol(s) => write!(f, "@{s}"),
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

    #[test]
    fn stable_key_serde_json_shape() {
        use serde_json::{Value, json};

        let uuid = uuid::Uuid::from_bytes([3u8; 16]);
        let fp = utils::fingerprint::Fingerprint([7u8; 16]);

        let cases: Vec<(StableKey, Value)> = vec![
            (StableKey::Null, Value::Null),
            (StableKey::Bool(true), json!(true)),
            (StableKey::Int(-7), json!(-7)),
            (StableKey::Str(Arc::from("hi")), json!("hi")),
            (
                StableKey::Bytes(Arc::from(&b"\x00\x01\xff"[..])),
                json!([0, 1, 255]),
            ),
            (StableKey::Uuid(uuid), json!({ "uuid": uuid.to_string() })),
            (
                StableKey::Fingerprint(fp),
                json!({ "fp": serde_json::to_value(fp).expect("fp to value") }),
            ),
            (
                StableKey::Symbol(Arc::from("cocoindex/setup")),
                json!({ "sym": "cocoindex/setup" }),
            ),
            (
                StableKey::Array(Arc::from([
                    StableKey::Int(1),
                    StableKey::Str(Arc::from("a")),
                ])),
                json!([1, "a"]),
            ),
        ];

        for (key, expected) in cases {
            let got = serde_json::to_value(&key).expect("serialize");
            assert_eq!(got, expected);
            let roundtrip: StableKey = serde_json::from_value(got).expect("deserialize");
            assert_eq!(roundtrip, key);
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

        let roundtrip: StablePath = serde_json::from_value(got).expect("deserialize");
        assert_eq!(roundtrip, path);
    }

    #[test]
    fn serde_msgpack_bytes_roundtrip() {
        // `StableKey::Bytes` must survive a serde msgpack round-trip — it rides
        // inside component paths and target-state keys, which may embed raw
        // bytes. msgpack serializes a `Vec<u8>` as a native `bin`, so the
        // deserializer has to accept a `bin` (not just an int sequence).
        for key in [
            StableKey::Bytes(Arc::from(&b"\x00\x01\xff sha"[..])),
            // A nested array carrying a symbol, strings, and a raw-bytes key.
            StableKey::Array(Arc::from(vec![
                StableKey::Array(Arc::from(vec![
                    StableKey::Symbol(Arc::from("obj")),
                    StableKey::Str(Arc::from("tenant")),
                ])),
                StableKey::Str(Arc::from("src/main.rs")),
                StableKey::Bytes(Arc::from(&[0xde, 0xad, 0xbe, 0xef][..])),
            ])),
        ] {
            let bytes = rmp_serde::to_vec_named(&key).expect("encode");
            let decoded: StableKey = rmp_serde::from_slice(&bytes).expect("decode bytes key");
            assert_eq!(decoded, key);
        }
    }
}
