use crate::prelude::*;
use std::fmt::Write as FmtWrite;

#[derive(Clone)]
pub enum StateKey {
    Null,
    Bool(bool),
    Int(i64),

    // Note: we use Arc<String> instead of Arc<str> to keep the inlined size small. Similar for other `Arc<T>` types below.
    Str(Arc<String>),
    Bytes(Arc<Vec<u8>>),
    Uuid(Arc<uuid::Uuid>),
    Array(Arc<Vec<StateKey>>),
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
                for &byte in b.as_slice() {
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
        }
    }
}

#[derive(Clone)]
pub struct StatePath(pub Arc<[StateKey]>);

static ROOT_PATH: LazyLock<StatePath> = LazyLock::new(|| StatePath(Arc::new([])));

impl StatePath {
    pub fn root() -> Self {
        ROOT_PATH.clone()
    }

    pub fn concat(&self, part: StateKey) -> Self {
        let result = self
            .0
            .iter()
            .cloned()
            .chain(std::iter::once(part))
            .collect::<Arc<_>>();
        Self(result)
    }
}

impl std::fmt::Display for StatePath {
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
