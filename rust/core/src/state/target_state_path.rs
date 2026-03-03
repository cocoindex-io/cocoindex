use std::borrow::Cow;

use crate::prelude::*;
use serde::{Deserialize, Serialize};

use crate::state::stable_path::StableKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TargetStatePath(Arc<[utils::fingerprint::Fingerprint]>);

impl std::borrow::Borrow<[utils::fingerprint::Fingerprint]> for TargetStatePath {
    fn borrow(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.0
    }
}

impl std::fmt::Display for TargetStatePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in self.0.iter() {
            write!(f, "/{part}")?;
        }
        Ok(())
    }
}

impl storekey::Encode for TargetStatePath {
    fn encode<W: std::io::Write>(
        &self,
        e: &mut storekey::Writer<W>,
    ) -> Result<(), storekey::EncodeError> {
        self.0.encode(e)
    }
}

impl storekey::Decode for TargetStatePath {
    fn decode<D: std::io::BufRead>(
        d: &mut storekey::Reader<D>,
    ) -> Result<Self, storekey::DecodeError> {
        let parts: Vec<utils::fingerprint::Fingerprint> = storekey::Decode::decode(d)?;
        Ok(Self(Arc::from(parts)))
    }
}

impl TargetStatePath {
    pub fn new(key_part: utils::fingerprint::Fingerprint, parent: Option<&Self>) -> Self {
        let inner: Arc<[utils::fingerprint::Fingerprint]> = match parent {
            Some(parent) => parent
                .0
                .iter()
                .chain(std::iter::once(&key_part))
                .cloned()
                .collect(),
            None => Arc::new([key_part]),
        };
        Self(inner)
    }

    pub fn concat(&self, part: &StableKey) -> Self {
        let fp = utils::fingerprint::Fingerprint::from(&part).unwrap();
        let inner: Arc<[utils::fingerprint::Fingerprint]> =
            self.0.iter().chain(std::iter::once(&fp)).cloned().collect();
        Self(inner)
    }

    pub fn provider_path(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.0[..self.0.len() - 1]
    }

    pub fn as_slice(&self) -> &[utils::fingerprint::Fingerprint] {
        &self.0
    }
}

#[derive(Debug, Clone, Default)]
pub struct TargetStateProviderGeneration {
    pub provider_id: u64,
    pub provider_schema_version: u64,
}

impl Serialize for TargetStateProviderGeneration {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.provider_schema_version == 0 {
            self.provider_id.serialize(serializer)
        } else {
            (self.provider_id, self.provider_schema_version).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TargetStateProviderGeneration {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = TargetStateProviderGeneration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a u64 or a tuple of (u64, u64)")
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(TargetStateProviderGeneration {
                    provider_id: v,
                    provider_schema_version: 0,
                })
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let provider_id = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let schema_version = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                Ok(TargetStateProviderGeneration {
                    provider_id,
                    provider_schema_version: schema_version,
                })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TargetStatePathWithProviderId {
    pub target_state_path: TargetStatePath,
    pub provider_id: Option<u64>,
}

/// Internal representation for `TargetStatePathWithProviderId` serde.
/// Uses `Cow` so serialization can borrow and deserialization can own.
#[derive(Serialize, Deserialize)]
struct TargetStatePathWithProviderIdInternal<'a> {
    #[serde(rename = "P")]
    target_state_path: Cow<'a, TargetStatePath>,
    #[serde(rename = "I", default, skip_serializing_if = "Option::is_none")]
    provider_id: Option<u64>,
}

impl Serialize for TargetStatePathWithProviderId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        TargetStatePathWithProviderIdInternal {
            target_state_path: Cow::Borrowed(&self.target_state_path),
            provider_id: self.provider_id,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TargetStatePathWithProviderId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = TargetStatePathWithProviderId;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a target state path with optional provider id")
            }

            /// Old format: flat array of fingerprints (no provider_id).
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut fps = Vec::new();
                while let Some(fp) = seq.next_element()? {
                    fps.push(fp);
                }
                Ok(TargetStatePathWithProviderId {
                    target_state_path: TargetStatePath(Arc::from(fps)),
                    provider_id: None,
                })
            }

            /// New format: map with "P" (path) and optional "I" (provider_id).
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let internal = TargetStatePathWithProviderIdInternal::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(TargetStatePathWithProviderId {
                    target_state_path: internal.target_state_path.into_owned(),
                    provider_id: internal.provider_id,
                })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl std::fmt::Display for TargetStatePathWithProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.target_state_path)?;
        if let Some(id) = self.provider_id {
            write!(f, "[provider_id={id}]")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_path(fps: &[[u8; 16]]) -> TargetStatePath {
        TargetStatePath(Arc::from(
            fps.iter()
                .map(|b| utils::fingerprint::Fingerprint(*b))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_roundtrip_msgpack_none() {
        let original = TargetStatePathWithProviderId {
            target_state_path: make_path(&[[1u8; 16], [2u8; 16]]),
            provider_id: None,
        };
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: TargetStatePathWithProviderId = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.target_state_path, original.target_state_path);
        assert_eq!(decoded.provider_id, None);
    }

    #[test]
    fn test_roundtrip_msgpack_some() {
        let original = TargetStatePathWithProviderId {
            target_state_path: make_path(&[[1u8; 16], [2u8; 16]]),
            provider_id: Some(42),
        };
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: TargetStatePathWithProviderId = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.target_state_path, original.target_state_path);
        assert_eq!(decoded.provider_id, Some(42));
    }

    #[test]
    fn test_roundtrip_msgpack_btreemap_key() {
        let mut map: BTreeMap<TargetStatePathWithProviderId, String> = BTreeMap::new();
        map.insert(
            TargetStatePathWithProviderId {
                target_state_path: make_path(&[[1u8; 16]]),
                provider_id: None,
            },
            "none".into(),
        );
        map.insert(
            TargetStatePathWithProviderId {
                target_state_path: make_path(&[[2u8; 16]]),
                provider_id: Some(42),
            },
            "some".into(),
        );
        let bytes = rmp_serde::to_vec_named(&map).unwrap();
        let decoded: BTreeMap<TargetStatePathWithProviderId, String> =
            rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[&TargetStatePathWithProviderId {
                target_state_path: make_path(&[[1u8; 16]]),
                provider_id: None,
            }],
            "none"
        );
        assert_eq!(
            decoded[&TargetStatePathWithProviderId {
                target_state_path: make_path(&[[2u8; 16]]),
                provider_id: Some(42),
            }],
            "some"
        );
    }

    #[test]
    fn test_backward_compat_old_format() {
        // Old format: TargetStatePath serialized directly (flat array of fingerprints).
        let old_path = make_path(&[[3u8; 16], [4u8; 16]]);
        let bytes = rmp_serde::to_vec_named(&old_path).unwrap();
        let decoded: TargetStatePathWithProviderId = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.target_state_path, old_path);
        assert_eq!(decoded.provider_id, None);
    }
}
