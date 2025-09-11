use anyhow::{Result, anyhow};
use serde::de::DeserializeOwned;

fn map_serde_path_err<T: DeserializeOwned>(
    err: serde_path_to_error::Error<serde_json::Error>,
) -> anyhow::Error {
    let ty = std::any::type_name::<T>().replace("::", ".");
    let path = err.path();
    let full_path = if path.iter().next().is_none() {
        format!("<{ty}>")
    } else {
        format!("<{ty}>.{path}")
    };
    let inner = err.into_inner();
    anyhow!("while deserializing `{full_path}`: {inner}")
}

pub fn from_json_value<T: DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    serde_path_to_error::deserialize::<_, T>(value).map_err(map_serde_path_err::<T>)
}

pub fn from_json_str<T: DeserializeOwned>(s: &str) -> Result<T> {
    let mut de = serde_json::Deserializer::from_str(s);
    serde_path_to_error::deserialize::<_, T>(&mut de).map_err(map_serde_path_err::<T>)
}
