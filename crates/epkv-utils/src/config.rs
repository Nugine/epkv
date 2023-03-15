use std::fs;

use anyhow::{anyhow, Result};
use camino::Utf8Path;
use serde::de::DeserializeOwned;

#[inline]
pub fn read_config_file<T>(path: &Utf8Path) -> Result<T>
where
    T: DeserializeOwned,
{
    match path.extension() {
        Some("toml") => {
            let content = fs::read_to_string(path)?;
            let config: T = toml::from_str(&content)?;
            Ok(config)
        }
        Some("json") => {
            let content = fs::read(path)?;
            let config: T = serde_json::from_slice(&content)?;
            Ok(config)
        }
        _ => Err(anyhow!("unknown config file type")),
    }
}
