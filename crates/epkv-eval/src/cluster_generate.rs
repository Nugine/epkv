use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;

use anyhow::{ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use epkv_utils::config::read_config_file;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub monitor: MergeConfig,
    pub servers: BTreeMap<String, MergeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeConfig {
    pub base: Utf8PathBuf,
    pub overwrite: serde_json::Value,
}

impl MergeConfig {
    pub fn resolve(&self, config_path: &Utf8Path) -> Result<serde_json::Value> {
        let base_doc_path = match config_path.parent() {
            Some(p) => Cow::Owned(p.join(&self.base)),
            None => Cow::Borrowed(&self.base),
        };
        let mut config: serde_json::Value = read_config_file(&base_doc_path)
            .with_context(|| format!("failed to read config file at {base_doc_path}"))?;

        Self::merge(&mut config, &self.overwrite)?;

        Ok(config)
    }

    fn merge(base: &mut serde_json::Value, overwrite: &serde_json::Value) -> Result<()> {
        match overwrite {
            serde_json::Value::Null => {
                ensure!(base.is_null());
            }
            serde_json::Value::Bool(_) => {
                ensure!(base.is_boolean());
                base.clone_from(overwrite);
            }
            serde_json::Value::Number(_) => {
                ensure!(base.is_number());
                base.clone_from(overwrite);
            }
            serde_json::Value::String(_) => {
                ensure!(base.is_string());
                base.clone_from(overwrite);
            }
            serde_json::Value::Array(_) => {
                ensure!(base.is_array());
                base.clone_from(overwrite);
            }
            serde_json::Value::Object(obj) => {
                ensure!(base.is_object());
                let base = base.as_object_mut().unwrap();
                for (key, value) in obj {
                    match base.entry(key) {
                        serde_json::map::Entry::Vacant(e) => {
                            e.insert(value.clone());
                        }
                        serde_json::map::Entry::Occupied(mut e) => {
                            Self::merge(e.get_mut(), value)?;
                        }
                    }
                }
            }
        };
        Ok(())
    }
}

impl Config {
    pub fn generate(&self, config_path: &Utf8Path) -> Result<BTreeMap<String, serde_json::Value>> {
        ensure!(self.servers.keys().all(|k| k != "monitor"));

        let mut files = BTreeMap::new();
        files.insert("monitor".to_owned(), self.monitor.resolve(config_path)?);

        for (server, m) in &self.servers {
            files.insert(server.to_owned(), m.resolve(config_path)?);
        }

        Ok(files)
    }
}

pub fn run(config_path: &Utf8Path, target_dir: &Utf8Path) -> Result<()> {
    fs::create_dir_all(target_dir)?;

    let config: Config = read_config_file(config_path)?;
    println!("read  config: {}", config_path);

    let files = config.generate(config_path)?;

    let formatter = serde_json::ser::PrettyFormatter::with_indent("    ".as_ref());

    for (name, value) in &files {
        let path = target_dir.join(format!("{name}.json"));

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = fs::File::create(&path)?;
        let mut serializer: _ = serde_json::Serializer::with_formatter(file, formatter.clone());

        value.serialize(&mut serializer)?;

        println!("write config: {}", path);
    }

    println!("done");

    Ok(())
}
