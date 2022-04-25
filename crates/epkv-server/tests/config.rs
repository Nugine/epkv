use epkv_server::config::Config;

use std::env;
use std::fs;

use camino::Utf8PathBuf;

fn tests_dir() -> Utf8PathBuf {
    let mut dir = Utf8PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    dir.push("tests");
    dir
}

#[test]
fn example_config() {
    let example_config_path = tests_dir().join("example-config.toml");
    let content = fs::read_to_string(&example_config_path).unwrap();
    let config: Config = toml::from_str(&content).unwrap();
    println!("{:#?}", config);
}
