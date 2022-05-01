use epkv_server::config::Config;
use epkv_utils::config::read_config_file;

use std::env;

use camino::Utf8PathBuf;

fn tests_dir() -> Utf8PathBuf {
    let mut dir = Utf8PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    dir.push("tests");
    dir
}

#[test]
fn example_config() {
    let example_config_path = tests_dir().join("example-config.toml");
    let config: Config = read_config_file(&example_config_path).unwrap();
    println!("{:#?}", config);
}
