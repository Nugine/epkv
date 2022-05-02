use epkv_monitor::config::Config;
use epkv_utils::config::read_config_file;

use std::env;

use camino::Utf8PathBuf;

fn tests_dir() -> Utf8PathBuf {
    let mut dir = Utf8PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    dir.push("tests");
    dir
}

#[test]
fn verify_config() {
    let path = tests_dir().join("local-monitor.toml");
    let config: Config = read_config_file(&path).unwrap();
    println!("{:#?}", config);
}
