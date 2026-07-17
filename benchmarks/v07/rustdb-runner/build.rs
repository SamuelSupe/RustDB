use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest directory"))
        .join("../../..")
        .join("Cargo.toml");
    println!("cargo:rerun-if-changed={}", root.display());
    let manifest = fs::read_to_string(&root).expect("read RustDB Cargo.toml");
    let version = package_version(&manifest).expect("find RustDB package version");
    println!("cargo:rustc-env=RUSTDB_ENGINE_VERSION={version}");
}

fn package_version(manifest: &str) -> Option<&str> {
    let mut package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            package = line == "[package]";
            continue;
        }
        if package && line.starts_with("version") {
            return line
                .split_once('=')?
                .1
                .trim()
                .strip_prefix('"')?
                .strip_suffix('"');
        }
    }
    None
}
