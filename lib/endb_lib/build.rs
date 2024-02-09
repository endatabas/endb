extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_parse_deps(true)
        .with_parse_include(&["arrow"])
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("endb.h");

    let git_describe = if let Some(git_describe) = option_env!("ENDB_GIT_DESCRIBE") {
        String::from(git_describe)
    } else {
        String::from_utf8(
            std::process::Command::new("git")
                .args(["describe", "--always", "--dirty", "--tags"])
                .output()
                .unwrap()
                .stdout,
        )
        .map(|x| x.trim().to_string())
        .unwrap()
    };

    println!("cargo:rustc-env=ENDB_GIT_DESCRIBE={}", git_describe);
}
