fn main() {
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
