use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
};

/// Builds a mvn java module
pub fn build_java_maven_module(path: &str) {
    println!("cargo:rerun-if-changed={}/src", path);
    println!("cargo:rerun-if-changed={}/pom.xml", path);

    println!("Running mvn build...");

    Command::new("mvn")
        .args(&[
            "clean",
            "compile",
            "package",
            "dependency:copy-dependencies",
        ])
        .current_dir(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    let target_dir = get_target_dir();

    println!("Copying jar to target dir {} ...", target_dir.display());
    for entry in fs::read_dir(format!("{path}/target")).unwrap() {
        let jar = match entry {
            Ok(dir) if dir.file_name().to_string_lossy().ends_with(".jar") => dir,
            _ => continue,
        };

        let dest = target_dir.join(jar.file_name());
        println!("Copying {} to {}", jar.path().display(), dest.display());
        fs::copy(jar.path(), dest).unwrap();
    }
}

fn get_target_dir() -> PathBuf {
    let mut out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    while out_dir
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        != "target"
    {
        out_dir = out_dir.parent().unwrap().to_path_buf();
    }

    out_dir
}
