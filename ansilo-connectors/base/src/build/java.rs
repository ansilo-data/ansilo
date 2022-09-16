use std::{
    env,
    fs::{self, OpenOptions},
    path::PathBuf,
    process::{Command, Stdio},
};

use fd_lock::RwLock;

/// Builds a mvn java module
pub fn build_java_maven_module(path: &str) {
    println!("cargo:rerun-if-changed={}/src", path);
    println!("cargo:rerun-if-changed={}/pom.xml", path);

    println!("Acquiring file lock...");
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(PathBuf::from(path).join(".lock"))
        .unwrap();
    let mut lock = RwLock::new(lock_file);
    let _guard = lock.write().unwrap();

    println!("Running mvn build...");

    let res = Command::new("mvn")
        .args(&[
            "clean",
            "compile",
            "package",
            "dependency:copy-dependencies",
            "install",
        ])
        .current_dir(path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());

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
