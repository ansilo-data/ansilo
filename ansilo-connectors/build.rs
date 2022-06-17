use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

fn main() {
    compile_jdbc_java();
}

fn compile_jdbc_java() {
    println!("cargo:rerun-if-changed=src/jdbc/java/src");
    println!("cargo:rerun-if-changed=src/jdbc/java/pom.xml");

    println!("Running mvn build...");

    Command::new("mvn")
        .args(&["clean", "compile", "package"])
        .current_dir("src/jdbc/java")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    let target_dir = get_target_dir();

    println!("Copying jar to target dir {} ...", target_dir.display());
    fs::copy(
        "src/jdbc/java/target/ansilo-jdbc-1.0-SNAPSHOT.jar",
        target_dir.join("ansilo-jdbc-1.0-SNAPSHOT.jar"),
    )
    .unwrap();
}

fn get_target_dir() -> PathBuf {
    let mut out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    while out_dir.parent().unwrap().file_name().unwrap().to_string_lossy() != "target" {
        out_dir = out_dir.parent().unwrap().to_path_buf();
    }

    out_dir
}
