use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
};

fn main() {
    // Build the next.js frontend app
    println!("cargo:rerun-if-changed=./frontend/src");
    println!("cargo:rerun-if-changed=./frontend/styles");
    println!("cargo:rerun-if-changed=./frontend/package-lock.json");
    println!("cargo:rerun-if-changed=./frontend/package.json");
    println!("cargo:rerun-if-changed=./frontend/next.config.js");
    println!("cargo:rerun-if-changed=./frontend/build.sh");

    println!("Building frontend...");
    let res = Command::new("bash")
        .args(["build.sh"])
        .current_dir("./frontend")
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());

    let target_dir = get_target_dir();

    println!(
        "Copying frontend to target dir {} ...",
        target_dir.display()
    );
    fs::remove_dir_all(target_dir.join("frontend")).unwrap();

    let res = Command::new("cp")
        .args([
            "-ar",
            "frontend/out/",
            target_dir
                .join("frontend")
                .to_string_lossy()
                .to_string()
                .as_str(),
        ])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
    assert!(res.success());
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
