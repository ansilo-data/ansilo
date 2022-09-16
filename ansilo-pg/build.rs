use std::{
    env,
    process::{Command, Stdio},
};

fn main() {
    if env::var("ANSILO_PGX_INSTALL").is_ok() {
        print!("Running in pgx install, skipping build...");
        return;
    }

    // Build and install the ansilo-pgx so it can be loaded
    // via CREATE EXTENSION in the current postgres installation
    println!("cargo:rerun-if-changed=../ansilo-core/src");
    println!("cargo:rerun-if-changed=../ansilo-connectors/base/src");
    println!("cargo:rerun-if-changed=../ansilo-pg/src/fdw");
    println!("cargo:rerun-if-changed=../ansilo-pgx/src");
    println!("cargo:rerun-if-env-changed=SSH_CONNECTION");

    println!("Granting access to postgres ext/lib dirs...");
    let res = Command::new("bash")
        .args(["-c", &format!("sudo setfacl -m u:$USER:rwx $(pg_config --sharedir)/extension/ $(pg_config --pkglibdir)")])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
    assert!(res.success());

    println!("Installing ansilo-pgx extension...");
    // We build the extension to a separated target dir
    // as we conflict against the outer cargo build if we use
    // the inherited one.
    let res = Command::new("cargo")
        .args(["pgx", "install"])
        .env("ANSILO_PGX_INSTALL", "true")
        .env(
            "CARGO_TARGET_DIR",
            format!(
                "{}/tmp/ansilo-pgx-build/target/",
                env::var("WORKSPACE_HOME").unwrap_or("".into())
            ),
        )
        .current_dir("../ansilo-pgx")
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());
}
