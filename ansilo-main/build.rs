use std::{
    env,
    process::{Command, Stdio},
};

fn main() {
    // Build and install the ansilo-pgx so it can be loaded
    // via CREATE EXTENSION in the current postgres installation
    println!("Granting access to postgres ext/lib dirs...");
    let res = Command::new("bash")
        .args(["-c", "sudo setfacl -m u:$USER:rwx /usr/share/postgresql/14/extension/ /usr/lib/postgresql/14/lib/"])
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
        .env(
            "CARGO_TARGET_DIR",
            format!(
                "{}/tmp/ansilo-pgx-build/target/",
                env::var("EFS_HOME").unwrap_or("".into())
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
