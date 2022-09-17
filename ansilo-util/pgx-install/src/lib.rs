use std::{
    env,
    process::{Command, Stdio},
};

pub use ctor::ctor;

macro_rules! workspace_dir {
    () => {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned()
    };
}

#[macro_export]
macro_rules! install_ansilo_pgx {
    () => {
        #[ansilo_util_pgx_install::ctor]
        fn install_ansilo_pgx_ctor() {
            ansilo_util_pgx_install::install_ansilo_pgx();
        }
    }
}

pub fn install_ansilo_pgx() {
    println!("Granting access to postgres ext/lib dirs...");
    let res = Command::new("bash")
        .args(["-c", &format!("sudo setfacl -m u:$(id -u):rwx $(pg_config --sharedir)/extension/ $(pg_config --pkglibdir)")])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
    assert!(res.success());

    println!("Installing ansilo-pgx extension... {}", workspace_dir!().display());
    // We build the extension to a separated target dir
    // as we conflict against the outer cargo build if we use
    // the inherited one.
    let res = Command::new("cargo")
        .args(["pgx", "install"])
        .current_dir(workspace_dir!().join("ansilo-pgx"))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    assert!(res.success());
}
