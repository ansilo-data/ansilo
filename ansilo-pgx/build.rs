use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-env-changed=SSH_CONNECTION");

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
}
