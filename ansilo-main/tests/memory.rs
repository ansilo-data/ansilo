use assert_cmd::prelude::*;
use nix::libc::SIGINT;
use predicates::prelude::*;
use serial_test::serial;
use std::{process::{Command, Stdio}, thread, time::Duration};

mod common;

#[test]
#[serial]
fn test_memory_conf_build() {
    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args([
        "build",
        "-c",
        format!("{}/confs/memory/config.yml", current_dir!()).as_str(),
    ]);
    cmd.assert()
        .success()
        .stderr(predicate::str::contains("Build complete"));
}

#[test]
#[serial]
fn test_memory_conf_run() {
    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args([
        "run",
        "-c",
        format!("{}/confs/memory/config.yml", current_dir!()).as_str(),
    ]);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = cmd.spawn().unwrap();
    let pid = child.id();

    thread::spawn(move || {
        thread::sleep(Duration::from_secs(5));
        unsafe {
            nix::libc::kill(pid as _, SIGINT);
        }
    });

    child
        .wait_with_output()
        .unwrap()
        .assert()
        .success()
        .stderr(predicate::str::contains("Start up complete"));
}
