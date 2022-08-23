use assert_cmd::prelude::*;
use nix::libc::SIGINT;
use predicates::prelude::*;
use serial_test::serial;
use std::{
    fs, path,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

mod common;

fn setup() {
    // Remove ansilo & postgres data files before each test
    let _ = fs::remove_dir_all("/tmp/ansilo/pg-main/memory/");
}

fn conf() -> String {
    format!("{}/confs/memory/config.yml", current_dir!())
}

#[test]
#[serial]
fn test_memory_conf_build() {
    setup();

    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args(["build", "-c", conf().as_str()]);
    cmd.assert()
        .success()
        .stderr(predicate::str::contains("Build complete"));

    assert!(path::Path::new("/tmp/ansilo/pg-main/memory/build-info.json").is_file());
}

#[test]
#[serial]
fn test_memory_conf_run() {
    setup();

    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args(["run", "-c", conf().as_str()]);
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

#[test]
#[serial]
fn test_memory_conf_build_then_run() {
    setup();

    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args(["build", "-c", conf().as_str()]);
    cmd.assert()
        .success()
        .stderr(predicate::str::contains("Build complete"));

    assert!(path::Path::new("/tmp/ansilo/pg-main/memory/build-info.json").is_file());

    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args(["run", "-c", conf().as_str()]);
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

#[test]
#[serial]
fn test_memory_conf_dev() {
    setup();

    let mut cmd = Command::cargo_bin("ansilo-main").unwrap();

    cmd.args(["dev", "-c", conf().as_str()]);
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
        .stderr(predicate::str::contains("Build complete"))
        .stderr(predicate::str::contains("Start up complete"));
}
