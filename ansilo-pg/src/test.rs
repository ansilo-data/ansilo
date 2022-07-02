use std::process::{Command, Stdio};

pub fn assert_not_running(pid: u32) {
    assert_ps_output_lines(pid, 1);
}

pub fn assert_running(pid: u32) {
    assert_ps_output_lines(pid, 2);
}

pub fn assert_ps_output_lines(pid: u32, expected_lines: usize) {
    let ps = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap();
    let lines: Vec<String> = String::from_utf8_lossy(ps.stdout.as_slice())
        .lines()
        .map(|i| i.to_owned())
        .collect();
    assert_eq!(lines.len(), expected_lines);
}
