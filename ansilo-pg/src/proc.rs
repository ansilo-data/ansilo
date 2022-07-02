use std::{
    io::{self, BufRead},
    process::{self, Command, ExitStatus, Stdio},
    thread,
    time::Duration,
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, error, info, warn};
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};

/// Class for dealing with child procs
#[derive(Debug)]
pub(crate) struct ChildProc {
    /// Log prefix for stdout/stderr
    log_prefix: &'static str,
    /// Signal used to terminate the process gracefully
    term_signal: Signal,
    /// Duration to wait for the process to gracefully shutdown
    term_timeout: Duration,
    /// The child postgres process
    pub proc: process::Child,
}

impl ChildProc {
    /// Constructs the child proc
    pub fn new(
        log_prefix: &'static str,
        term_signal: Signal,
        term_timeout: Duration,
        mut cmd: Command,
    ) -> Result<Self> {
        Ok(Self {
            log_prefix,
            term_signal,
            term_timeout,
            proc: cmd
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| format!("{} Failed to spawn process", log_prefix))?,
        })
    }

    /// Waits for the process to exit and streams any stdout/stderr to the logs
    pub fn wait(&mut self) -> Result<ExitStatus> {
        let stdout = self.proc.stdout.take().context("Failed to take stdout")?;
        let stderr = self.proc.stderr.take().context("Failed to take stdout")?;
        let prefix = self.log_prefix;

        thread::spawn(move || {
            for i in io::BufReader::new(stdout).lines() {
                let i = i.unwrap_or_else(|e| format!("{} failed to read: {}", prefix, e));
                info!("{} {}", prefix, i)
            }
        });

        thread::spawn(move || {
            for i in io::BufReader::new(stderr).lines() {
                let i = i.unwrap_or_else(|e| format!("{} failed to read: {}", prefix, e));
                warn!("{} {}", prefix, i)
            }
        });

        self.proc
            .wait()
            .with_context(|| format!("{} Error while waiting for process", self.log_prefix))
    }

    /// Gets the pid of the proc
    pub fn pid(&self) -> u32 {
        self.proc.id()
    }
}

impl Drop for ChildProc {
    fn drop(&mut self) {
        // If already exited
        if let Ok(Some(status)) = self.proc.try_wait() {
            info!("{} Exited with code: {}", self.log_prefix, status);
            return;
        }

        // Attempt a graceful shutdown
        debug!("{} Sending {} signal...", self.log_prefix, self.term_signal);
        let res = kill(Pid::from_raw(self.proc.id() as _), self.term_signal);

        if let Err(err) = res {
            error!(
                "{} Failed to send SIGINT to postgres: {}",
                self.log_prefix, err
            );
            return;
        }

        let mut attempts = 0;
        let timeout_ms = self.term_timeout.as_millis();

        let status = loop {
            match self.proc.try_wait() {
                Ok(Some(status)) => {
                    break status;
                }
                Ok(None) => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => {
                    error!("{} Failed to wait: {}", self.log_prefix, err);
                    return;
                }
            }

            if attempts * 10 > timeout_ms {
                warn!(
                    "{} Failed to terminate after {}ms, killing...",
                    self.log_prefix, timeout_ms
                );

                if let Err(err) = self.proc.kill() {
                    error!("{} Failed to kill process: {}", self.log_prefix, err);
                    return;
                }

                match self.proc.wait() {
                    Ok(status) => {
                        break status;
                    }
                    Err(err) => {
                        error!("{} Failed to wait: {}", self.log_prefix, err);
                        return;
                    }
                }
            }

            attempts += 1;
        };

        info!("{} Exited with code: {}", self.log_prefix, status);
        return;
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use crate::test::{assert_running, assert_not_running};

    use super::*;

    #[test]
    fn test_child_proc_wait() {
        ansilo_logging::init_for_tests();
        let mut proc = ChildProc::new(
            "cmd",
            Signal::SIGINT,
            Duration::from_millis(10),
            Command::new("/bin/true"),
        )
        .unwrap();

        assert!(proc.wait().unwrap().success());
    }

    #[test]
    fn test_child_proc_wait_error() {
        ansilo_logging::init_for_tests();
        let mut proc = ChildProc::new(
            "cmd",
            Signal::SIGINT,
            Duration::from_millis(10),
            Command::new("/bin/false"),
        )
        .unwrap();

        assert!(!proc.wait().unwrap().success());
    }

    #[test]
    fn test_child_proc_drop_sigints_proc() {
        ansilo_logging::init_for_tests();
        let mut cmd = Command::new("sleep");
        cmd.arg("10");
        let proc = ChildProc::new("cmd", Signal::SIGINT, Duration::from_millis(100), cmd).unwrap();
        let pid = proc.proc.id();

        // sigint should terminate process
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            assert_not_running(pid);
        });
        drop(proc);

        thread.join().unwrap();
        assert_not_running(pid);
    }

    #[test]
    fn test_child_proc_drop_sigkills_proc_after_timeout() {
        ansilo_logging::init_for_tests();
        let mut cmd = Command::new("sleep");
        cmd.arg("10");
        let proc = ChildProc::new("cmd", Signal::SIGCONT, Duration::from_millis(100), cmd).unwrap();
        let pid = proc.proc.id();

        // first SIGCONT signal from drop should be ignored
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            assert_running(pid);
        });
        drop(proc);

        thread.join().unwrap();
        assert_not_running(pid);
    }
}
