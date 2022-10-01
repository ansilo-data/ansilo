use std::{
    ffi::CString,
    path::Path,
    sync::mpsc::{self, channel},
    time::Duration,
};

use ansilo_core::err::Context;
use ansilo_logging::{info, trace, warn};
use nix::sys::signal;
use notify::{watcher, RecursiveMode, Watcher};

use crate::conf::AppConf;

/// We support a fast-reload mode for development using `ansilo dev`.
/// We will trigger a term signal when configuration files are updated.
pub fn signal_on_config_update(path: &Path) {
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_secs(10)).unwrap();

    // Watch for changes on root config file
    trace!("Watching on changes for {}", path.display());
    watcher
        .watch(&path, RecursiveMode::NonRecursive)
        .context(path.clone().to_string_lossy().to_string())
        .unwrap();

    terminate_on_event(rx)
}

pub fn signal_on_sql_update(conf: &AppConf) {
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_secs(10)).unwrap();

    // Watch for changes on sql files
    for stage in conf.node.build.stages.iter() {
        // Watch on the parent dir to enable new files when using glob "/a/b/c/*.sql" etc
        let mut path = stage.sql.as_path();
        while path.file_name().is_some()
            && path.file_name().unwrap().to_string_lossy().contains("*")
        {
            path = if let Some(p) = path.parent() {
                p
            } else {
                break;
            };
        }

        trace!("Watching on changes for {}", path.display());
        watcher
            .watch(path, RecursiveMode::Recursive)
            .context(path.clone().to_string_lossy().to_string())
            .unwrap();
    }

    terminate_on_event(rx)
}

fn terminate_on_event(rx: mpsc::Receiver<notify::DebouncedEvent>) -> ! {
    loop {
        match rx.recv() {
            Ok(event) => {
                info!("Configuration file change detected: {:?}", event);
                terminate();
            }
            Err(e) => {
                warn!("Failed to watch for file changes: {:?}", e);
            }
        }
    }
}

// Signal the current process to terminate
fn terminate() {
    let pid = nix::unistd::getpid();
    signal::kill(pid, signal::SIGUSR1).unwrap();
}

// Restart the current process with the same arguments
pub fn restart() {
    info!("Restarting...");
    let path = std::env::current_exe().unwrap();
    let args = std::env::args();

    nix::unistd::execv(
        &CString::new(path.to_str().unwrap()).unwrap(),
        args.into_iter()
            .map(|i| CString::new(i).unwrap())
            .collect::<Vec<_>>()
            .as_slice(),
    )
    .unwrap();
}
