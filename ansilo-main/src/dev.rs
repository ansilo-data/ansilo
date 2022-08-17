use std::{ffi::CString, sync::mpsc::channel, time::Duration};

use ansilo_logging::{info, warn};
use nix::sys::signal;
use notify::{watcher, RecursiveMode, Watcher};

use crate::conf::{conf, conf_path};

/// We support a fast-reload mode for development using `ansilo dev`.
/// We will trigger a term signal when configuration files are updated.
pub fn signal_on_config_update() {
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_secs(10)).unwrap();

    // Watch for changes on root config file
    watcher
        .watch(conf_path(), RecursiveMode::NonRecursive)
        .unwrap();

    // Watch for changes on sql files
    if let Some(mut init_sql_path) = conf()
        .postgres
        .as_ref()
        .and_then(|i| i.init_sql_path.as_ref().map(|i| i.as_path()))
    {
        // Watch on the parent dir to enable new files when using glob "/a/b/c/*.sql" etc
        while init_sql_path.file_name().is_some()
            && init_sql_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("*")
        {
            init_sql_path = if let Some(p) = init_sql_path.parent() {
                p
            } else {
                break;
            };
        }

        watcher
            .watch(init_sql_path, RecursiveMode::Recursive)
            .unwrap();
    }

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
