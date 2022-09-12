use std::{
    path::PathBuf,
    process::{self, Stdio},
};

use ansilo_core::err::{bail, Context, Result};

pub(crate) fn get_shell(path: PathBuf, args: Option<String>) -> Result<Vec<u8>> {
    let dbg_cmd = if let Some(args) = args.as_ref() {
        format!("{} {}", path.display(), args)
    } else {
        path.display().to_string()
    };

    let output = process::Command::new(&path)
        .args(
            args.clone()
                .map(|a| a.split(' ').map(|a| a.to_string()).collect::<Vec<_>>())
                .unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to spawn '{dbg_cmd}', please check file exists and has correct permissions",
            )
        })?
        .wait_with_output()
        .with_context(|| format!("Failed to wait on process: {dbg_cmd}",))?;

    if !output.status.success() {
        bail!(
            "Running process '{dbg_cmd}' failed with exit code: {:?}",
            output.status.code()
        );
    }

    Ok(output.stdout)
}
