use std::path::PathBuf;

use ansilo_core::err::{Error, Result};
use clap::Parser;

/// Arguments for running the Ansilo main program
#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub enum Command {
    /// Runs postgres so it is ready to accept connnections.
    ///
    /// If the databasde has not been initialized it will be initialised
    /// with the current configuration.
    Run(Args),
    /// Runs in development mode with hot-reload enabled
    Dev(Args),
    /// Initializes postgres so it can be booted rapidly.
    Build(Args),
    /// Prints the config, after evaluating all expressions, to stdout
    DumpConfig(Args),
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// The path of the main configuration file
    #[clap(short, long, value_parser)]
    pub config: Option<PathBuf>,

    /// Arguments applied to the configuration itself
    /// Defined by "-D MY_ARG_NAME=value" and can be referenced
    /// in the config using ${arg:MY_ARG_NAME}
    #[clap(short = 'D', long, value_parser = parse_key_val)]
    pub config_args: Vec<(String, String)>,

    /// Whether to force a build of the postgres database
    #[clap(short, long, value_parser)]
    pub force_build: bool,
}

impl Command {
    pub(crate) fn args(&self) -> &Args {
        match self {
            Command::Run(args) => args,
            Command::Build(args) => args,
            Command::Dev(args) => args,
            Command::DumpConfig(args) => args,
        }
    }

    /// Returns `true` if the command is [`Run`].
    ///
    /// [`Run`]: Command::Run
    #[must_use]
    #[allow(unused)]
    pub(crate) fn is_run(&self) -> bool {
        matches!(self, Self::Run(..))
    }

    /// Returns `true` if the command is [`Dev`].
    ///
    /// [`Dev`]: Command::Dev
    #[must_use]
    pub(crate) fn is_dev(&self) -> bool {
        matches!(self, Self::Dev(..))
    }

    /// Returns `true` if the command is [`Build`].
    ///
    /// [`Build`]: Command::Build
    #[must_use]
    pub(crate) fn is_build(&self) -> bool {
        matches!(self, Self::Build(..))
    }

    /// Returns `true` if the command is [`DumpConfig`].
    ///
    /// [`DumpConfig`]: Command::DumpConfig
    #[must_use]
    pub fn is_dump_config(&self) -> bool {
        matches!(self, Self::DumpConfig(..))
    }
}

impl Args {
    pub(crate) fn config(&self) -> std::path::PathBuf {
        self.config
            .clone()
            .unwrap_or("/app/ansilo.yml".into())
            .to_path_buf()
    }
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| Error::msg(format!("invalid KEY=value: no `=` found in `{}`", s)))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
