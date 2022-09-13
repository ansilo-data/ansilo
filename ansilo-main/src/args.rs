use std::path::PathBuf;

use ansilo_core::err::{Error, Result};
use clap::Parser;

/// Arguments for running the Ansilo main program
///
/// TODO[docs]: Add about strings below
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub enum Command {
    Run(Args),
    Dev(Args),
    Build(Args),
}

#[derive(Parser, Debug)]
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
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| Error::msg(format!("invalid KEY=value: no `=` found in `{}`", s)))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
