use std::path::PathBuf;

use clap::Parser;

/// Arguments for running the Ansilo main program
///
/// TODO[docs]: Add about strings below
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub(crate) enum Command {
    Run(Args),
    Dev(Args),
    Build(Args),
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub(crate) struct Args {
    /// The path of the main configuration file
    #[clap(short, long, value_parser)]
    pub config: Option<PathBuf>,
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
