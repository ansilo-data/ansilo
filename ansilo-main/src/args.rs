use std::path::PathBuf;

use clap::Parser;

/// Arguments for running the Ansilo main program
///
/// TODO[docs]: Add about strings below
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub(crate) enum Command {
    Run(Args),
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
        }
    }
}
