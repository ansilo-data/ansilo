use std::fmt::{self, Display};

use miette::{Diagnostic, SourceSpan};

#[derive(Debug, Diagnostic)]
#[diagnostic(code(config_parse_error))]
pub struct ConfigParseError {
    #[source_code]
    src: String,
    #[label("Error occurred here")]
    loc: Option<SourceSpan>,
    #[help]
    error: String,
}

impl Display for ConfigParseError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl std::error::Error for ConfigParseError {}

impl ConfigParseError {
    pub fn new(src: String, loc: serde_yaml::Location, error: String) -> Self {
        let loc = if loc.index() > 0 {
            Some(SourceSpan::new(
                loc.index().into(),
                src[loc.index()..].find('\n').unwrap_or(1).into(),
            ))
        } else {
            None
        };

        Self { src, loc, error }
    }

    pub fn print(self) {
        let _ = miette::set_hook(Box::new(|_| {
            Box::new(miette::MietteHandlerOpts::new().context_lines(3).build())
        }));
        eprintln!("Error: {:?}", miette::Report::new(self));
    }
}
