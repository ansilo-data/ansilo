use ansilo_core::err::{ensure, Context, Result};
use ansilo_logging::trace;

use crate::{ctx::Ctx, processor::util::expression_to_string};

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration that fetchs the output of the supplied url
/// This will return the output as UTF8 string
#[derive(Default)]
pub struct FetchConfigProcessor {}

impl ConfigExprProcessor for FetchConfigProcessor {
    fn display_name(&self) -> &str {
        "fetch"
    }

    fn process(&self, _ctx: &Ctx, expr: X) -> Result<ConfigExprResult> {
        Ok(match match_interpolation(&expr, &["fetch"]) {
            Some(p) => {
                ensure!(p.len() > 1, "${{fetch:...}} expression must have arguments");

                let url = p[1..].join(":");

                trace!("Retrieving data from url {url}");
                let output = ansilo_util_url::get(url.clone())
                    .with_context(|| format!("Failed to retrieved {url}"))?;

                let output = String::from_utf8(output)
                    .with_context(|| format!("Failed to parse output from url as UTF8: {url}"))?;

                trace!(
                    "Replaced configuration expression '{}' with '{}'",
                    expression_to_string(&expr),
                    output
                );

                ConfigExprResult::Expr(X::Constant(output))
            }
            _ => ConfigExprResult::Expr(expr),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_fetch_processor_ignores_constants() {
        let ctx = Ctx::mock();
        let processor = FetchConfigProcessor::default();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_fetch_processor_ignores_unknown_prefix() {
        let ctx = Ctx::mock();
        let processor = FetchConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_fetch_processor_replaces_fetch_file_as_string() {
        let ctx = Ctx::mock();
        let processor = FetchConfigProcessor::default();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let input = X::Interpolation(vec![
            X::Constant("fetch".to_owned()),
            X::Constant(format!(
                "file://{}",
                file.path().to_string_lossy().to_string()
            )),
        ]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("hello world".into()))
        );
    }
}
