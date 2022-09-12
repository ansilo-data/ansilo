use ansilo_core::err::{ensure, Context, Result};
use ansilo_logging::trace;

use crate::{ctx::Ctx, processor::util::expression_to_string};

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration that embeds the output of the supplied url
/// This expects the output to parse as valid YAML.
/// This feature allows for a form of easy confirmation splitting.
#[derive(Default)]
pub struct EmbedConfigProcessor {}

impl ConfigExprProcessor for EmbedConfigProcessor {
    fn display_name(&self) -> &str {
        "embed"
    }

    fn process(&self, _ctx: &Ctx, expr: X) -> Result<ConfigExprResult> {
        Ok(match match_interpolation(&expr, &["embed"]) {
            Some(p) => {
                ensure!(p.len() > 1, "${{embed:...}} expression must have arguments");

                let url = p[1..].join(":");

                trace!("Retrieving data from url {url}");
                let output = ansilo_util_url::get(url.clone())
                    .with_context(|| format!("Failed to retrieved {url}"))?;

                let yaml = serde_yaml::from_slice::<serde_yaml::Value>(output.as_slice())
                    .with_context(|| format!("Failed to parse output from {url} as yaml"))?;

                trace!(
                    "Replaced configuration expression '{}' with '{}'",
                    expression_to_string(&expr),
                    serde_yaml::to_string(&yaml).unwrap_or("invalid".into())
                );

                ConfigExprResult::Yaml(yaml)
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
    fn test_embed_processor_ignores_constants() {
        let ctx = Ctx::mock();
        let processor = EmbedConfigProcessor::default();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_embed_processor_ignores_unknown_prefix() {
        let ctx = Ctx::mock();
        let processor = EmbedConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_embed_processor_replaces_embed_file_as_parsed_yaml() {
        let ctx = Ctx::mock();
        let processor = EmbedConfigProcessor::default();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"foo: bar").unwrap();

        let input = X::Interpolation(vec![
            X::Constant("embed".to_owned()),
            X::Constant(format!(
                "file://{}",
                file.path().to_string_lossy().to_string()
            )),
        ]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Yaml(serde_yaml::Value::Mapping(
                [(
                    serde_yaml::Value::String("foo".into()),
                    serde_yaml::Value::String("bar".into())
                )]
                .into_iter()
                .collect()
            ))
        );
    }
}
