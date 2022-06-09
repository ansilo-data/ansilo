use std::env;

use ansilo_core::err::Result;
use ansilo_logging::warn;

use crate::loader::ConfigLoader;

use super::{
    util::{expression_to_string, match_interpolation},
    ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration using environment variables
#[derive(Default)]
pub struct EnvConfigProcessor {}

impl EnvConfigProcessor {
    fn new() -> Self {
        Self {}
    }
}

impl ConfigExprProcessor for EnvConfigProcessor {
    fn display_name(&self) -> &str {
        "environment"
    }

    fn process(&self, _loader: &ConfigLoader, expr: X) -> Result<ConfigExprResult> {
        Ok(ConfigExprResult::Expr(
            match match_interpolation(&expr, &["env"]) {
                Some(p) => {
                    let name = p
                        .get(1)
                        .map(|i| i.to_string())
                        .unwrap_or_else(|| "".to_owned());
                    let default = p
                        .get(2)
                        .map(|i| i.to_string())
                        .unwrap_or_else(|| "".to_owned());

                    let var = match env::var(name.clone()) {
                        Err(err) => {
                            warn!("Failed to get env var \"{}\": {}", name, err);
                            None
                        }
                        Ok(var) if var.is_empty() => None,
                        Ok(var) => Some(var),
                    };

                    X::Constant(var.unwrap_or(default))
                }
                _ => expr,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::path;

    use crate::loader::ConfigLoader;

    use super::*;

    #[test]
    fn test_env_processor_ignores_constants() {
        let loader = ConfigLoader::mock();
        let processor = EnvConfigProcessor::new();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&loader, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_env_processor_ignores_unknown_prefix() {
        let loader = ConfigLoader::mock();
        let processor = EnvConfigProcessor::new();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&loader, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_env_processor_replaces_env_var() {
        let loader = ConfigLoader::mock();
        let processor = EnvConfigProcessor::new();

        env::set_var("ANSILO_TEST_VAR1", "FROM_ENV");
        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR1".to_owned()),
        ]);
        let result = processor.process(&loader, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("FROM_ENV".to_string()))
        );
    }

    #[test]
    fn test_env_processor_default_value() {
        let loader = ConfigLoader::mock();
        let processor = EnvConfigProcessor::new();

        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR2".to_owned()),
            X::Constant("DEFAULT_VAL".to_owned())
        ]);
        let result = processor.process(&loader, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("DEFAULT_VAL".to_string()))
        );
    }

    #[test]
    fn test_env_processor_uses_default_value_if_env_var_is_empty() {
        let loader = ConfigLoader::mock();
        let processor = EnvConfigProcessor::new();

        env::set_var("ANSILO_TEST_VAR3", "");
        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR3".to_owned()),
            X::Constant("DEFAULT_VAL".to_owned())
        ]);
        let result = processor.process(&loader, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("DEFAULT_VAL".to_string()))
        );
    }
}
