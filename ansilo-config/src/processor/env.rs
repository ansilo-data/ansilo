use std::env;

use ansilo_core::err::Result;
use ansilo_logging::{warn, trace};

use crate::ctx::Ctx;

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration using environment variables
#[derive(Default)]
pub struct EnvConfigProcessor {}

impl ConfigExprProcessor for EnvConfigProcessor {
    fn display_name(&self) -> &str {
        "environment"
    }

    fn process(&self, _ctx: &Ctx, expr: X) -> Result<ConfigExprResult> {
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

                    let replacement = var.unwrap_or(default);
                    trace!("Replaced configuration expression '{}' with '{}'", name, replacement);
                    X::Constant(replacement)
                }
                _ => expr,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_processor_ignores_constants() {
        let ctx = Ctx::mock();
        let processor = EnvConfigProcessor::default();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_env_processor_ignores_unknown_prefix() {
        let ctx = Ctx::mock();
        let processor = EnvConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_env_processor_replaces_env_var() {
        let ctx = Ctx::mock();
        let processor = EnvConfigProcessor::default();

        env::set_var("ANSILO_TEST_VAR1", "FROM_ENV");
        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR1".to_owned()),
        ]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("FROM_ENV".to_string()))
        );
    }

    #[test]
    fn test_env_processor_default_value() {
        let ctx = Ctx::mock();
        let processor = EnvConfigProcessor::default();

        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR2".to_owned()),
            X::Constant("DEFAULT_VAL".to_owned()),
        ]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("DEFAULT_VAL".to_string()))
        );
    }

    #[test]
    fn test_env_processor_uses_default_value_if_env_var_is_empty() {
        let ctx = Ctx::mock();
        let processor = EnvConfigProcessor::default();

        env::set_var("ANSILO_TEST_VAR3", "");
        let input = X::Interpolation(vec![
            X::Constant("env".to_owned()),
            X::Constant("ANSILO_TEST_VAR3".to_owned()),
            X::Constant("DEFAULT_VAL".to_owned()),
        ]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("DEFAULT_VAL".to_string()))
        );
    }
}
