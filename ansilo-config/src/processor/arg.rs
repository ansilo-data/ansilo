use ansilo_core::err::{bail, ensure, Result};
use ansilo_logging::trace;

use crate::{ctx::Ctx, processor::util::expression_to_string};

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates confugration arguments from the command line
#[derive(Default)]
pub struct ArgConfigProcessor {}

impl ConfigExprProcessor for ArgConfigProcessor {
    fn display_name(&self) -> &str {
        "argument"
    }

    fn process(&self, ctx: &mut Ctx, expr: X) -> Result<ConfigExprResult> {
        Ok(ConfigExprResult::Expr(
            match match_interpolation(&expr, &["arg"]) {
                Some(p) => {
                    ensure!(p.len() > 1, "${{arg:...}} expression cannot be empty");

                    let replacement = match ctx.args.get(&p[1]) {
                        Some(v) => v,
                        None => {
                            bail!("Configuration argument '{}' does not exist", &p[1]);
                        }
                    };

                    trace!(
                        "Replaced configuration expression '{}' with '{}'",
                        expression_to_string(&expr),
                        replacement
                    );

                    X::Constant(replacement.clone())
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
    fn test_arg_processor_ignores_constants() {
        let mut ctx = Ctx::mock();
        let processor = ArgConfigProcessor::default();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&mut ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_arg_processor_ignores_unknown_prefix() {
        let mut ctx = Ctx::mock();
        let processor = ArgConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&mut ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_arg_processor_replaces_arg_expr() {
        let mut ctx = Ctx::mock();
        ctx.args.insert("TEST_ARG".into(), "arg val".into());
        let processor = ArgConfigProcessor::default();

        let input = X::Interpolation(vec![
            X::Constant("arg".into()),
            X::Constant("TEST_ARG".into()),
        ]);
        let result = processor.process(&mut ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("arg val".to_string()))
        );
    }

    #[test]
    fn test_arg_processor_errors_when_arg_not_set() {
        let mut ctx = Ctx::mock();
        let processor = ArgConfigProcessor::default();

        let input = X::Interpolation(vec![
            X::Constant("arg".into()),
            X::Constant("NON_EXISTANT".into()),
        ]);
        let result = processor.process(&mut ctx, input.clone());

        result.unwrap_err();
    }
}
