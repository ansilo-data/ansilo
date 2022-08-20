use ansilo_core::err::Result;

use crate::ctx::Ctx;

use super::{
    util::match_interpolation, ConfigExprProcessor, ConfigExprResult, ConfigStringExpr as X,
};

/// Interpolates configuration referncing the current directory
#[derive(Default)]
pub struct DirConfigProcessor {}

impl ConfigExprProcessor for DirConfigProcessor {
    fn display_name(&self) -> &str {
        "current_dir"
    }

    fn process(&self, ctx: &Ctx, expr: X) -> Result<ConfigExprResult> {
        Ok(ConfigExprResult::Expr(
            match (match_interpolation(&expr, &["dir"]), &ctx.path) {
                (Some(_), Some(path)) => X::Constant(path.to_string_lossy().to_string()),
                _ => expr,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_processor_ignores_constants() {
        let ctx = Ctx::mock();
        let processor = DirConfigProcessor::default();

        let input = X::Constant("test".to_owned());
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_dir_processor_ignores_unknown_prefix() {
        let ctx = Ctx::mock();
        let processor = DirConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("test".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }

    #[test]
    fn test_dir_processor_replaces_dir_expr() {
        let mut ctx = Ctx::mock();
        ctx.path = Some("/a/b/c".into());
        let processor = DirConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("dir".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(
            result.unwrap(),
            ConfigExprResult::Expr(X::Constant("/a/b/c".to_string()))
        );
    }

    #[test]
    fn test_dir_processor_ignores_when_no_present_dir() {
        let mut ctx = Ctx::mock();
        ctx.path = None;
        let processor = DirConfigProcessor::default();

        let input = X::Interpolation(vec![X::Constant("dir".to_owned())]);
        let result = processor.process(&ctx, input.clone());

        assert_eq!(result.unwrap(), ConfigExprResult::Expr(input));
    }
}
