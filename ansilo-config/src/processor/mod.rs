use ansilo_core::err::Result;

use crate::ctx::Ctx;

/// A config processor applies transformations to the yaml config
/// This is used for interpolating config items from various sources
pub(crate) trait ConfigExprProcessor {
    /// Gets the human readable display name for the processor
    fn display_name(&self) -> &str;
    /// Applies any transformations to the config
    /// The transformations may be recursively applied using the supplied &ConfigLoader
    fn process(&self, ctx: &Ctx, expr: ConfigStringExpr) -> Result<ConfigExprResult>;
}

/// AST used to represent configuration expressions
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum ConfigStringExpr {
    Constant(String),
    Concat(Vec<ConfigStringExpr>),
    /// Represents an interpolated value used in the configuration
    /// Format ${[part 1]:[part 2]..:[part n]}
    /// For instance, ${ENV:some_env_var}
    Interpolation(Vec<ConfigStringExpr>),
}

/// Result from parsing config expression
#[allow(unused)]
#[derive(Debug, PartialEq)]
pub(crate) enum ConfigExprResult {
    Expr(ConfigStringExpr),
    Yaml(serde_yaml::Value),
}

pub(crate) mod dir;
pub(crate) mod env;
pub(crate) mod util;
